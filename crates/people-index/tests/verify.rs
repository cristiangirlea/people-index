//! `people-index verify` in both modes, through the CLI's arguments: a local store and a mock
//! Elasticsearch.

use clap::Parser as _;
use people_index::cli::{Cli, Command, VerifyArgs};
use people_index::commands::verify;
use pi_stages::index_name;
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn shards() -> Value {
    json!({"total": 2, "successful": 2, "failed": 0})
}

fn args(store: &TempDir, server: &MockServer, extra: &[&str]) -> VerifyArgs {
    let store_url = format!("file://{}", store.path().display());
    let mut argv = vec![
        "people-index".to_owned(),
        "verify".to_owned(),
        "--store-url".to_owned(),
        store_url,
        "--es-url".to_owned(),
        server.uri(),
    ];
    argv.extend(extra.iter().map(|a| (*a).to_owned()));
    let Command::Verify(parsed) = Cli::try_parse_from(argv).expect("arguments").command else {
        panic!("not verify")
    };
    parsed
}

/// Writes `runs/<run_id>/summary.json` as `publish` does: `alias`, its lowercased index
/// name, `docs` documents.
fn write_summary(store: &TempDir, run_id: &str, alias: &str, docs: u64) {
    let dir = store.path().join("runs").join(run_id);
    std::fs::create_dir_all(&dir).expect("run dir");
    let summary = json!({
        "run_id": run_id,
        "alias": alias,
        "index": index_name(alias, run_id),
        "partitions": 16,
        "started_at": "2026-10-26T10:00:00Z",
        "finished_at": "2026-10-26T10:05:00Z",
        "wall_ms": 300_000,
        "documents": docs,
        "persons_per_second": 1.0,
        "input_bytes": 1000,
        "stages": [],
        "publish": {"docs": docs, "previous": [], "deleted": [], "not_deleted": []},
        "orphans_deleted": 0,
        "orphans_left": 0,
    });
    std::fs::write(dir.join("summary.json"), summary.to_string()).expect("summary");
}

async fn mount_alias(server: &MockServer, index: &str) {
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ index: {"aliases": {"persons": {}}} })),
        )
        .mount(server)
        .await;
}

struct EmptyBody;

impl wiremock::Match for EmptyBody {
    fn matches(&self, request: &Request) -> bool {
        request.body.is_empty()
    }
}

/// Counts and the sample search on `target`: the match-all count answers `total`, the `_id`
/// lookup 1, every other query `count`; the sample is Grace Hopper (`_id` 7), with no
/// organization.
async fn mount_cluster(server: &MockServer, target: &str, total: u64, count: u64) {
    let count_path = format!("/{target}/_count");
    Mock::given(method("POST"))
        .and(path(count_path.as_str()))
        .and(EmptyBody)
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": total, "_shards": shards()})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(count_path.as_str()))
        .and(body_json(json!({"query": {"ids": {"values": ["7"]}}})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 1, "_shards": shards()})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(count_path.as_str()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": count, "_shards": shards()})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/{target}/_search")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "timed_out": false,
            "_shards": shards(),
            "hits": {"hits": [{"_id": "7", "_source": {"first_name": "Grace", "last_name": "Hopper"}}]},
        })))
        .mount(server)
        .await;
}

/// A point in time over `target` with no documents, opened once and closed once.
async fn mount_empty_pit(server: &MockServer, target: &str) {
    Mock::given(method("POST"))
        .and(path(format!("/{target}/_pit")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "pit-1"})))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "pit_id": "pit-1", "timed_out": false, "_shards": shards(),
        })))
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_pit"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"succeeded": true})))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn summary_mode_checks_the_run_the_alias_points_at() {
    // A generated run id has uppercase letters; its index name does not, so the run is found
    // by listing the store (beside an older run), not read back from the index name.
    let store = TempDir::new().expect("tempdir");
    let run_id = "20261026T100000Z-ab12cd";
    write_summary(&store, "20261025T090000Z-000001", "persons", 40);
    write_summary(&store, run_id, "persons", 50);
    let index = index_name("persons", run_id);
    let server = MockServer::start().await;
    mount_alias(&server, &index).await;
    mount_cluster(&server, &index, 50, 20).await;
    mount_empty_pit(&server, &index).await;
    // An empty run id (compose's unset VERIFY_RUN_ID) means the same as none.
    let outcome = verify::run(args(
        &store,
        &server,
        &["--source", "summary", "--run-id=", "--digest"],
    ))
    .await
    .expect("verified");
    assert_eq!(outcome.run_id.as_deref(), Some(run_id));
    assert!(outcome.report.passed(), "{:?}", outcome.report);
    let names: Vec<&str> = outcome
        .report
        .checks
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["count", "sample:ids", "sample:full_name"]);
    // The digest reads the run's index, like the checks.
    assert_eq!(outcome.digest.expect("digest").docs, 0);
}

#[tokio::test]
async fn summary_mode_fails_for_a_run_the_alias_moved_away_from() {
    let store = TempDir::new().expect("tempdir");
    write_summary(&store, "r1", "persons", 50);
    write_summary(&store, "r2", "persons", 50);
    let server = MockServer::start().await;
    mount_alias(&server, "persons-r2").await;
    mount_cluster(&server, "persons-r1", 50, 20).await;
    let outcome = verify::run(args(
        &store,
        &server,
        &["--source", "summary", "--run-id", "r1"],
    ))
    .await
    .expect("verified");
    assert_eq!(outcome.run_id.as_deref(), Some("r1"));
    assert!(!outcome.report.passed());
    let alias = outcome.report.alias.expect("alias check");
    assert_eq!(alias.expected, "persons-r1");
    assert_eq!(alias.actual, ["persons-r2"]);
}

#[tokio::test]
async fn summary_mode_without_a_summary_is_an_error() {
    // The run exists (it has a plan) but never published.
    let store = TempDir::new().expect("tempdir");
    let run = store.path().join("runs").join("r9");
    std::fs::create_dir_all(&run).expect("run dir");
    std::fs::write(run.join("plan.json"), "{}").expect("plan");
    let server = MockServer::start().await;
    mount_alias(&server, "persons-r9").await;
    let error = verify::run(args(&store, &server, &["--source", "summary"]))
        .await
        .expect_err("no summary");
    let message = format!("{error:#}");
    assert!(
        message.contains("runs/r9/summary.json") && message.contains("published"),
        "{message}"
    );
}

#[tokio::test]
async fn summary_mode_refuses_an_index_no_run_names() {
    let store = TempDir::new().expect("tempdir");
    write_summary(&store, "r1", "persons", 50);
    let server = MockServer::start().await;
    mount_alias(&server, "people-v1").await;
    let error = verify::run(args(&store, &server, &["--source", "summary"]))
        .await
        .expect_err("no run names that index");
    let message = format!("{error:#}");
    assert!(
        message.contains("people-v1") && message.contains("pass --run-id"),
        "{message}"
    );
}

#[tokio::test]
async fn summary_mode_refuses_a_summary_of_another_alias() {
    let store = TempDir::new().expect("tempdir");
    write_summary(&store, "r1", "people", 50);
    let server = MockServer::start().await;
    let error = verify::run(args(
        &store,
        &server,
        &["--source", "summary", "--run-id", "r1"],
    ))
    .await
    .expect_err("another alias");
    assert!(
        format!("{error:#}").contains("published alias people, not persons"),
        "{error:#}"
    );
}

#[tokio::test]
async fn expected_mode_reads_expected_json_as_before() {
    let store = TempDir::new().expect("tempdir");
    let raw = store.path().join("raw");
    std::fs::create_dir_all(&raw).expect("raw");
    std::fs::write(
        raw.join("expected.json"),
        json!({
            "persons": 50,
            "organizations": 10,
            "malformed": {"persons": 0, "organizations": 0},
            "persons_with_unresolved": 20,
            "role_title_counts": {},
            "organization_name_counts": {},
        })
        .to_string(),
    )
    .expect("expected.json");
    let server = MockServer::start().await;
    mount_cluster(&server, "persons", 50, 20).await;
    let outcome = verify::run(args(&store, &server, &[]))
        .await
        .expect("verified");
    assert_eq!(outcome.run_id, None);
    assert!(outcome.report.passed(), "{:?}", outcome.report);
    assert_eq!(outcome.report.alias, None);
    assert_eq!(outcome.report.checks.len(), 2);
}

#[tokio::test]
async fn expected_mode_reads_selected_delivery() {
    let store = TempDir::new().unwrap();
    let raw = store.path().join("raw/batch");
    std::fs::create_dir_all(&raw).unwrap();
    std::fs::write(
        raw.join("expected.json"),
        json!({
            "persons": 50, "organizations": 10,
            "malformed": {"persons": 0, "organizations": 0},
            "persons_with_unresolved": 20, "role_title_counts": {}, "organization_name_counts": {}
        })
        .to_string(),
    )
    .unwrap();
    let server = MockServer::start().await;
    mount_cluster(&server, "persons", 50, 20).await;
    let outcome = verify::run(args(&store, &server, &["--delivery", "batch"]))
        .await
        .unwrap();
    assert!(outcome.report.passed());
}
