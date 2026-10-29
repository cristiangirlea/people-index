//! `Verifier` against a mock Elasticsearch.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};

use common::{client, shards};
use pi_es::{DocumentDigest, EsError, Expected, IndexAdmin, Published, Verifier};
use serde_json::{Value, json};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn expected() -> Expected {
    Expected::from_json(
        br#"{
            "persons": 100,
            "organizations": 20,
            "malformed": {"persons": 2, "organizations": 1},
            "persons_with_unresolved": 7,
            "role_title_counts": {"Engineer": 40, "Lighthouse Keeper": 1},
            "organization_name_counts": {"Acme": 12}
        }"#,
    )
    .expect("expected")
}

async fn mount_count(server: &MockServer, query: Option<Value>, count: u64) {
    let mut mock = Mock::given(method("POST")).and(path("/persons/_count"));
    if let Some(query) = query {
        mock = mock.and(body_json(json!({ "query": query })));
    }
    mock.respond_with(
        ResponseTemplate::new(200).set_body_json(json!({"count": count, "_shards": shards(4)})),
    )
    .mount(server)
    .await;
}

/// Answers every count query the verifier sends, with `title_engineer` for the Engineer check.
async fn mount_all(server: &MockServer, title_engineer: u64) {
    mount_count(
        server,
        Some(json!({"exists": {"field": "unresolved_organization_ids"}})),
        7,
    )
    .await;
    mount_count(
        server,
        Some(json!({"term": {"roles.title.keyword": "Engineer"}})),
        title_engineer,
    )
    .await;
    mount_count(
        server,
        Some(json!({"term": {"roles.title.keyword": "Lighthouse Keeper"}})),
        1,
    )
    .await;
    mount_count(
        server,
        Some(json!({"term": {"organizations.name.keyword": "Acme"}})),
        12,
    )
    .await;
    // The match-all count has no body; mounted last so the query mocks win.
    Mock::given(method("POST"))
        .and(path("/persons/_count"))
        .and(EmptyBody)
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 100, "_shards": shards(4)})),
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

#[tokio::test]
async fn verifier_passes_when_every_count_matches() {
    let server = MockServer::start().await;
    mount_all(&server, 40).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify("persons", &expected())
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(report.checks.len(), 5);
    assert_eq!(report.failures(), [] as [&pi_es::Check; 0]);
    // The datagen report prints as it always has: no alias check, no at-least flags.
    let printed = serde_json::to_value(&report).expect("json");
    assert_eq!(printed.get("alias"), None);
    assert_eq!(
        printed["checks"][0],
        json!({"name": "count", "expected": 100, "actual": 100})
    );
}

#[tokio::test]
async fn verifier_reports_a_differing_count() {
    let server = MockServer::start().await;
    mount_all(&server, 39).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify("persons", &expected())
        .await
        .expect("verified");
    assert!(!report.passed());
    let failures = report.failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].name, "role_title:Engineer");
    assert_eq!(failures[0].expected, 40);
    assert_eq!(failures[0].actual, 39);
}

#[tokio::test]
async fn verifier_errors_when_a_count_is_incomplete() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/persons/_count"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 100, "_shards": {"total": 4, "successful": 2, "failed": 2}
        })))
        .mount(&server)
        .await;
    let outcome = Verifier::new(IndexAdmin::new(client(&server)))
        .verify("persons", &expected())
        .await;
    assert!(
        matches!(outcome, Err(EsError::IncompleteShards { .. })),
        "{outcome:?}"
    );
}

/// Serves `docs` two per page through a point in time, keyed on `search_after`.
struct PitPages {
    docs: Vec<(String, Value)>,
}

impl Respond for PitPages {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).expect("search body");
        assert_eq!(body["pit"]["id"], "pit-1");
        assert_eq!(body["sort"], json!([{"_shard_doc": "asc"}]));
        let start = body["search_after"][0]
            .as_u64()
            .map_or(0, |after| usize::try_from(after).expect("small") + 1);
        let hits: Vec<Value> = self
            .docs
            .iter()
            .enumerate()
            .skip(start)
            .take(2)
            .map(|(n, (id, source))| json!({"_id": id, "_source": source, "sort": [n]}))
            .collect();
        let mut response = json!({"pit_id": "pit-1", "timed_out": false, "_shards": shards(2)});
        if !hits.is_empty() {
            response["hits"] = json!({ "hits": hits });
        }
        ResponseTemplate::new(200).set_body_json(response)
    }
}

#[tokio::test]
async fn digest_reads_every_document_and_closes_the_pit() {
    let server = MockServer::start().await;
    let docs: Vec<(String, Value)> = (1..=5)
        .map(|n| {
            (
                n.to_string(),
                json!({"id": n, "first_name": format!("p{n}")}),
            )
        })
        .collect();
    Mock::given(method("POST"))
        .and(path("/persons/_pit"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "pit-1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(PitPages { docs: docs.clone() })
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_pit"))
        .and(body_json(json!({"id": "pit-1"})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"succeeded": true, "num_freed": 1})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .digest("persons", 2)
        .await
        .expect("digest");

    let mut local = DocumentDigest::new();
    for (id, source) in docs.iter().rev() {
        local.add(id, source);
    }
    assert_eq!(report, local.finish());
    assert_eq!(report.docs, 5);
}

fn alias_body(indexes: &[&str]) -> Value {
    let body: serde_json::Map<String, Value> = indexes
        .iter()
        .map(|index| ((*index).to_owned(), json!({"aliases": {"persons": {}}})))
        .collect();
    Value::Object(body)
}

/// Mounts the alias lookup: `persons` points at each of `indexes`.
async fn mount_alias(server: &MockServer, indexes: &[&str]) {
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(200).set_body_json(alias_body(indexes)))
        .mount(server)
        .await;
}

/// Answers the alias lookup with `first` once, then with `then`: a publish in between.
struct AliasMoves {
    calls: AtomicUsize,
    first: &'static str,
    then: &'static str,
}

impl Respond for AliasMoves {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let index = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first
        } else {
            self.then
        };
        ResponseTemplate::new(200).set_body_json(alias_body(&[index]))
    }
}

const RUN_INDEX: &str = "/persons-r2";

/// A count against the run's index (never the alias).
async fn mount_index_count(server: &MockServer, query: Option<Value>, count: u64) {
    let mut mock = Mock::given(method("POST")).and(path(format!("{RUN_INDEX}/_count")));
    mock = match query {
        Some(query) => mock.and(body_json(json!({ "query": query }))),
        None => mock.and(EmptyBody),
    };
    mock.respond_with(
        ResponseTemplate::new(200).set_body_json(json!({"count": count, "_shards": shards(4)})),
    )
    .mount(server)
    .await;
}

/// The sample search, answered with one hit (`_id`, `_source`).
async fn mount_sample(server: &MockServer, id: &str, source: Value) {
    Mock::given(method("POST"))
        .and(path(format!("{RUN_INDEX}/_search")))
        .and(body_json(json!({
            "size": 1,
            "query": {"bool": {
                "must": {"match_all": {}},
                "should": [
                    {"exists": {"field": "last_name"}},
                    {"exists": {"field": "organizations.id"}},
                ],
                "minimum_should_match": 0,
            }},
            "_source": ["first_name", "last_name", "organizations.id"],
            "track_total_hits": false,
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "timed_out": false,
            "_shards": shards(4),
            "hits": {"hits": [{"_id": id, "_source": source}]},
        })))
        .mount(server)
        .await;
}

fn name_query(names: &str) -> Value {
    json!({"match": {"full_name": {
        "query": names, "operator": "and", "zero_terms_query": "all",
    }}})
}

/// Answers every query `verify_published` sends for a sample with names and an organization:
/// `docs` documents in all and `name_hits` for the match on the sample's names.
async fn mount_published(server: &MockServer, docs: u64, name_hits: u64) {
    mount_sample(
        server,
        "42",
        json!({
            "first_name": "Ada",
            "last_name": ["Lovelace", "King"],
            "organizations": [{"id": null}, {"id": "7"}],
        }),
    )
    .await;
    mount_index_count(server, Some(json!({"ids": {"values": ["42"]}})), 1).await;
    mount_index_count(server, Some(name_query("Ada Lovelace")), name_hits).await;
    mount_index_count(
        server,
        Some(json!({"term": {"organizations.id": 7}})),
        docs / 2,
    )
    .await;
    mount_index_count(server, None, docs).await;
}

fn published(docs: u64) -> Published {
    Published {
        index: "persons-r2".into(),
        docs,
        builtin_names: true,
    }
}

fn check_names(report: &pi_es::VerifyReport) -> Vec<&str> {
    report.checks.iter().map(|c| c.name.as_str()).collect()
}

#[tokio::test]
async fn verify_published_passes_when_the_alias_serves_the_run() {
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    mount_published(&server, 100, 3).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(100))
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(
        check_names(&report),
        [
            "count",
            "sample:ids",
            "sample:full_name",
            "sample:organization_id"
        ]
    );
    let printed = serde_json::to_value(&report).expect("json");
    assert_eq!(
        printed["alias"],
        json!({"expected": "persons-r2", "actual": ["persons-r2"]})
    );
    // Exact checks print as before; only the at-least ones say so.
    assert_eq!(
        printed["checks"][0],
        json!({"name": "count", "expected": 100, "actual": 100})
    );
    assert_eq!(printed["checks"][2]["at_least"], true);
}

#[tokio::test]
async fn verify_published_leaves_out_the_name_samples_for_another_schema() {
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    // No document matches the names: with the built-in names that would fail.
    mount_published(&server, 100, 0).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published(
            "persons",
            &Published {
                builtin_names: false,
                ..published(100)
            },
        )
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(check_names(&report), ["count", "sample:ids"]);
}

#[tokio::test]
async fn verify_published_fails_on_another_index_a_count_or_a_lost_name() {
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r1", "persons-r2"]).await;
    mount_published(&server, 99, 0).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(100))
        .await
        .expect("verified");
    assert!(!report.passed());
    let alias = report.alias.as_ref().expect("alias check");
    assert!(!alias.passed());
    assert_eq!(alias.actual, ["persons-r1", "persons-r2"]);
    let failed: Vec<&str> = report.failures().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(failed, ["count", "sample:full_name"]);
}

#[tokio::test]
async fn verify_published_needs_no_names_or_organizations() {
    // A real feed may have persons without names and organizations it never resolved. The
    // sample's id is past i64::MAX (still a valid _id), its organization id too (the long
    // mapping drops it, so no term check), and its only name is punctuation.
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    let big = "18446744073709551615";
    mount_sample(
        &server,
        big,
        json!({"first_name": "-", "last_name": null, "organizations": [{"id": big}]}),
    )
    .await;
    mount_index_count(&server, Some(json!({"ids": {"values": [big]}})), 1).await;
    mount_index_count(&server, Some(name_query("-")), 5).await;
    mount_index_count(&server, None, 5).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(5))
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(
        check_names(&report),
        ["count", "sample:ids", "sample:full_name"]
    );

    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    mount_sample(&server, "9", json!({"roles": []})).await;
    mount_index_count(&server, Some(json!({"ids": {"values": ["9"]}})), 1).await;
    mount_index_count(&server, None, 5).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(5))
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(check_names(&report), ["count", "sample:ids"]);
}

#[tokio::test]
async fn verify_published_fails_when_the_alias_moves_during_the_checks() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(AliasMoves {
            calls: AtomicUsize::new(0),
            first: "persons-r2",
            then: "persons-r3",
        })
        .expect(2)
        .mount(&server)
        .await;
    // The new run has the same count: only the second alias reading can tell.
    mount_published(&server, 100, 3).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(100))
        .await
        .expect("verified");
    assert!(!report.passed());
    assert_eq!(report.failures(), [] as [&pi_es::Check; 0]);
    let alias = report.alias.expect("alias check");
    assert_eq!(alias.actual, ["persons-r3"]);
}

#[tokio::test]
async fn verify_published_on_an_empty_index_checks_only_alias_and_count() {
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    mount_index_count(&server, None, 0).await;
    Mock::given(method("POST"))
        .and(path(format!("{RUN_INDEX}/_search")))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(0))
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(check_names(&report), ["count"]);
}

#[tokio::test]
async fn verify_published_errors_when_the_sample_search_is_incomplete() {
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    Mock::given(method("POST"))
        .and(path(format!("{RUN_INDEX}/_search")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "timed_out": false, "_shards": {"total": 4, "successful": 3, "failed": 1}
        })))
        .mount(&server)
        .await;
    mount_index_count(&server, None, 10).await;
    let outcome = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(10))
        .await;
    assert!(
        matches!(outcome, Err(EsError::IncompleteShards { .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn verify_published_fails_when_a_counted_index_returns_no_sample() {
    let server = MockServer::start().await;
    mount_alias(&server, &["persons-r2"]).await;
    mount_index_count(&server, None, 5).await;
    Mock::given(method("POST"))
        .and(path(format!("{RUN_INDEX}/_search")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"timed_out": false, "_shards": shards(4)})),
        )
        .mount(&server)
        .await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify_published("persons", &published(5))
        .await
        .expect("verified");
    assert!(!report.passed());
    let failed: Vec<&str> = report.failures().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(failed, ["sample:ids"]);
    assert!(report.alias.expect("alias check").passed());
}
