//! The Elasticsearch stages (`prepare-index`, `load`, `publish`) against a mock cluster.

mod common;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{OFFLINE, config, generate, read_docs, run_stages, small_config};
use pi_coord::{RunContext, StageName};
use pi_stages::{Plan, PublishRecord, StagesConfig, Summary};
use serde_json::{Value, json};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Accepts every bulk item, except that each document's first attempt is rejected with 429
/// when `reject_first` is set, and records every id it acknowledged with its body.
struct Bulk {
    calls: AtomicUsize,
    reject_first: bool,
    seen: Mutex<BTreeMap<u64, (usize, Vec<u8>)>>,
}

impl Respond for Bulk {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = std::str::from_utf8(&request.body).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        let mut seen = self.seen.lock().unwrap();
        let mut errors = false;
        let items: Vec<Value> = lines
            .chunks(2)
            .map(|pair| {
                let action: Value = serde_json::from_str(pair[0]).unwrap();
                let id: u64 = action["index"]["_id"].as_str().unwrap().parse().unwrap();
                let entry = seen.entry(id).or_insert((0, Vec::new()));
                entry.0 += 1;
                if self.reject_first && entry.0 == 1 {
                    errors = true;
                    return json!({"index": {"status": 429, "error": {"type": "es_rejected_execution_exception", "reason": "busy"}}});
                }
                entry.1 = pair[1].as_bytes().to_vec();
                json!({"index": {"status": 201}})
            })
            .collect();
        ResponseTemplate::new(200).set_body_json(json!({"errors": errors, "items": items}))
    }
}

fn es_config(server: &MockServer, base: StagesConfig) -> StagesConfig {
    let mut cfg = base;
    cfg.es.client = pi_es::EsConfig::new(server.uri());
    cfg.es.client.retry = pi_es::RetryPolicy {
        max_attempts: 5,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
    };
    cfg.es.bulk.max_bytes = 64 << 10;
    cfg.es.health_timeout = Duration::from_secs(5);
    cfg
}

async fn mount_previous_count(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/persons-old/_count"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 1, "_shards": shards()})),
        )
        .mount(server)
        .await;
}

fn shards() -> Value {
    json!({"total": 4, "successful": 4, "skipped": 0, "failed": 0})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_sends_every_document_once_and_resends_rejected_items() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "run-l", config(4), &OFFLINE)
        .await
        .unwrap();
    let docs = read_docs(&data.store, "run-l").await;
    let ctx = RunContext::new("run-l", data.store.clone()).unwrap();
    let plan = Plan::load(&ctx).await.unwrap();

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{}", plan.index)))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let bulk = Bulk {
        calls: AtomicUsize::new(0),
        reject_first: true,
        seen: Mutex::new(BTreeMap::new()),
    };
    Mock::given(method("POST"))
        .and(path(format!("/{}/_bulk", plan.index)))
        .respond_with(bulk)
        .mount(&server)
        .await;

    let reports = run_stages(
        &data.store,
        "run-l",
        es_config(&server, config(4)),
        &[StageName::Load],
    )
    .await
    .unwrap();
    assert_eq!(reports[0].tasks(), 4);
    assert_eq!(reports[0].counter("docs_indexed"), docs.len() as u64);
    assert_eq!(reports[0].counter("items_retried"), docs.len() as u64);
    assert!(
        reports[0].counter("bulk_requests") > 4,
        "byte-bounded requests"
    );

    let requests = server.received_requests().await.unwrap();
    let mut acknowledged: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut attempts: BTreeMap<u64, usize> = BTreeMap::new();
    for r in &requests {
        let text = std::str::from_utf8(&r.body).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        for pair in lines.chunks(2) {
            let action: Value = serde_json::from_str(pair[0]).unwrap();
            let id: u64 = action["index"]["_id"].as_str().unwrap().parse().unwrap();
            *attempts.entry(id).or_default() += 1;
            acknowledged.insert(id, pair[1].as_bytes().to_vec());
        }
    }
    // Each document: rejected once, accepted once, never sent a third time.
    assert!(
        attempts.values().all(|n| *n == 2),
        "every document sent exactly twice"
    );
    assert_eq!(
        acknowledged, docs,
        "the bulk bodies are the committed documents"
    );

    // A second run of the stage finds every task committed and sends nothing.
    let before = server.received_requests().await.unwrap().len();
    let again = run_stages(
        &data.store,
        "run-l",
        es_config(&server, config(4)),
        &[StageName::Load],
    )
    .await
    .unwrap();
    assert_eq!(again[0].skipped, 4);
    assert_eq!(server.received_requests().await.unwrap().len(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_with_a_buffer_smaller_than_one_document_sends_every_document() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "run-b", config(2), &OFFLINE)
        .await
        .unwrap();
    let docs = read_docs(&data.store, "run-b").await;
    let ctx = RunContext::new("run-b", data.store.clone()).unwrap();
    let plan = Plan::load(&ctx).await.unwrap();

    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{}", plan.index)))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let bulk = Bulk {
        calls: AtomicUsize::new(0),
        reject_first: false,
        seen: Mutex::new(BTreeMap::new()),
    };
    Mock::given(method("POST"))
        .and(path(format!("/{}/_bulk", plan.index)))
        .respond_with(bulk)
        .mount(&server)
        .await;

    // One byte: every document is larger than the buffer, so each passes alone.
    let mut cfg = es_config(&server, config(2));
    cfg.load_buffer_bytes = Some(1);
    let reports = run_stages(&data.store, "run-b", cfg, &[StageName::Load])
        .await
        .unwrap();
    assert_eq!(reports[0].counter("docs_indexed"), docs.len() as u64);
    let mut acknowledged: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    for r in &server.received_requests().await.unwrap() {
        let text = std::str::from_utf8(&r.body).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        for pair in lines.chunks(2) {
            let action: Value = serde_json::from_str(pair[0]).unwrap();
            let id: u64 = action["index"]["_id"].as_str().unwrap().parse().unwrap();
            assert!(
                acknowledged
                    .insert(id, pair[1].as_bytes().to_vec())
                    .is_none()
            );
        }
    }
    assert_eq!(acknowledged, docs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one mock per cluster call, in request order"
)]
async fn prepare_and_publish_against_a_mock_cluster() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "run-p", config(2), &OFFLINE)
        .await
        .unwrap();
    let ctx = RunContext::new("run-p", data.store.clone()).unwrap();
    let plan = Plan::load(&ctx).await.unwrap();
    let index = plan.index.clone();
    assert_eq!(index, "persons-run-p");
    let documents = data.expected.persons;

    let server = MockServer::start().await;
    mount_previous_count(&server).await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{index}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_nodes/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_nodes": {"total": 3, "successful": 3, "failed": 0},
            "nodes": {
                "a": {"roles": ["data", "ingest"]},
                "b": {"roles": ["data_content"]},
                "c": {"roles": ["master"]}
            }
        })))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/{index}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"acknowledged": true, "shards_acknowledged": true})),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_bulk")))
        .respond_with(Bulk {
            calls: AtomicUsize::new(0),
            reject_first: false,
            seen: Mutex::new(BTreeMap::new()),
        })
        .mount(&server)
        .await;
    for op in ["_refresh", "_flush"] {
        Mock::given(method("POST"))
            .and(path(format!("/{index}/{op}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards()})))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_count")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"count": documents, "_shards": shards()})),
        )
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/{index}/_settings")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/_cluster/health/{index}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": "green", "timed_out": false})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"persons-old": {"aliases": {"persons": {}}}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/persons-old"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;

    let reports = run_stages(
        &data.store,
        "run-p",
        es_config(&server, config(2)),
        &[StageName::PrepareIndex, StageName::Load, StageName::Publish],
    )
    .await
    .unwrap();
    // Two data nodes, two primaries each.
    assert_eq!(reports[0].counter("shards"), 4);
    let create = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.method.as_str() == "PUT" && r.url.path() == format!("/{index}"))
        .unwrap();
    let body: Value = serde_json::from_slice(&create.body).unwrap();
    assert_eq!(body["settings"]["index"]["number_of_shards"], 4);
    assert_eq!(body["settings"]["index"]["number_of_replicas"], 0);
    assert_eq!(body["settings"]["index"]["refresh_interval"], "-1");

    let summary: Summary = ctx
        .store
        .get_json(&ctx.layout.summary())
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(summary.documents, documents);
    assert_eq!(summary.publish.docs, documents);
    assert_eq!(summary.publish.previous, ["persons-old"]);
    assert_eq!(summary.publish.previous_docs, Some(1));
    assert_eq!(summary.publish.deleted, ["persons-old"]);
    assert_eq!(summary.stages.len(), 8);
    assert_eq!(summary.stages.last().unwrap().stage, StageName::Publish);
    assert!(summary.persons_per_second > 0.0);
    // The generated feeds have no values the index would skip; the counter is there anyway.
    for stage in [StageName::ExtractOrgs, StageName::ExtractPersons] {
        let extract = summary.stages.iter().find(|s| s.stage == stage).unwrap();
        assert_eq!(extract.counters.get("values_ignored_by_index"), Some(&0));
    }
    assert_eq!(reports[2].counter("documents"), documents);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_mismatch_never_moves_the_alias() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "run-x", config(1), &OFFLINE)
        .await
        .unwrap();
    let index = "persons-run-x";
    let server = MockServer::start().await;
    for op in ["_refresh", "_flush"] {
        Mock::given(method("POST"))
            .and(path(format!("/{index}/{op}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards()})))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_count")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 1, "_shards": shards()})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(&server)
        .await;
    let err = run_stages(
        &data.store,
        "run-x",
        es_config(&server, config(1)),
        &[StageName::Publish],
    )
    .await
    .unwrap_err();
    assert!(pi_coord::is_permanent(&err), "{err:#}");
    assert!(format!("{err:#}").contains("expected"), "{err:#}");
}

/// Mounts what `publish` asks before the alias: refresh, flush, count, settings, health.
async fn mount_publish_checks(server: &MockServer, index: &str, documents: u64) {
    for op in ["_refresh", "_flush"] {
        Mock::given(method("POST"))
            .and(path(format!("/{index}/{op}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards()})))
            .mount(server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_count")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"count": documents, "_shards": shards()})),
        )
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/{index}/_settings")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/_cluster/health/{index}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": "green", "timed_out": false})),
        )
        .mount(server)
        .await;
}

/// Answers `GET /_alias/persons` with `target`.
async fn mount_alias_on(server: &MockServer, target: &str) {
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ target: {"aliases": {"persons": {}}} })),
        )
        .mount(server)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "exercise interrupted publish and resume"
)]
async fn a_publish_stopped_after_the_alias_moved_deletes_the_old_index_on_rerun() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "run-c", config(1), &OFFLINE)
        .await
        .unwrap();
    let ctx = RunContext::new("run-c", data.store.clone()).unwrap();
    let index = "persons-run-c";
    let documents = data.expected.persons;

    // First attempt: the alias moves, then the coordinator stops while the delete is in flight.
    let server = MockServer::start().await;
    mount_previous_count(&server).await;
    mount_publish_checks(&server, index, documents).await;
    mount_alias_on(&server, "persons-old").await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/persons-old"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"acknowledged": true}))
                .set_delay(Duration::from_secs(60)),
        )
        .mount(&server)
        .await;
    let store = data.store.clone();
    let cfg = es_config(&server, config(1));
    let first =
        tokio::spawn(async move { run_stages(&store, "run-c", cfg, &[StageName::Publish]).await });
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let requests = server.received_requests().await.unwrap();
            if requests
                .iter()
                .any(|r| r.method.as_str() == "DELETE" && r.url.path() == "/persons-old")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first attempt reaches the delete");
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(
        ctx.store
            .head(&ctx.layout.summary())
            .await
            .unwrap()
            .is_none(),
        "the first attempt never finished"
    );

    // Second attempt: the alias already points at the run's index; only publish.json still
    // names the old one.
    server.reset().await;
    mount_publish_checks(&server, index, documents).await;
    mount_alias_on(&server, index).await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(json!({"actions": [
            {"add": {"index": index, "alias": "persons"}}
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/persons-old"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    run_stages(
        &data.store,
        "run-c",
        es_config(&server, config(1)),
        &[StageName::Publish],
    )
    .await
    .unwrap();

    let record: PublishRecord = ctx
        .store
        .get_json(&ctx.layout.publish_record())
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(
        (record.previous.as_slice(), record.previous_docs),
        (["persons-old".to_owned()].as_slice(), Some(1))
    );
    let summary: Summary = ctx
        .store
        .get_json(&ctx.layout.summary())
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(summary.publish.previous, ["persons-old"]);
    assert_eq!(summary.publish.previous_docs, Some(1));
    assert_eq!(summary.publish.deleted, ["persons-old"]);
    assert_eq!(
        summary.publish.not_deleted,
        [] as [(std::string::String, std::string::String); 0]
    );
}
