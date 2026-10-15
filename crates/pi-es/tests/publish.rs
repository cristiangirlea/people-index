//! `Publisher` against a mock Elasticsearch: the happy path, and failures that must leave the
//! alias alone.

mod common;

use std::time::Duration;

use common::{client, shards};
use pi_es::{EsError, IndexAdmin, PublishSpec, Publisher};
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mounts refresh, flush, count (answering `count`), settings and health for `persons-new`.
async fn mount_checks(server: &MockServer, count: u64) {
    Mock::given(method("POST"))
        .and(path("/persons-new/_refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards(4)})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/persons-new/_flush"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards(4)})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/persons-new/_count"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": count, "_shards": shards(4)})),
        )
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/persons-new/_settings"))
        .and(body_json(json!({"index": {
            "number_of_replicas": 1,
            "refresh_interval": "1s",
            "translog": {"durability": "request"}
        }})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_cluster/health/persons-new"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": "green", "timed_out": false})),
        )
        .mount(server)
        .await;
}

fn spec(expected: u64) -> PublishSpec {
    let mut spec = PublishSpec::new("persons", "persons-new", expected);
    spec.health_timeout = Duration::from_secs(5);
    spec
}

#[tokio::test]
async fn publish_moves_the_alias_and_drops_the_old_index() {
    let server = MockServer::start().await;
    mount_checks(&server, 1000).await;
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
        .and(body_json(json!({"actions": [
            {"remove": {"index": "persons-old", "alias": "persons"}},
            {"add": {"index": "persons-new", "alias": "persons"}}
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

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(1000))
        .await
        .expect("published");

    assert_eq!(report.docs, 1000);
    assert_eq!(report.previous, vec!["persons-old".to_owned()]);
    assert_eq!(report.deleted, vec!["persons-old".to_owned()]);
    assert!(report.not_deleted.is_empty());

    // The alias moves only after the count and the restored settings.
    let order: Vec<String> = server
        .received_requests()
        .await
        .expect("recorded")
        .iter()
        .map(|r| format!("{} {}", r.method, r.url.path()))
        .collect();
    let at = |needle: &str| {
        order
            .iter()
            .position(|line| line == needle)
            .unwrap_or_else(|| panic!("{needle} not in {order:?}"))
    };
    assert!(at("POST /persons-new/_count") < at("POST /_aliases"));
    assert!(at("PUT /persons-new/_settings") < at("POST /_aliases"));
    assert!(at("POST /_aliases") < at("DELETE /persons-old"));
}

#[tokio::test]
async fn first_publish_has_nothing_to_remove() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/persons"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(json!({"actions": [
            {"add": {"index": "persons-new", "alias": "persons"}}
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await
        .expect("published");
    assert!(report.previous.is_empty());
}

#[tokio::test]
async fn count_mismatch_never_touches_the_alias() {
    let server = MockServer::start().await;
    mount_checks(&server, 999).await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/persons-new/_settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .with_priority(1)
        .mount(&server)
        .await;

    let outcome = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(1000))
        .await;
    match outcome {
        Err(EsError::CountMismatch {
            expected, actual, ..
        }) => {
            assert_eq!(expected, 1000);
            assert_eq!(actual, 999);
        }
        other => panic!("expected a count mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn a_concrete_index_at_the_alias_name_stops_publish() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/persons"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(&server)
        .await;

    let outcome = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await;
    assert!(
        matches!(outcome, Err(EsError::ConcreteIndexAtAlias(_))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn incomplete_refresh_stops_publish() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/persons-new/_refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_shards": {"total": 4, "successful": 3, "failed": 1}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(&server)
        .await;
    let outcome = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await;
    assert!(
        matches!(outcome, Err(EsError::IncompleteShards { .. })),
        "{outcome:?}"
    );
}
