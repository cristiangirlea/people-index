//! `IndexAdmin` and `EsClient` against a mock Elasticsearch.

mod common;

use std::time::Duration;

use common::{client, config, shards};
use pi_es::{
    Auth, Codec, CreateOutcome, EsClient, EsError, HealthStatus, IndexAdmin, IndexSpec, Secret,
};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn count_requires_every_shard() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/persons/_count"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 41,
            "_shards": {"total": 4, "successful": 3, "skipped": 0, "failed": 1}
        })))
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    let outcome = admin.count("persons", None).await;
    assert!(
        matches!(outcome, Err(EsError::IncompleteShards { .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn count_with_complete_shards_passes_the_query() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/persons/_count"))
        .and(body_json(
            json!({"query": {"term": {"roles.title.keyword": "Engineer"}}}),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 12, "_shards": shards(4)})),
        )
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    let query = json!({"term": {"roles.title.keyword": "Engineer"}});
    assert_eq!(admin.count("persons", Some(&query)).await.ok(), Some(12));
}

#[tokio::test]
async fn data_nodes_count_only_content_holders() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_nodes/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_nodes": {"total": 5, "successful": 5, "failed": 0},
            "nodes": {
                "m1": {"roles": ["master"]},
                "d1": {"roles": ["data", "ingest"]},
                "d2": {"roles": ["data_content", "data_hot"]},
                "d3": {"roles": ["data_hot"]},
                "c1": {"roles": []}
            }
        })))
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    assert_eq!(admin.data_node_count().await.ok(), Some(2));
}

#[tokio::test]
async fn create_index_sends_mapping_and_load_settings() {
    let server = MockServer::start().await;
    let spec = IndexSpec::new(4, Codec::BestCompression);
    Mock::given(method("PUT"))
        .and(path("/persons-r1"))
        .and(body_json(spec.create_body()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "acknowledged": true, "shards_acknowledged": true, "index": "persons-r1"
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/persons-r1"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {"type": "resource_already_exists_exception", "reason": "exists"},
            "status": 400
        })))
        .with_priority(2)
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    assert_eq!(
        admin.create_index("persons-r1", &spec).await.ok(),
        Some(CreateOutcome::Created)
    );
    assert_eq!(
        admin.create_index("persons-r1", &spec).await.ok(),
        Some(CreateOutcome::AlreadyExists)
    );
    assert!(admin.create_index("Persons", &spec).await.is_err());
}

#[tokio::test]
async fn alias_lookup_handles_missing_alias() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "persons-b": {"aliases": {"persons": {}}},
            "persons-a": {"aliases": {"persons": {}}}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_alias/nothing"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"error": "alias [nothing] missing", "status": 404})),
        )
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    assert_eq!(
        admin.alias_targets("persons").await.ok(),
        Some(vec!["persons-a".to_owned(), "persons-b".to_owned()])
    );
    assert_eq!(admin.alias_targets("nothing").await.ok(), Some(vec![]));
}

#[tokio::test]
async fn alias_swap_is_one_atomic_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(json!({"actions": [
            {"remove": {"index": "persons-old1", "alias": "persons", "must_exist": false}},
            {"remove": {"index": "persons-old2", "alias": "persons", "must_exist": false}},
            {"add": {"index": "persons-new", "alias": "persons"}}
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    admin
        .swap_alias(
            "persons",
            "persons-new",
            &[
                "persons-old1".to_owned(),
                "persons-new".to_owned(),
                "persons-old2".to_owned(),
            ],
        )
        .await
        .expect("swapped");
}

#[tokio::test]
async fn health_wait_polls_until_the_status_is_reached() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_cluster/health/persons-r1"))
        .and(query_param("wait_for_status", "green"))
        .respond_with(
            ResponseTemplate::new(408)
                .set_body_json(json!({"status": "yellow", "timed_out": true})),
        )
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_cluster/health/persons-r1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"status": "green", "timed_out": false})),
        )
        .with_priority(2)
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    let reached = admin
        .wait_for_health(
            Some("persons-r1"),
            HealthStatus::Green,
            Duration::from_secs(10),
        )
        .await;
    assert_eq!(reached.ok(), Some(HealthStatus::Green));
}

#[tokio::test]
async fn health_wait_gives_up_at_the_deadline() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_cluster/health"))
        .respond_with(
            ResponseTemplate::new(408).set_body_json(json!({"status": "red", "timed_out": true})),
        )
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    let outcome = admin
        .wait_for_health(None, HealthStatus::Yellow, Duration::ZERO)
        .await;
    assert!(
        matches!(outcome, Err(EsError::HealthTimeout { .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn refresh_and_delete() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/persons-r1/_refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards(2)})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/persons-gone"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"status": 404})))
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    assert_eq!(
        admin.refresh("persons-r1").await.map(|s| s.total).ok(),
        Some(2)
    );
    assert!(admin.delete_index("persons-gone").await.is_ok());
}

#[tokio::test]
async fn admin_calls_retry_busy_answers() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/persons-r1/_settings"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/persons-r1/_settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .with_priority(2)
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    admin
        .update_settings("persons-r1", &json!({"index": {"refresh_interval": "1s"}}))
        .await
        .expect("applied after a retry");
}

#[tokio::test]
async fn credentials_go_in_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_alias/basic"))
        // base64("elastic:changeme")
        .and(header("authorization", "Basic ZWxhc3RpYzpjaGFuZ2VtZQ=="))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_alias/apikey"))
        .and(header("authorization", "ApiKey a2V5OnNlY3JldA=="))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let mut basic = config(&server);
    basic.auth = Auth::Basic {
        username: "elastic".into(),
        password: Secret::new("changeme"),
    };
    let admin = IndexAdmin::new(EsClient::new(&basic).expect("client"));
    assert_eq!(admin.alias_targets("basic").await.ok(), Some(vec![]));

    let mut api_key = config(&server);
    api_key.auth = Auth::ApiKey(Secret::new("a2V5OnNlY3JldA=="));
    let admin = IndexAdmin::new(EsClient::new(&api_key).expect("client"));
    assert_eq!(admin.alias_targets("apikey").await.ok(), Some(vec![]));
}

#[tokio::test]
async fn oversized_responses_are_refused() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_alias/big"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(4096)))
        .mount(&server)
        .await;
    let mut small = config(&server);
    small.response_limit = 1024;
    let admin = IndexAdmin::new(EsClient::new(&small).expect("client"));
    let outcome = admin.alias_targets("big").await;
    assert!(
        matches!(outcome, Err(EsError::ResponseTooLarge { .. })),
        "{outcome:?}"
    );
}
