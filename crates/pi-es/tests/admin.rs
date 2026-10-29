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
        .and(body_json({
            let mut body = spec.create_body();
            body["mappings"]["_meta"] = json!({"people_index": {"run_id": "R1"}});
            body
        }))
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
    Mock::given(method("GET"))
        .and(path("/persons-r1/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"persons-r1": {"mappings": {"_meta": {"people_index": {"run_id": "R1"}}}}}),
        ))
        .mount(&server)
        .await;
    let admin = IndexAdmin::new(client(&server));
    assert_eq!(
        admin.create_index("persons-r1", &spec, "R1").await.ok(),
        Some(CreateOutcome::Created)
    );
    assert_eq!(
        admin.create_index("persons-r1", &spec, "R1").await.ok(),
        Some(CreateOutcome::AlreadyExists)
    );
    let err = admin
        .create_index("persons-r1", &spec, "r1")
        .await
        .unwrap_err();
    assert!(matches!(err, EsError::IndexOwnership { owner: Some(ref owner), .. } if owner == "R1"));
    assert_eq!(
        err.to_string(),
        "index persons-r1 belongs to run R1; this run is r1"
    );
    assert!(admin.create_index("Persons", &spec, "R1").await.is_err());
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

#[tokio::test]
async fn existing_index_without_owner_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/persons-r1"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": {"type": "resource_already_exists_exception"}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-r1/_mapping"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"persons-r1": {"mappings": {}}})),
        )
        .mount(&server)
        .await;
    let err = IndexAdmin::new(client(&server))
        .create_index("persons-r1", &IndexSpec::new(1, Codec::Default), "r1")
        .await
        .unwrap_err();
    assert!(matches!(err, EsError::IndexOwnership { owner: None, .. }));
    assert_eq!(
        err.to_string(),
        "index persons-r1 has no owning run recorded (created before ownership was recorded, or by hand); this run is r1"
    );
}

#[tokio::test]
async fn existing_index_mapping_read_retries_busy_answer() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/persons-r1"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": {"type": "resource_already_exists_exception"}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-r1/_mapping"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .with_priority(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-r1/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"persons-r1": {"mappings": {"_meta": {"people_index": {"run_id": "R1"}}}}}),
        ))
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;
    let outcome = IndexAdmin::new(client(&server))
        .create_index("persons-r1", &IndexSpec::new(1, Codec::Default), "R1")
        .await;
    assert!(
        matches!(outcome, Ok(CreateOutcome::AlreadyExists)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn effective_index_validation_rejects_template_changes() {
    for component in [
        "analyzer",
        "normalizer",
        "tokenizer",
        "char_filter",
        "filter",
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/persons-r1/_settings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"persons-r1": {"settings": {"index": {"analysis": {component: {"default": {"type": "keyword"}}}}}}})))
            .mount(&server).await;
        let err = IndexAdmin::new(client(&server))
            .check_index("persons-r1", &IndexSpec::new(1, Codec::Default), "r1")
            .await
            .unwrap_err();
        assert!(matches!(err, EsError::Config(_)));
        assert!(
            err.to_string()
                .contains(&format!("index.analysis.{component}"))
        );
    }
    for extra in [false, true] {
        let server = MockServer::start().await;
        let spec = IndexSpec::new(1, Codec::Default);
        let mut mapping = spec.mapping.clone();
        mapping["_meta"] = json!({"people_index": {"run_id": "r1"}});
        // The forms observed in the Elasticsearch response are semantically equivalent.
        mapping["dynamic"] = json!("false");
        mapping["properties"]["roles"]
            .as_object_mut()
            .unwrap()
            .remove("type");
        mapping["properties"]["organizations"]
            .as_object_mut()
            .unwrap()
            .remove("type");
        mapping["properties"]["first_name"]["copy_to"] = json!(["full_name"]);
        mapping["properties"]["last_name"]["copy_to"] = json!(["full_name"]);
        if extra {
            mapping["properties"]["injected"] = json!({"type": "keyword"});
        }
        Mock::given(method("GET"))
            .and(path("/persons-r1/_settings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1": {"settings": {"index": {}}}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/persons-r1/_mapping"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1": {"mappings": mapping}})),
            )
            .mount(&server)
            .await;
        let result = IndexAdmin::new(client(&server))
            .check_index("persons-r1", &spec, "r1")
            .await;
        assert_eq!(result.is_err(), extra, "{result:?}");
    }
}

#[tokio::test]
async fn mapping_defaults_metadata_and_differences() {
    let mut spec = IndexSpec::new(1, Codec::Default);
    spec.mapping["_meta"] = json!({"owner": "operator", "people_index": {"old": true}});
    spec.mapping["properties"]["first_name"]["index"] = json!(true);
    spec.mapping["properties"]["first_name"]["fields"]["keyword"]["doc_values"] = json!(true);
    spec.mapping["properties"]["roles"]["dynamic"] = json!(false);
    let mut actual = spec.mapping.clone();
    actual["_meta"]["people_index"] = json!({"run_id":"r1"});
    actual["properties"]["first_name"]
        .as_object_mut()
        .unwrap()
        .remove("index");
    actual["properties"]["first_name"]["fields"]["keyword"]
        .as_object_mut()
        .unwrap()
        .remove("doc_values");
    actual["properties"]["roles"]["dynamic"] = json!("false");
    actual["properties"]["roles"]
        .as_object_mut()
        .unwrap()
        .remove("type");
    for (change, expected) in [
        ("none", ""),
        ("type", "properties.first_name.type"),
        ("dynamic_templates", "dynamic_templates"),
        ("runtime", "runtime"),
        ("_source", "_source"),
        ("owner", "_meta.people_index.run_id"),
        ("field", "properties.injected"),
        ("ignore_above", "properties.country.ignore_above"),
        ("analyzer", "properties.headline.analyzer"),
        ("copy_to", "properties.first_name.copy_to"),
    ] {
        let server = MockServer::start().await;
        let mut returned = actual.clone();
        match change {
            "type" => returned["properties"]["first_name"]["type"] = json!("keyword"),
            "owner" => returned["_meta"]["people_index"]["run_id"] = json!("other"),
            "field" => returned["properties"]["injected"] = json!({"type":"keyword"}),
            "ignore_above" => returned["properties"]["country"]["ignore_above"] = json!(10),
            "analyzer" => returned["properties"]["headline"]["analyzer"] = json!("keyword"),
            "copy_to" => {
                returned["properties"]["first_name"]["copy_to"] = json!(["full_name", "headline"]);
            }
            "none" => (),
            _ => returned[change] = json!({"added": true}),
        }
        Mock::given(method("PUT"))
            .and(path("/persons-r1"))
            .and(body_json({
                let mut body = spec.create_body();
                body["mappings"]["_meta"]["people_index"] = json!({"run_id":"r1"});
                body
            }))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"acknowledged":true,"shards_acknowledged":true})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/persons-r1/_settings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1":{"settings":{"index":{}}}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/persons-r1/_mapping"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1":{"mappings":returned}})),
            )
            .mount(&server)
            .await;
        let admin = IndexAdmin::new(client(&server));
        assert_eq!(
            admin.create_index("persons-r1", &spec, "r1").await.unwrap(),
            CreateOutcome::Created
        );
        let result = admin.check_index("persons-r1", &spec, "r1").await;
        if change == "none" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            let err = result.unwrap_err();
            assert!(matches!(err, EsError::Config(_)));
            assert!(err.to_string().contains(expected), "{err}");
            assert!(err.to_string().contains("persons-*"));
        }
    }
}

fn omitted_default_cases() -> Vec<(&'static str, serde_json::Value, serde_json::Value)> {
    vec![
        (
            "index",
            json!({"type":"keyword", "index":true}),
            json!({"type":"keyword"}),
        ),
        (
            "doc_values",
            json!({"type":"keyword", "doc_values":true}),
            json!({"type":"keyword"}),
        ),
        (
            "store",
            json!({"type":"keyword", "store":false}),
            json!({"type":"keyword"}),
        ),
        (
            "text norms",
            json!({"type":"text", "norms":true}),
            json!({"type":"text"}),
        ),
        (
            "keyword norms",
            json!({"type":"keyword", "norms":false}),
            json!({"type":"keyword"}),
        ),
        (
            "eager_global_ordinals",
            json!({"type":"keyword", "eager_global_ordinals":false}),
            json!({"type":"keyword"}),
        ),
        (
            "text index_options",
            json!({"type":"text", "index_options":"positions"}),
            json!({"type":"text"}),
        ),
        (
            "keyword index_options",
            json!({"type":"keyword", "index_options":"docs"}),
            json!({"type":"keyword"}),
        ),
        (
            "split_queries_on_whitespace",
            json!({"type":"keyword", "split_queries_on_whitespace":false}),
            json!({"type":"keyword"}),
        ),
        (
            "fielddata",
            json!({"type":"text", "fielddata":false}),
            json!({"type":"text"}),
        ),
        (
            "position_increment_gap",
            json!({"type":"text", "position_increment_gap":100}),
            json!({"type":"text"}),
        ),
        (
            "term_vector",
            json!({"type":"text", "term_vector":"no"}),
            json!({"type":"text"}),
        ),
        (
            "enabled",
            json!({"type":"object", "enabled":true, "properties":{"child":{"type":"keyword"}}}),
            json!({"properties":{"child":{"type":"keyword"}}}),
        ),
        (
            "locale",
            json!({"type":"date", "locale":"ROOT"}),
            json!({"type":"date"}),
        ),
        (
            "date format",
            json!({"type":"date", "format":"strict_date_optional_time||epoch_millis"}),
            json!({"type":"date"}),
        ),
        (
            "disabled object type",
            json!({"enabled":false}),
            json!({"type":"object", "enabled":false}),
        ),
    ]
}

#[tokio::test]
async fn omitted_mapping_defaults_are_accepted() {
    for (name, expected, actual) in omitted_default_cases() {
        let server = MockServer::start().await;
        let mut spec = IndexSpec::new(1, Codec::Default);
        spec.mapping = json!({"properties":{"test":expected}});
        let returned =
            json!({"properties":{"test":actual}, "_meta":{"people_index":{"run_id":"r1"}}});
        Mock::given(method("GET"))
            .and(path("/persons-r1/_settings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1":{"settings":{"index":{}}}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/persons-r1/_mapping"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1":{"mappings":returned}})),
            )
            .mount(&server)
            .await;
        let result = IndexAdmin::new(client(&server))
            .check_index("persons-r1", &spec, "r1")
            .await;
        assert!(result.is_ok(), "{name}: {result:?}");
    }
}

#[tokio::test]
async fn mapping_refusal_names_configured_source_even_for_builtin_content() {
    let server = MockServer::start().await;
    let mut actual = pi_es::mapping();
    actual["_meta"] = json!({"people_index":{"run_id":"r1"}});
    actual["properties"]["injected"] = json!({"type":"keyword"});
    Mock::given(method("GET"))
        .and(path("/persons-r1/_settings"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"persons-r1":{"settings":{"index":{}}}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-r1/_mapping"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"persons-r1":{"mappings":actual}})),
        )
        .mount(&server)
        .await;
    for source in ["built-in mapping", "PI_MAPPING_FILE mapping.json"] {
        let mut spec = IndexSpec::new(1, Codec::Default);
        spec.mapping_source = source.into();
        let err = IndexAdmin::new(client(&server))
            .check_index("persons-r1", &spec, "r1")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(&format!("differs from {source}:")),
            "{err}"
        );
    }
}

#[tokio::test]
async fn dangerous_settings_and_malformed_response() {
    for (key, value) in [
        ("default_pipeline", json!("pipeline")),
        ("final_pipeline", json!("pipeline")),
        ("mode", json!("time_series")),
        ("mapping.coerce", json!("false")),
        ("mapping.ignore_malformed", json!("true")),
        ("mapping.ignore_above", json!("10")),
        ("mapping.source.mode", json!("synthetic")),
        ("mapping.source.mode", json!("disabled")),
        ("similarity.custom.type", json!("BM25")),
        ("analysis.analyzer.default.type", json!("keyword")),
    ] {
        for flat in [false, true] {
            let server = MockServer::start().await;
            let mut settings = json!({"index":{}});
            if flat {
                settings = json!({format!("index.{key}"): value});
            } else {
                let mut current = &mut settings["index"];
                let parts: Vec<_> = key.split('.').collect();
                for part in &parts[..parts.len() - 1] {
                    current[*part] = json!({});
                    current = &mut current[*part];
                }
                current[parts[parts.len() - 1]] = value.clone();
            }
            Mock::given(method("GET"))
                .and(path("/persons-r1/_settings"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(json!({"persons-r1":{"settings":settings}})),
                )
                .mount(&server)
                .await;
            let err = IndexAdmin::new(client(&server))
                .check_index("persons-r1", &IndexSpec::new(1, Codec::Default), "r1")
                .await
                .unwrap_err();
            assert!(matches!(err, EsError::Config(_)), "{err}");
            assert!(err.to_string().contains(&format!("index.{key}")), "{err}");
        }
    }
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/persons-r1/_settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"persons-r1":{"settings":{"index":{"analysis":{"analyzer":{"default":{"type":"keyword"}},"filter":{"custom":{"type":"stop"}}},"mapping":{"ignore_above":"10","source":{"mode":"disabled"}}}}}})))
        .mount(&server).await;
    let err = IndexAdmin::new(client(&server))
        .check_index("persons-r1", &IndexSpec::new(1, Codec::Default), "r1")
        .await
        .unwrap_err()
        .to_string();
    for key in [
        "index.analysis.analyzer.default.type",
        "index.analysis.filter.custom.type",
        "index.mapping.ignore_above",
        "index.mapping.source.mode",
    ] {
        assert!(err.contains(key), "{err}");
    }
    for mode in ["stored", "STORED"] {
        let server = MockServer::start().await;
        let spec = IndexSpec::new(1, Codec::Default);
        let mut mapping = spec.mapping.clone();
        mapping["_meta"] = json!({"people_index":{"run_id":"r1"}});
        Mock::given(method("GET")).and(path("/persons-r1/_settings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"persons-r1":{"settings":{"index":{"default_pipeline":"_none","final_pipeline":"_none","mode":"standard","mapping":{"source":{"mode":mode}}}}}})))
            .mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/persons-r1/_mapping"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1":{"mappings":mapping}})),
            )
            .mount(&server)
            .await;
        assert!(
            IndexAdmin::new(client(&server))
                .check_index("persons-r1", &spec, "r1")
                .await
                .is_ok()
        );
    }
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/persons-r1/_settings"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"persons-r1":{"settings":{"index":false}}})),
        )
        .mount(&server)
        .await;
    let err = IndexAdmin::new(client(&server))
        .check_index("persons-r1", &IndexSpec::new(1, Codec::Default), "r1")
        .await
        .unwrap_err();
    assert!(matches!(err, EsError::Decode { .. }));
}
