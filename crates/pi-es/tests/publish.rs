//! `Publisher` against a mock Elasticsearch: the happy path, failures that must leave the
//! alias alone, and the retries and resumes that must still clean up.

mod common;

use std::time::Duration;

use common::{client, shards};
use pi_es::{EsError, IndexAdmin, PublishSpec, Publisher};
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mounts refresh, flush, count (answering `count`), settings and health for `persons-new`.
async fn mount_checks(server: &MockServer, count: u64) {
    mount_checks_for(server, "persons-new", count).await;
}

/// [`mount_checks`] for any index.
async fn mount_checks_for(server: &MockServer, index: &str, count: u64) {
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_refresh")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards(4)})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_flush")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards(4)})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_count")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": count, "_shards": shards(4)})),
        )
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/{index}/_settings")))
        .and(body_json(json!({"index": {
            "number_of_replicas": 1,
            "refresh_interval": "1s",
            "translog": {"durability": "request"}
        }})))
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
            {"remove": {"index": "persons-old", "alias": "persons", "must_exist": false}},
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
    assert_eq!(
        report.not_deleted,
        [] as [(std::string::String, std::string::String); 0]
    );

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
    assert_eq!(report.previous, [] as [std::string::String; 0]);
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

/// The `_aliases` request moving `persons` from `persons-old` to `persons-new`.
fn swap_body() -> serde_json::Value {
    json!({"actions": [
        {"remove": {"index": "persons-old", "alias": "persons", "must_exist": false}},
        {"add": {"index": "persons-new", "alias": "persons"}}
    ]})
}

/// Answers `GET /_alias/persons` with `persons-old` for the first `old_reads` reads, then
/// with `persons-new`.
async fn mount_alias_moving_after(server: &MockServer, old_reads: u64) {
    if old_reads > 0 {
        Mock::given(method("GET"))
            .and(path("/_alias/persons"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-old": {"aliases": {"persons": {}}}})),
            )
            .up_to_n_times(old_reads)
            .with_priority(1)
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"persons-new": {"aliases": {"persons": {}}}})),
        )
        .with_priority(2)
        .mount(server)
        .await;
}

async fn mount_delete_old(server: &MockServer, status: u16) {
    Mock::given(method("DELETE"))
        .and(path("/persons-old"))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_resent_alias_request_after_a_lost_answer_succeeds() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    mount_alias_moving_after(&server, 1).await;
    // The first copy is applied but its answer is a 502; the client resends the same body.
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(swap_body()))
        .respond_with(ResponseTemplate::new(502))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(swap_body()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .with_priority(2)
        .mount(&server)
        .await;
    mount_delete_old(&server, 200).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await
        .expect("published");
    assert_eq!(report.previous, ["persons-old"]);
    assert_eq!(report.deleted, ["persons-old"]);
}

#[tokio::test]
async fn a_failed_alias_request_whose_effect_is_visible_counts_as_done() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    mount_alias_moving_after(&server, 1).await;
    // A cluster that answers the resent copy with a 404 anyway: the alias already moved.
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(502))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"type": "aliases_not_found_exception"}, "status": 404
        })))
        .with_priority(2)
        .mount(&server)
        .await;
    mount_delete_old(&server, 200).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await
        .expect("published");
    assert_eq!(report.deleted, ["persons-old"]);
}

#[tokio::test]
async fn a_failed_alias_request_without_effect_stays_a_failure() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    mount_alias_moving_after(&server, u64::MAX).await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let outcome = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await;
    assert!(
        matches!(outcome, Err(EsError::Http { status: 400, .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_404_on_delete_counts_as_deleted() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    mount_alias_moving_after(&server, u64::MAX).await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(swap_body()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    mount_delete_old(&server, 404).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await
        .expect("published");
    assert_eq!(report.deleted, ["persons-old"]);
    assert_eq!(
        report.not_deleted,
        [] as [(std::string::String, std::string::String); 0]
    );
}

#[tokio::test]
async fn a_resumed_publish_deletes_what_the_record_names() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    // The earlier attempt moved the alias and stopped before deleting.
    mount_alias_moving_after(&server, 0).await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(json!({"actions": [
            {"add": {"index": "persons-new", "alias": "persons"}}
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    mount_delete_old(&server, 200).await;

    let seen = std::sync::Mutex::new(None);
    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish_recording(&spec(5), |on_alias| {
            *seen.lock().expect("lock") = Some(on_alias);
            async { Ok(vec!["persons-old".to_owned(), "persons-new".to_owned()]) }
        })
        .await
        .expect("published");
    assert_eq!(seen.lock().expect("lock").take(), Some(Vec::new()));
    assert_eq!(
        report.previous,
        ["persons-old"],
        "never the run's own index"
    );
    assert_eq!(report.deleted, ["persons-old"]);
}

#[tokio::test]
async fn a_failed_record_never_touches_the_alias() {
    let server = MockServer::start().await;
    mount_checks(&server, 5).await;
    mount_alias_moving_after(&server, u64::MAX).await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(&server)
        .await;

    let outcome = Publisher::new(IndexAdmin::new(client(&server)))
        .publish_recording(&spec(5), |_| async {
            Err(EsError::Record("store unavailable".into()))
        })
        .await;
    assert!(matches!(outcome, Err(EsError::Record(_))), "{outcome:?}");
}

/// A run id with a start stamp, so publish also looks for older runs' indexes.
const STAMPED: &str = "persons-20261015t120000z-cccccc";

fn stamped_spec() -> PublishSpec {
    let mut spec = PublishSpec::new("persons", STAMPED, 5);
    spec.health_timeout = Duration::from_secs(5);
    spec
}

/// A first publish of [`STAMPED`] (no alias yet) whose `GET /persons-*/_alias` answers
/// `listing`.
async fn mount_stamped_first_publish(server: &MockServer, listing: ResponseTemplate) {
    mount_checks_for(server, STAMPED, 5).await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/persons"))
        .respond_with(ResponseTemplate::new(404))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .and(body_json(json!({"actions": [
            {"add": {"index": STAMPED, "alias": "persons"}}
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-*/_alias"))
        .respond_with(listing)
        .expect(1)
        .mount(server)
        .await;
}

/// `DELETE /<index>`, answering `status`, expected `times` times.
async fn mount_delete(server: &MockServer, index: &str, status: u16, times: u64) {
    Mock::given(method("DELETE"))
        .and(path(format!("/{index}")))
        .respond_with(ResponseTemplate::new(status).set_body_json(json!({"acknowledged": true})))
        .expect(times)
        .mount(server)
        .await;
}

fn listing(entries: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(entries)
}

/// Any `DELETE` at all fails the test.
async fn mount_no_delete(server: &MockServer) {
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(server)
        .await;
}

#[tokio::test]
async fn an_older_failed_runs_index_is_deleted_after_the_previous_ones() {
    let server = MockServer::start().await;
    mount_checks_for(&server, STAMPED, 5).await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"persons-20261014-090000": {"aliases": {"persons": {}}}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    // Both id forms count, compared by their start second.
    Mock::given(method("GET"))
        .and(path("/persons-*/_alias"))
        .respond_with(listing(json!({
            STAMPED: {"aliases": {"persons": {}}},
            "persons-20261015-110000": {"aliases": {}},
            "persons-20261015t115959z-aaaaaa": {"aliases": {}},
        })))
        .expect(1)
        .mount(&server)
        .await;
    mount_delete(&server, "persons-20261014-090000", 200, 1).await;
    mount_delete(&server, "persons-20261015-110000", 200, 1).await;
    mount_delete(&server, "persons-20261015t115959z-aaaaaa", 200, 1).await;
    mount_delete(&server, STAMPED, 200, 0).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published");
    assert_eq!(report.previous, ["persons-20261014-090000"]);
    assert_eq!(
        report.deleted,
        [
            "persons-20261014-090000",
            "persons-20261015-110000",
            "persons-20261015t115959z-aaaaaa"
        ]
    );
    assert!(report.not_deleted.is_empty(), "{:?}", report.not_deleted);

    // The older runs are looked up only once the alias has moved.
    let order: Vec<String> = server
        .received_requests()
        .await
        .expect("recorded")
        .iter()
        .map(|r| format!("{} {}", r.method, r.url.path()))
        .collect();
    let at = |needle: &str| order.iter().position(|line| line == needle).unwrap();
    assert!(
        at("POST /_aliases") < at("GET /persons-*/_alias"),
        "{order:?}"
    );
}

#[tokio::test]
async fn newer_same_second_and_unstamped_indexes_are_kept() {
    let server = MockServer::start().await;
    mount_stamped_first_publish(
        &server,
        listing(json!({
            STAMPED: {"aliases": {"persons": {}}},
            // A newer run may still be loading.
            "persons-20261015t120001z-bbbbbb": {"aliases": {}},
            "persons-20261016-000000": {"aliases": {}},
            // Started in the same second: possibly running alongside this one.
            "persons-20261015t120000z-dddddd": {"aliases": {}},
            "persons-20261015-120000": {"aliases": {}},
            // No start stamp, or another alias's run: no order.
            "persons-nightly-1": {"aliases": {}},
            "persons-eu-20261014-000000": {"aliases": {}},
        })),
    )
    .await;
    mount_no_delete(&server).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published");
    assert!(report.deleted.is_empty(), "{:?}", report.deleted);
    assert!(report.not_deleted.is_empty(), "{:?}", report.not_deleted);
}

#[tokio::test]
async fn an_index_an_alias_points_at_is_never_deleted() {
    let server = MockServer::start().await;
    mount_stamped_first_publish(
        &server,
        listing(json!({
            STAMPED: {"aliases": {"persons": {}}},
            // Older, but an alias serves it: a racing publish moved `persons` back, or an
            // operator pinned it under another alias.
            "persons-20261014-000000": {"aliases": {"persons": {}}},
            "persons-20261013-000000": {"aliases": {"persons-pinned": {}}},
        })),
    )
    .await;
    mount_no_delete(&server).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published");
    assert!(report.deleted.is_empty(), "{:?}", report.deleted);
}

#[tokio::test]
async fn a_failed_delete_of_an_older_index_is_reported_not_fatal() {
    let server = MockServer::start().await;
    mount_stamped_first_publish(
        &server,
        listing(json!({
            STAMPED: {"aliases": {"persons": {}}},
            "persons-20261014-000000": {"aliases": {}},
            "persons-20261014-000001": {"aliases": {}},
        })),
    )
    .await;
    mount_delete(&server, "persons-20261014-000000", 403, 1).await;
    mount_delete(&server, "persons-20261014-000001", 200, 1).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published although a delete failed");
    assert_eq!(report.deleted, ["persons-20261014-000001"]);
    assert_eq!(report.not_deleted.len(), 1, "{:?}", report.not_deleted);
    assert_eq!(report.not_deleted[0].0, "persons-20261014-000000");
    assert!(
        report.not_deleted[0].1.contains("403"),
        "{:?}",
        report.not_deleted
    );
}

#[tokio::test]
async fn a_failed_listing_is_reported_not_fatal() {
    let server = MockServer::start().await;
    mount_stamped_first_publish(
        &server,
        ResponseTemplate::new(403).set_body_json(json!({"error": "forbidden"})),
    )
    .await;
    mount_no_delete(&server).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published although the listing failed");
    assert!(report.deleted.is_empty());
    assert_eq!(report.not_deleted.len(), 1, "{:?}", report.not_deleted);
    assert_eq!(report.not_deleted[0].0, "persons-*");
}

#[tokio::test]
async fn an_unstamped_run_id_lists_nothing() {
    // `persons-new` has no start stamp, so no index is older than it: no listing at all.
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
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-*/_alias"))
        .respond_with(listing(json!({})))
        .expect(0)
        .mount(&server)
        .await;

    Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&spec(5))
        .await
        .expect("published");
}

#[tokio::test]
async fn an_older_run_never_takes_the_alias_back_from_a_newer_one() {
    // A run resumed after a newer run published: the alias must stay on the newer index.
    let server = MockServer::start().await;
    mount_checks_for(&server, STAMPED, 5).await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"persons-20261015t120001z-bbbbbb": {"aliases": {"persons": {}}}}),
        ))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(0)
        .mount(&server)
        .await;
    mount_no_delete(&server).await;

    let mut recorded = false;
    let outcome = Publisher::new(IndexAdmin::new(client(&server)))
        .publish_recording(&stamped_spec(), |on_alias| {
            recorded = true;
            async move { Ok(on_alias) }
        })
        .await;
    match outcome {
        Err(EsError::NewerRunServing { serving, .. }) => {
            assert_eq!(serving, "persons-20261015t120001z-bbbbbb");
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(!recorded, "nothing is recorded for a refused publish");
}

#[tokio::test]
async fn a_previous_index_that_failed_to_delete_is_reported_once() {
    let server = MockServer::start().await;
    mount_checks_for(&server, STAMPED, 5).await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"persons-20261014-090000": {"aliases": {"persons": {}}}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
        .expect(1)
        .mount(&server)
        .await;
    // The previous index is still there, now without the alias: an older run's index too.
    Mock::given(method("GET"))
        .and(path("/persons-*/_alias"))
        .respond_with(listing(json!({
            STAMPED: {"aliases": {"persons": {}}},
            "persons-20261014-090000": {"aliases": {}},
        })))
        .expect(1)
        .mount(&server)
        .await;
    mount_delete(&server, "persons-20261014-090000", 403, 1).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published although a delete failed");
    assert!(report.deleted.is_empty(), "{:?}", report.deleted);
    assert_eq!(report.not_deleted.len(), 1, "{:?}", report.not_deleted);
    assert_eq!(report.not_deleted[0].0, "persons-20261014-090000");
}

#[tokio::test]
async fn a_listing_that_matches_nothing_deletes_nothing() {
    let server = MockServer::start().await;
    mount_stamped_first_publish(&server, ResponseTemplate::new(404)).await;
    mount_no_delete(&server).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published");
    assert!(report.deleted.is_empty());
    assert!(report.not_deleted.is_empty(), "{:?}", report.not_deleted);
}

#[tokio::test]
async fn a_malformed_listing_is_reported_not_fatal() {
    let server = MockServer::start().await;
    mount_stamped_first_publish(
        &server,
        listing(json!({"persons-20261014-000000": {"settings": {}}})),
    )
    .await;
    mount_no_delete(&server).await;

    let report = Publisher::new(IndexAdmin::new(client(&server)))
        .publish(&stamped_spec())
        .await
        .expect("published although the listing was malformed");
    assert_eq!(report.not_deleted.len(), 1, "{:?}", report.not_deleted);
    assert_eq!(report.not_deleted[0].0, "persons-*");
    assert!(
        report.not_deleted[0].1.contains("no aliases object"),
        "{:?}",
        report.not_deleted
    );
}
