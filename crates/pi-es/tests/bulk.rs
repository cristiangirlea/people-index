//! `BulkIndexer` against a mock Elasticsearch.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{BulkResponder, body_of, bulk_ids, client, config};
use pi_es::{BulkConfig, BulkIndexer, EsClient, EsError};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn doc(id: u64) -> Vec<u8> {
    serde_json::to_vec(&json!({"id": id, "first_name": format!("p{id}")})).expect("json")
}

fn bulk_mock() -> wiremock::MockBuilder {
    Mock::given(method("POST"))
        .and(path("/people-test/_bulk"))
        .and(query_param(
            "filter_path",
            "errors,items.*.status,items.*.error.type,items.*.error.reason",
        ))
        .and(header("content-type", "application/x-ndjson"))
}

async fn index_all(
    client: EsClient,
    ids: &[u64],
    config: BulkConfig,
) -> pi_es::Result<pi_es::BulkStats> {
    let mut indexer = BulkIndexer::new(client, "people-test", config)?;
    for id in ids {
        indexer.add(*id, &doc(*id)).await?;
    }
    indexer.finish().await
}

#[tokio::test]
async fn bulk_success_sends_every_document_once() {
    let server = MockServer::start().await;
    let responder = Arc::new(BulkResponder::new(|_, _| 201));
    bulk_mock()
        .respond_with(ArcResponder(Arc::clone(&responder)))
        .mount(&server)
        .await;

    let stats = index_all(client(&server), &[1, 2, 3], BulkConfig::default())
        .await
        .expect("indexed");

    assert_eq!(stats.docs_added, 3);
    assert_eq!(stats.docs_indexed, 3);
    assert_eq!(stats.requests, 1);
    assert_eq!(stats.request_retries, 0);
    let requests = server.received_requests().await.expect("recorded");
    assert_eq!(requests.len(), 1);
    let body = body_of(&requests[0]);
    assert_eq!(bulk_ids(&body), vec![1, 2, 3]);
    let lines: Vec<&str> = std::str::from_utf8(&body).expect("utf8").lines().collect();
    assert_eq!(lines[1], std::str::from_utf8(&doc(1)).expect("utf8"));
}

#[tokio::test]
async fn byte_limit_splits_requests_and_bounded_senders_deliver_everything() {
    let server = MockServer::start().await;
    bulk_mock()
        .respond_with(BulkResponder::new(|_, _| 201).delayed(Duration::from_millis(5)))
        .mount(&server)
        .await;
    let ids: Vec<u64> = (1..=200).collect();
    let config = BulkConfig {
        max_bytes: 2048,
        senders: 2,
        max_reasons: 3,
    };
    let stats = index_all(client(&server), &ids, config)
        .await
        .expect("indexed");
    assert_eq!(stats.docs_indexed, 200);
    let requests = server.received_requests().await.expect("recorded");
    assert!(requests.len() > 5, "{} requests", requests.len());
    let mut seen: Vec<u64> = Vec::new();
    for request in &requests {
        let body = body_of(request);
        assert!(body.len() <= 2048, "body of {} bytes", body.len());
        seen.extend(bulk_ids(&body));
    }
    seen.sort_unstable();
    assert_eq!(seen, ids);
    assert_eq!(
        stats.requests,
        u64::try_from(requests.len()).expect("small")
    );
}

#[tokio::test]
async fn partial_failure_resends_only_the_failed_items() {
    let server = MockServer::start().await;
    // First call: ids 2 and 4 are rejected (429); every later call accepts everything.
    let responder = Arc::new(BulkResponder::new(|call, id| {
        if call == 0 && (id == 2 || id == 4) {
            429
        } else {
            201
        }
    }));
    bulk_mock()
        .respond_with(ArcResponder(Arc::clone(&responder)))
        .mount(&server)
        .await;

    let stats = index_all(client(&server), &[1, 2, 3, 4, 5], BulkConfig::default())
        .await
        .expect("indexed");

    let seen = responder.seen.lock().expect("lock").clone();
    assert_eq!(seen, vec![vec![1, 2, 3, 4, 5], vec![2, 4]]);
    assert_eq!(stats.docs_indexed, 5);
    assert_eq!(stats.items_retried, 2);
    assert_eq!(stats.request_retries, 1);
    assert_eq!(stats.requests, 2);
}

#[tokio::test]
async fn whole_request_429_backs_off_then_succeeds() {
    let server = MockServer::start().await;
    bulk_mock()
        .respond_with(ResponseTemplate::new(429).set_body_string("too many requests"))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&server)
        .await;
    bulk_mock()
        .respond_with(BulkResponder::new(|_, _| 201))
        .with_priority(2)
        .mount(&server)
        .await;

    let stats = index_all(client(&server), &[10, 11], BulkConfig::default())
        .await
        .expect("indexed");

    assert_eq!(stats.requests, 3);
    assert_eq!(stats.request_retries, 2);
    assert_eq!(stats.docs_indexed, 2);
    let requests = server.received_requests().await.expect("recorded");
    for request in &requests {
        assert_eq!(bulk_ids(&body_of(request)), vec![10, 11]);
    }
}

#[tokio::test]
async fn permanent_item_error_fails_with_reasons() {
    let server = MockServer::start().await;
    bulk_mock()
        .respond_with(BulkResponder::new(
            |_, id| if id % 2 == 0 { 400 } else { 201 },
        ))
        .mount(&server)
        .await;

    let config = BulkConfig {
        max_reasons: 2,
        ..BulkConfig::default()
    };
    let outcome = index_all(client(&server), &[1, 2, 3, 4, 5, 6], config).await;

    match outcome {
        Err(EsError::BulkItems { failed, reasons }) => {
            assert_eq!(failed, 3);
            assert_eq!(reasons.len(), 2);
            assert_eq!(reasons[0].id, 2);
            assert_eq!(reasons[0].status, 400);
            assert_eq!(reasons[0].kind, "mapper_parsing_exception");
            assert_eq!(reasons[1].id, 4);
        }
        other => panic!("expected a permanent item failure, got {other:?}"),
    }
    // Permanent failures are not retried.
    assert_eq!(server.received_requests().await.expect("recorded").len(), 1);
}

#[tokio::test]
async fn transport_errors_are_retried() {
    let server = MockServer::start().await;
    // The first answer comes after the client has given up: a transport-level timeout.
    bulk_mock()
        .respond_with(BulkResponder::new(|_, _| 201).delayed(Duration::from_secs(3)))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    bulk_mock()
        .respond_with(BulkResponder::new(|_, _| 201))
        .with_priority(2)
        .mount(&server)
        .await;
    let mut config = config(&server);
    config.request_timeout = Duration::from_millis(300);
    let client = EsClient::new(&config).expect("client");

    let stats = index_all(client, &[7], BulkConfig::default())
        .await
        .expect("indexed");

    assert_eq!(stats.docs_indexed, 1);
    assert_eq!(stats.request_retries, 1);
    assert_eq!(stats.requests, 2);
}

#[tokio::test]
async fn retries_run_out() {
    let server = MockServer::start().await;
    bulk_mock()
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let outcome = index_all(client(&server), &[1, 2], BulkConfig::default()).await;
    match outcome {
        Err(EsError::RetriesExhausted {
            attempts, items, ..
        }) => {
            assert_eq!(attempts, 4);
            assert_eq!(items, 2);
        }
        other => panic!("expected exhausted retries, got {other:?}"),
    }
    assert_eq!(server.received_requests().await.expect("recorded").len(), 4);
}

#[tokio::test]
async fn other_http_errors_are_permanent() {
    let server = MockServer::start().await;
    bulk_mock()
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;
    let outcome = index_all(client(&server), &[1], BulkConfig::default()).await;
    assert!(
        matches!(outcome, Err(EsError::Http { status: 401, .. })),
        "{outcome:?}"
    );
    assert_eq!(server.received_requests().await.expect("recorded").len(), 1);
}

#[tokio::test]
async fn gzip_bodies_are_marked_and_decode() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/people-test/_bulk"))
        .and(header("content-encoding", "gzip"))
        .respond_with(BulkResponder::new(|_, _| 201))
        .mount(&server)
        .await;
    let mut config = config(&server);
    config.gzip_requests = true;
    let client = EsClient::new(&config).expect("client");

    let stats = index_all(client, &[1, 2, 3], BulkConfig::default())
        .await
        .expect("indexed");

    assert_eq!(stats.docs_indexed, 3);
    assert!(stats.wire_bytes > 0 && stats.wire_bytes != stats.bytes_sent);
}

#[tokio::test]
async fn invalid_documents_are_refused_before_sending() {
    let server = MockServer::start().await;
    let mut indexer = BulkIndexer::new(
        client(&server),
        "people-test",
        BulkConfig {
            max_bytes: 1024,
            ..BulkConfig::default()
        },
    )
    .expect("indexer");
    assert!(matches!(
        indexer.add(1, b"{\"a\":\n1}").await,
        Err(EsError::InvalidDocument { id: 1, .. })
    ));
    assert!(matches!(
        indexer.add(2, b"[1]").await,
        Err(EsError::InvalidDocument { id: 2, .. })
    ));
    let big = format!("{{\"x\":\"{}\"}}", "a".repeat(2000));
    assert!(matches!(
        indexer.add(3, big.as_bytes()).await,
        Err(EsError::DocumentTooLarge { id: 3, .. })
    ));
    assert!(BulkIndexer::new(client(&server), "Bad/Name", BulkConfig::default()).is_err());
    let stats = indexer.finish().await.expect("nothing to send");
    assert_eq!(stats.requests, 0);
}

/// Lets a test keep a handle on a responder it mounted.
struct ArcResponder<R>(Arc<R>);

impl<R: wiremock::Respond> wiremock::Respond for ArcResponder<R> {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        self.0.respond(request)
    }
}

/// Adds a fixed delay to another responder.
struct Delayed<R> {
    inner: R,
    delay: Duration,
}

impl<R: wiremock::Respond> wiremock::Respond for Delayed<R> {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        self.inner.respond(request).set_delay(self.delay)
    }
}

trait DelayExt: Sized {
    fn delayed(self, delay: Duration) -> Delayed<Self>;
}

impl<R: wiremock::Respond> DelayExt for R {
    fn delayed(self, delay: Duration) -> Delayed<Self> {
        Delayed { inner: self, delay }
    }
}
