//! Shared helpers for the mock-server tests.
#![allow(dead_code)]

use std::io::Read as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use pi_es::{EsClient, EsConfig, RetryPolicy};
use serde_json::{Value, json};
use wiremock::{MockServer, Request, Respond, ResponseTemplate};

/// Retries fast enough for a unit test.
pub fn fast_retry(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
    }
}

pub fn config(server: &MockServer) -> EsConfig {
    let mut config = EsConfig::new(server.uri());
    config.retry = fast_retry(4);
    config.request_timeout = Duration::from_secs(5);
    config
}

pub fn client(server: &MockServer) -> EsClient {
    EsClient::new(&config(server)).expect("client")
}

/// The request body, gunzipped when it was sent gzipped.
pub fn body_of(request: &Request) -> Vec<u8> {
    let gzipped = request
        .headers
        .get("content-encoding")
        .is_some_and(|v| v.as_bytes() == b"gzip");
    if gzipped {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(&request.body[..])
            .read_to_end(&mut out)
            .expect("gunzip");
        out
    } else {
        request.body.clone()
    }
}

/// The document ids of a bulk body, in order.
pub fn bulk_ids(body: &[u8]) -> Vec<u64> {
    let text = std::str::from_utf8(body).expect("utf8");
    assert!(text.ends_with('\n'), "a bulk body ends with a newline");
    text.lines()
        .step_by(2)
        .map(|action| {
            let action: Value = serde_json::from_str(action).expect("action line");
            action["index"]["_id"]
                .as_str()
                .expect("_id")
                .parse()
                .expect("numeric id")
        })
        .collect()
}

/// A bulk responder whose item statuses come from a function of (call number, document id).
pub struct BulkResponder<F> {
    calls: AtomicUsize,
    status: F,
    pub seen: Mutex<Vec<Vec<u64>>>,
}

impl<F> BulkResponder<F>
where
    F: Fn(usize, u64) -> u16 + Send + Sync,
{
    pub fn new(status: F) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            status,
            seen: Mutex::new(Vec::new()),
        }
    }
}

impl<F> Respond for BulkResponder<F>
where
    F: Fn(usize, u64) -> u16 + Send + Sync,
{
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let ids = bulk_ids(&body_of(request));
        let mut errors = false;
        let items: Vec<Value> = ids
            .iter()
            .map(|id| {
                let status = (self.status)(call, *id);
                if (200..300).contains(&status) {
                    json!({"index": {"status": status}})
                } else {
                    errors = true;
                    let kind = if status == 429 {
                        "es_rejected_execution_exception"
                    } else {
                        "mapper_parsing_exception"
                    };
                    json!({"index": {"status": status, "error": {"type": kind, "reason": format!("doc {id}")}}})
                }
            })
            .collect();
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(ids);
        }
        ResponseTemplate::new(200).set_body_json(json!({"errors": errors, "items": items}))
    }
}

/// A complete `_shards` header.
pub fn shards(total: u64) -> Value {
    json!({"total": total, "successful": total, "skipped": 0, "failed": 0})
}
