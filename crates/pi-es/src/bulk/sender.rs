//! Sends one bulk buffer until every item in it is acknowledged: whole-request retries for
//! rejections, gateways and the transport, item-level retries that resend only what failed,
//! and an immediate stop on anything a resend cannot fix.

use std::io::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::Method;

use super::buffer::BulkBuffer;
use super::counters::BulkCounters;
use super::response::{FILTER_PATH, settle};
use crate::client::{Body, EsClient};
use crate::error::{EsError, Result, is_retryable_status, snippet};
use crate::retry::RetryPolicy;

/// Everything a sender task needs, shared by all of one indexer's tasks.
#[derive(Debug)]
pub(crate) struct Sender {
    client: EsClient,
    path: String,
    policy: RetryPolicy,
    max_reasons: usize,
    counters: Arc<BulkCounters>,
}

impl Sender {
    pub(crate) fn new(
        client: EsClient,
        index: &str,
        max_reasons: usize,
        counters: Arc<BulkCounters>,
    ) -> Self {
        Self {
            policy: client.retry_policy(),
            path: format!("/{index}/_bulk?{FILTER_PATH}"),
            client,
            max_reasons,
            counters,
        }
    }

    /// Sends `buffer` to completion and hands it back cleared, ready for reuse.
    pub(crate) async fn send(&self, mut buffer: BulkBuffer) -> Result<BulkBuffer> {
        let mut attempt: u32 = 1;
        loop {
            let Some(last) = self.attempt(&mut buffer).await? else {
                buffer.clear();
                return Ok(buffer);
            };
            if !self.policy.allows_retry_after(attempt) {
                return Err(EsError::RetriesExhausted {
                    attempts: attempt,
                    items: buffer.len(),
                    last,
                });
            }
            self.counters.retry();
            let delay = self.policy.delay(attempt - 1);
            tracing::debug!(attempt, items = buffer.len(), ?delay, %last, "bulk retry");
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    /// One request. `Ok(None)`: every item is in. `Ok(Some(reason))`: something retryable
    /// failed and `buffer` now holds only what must go again. `Err`: stop.
    async fn attempt(&self, buffer: &mut BulkBuffer) -> Result<Option<String>> {
        let raw = buffer.freeze();
        let raw_len = raw.len();
        let body = if self.client.gzip_requests() {
            let input = raw.clone();
            let gzipped = tokio::task::spawn_blocking(move || gzip(&input))
                .await
                .map_err(|e| EsError::Task(format!("gzip: {e}")))??;
            Body::Ndjson {
                bytes: gzipped,
                gzipped: true,
            }
        } else {
            Body::Ndjson {
                bytes: raw.clone(),
                gzipped: false,
            }
        };
        let wire = match &body {
            Body::Ndjson { bytes, .. } => bytes.len(),
            _ => raw_len,
        };
        self.counters.request(raw_len, wire);
        let outcome = self.client.send(Method::POST, &self.path, body).await;
        // The request is over and dropped its handle on the body: take the allocation back.
        buffer.thaw(raw);
        let response = match outcome {
            Ok(response) => response,
            Err(error) if error.is_retryable() => return Ok(Some(error.to_string())),
            Err(error) => return Err(error),
        };
        if is_retryable_status(response.status) {
            return Ok(Some(format!(
                "HTTP {}: {}",
                response.status,
                snippet(&response.body)
            )));
        }
        if !(200..300).contains(&response.status) {
            return Err(EsError::Http {
                method: "POST".into(),
                path: self.path.clone(),
                status: response.status,
                body: snippet(&response.body),
            });
        }
        let settled = settle(
            &self.path,
            &response.body,
            buffer.len(),
            self.max_reasons,
            |n| buffer.id(n),
        )?;
        self.counters.indexed(settled.indexed);
        if settled.retry.is_empty() {
            return Ok(None);
        }
        self.counters.items_retried(settled.retry.len());
        buffer.compact(&settled.retry);
        Ok(Some(
            settled
                .retry_reason
                .unwrap_or_else(|| "retryable item failures".to_owned()),
        ))
    }
}

/// Gzip at the fastest level: the point is fewer bytes on the wire, not the smallest file.
fn gzip(input: &[u8]) -> Result<Bytes> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(input.len() / 4), Compression::fast());
    encoder
        .write_all(input)
        .and_then(|()| encoder.finish())
        .map(Bytes::from)
        .map_err(|e| EsError::Task(format!("gzip: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;

    #[test]
    fn gzip_round_trips() {
        let input = b"{\"index\":{\"_id\":\"1\"}}\n{}\n".repeat(100);
        let packed = gzip(&input).expect("gzip");
        assert!(packed.len() < input.len());
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(&packed[..])
            .read_to_end(&mut out)
            .expect("gunzip");
        assert_eq!(out, input);
    }
}
