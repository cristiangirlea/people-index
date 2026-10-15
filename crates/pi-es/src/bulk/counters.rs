//! Load counters, shared between the indexer and its sender tasks. Plain atomics so a metrics
//! exporter or a progress log can read them at any time without a lock; each increment also
//! goes to the `metrics` facade (a no-op until the binary installs a recorder).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;

/// Live counters of one [`crate::BulkIndexer`].
#[derive(Debug, Default)]
pub struct BulkCounters {
    docs_added: AtomicU64,
    docs_indexed: AtomicU64,
    requests: AtomicU64,
    request_retries: AtomicU64,
    items_retried: AtomicU64,
    bytes_sent: AtomicU64,
    wire_bytes: AtomicU64,
    backpressure_nanos: AtomicU64,
}

/// A point-in-time copy of [`BulkCounters`], for task reports and logs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct BulkStats {
    /// Documents handed to the indexer.
    pub docs_added: u64,
    /// Documents Elasticsearch acknowledged (each counted once, however many attempts it took).
    pub docs_indexed: u64,
    /// Bulk requests sent, retries included.
    pub requests: u64,
    /// Requests that were sent again (whole or compacted).
    pub request_retries: u64,
    /// Items resent after an item-level retryable failure.
    pub items_retried: u64,
    /// Bulk body bytes before any compression, retries included.
    pub bytes_sent: u64,
    /// Bytes on the wire (equal to `bytes_sent` unless bodies are gzipped).
    pub wire_bytes: u64,
    /// Time `add` spent waiting for a free buffer: how long the cluster held the producer back.
    #[serde(with = "millis")]
    pub backpressure: Duration,
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

impl BulkCounters {
    pub(crate) fn added(&self) {
        self.docs_added.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn indexed(&self, n: usize) {
        self.docs_indexed.fetch_add(to_u64(n), Ordering::Relaxed);
        metrics::counter!("pi_es_bulk_docs_indexed_total").increment(to_u64(n));
    }

    pub(crate) fn request(&self, raw: usize, wire: usize) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(to_u64(raw), Ordering::Relaxed);
        self.wire_bytes.fetch_add(to_u64(wire), Ordering::Relaxed);
        metrics::counter!("pi_es_bulk_requests_total").increment(1);
        metrics::counter!("pi_es_bulk_wire_bytes_total").increment(to_u64(wire));
    }

    pub(crate) fn retry(&self) {
        self.request_retries.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("pi_es_bulk_retries_total").increment(1);
    }

    pub(crate) fn items_retried(&self, n: usize) {
        self.items_retried.fetch_add(to_u64(n), Ordering::Relaxed);
        metrics::counter!("pi_es_bulk_items_retried_total").increment(to_u64(n));
    }

    pub(crate) fn backpressure(&self, waited: Duration) {
        let nanos = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.backpressure_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    /// The counters as they are now.
    #[must_use]
    pub fn snapshot(&self) -> BulkStats {
        BulkStats {
            docs_added: self.docs_added.load(Ordering::Relaxed),
            docs_indexed: self.docs_indexed.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            request_retries: self.request_retries.load(Ordering::Relaxed),
            items_retried: self.items_retried.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            wire_bytes: self.wire_bytes.load(Ordering::Relaxed),
            backpressure: Duration::from_nanos(self.backpressure_nanos.load(Ordering::Relaxed)),
        }
    }
}

mod millis {
    use std::time::Duration;

    use serde::Serializer;

    pub(super) fn serialize<S: Serializer>(value: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
    }
}
