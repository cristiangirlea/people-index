//! One error type for the crate, so callers can tell a retryable cluster hiccup from an answer
//! that means the run must stop.

use std::fmt;

/// The longest slice of a response body kept in an error: enough to read the cause, small enough
/// that a misbehaving proxy cannot flood the logs.
pub(crate) const BODY_SNIPPET: usize = 1000;

/// Everything that can go wrong talking to Elasticsearch.
#[derive(Debug, thiserror::Error)]
pub enum EsError {
    /// The configuration cannot work (bad URL, unreadable CA file, invalid index name).
    #[error("configuration: {0}")]
    Config(String),

    /// The request never got an HTTP answer (connect, TLS, timeout, reset).
    #[error("{method} {path}: transport error: {message}")]
    Transport {
        method: String,
        path: String,
        message: String,
    },

    /// Elasticsearch answered with a status that is not a success.
    #[error("{method} {path}: HTTP {status}: {body}")]
    Http {
        method: String,
        path: String,
        status: u16,
        body: String,
    },

    /// The answer was not the JSON we expected.
    #[error("{path}: unexpected response: {message}")]
    Decode { path: String, message: String },

    /// The response body went over the configured limit.
    #[error("{path}: response exceeds {limit} bytes")]
    ResponseTooLarge { path: String, limit: usize },

    /// Not every shard answered, so a count or refresh would be a guess.
    #[error("{path}: incomplete shard response: {shards}")]
    IncompleteShards { path: String, shards: String },

    /// Elasticsearch reported `timed_out: true`.
    #[error("{path}: Elasticsearch timed out")]
    TimedOut { path: String },

    /// A change was not acknowledged by the master.
    #[error("{0} was not acknowledged")]
    NotAcknowledged(String),

    /// Bulk items failed with an error a resend cannot fix (mapping errors, bad documents).
    #[error("{failed} bulk item(s) failed permanently; first: {}", ItemFailures(.reasons))]
    BulkItems {
        failed: usize,
        reasons: Vec<ItemFailure>,
    },

    /// Retryable failures kept coming until the attempts ran out.
    #[error("bulk request still failing after {attempts} attempts ({items} item(s) left): {last}")]
    RetriesExhausted {
        attempts: u32,
        items: usize,
        last: String,
    },

    /// One document is larger than a whole bulk request may be.
    #[error("document {id} needs {size} bytes, over the bulk limit of {max}")]
    DocumentTooLarge { id: u64, size: usize, max: usize },

    /// A document cannot be framed as one NDJSON line.
    #[error("document {id}: {reason}")]
    InvalidDocument { id: u64, reason: String },

    /// The index does not hold the number of documents the pipeline committed.
    #[error("{index} holds {actual} documents, expected {expected}")]
    CountMismatch {
        index: String,
        expected: u64,
        actual: u64,
    },

    /// The cluster did not reach the wanted health in time.
    #[error("{target}: health {last} after waiting, wanted {wanted}")]
    HealthTimeout {
        target: String,
        wanted: String,
        last: String,
    },

    /// A concrete index already has the alias's name, so the alias cannot be created.
    #[error("a concrete index is named {0}; remove or rename it before publishing")]
    ConcreteIndexAtAlias(String),

    /// The alias serves a newer run's index: publishing this older run would move the alias
    /// back and delete the newer index.
    #[error("{alias} serves {serving}, a newer run than {index}; start a new run instead")]
    NewerRunServing {
        alias: String,
        index: String,
        serving: String,
    },

    /// The expectations file could not be read or parsed.
    #[error("expected counts: {0}")]
    Expected(String),

    /// The record of the indexes the alias served before (written by the caller of `publish`)
    /// could not be written or read back.
    #[error("record of the previous indexes: {0}")]
    Record(String),

    /// A background task panicked or was cancelled, or an earlier failure stopped the work.
    #[error("background task: {0}")]
    Task(String),
}

impl EsError {
    /// Whether a whole request failing this way is worth sending again: rejections, gateways
    /// and the transport, never an answer about the request itself.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { .. } => true,
            Self::Http { status, .. } => is_retryable_status(*status),
            _ => false,
        }
    }
}

/// Statuses that mean "the cluster is busy or a hop is down", per request and per bulk item.
#[must_use]
pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 502 | 503 | 504)
}

/// Why one bulk item failed, as Elasticsearch said it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemFailure {
    /// The document id (`_id`, the person id).
    pub id: u64,
    pub status: u16,
    /// The error type, e.g. `mapper_parsing_exception`.
    pub kind: String,
    pub reason: String,
}

impl fmt::Display for ItemFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "id {} status {} {}: {}",
            self.id, self.status, self.kind, self.reason
        )
    }
}

/// Display helper for the first few item failures.
struct ItemFailures<'a>(&'a [ItemFailure]);

impl fmt::Display for ItemFailures<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (n, item) in self.0.iter().enumerate() {
            if n > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{item}")?;
        }
        Ok(())
    }
}

/// The crate's result type.
pub type Result<T, E = EsError> = std::result::Result<T, E>;

/// A lossy, bounded view of a response body for error messages.
pub(crate) fn snippet(body: &[u8]) -> String {
    let end = body.len().min(BODY_SNIPPET);
    String::from_utf8_lossy(&body[..end]).into_owned()
}
