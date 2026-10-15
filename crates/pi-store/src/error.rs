//! The errors a [`Store`](crate::Store) call can return.
//!
//! The variants the coordination protocol branches on (`NotFound`, `PreconditionFailed`,
//! `Unsupported`) are lifted out of `object_store::Error` so callers never match on backend
//! error strings.

/// What went wrong talking to the object store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The object does not exist.
    #[error("object not found: {key}")]
    NotFound { key: String },

    /// A conditional update lost: the object changed (or vanished) since its version was read.
    #[error("precondition failed for {key}: the object changed since it was read")]
    PreconditionFailed { key: String },

    /// The backend cannot do this operation (the local filesystem has no update-if-match).
    #[error("{operation} is not supported by the {backend} store")]
    Unsupported {
        operation: &'static str,
        backend: &'static str,
    },

    /// The store URL could not be understood.
    #[error("invalid store url {url:?}: {reason}")]
    InvalidUrl { url: String, reason: String },

    /// A key is not a valid object path (empty segment, `.` or `..`, control characters).
    #[error("invalid object key {key:?}: {reason}")]
    InvalidKey { key: String, reason: String },

    /// A JSON document could not be encoded or decoded.
    #[error("json error for {key}: {source}")]
    Json {
        key: String,
        #[source]
        source: serde_json::Error,
    },

    /// A local filesystem error while preparing a store (creating the root directory).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Any other backend error (network, auth, throttling after the client's own retries).
    #[error(transparent)]
    Backend(object_store::Error),
}

impl StoreError {
    /// Lifts the variants the protocol depends on out of a raw backend error.
    pub(crate) fn from_backend(key: &str, err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { .. } => Self::NotFound {
                key: key.to_owned(),
            },
            object_store::Error::Precondition { .. } => Self::PreconditionFailed {
                key: key.to_owned(),
            },
            other => Self::Backend(other),
        }
    }

    /// True when the error means the object is absent.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    /// True when a conditional write lost the race.
    #[must_use]
    pub fn is_precondition_failed(&self) -> bool {
        matches!(self, Self::PreconditionFailed { .. })
    }
}

/// Shorthand for results of store calls.
pub type StoreResult<T> = Result<T, StoreError>;
