//! The generator's error type.

use std::io;

/// Why generation stopped.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The configuration cannot produce a valid data set.
    #[error("invalid generator config: {0}")]
    Config(String),
    /// The sink failed while writing `path`.
    #[error("writing {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    /// A record could not be encoded (not expected with the types used here).
    #[error("encoding JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The simulation disagrees with itself; a bug in the generator, never in the input.
    #[error("generator invariant broken: {0}")]
    Internal(String),
}

impl Error {
    pub(crate) fn io(path: &str, source: io::Error) -> Self {
        Self::Io {
            path: path.to_owned(),
            source,
        }
    }
}

/// The generator's result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
