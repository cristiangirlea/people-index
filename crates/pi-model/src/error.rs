//! The error a line gets when it cannot be indexed safely.
//!
//! A malformed line is data, not a failure: the extract stages count it, write it to the
//! quarantine with its origin and the reason below, and carry on until `max_malformed`. The kind
//! is coarse so it can label a metric; the reason is precise so a person reading the quarantine
//! can see what was wrong without rerunning anything.

use std::fmt;

/// Which layer of the line was wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MalformedKind {
    /// Not JSON at all: bad syntax, invalid UTF-8, an empty line, trailing data.
    Syntax,
    /// JSON, but not an envelope: not an object, `id` or `record` missing, duplicated or invalid.
    Envelope,
    /// A valid envelope whose record cannot be joined or indexed: `roles`, a role's
    /// `organization_id`, or a mapped field of the wrong JSON kind.
    Record,
}

impl MalformedKind {
    /// A stable lowercase label, for metrics and the quarantine file.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Syntax => "syntax",
            Self::Envelope => "envelope",
            Self::Record => "record",
        }
    }
}

impl fmt::Display for MalformedKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A line that is not an envelope this pipeline can index, and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed {kind}: {reason}")]
pub struct MalformedRecord {
    kind: MalformedKind,
    reason: String,
}

impl MalformedRecord {
    /// A malformed line of the given kind.
    pub fn new(kind: MalformedKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }

    /// Which layer was wrong.
    #[must_use]
    pub fn kind(&self) -> MalformedKind {
        self.kind
    }

    /// What was wrong, in words.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Classifies a `serde_json` error from the given layer: syntax and end-of-input errors are
    /// `Syntax` whatever layer was being read, the rest belong to that layer.
    pub(crate) fn from_json(layer: MalformedKind, err: &serde_json::Error) -> Self {
        let kind = if err.is_syntax() || err.is_eof() {
            MalformedKind::Syntax
        } else {
            layer
        };
        Self::new(kind, err.to_string())
    }
}
