//! [`Quarantine`]: the lines (and documents) a task could not use, with where they came from.
//!
//! Written once per task attempt to `runs/<id>/quarantine/<task>-<attempt>.ndjson` and listed
//! in the commit like any other output, so a losing attempt's quarantine is an orphan too.

use bytes::Bytes;
use serde::Serialize;

/// How much of an offending line is kept: enough to see what went wrong, bounded so the
/// quarantine of a feed stays small (`max_malformed` lines of at most this size).
pub(crate) const QUARANTINE_SAMPLE_BYTES: usize = 1024;

/// One quarantined line or document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct QuarantineEntry {
    /// `organizations` or `persons`.
    pub(crate) feed: &'static str,
    /// The input key.
    pub(crate) file: String,
    /// The 1-based line number in that file.
    pub(crate) line: u64,
    /// Why: `syntax`, `envelope`, `record`, `too_long` or `document_too_large`.
    pub(crate) kind: &'static str,
    /// The detail.
    pub(crate) reason: String,
    /// The first bytes of the line, lossily decoded.
    pub(crate) sample: String,
}

impl QuarantineEntry {
    /// An entry with the first [`QUARANTINE_SAMPLE_BYTES`] of `line` as its sample.
    pub(crate) fn new(
        feed: &'static str,
        file: &str,
        line: u64,
        kind: &'static str,
        reason: impl Into<String>,
        text: &[u8],
    ) -> Self {
        let cut = &text[..text.len().min(QUARANTINE_SAMPLE_BYTES)];
        Self {
            feed,
            file: file.to_owned(),
            line,
            kind,
            reason: reason.into(),
            sample: String::from_utf8_lossy(cut).into_owned(),
        }
    }
}

/// The entries of one task attempt, as NDJSON.
#[derive(Debug, Default)]
pub(crate) struct Quarantine {
    buf: Vec<u8>,
    entries: u64,
}

impl Quarantine {
    /// Adds an entry.
    pub(crate) fn push(&mut self, entry: &QuarantineEntry) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.buf, entry)?;
        self.buf.push(b'\n');
        self.entries += 1;
        Ok(())
    }

    /// The number of entries.
    pub(crate) fn len(&self) -> u64 {
        self.entries
    }

    /// The NDJSON body, or `None` when nothing was quarantined (no file is written then).
    pub(crate) fn into_bytes(self) -> Option<Bytes> {
        (self.entries > 0).then(|| Bytes::from(self.buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_are_ndjson_with_bounded_samples() {
        let mut q = Quarantine::default();
        assert!(Quarantine::default().into_bytes().is_none());
        let long = vec![b'x'; QUARANTINE_SAMPLE_BYTES * 3];
        q.push(&QuarantineEntry::new(
            "persons",
            "raw/persons/a",
            7,
            "syntax",
            "bad",
            &long,
        ))
        .unwrap();
        q.push(&QuarantineEntry::new(
            "persons",
            "raw/persons/a",
            9,
            "record",
            "no",
            b"\xff{",
        ))
        .unwrap();
        assert_eq!(q.len(), 2);
        let bytes = q.into_bytes().unwrap();
        let lines: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_slice(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["line"], 7);
        assert_eq!(
            lines[0]["sample"].as_str().unwrap().len(),
            QUARANTINE_SAMPLE_BYTES
        );
        assert_eq!(lines[1]["file"], "raw/persons/a");
        assert_eq!(lines[1]["kind"], "record");
    }
}
