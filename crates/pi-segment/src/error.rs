//! What can go wrong reading or writing a segment.
//!
//! A reader must tell three situations apart, because the caller reacts differently: the
//! storage failed (retry the task), the segment was cut short (an upload or a copy was
//! interrupted), or the bytes are not a segment or are damaged (a bug or bit rot). Committed
//! segments are immutable, so the last two mean the commit protocol was bypassed somewhere.

use std::io;

/// A segment error.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    /// The underlying reader or writer failed.
    #[error("segment I/O: {0}")]
    Io(#[from] io::Error),
    /// The file does not start with the segment magic.
    #[error("not a segment: expected magic {expected:?}, found {found:?}")]
    BadMagic {
        /// The magic every segment starts with.
        expected: &'static [u8],
        /// The bytes found instead (fewer than eight when the file is that short).
        found: Vec<u8>,
    },
    /// The segment ends early: in the magic, inside the zstd frame or inside a record.
    #[error("segment truncated after {records} records: {detail}")]
    Truncated {
        /// Records read successfully before the end.
        records: u64,
        /// Where it ended.
        detail: String,
    },
    /// The bytes are damaged: zstd or checksum errors, a bad length, a record that does not
    /// decode as the expected row type.
    #[error("segment corrupt after {records} records: {detail}")]
    Corrupt {
        /// Records read successfully before the damage.
        records: u64,
        /// What was wrong.
        detail: String,
    },
    /// A row's encoding is longer than the record limit; a reader would refuse it.
    #[error("record of {len} bytes exceeds the limit of {limit} bytes")]
    RecordTooLarge {
        /// The encoded length.
        len: usize,
        /// The limit.
        limit: usize,
    },
    /// A row could not be encoded.
    #[error("encoding a row: {0}")]
    Encode(postcard::Error),
}
