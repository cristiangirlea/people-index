//! The segment format's constants, settings and counters.
//!
//! ```text
//! segment := MAGIC zstd_frame+
//! payload := (varint(len) postcard(row))*      (the decompressed frames, concatenated)
//! ```
//!
//! The magic sits outside the compressed stream so a reader can reject a wrong file before
//! starting a decompressor, and so a future `PISEG002` can change everything after it. Frames
//! carry zstd's content checksum, so damage inside a frame is detected rather than decoded as
//! wrong rows.

/// The eight bytes every segment starts with.
pub const MAGIC: &[u8; 8] = b"PISEG001";

/// Writers roll to a new segment once this many compressed bytes are written.
pub const DEFAULT_TARGET_BYTES: u64 = 64 << 20;

/// The zstd level: level 3 compresses JSON-heavy rows well at a few hundred MB/s per core.
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// The largest encoded row a writer produces and a reader accepts. It bounds what a damaged
/// length can make a reader allocate; documents are limited far below it.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 256 << 20;

/// How a writer encodes and rolls segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentConfig {
    /// Roll to a new segment once this many compressed bytes (magic included) are written. The
    /// segment ends after the row that crossed it, so a segment is larger than the target by
    /// at most about one row.
    pub target_bytes: u64,
    /// The zstd compression level.
    pub zstd_level: i32,
    /// Refuse rows whose encoding is longer than this.
    pub max_record_bytes: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            target_bytes: DEFAULT_TARGET_BYTES,
            zstd_level: DEFAULT_ZSTD_LEVEL,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
        }
    }
}

/// What went into one finished segment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStats {
    /// Rows written.
    pub records: u64,
    /// Bytes before compression: varint lengths plus encoded rows.
    pub raw_bytes: u64,
    /// Bytes of the finished segment, magic included.
    pub bytes: u64,
}
