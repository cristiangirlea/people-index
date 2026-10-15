//! pi-segment: the segment file format (docs/ARCHITECTURE.md, "Segment files").
//!
//! A segment is the 8-byte magic `PISEG001` followed by a zstd stream of records, each a LEB128
//! length and the postcard encoding of a row. Stages write rows with a [`SegmentWriter`], which
//! rolls to a new segment past [`SegmentConfig::target_bytes`] and hands each finished segment
//! to the caller as soon as it is complete: as bytes with [`MemorySink`] (to put into the
//! object store) or as a file with [`FileSink`]. They read rows back with a streaming
//! [`SegmentReader`]. Everything here is synchronous CPU work: call it from `spawn_blocking`
//! or a rayon pool, never on the async reactor.

mod counting;
mod encoder;
mod error;
mod file_sink;
mod format;
mod reader;
mod sink;
mod varint;
mod writer;

pub use encoder::{SegmentEncoder, encode_segment};
pub use error::SegmentError;
pub use file_sink::{FileSink, FinishedFile};
pub use format::{
    DEFAULT_MAX_RECORD_BYTES, DEFAULT_TARGET_BYTES, DEFAULT_ZSTD_LEVEL, MAGIC, SegmentConfig,
    SegmentStats,
};
pub use reader::{SegmentReader, decode_segment};
pub use sink::{FinishedSegment, MemorySink, SegmentSink};
pub use writer::{SegmentWriter, WriterTotals};
