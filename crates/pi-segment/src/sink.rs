//! Where a rolling writer's segments go.
//!
//! The writer does not know about files or object stores: it asks a [`SegmentSink`] for a
//! fresh `io::Write` per segment and hands it back when the segment is finished. The sink
//! turns the finished writer into whatever its caller wants: [`MemorySink`] gives the bytes
//! (to put into an object store), [`crate::FileSink`] gives a path on disk; a stage can
//! implement its own (for example a multipart upload behind a channel).

use std::io::{self, Write};

use bytes::Bytes;

use crate::format::SegmentStats;

/// Opens one writer per segment and finishes it.
pub trait SegmentSink {
    /// What a segment is written into.
    type Writer: Write;
    /// What the caller gets for a finished segment.
    type Output;

    /// Opens the writer for segment number `index` (0, 1, 2, ... per rolling writer).
    fn open(&mut self, index: u32) -> io::Result<Self::Writer>;

    /// Finishes segment `index`: the encoder has already closed the frame and flushed.
    fn close(
        &mut self,
        index: u32,
        writer: Self::Writer,
        stats: SegmentStats,
    ) -> io::Result<Self::Output>;
}

impl<S: SegmentSink + ?Sized> SegmentSink for &mut S {
    type Writer = S::Writer;
    type Output = S::Output;

    fn open(&mut self, index: u32) -> io::Result<Self::Writer> {
        (**self).open(index)
    }

    fn close(
        &mut self,
        index: u32,
        writer: Self::Writer,
        stats: SegmentStats,
    ) -> io::Result<Self::Output> {
        (**self).close(index, writer, stats)
    }
}

/// A finished segment held in memory, ready to upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishedSegment {
    /// The segment's number within its writer.
    pub index: u32,
    /// The whole segment, magic included.
    pub bytes: Bytes,
    /// What went into it.
    pub stats: SegmentStats,
}

/// Builds each segment in a `Vec<u8>` and returns it as [`Bytes`]. Memory per open writer is
/// one segment (up to the target size plus a block), so a stage writing many partitions at
/// once should size `target_bytes` with that in mind.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemorySink {
    initial_capacity: usize,
}

impl MemorySink {
    /// A sink whose buffers start empty and grow as needed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A sink whose buffers start with `capacity` bytes, to avoid regrowing toward a known
    /// segment size.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            initial_capacity: capacity,
        }
    }
}

impl SegmentSink for MemorySink {
    type Writer = Vec<u8>;
    type Output = FinishedSegment;

    fn open(&mut self, _index: u32) -> io::Result<Vec<u8>> {
        Ok(Vec::with_capacity(self.initial_capacity))
    }

    fn close(
        &mut self,
        index: u32,
        writer: Vec<u8>,
        stats: SegmentStats,
    ) -> io::Result<FinishedSegment> {
        Ok(FinishedSegment {
            index,
            bytes: Bytes::from(writer),
            stats,
        })
    }
}
