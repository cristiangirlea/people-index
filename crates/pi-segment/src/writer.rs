//! The rolling writer: rows in, a sequence of segments of about `target_bytes` each out.
//!
//! A segment is finished as soon as the row that crossed the target is written, and handed to
//! the caller right away (from [`SegmentWriter::push`]), so an upload can start while later
//! rows are still being encoded and memory stays at one segment per writer. The next segment
//! is opened lazily on the next row, so a writer never leaves an empty trailing segment.

use std::marker::PhantomData;

use serde::Serialize;

use crate::encoder::SegmentEncoder;
use crate::error::SegmentError;
use crate::format::{SegmentConfig, SegmentStats};
use crate::sink::SegmentSink;

/// Writes rows of type `T` into segments produced by the sink `S`, rolling past the target.
pub struct SegmentWriter<T, S: SegmentSink> {
    sink: S,
    config: SegmentConfig,
    current: Option<SegmentEncoder<T, S::Writer>>,
    next_index: u32,
    totals: SegmentStats,
    segments: u32,
    _row: PhantomData<fn(&T)>,
}

impl<T: Serialize, S: SegmentSink> SegmentWriter<T, S> {
    /// A writer with the given settings.
    pub fn new(sink: S, config: SegmentConfig) -> Self {
        Self {
            sink,
            config,
            current: None,
            next_index: 0,
            totals: SegmentStats::default(),
            segments: 0,
            _row: PhantomData,
        }
    }

    /// Appends a row. Returns the finished segment when this row filled one. After an error
    /// the open segment is unusable: drop the writer and fail the task (its attempt's files
    /// are never committed, so they are never read).
    pub fn push(&mut self, row: &T) -> Result<Option<S::Output>, SegmentError> {
        let encoder = if let Some(encoder) = &mut self.current {
            encoder
        } else {
            let writer = self.sink.open(self.next_index)?;
            self.current.insert(SegmentEncoder::with_options(
                writer,
                self.config.zstd_level,
                self.config.max_record_bytes,
            )?)
        };
        encoder.push(row)?;
        // zstd holds up to a block of input before emitting anything, so the compressed count
        // lags. The estimate is an upper bound: only when it reaches the target is it worth
        // flushing a block to learn the exact size.
        let target = self.config.target_bytes;
        if encoder.estimated_bytes() >= target {
            if encoder.compressed_bytes() < target {
                encoder.flush_block()?;
            }
            if encoder.compressed_bytes() >= target {
                return self.roll().map(Some);
            }
        }
        Ok(None)
    }

    /// Finishes the open segment now, if it holds any rows, without waiting for the target.
    pub fn flush_segment(&mut self) -> Result<Option<S::Output>, SegmentError> {
        if self.current.is_some() {
            self.roll().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Finishes the last segment. Returns `None` when no rows were pushed since the last
    /// segment ended (or at all), and the sink with the totals over every segment.
    pub fn finish(mut self) -> Result<(Option<S::Output>, WriterTotals, S), SegmentError> {
        let last = self.flush_segment()?;
        let totals = WriterTotals {
            segments: self.segments,
            records: self.totals.records,
            raw_bytes: self.totals.raw_bytes,
            bytes: self.totals.bytes,
        };
        Ok((last, totals, self.sink))
    }

    /// Rows pushed so far, over every segment.
    pub fn records(&self) -> u64 {
        self.totals.records + self.current.as_ref().map_or(0, SegmentEncoder::records)
    }

    fn roll(&mut self) -> Result<S::Output, SegmentError> {
        let Some(encoder) = self.current.take() else {
            // Only called with an open segment (see push and flush_segment).
            return Err(SegmentError::Io(std::io::Error::other(
                "no open segment to finish",
            )));
        };
        let index = self.next_index;
        let (writer, stats) = encoder.finish()?;
        let output = self.sink.close(index, writer, stats)?;
        self.next_index += 1;
        self.segments += 1;
        self.totals.records += stats.records;
        self.totals.raw_bytes += stats.raw_bytes;
        self.totals.bytes += stats.bytes;
        Ok(output)
    }
}

/// Counters over every segment a writer finished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterTotals {
    /// Segments finished.
    pub segments: u32,
    /// Rows written.
    pub records: u64,
    /// Bytes before compression.
    pub raw_bytes: u64,
    /// Bytes of the finished segments.
    pub bytes: u64,
}
