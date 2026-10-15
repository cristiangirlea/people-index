//! One segment, written to any `io::Write`.
//!
//! [`SegmentEncoder`] is the single-segment building block: magic, one zstd frame with a
//! content checksum, varint-framed postcard rows. [`crate::SegmentWriter`] rolls several of
//! them; use the encoder directly when the caller decides where one segment ends (or wants an
//! explicitly empty one).

use std::io::Write;
use std::marker::PhantomData;

use serde::Serialize;

use crate::counting::CountingWriter;
use crate::error::SegmentError;
use crate::format::{DEFAULT_MAX_RECORD_BYTES, DEFAULT_ZSTD_LEVEL, MAGIC, SegmentStats};
use crate::varint;

/// Writes rows of type `T` as one segment into `W`.
pub struct SegmentEncoder<T, W: Write> {
    zstd: zstd::stream::write::Encoder<'static, CountingWriter<W>>,
    scratch: Vec<u8>,
    records: u64,
    raw_bytes: u64,
    /// `raw_bytes` at the last explicit flush: everything after it may still sit in zstd.
    raw_at_flush: u64,
    max_record_bytes: usize,
    // `fn(&T)`: the encoder consumes `T`s by reference and owns none, so it stays Send/Sync
    // whatever `T` is.
    _row: PhantomData<fn(&T)>,
}

impl<T: Serialize, W: Write> SegmentEncoder<T, W> {
    /// Starts a segment at the default level and record limit.
    pub fn new(writer: W) -> Result<Self, SegmentError> {
        Self::with_options(writer, DEFAULT_ZSTD_LEVEL, DEFAULT_MAX_RECORD_BYTES)
    }

    /// Starts a segment: writes the magic and opens the zstd frame.
    pub fn with_options(
        writer: W,
        zstd_level: i32,
        max_record_bytes: usize,
    ) -> Result<Self, SegmentError> {
        let mut counting = CountingWriter::new(writer);
        counting.write_all(MAGIC)?;
        let mut zstd = zstd::stream::write::Encoder::new(counting, zstd_level)?;
        zstd.include_checksum(true)?;
        Ok(Self {
            zstd,
            scratch: Vec::with_capacity(4096),
            records: 0,
            raw_bytes: 0,
            raw_at_flush: 0,
            max_record_bytes,
            _row: PhantomData,
        })
    }

    /// Appends one row.
    pub fn push(&mut self, row: &T) -> Result<(), SegmentError> {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        let scratch = postcard::to_extend(row, scratch).map_err(SegmentError::Encode)?;
        if scratch.len() > self.max_record_bytes {
            return Err(SegmentError::RecordTooLarge {
                len: scratch.len(),
                limit: self.max_record_bytes,
            });
        }
        let (len, n) = varint::encode(scratch.len() as u64);
        self.zstd.write_all(&len[..n])?;
        self.zstd.write_all(&scratch)?;
        self.records += 1;
        self.raw_bytes += (n + scratch.len()) as u64;
        self.scratch = scratch;
        Ok(())
    }

    /// Rows written so far.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Compressed bytes handed to the writer so far, magic included. zstd holds up to a block
    /// of input before emitting it, so this lags the rows pushed.
    pub fn compressed_bytes(&self) -> u64 {
        self.zstd.get_ref().count()
    }

    /// An upper bound of the segment size if it were finished now: the compressed bytes out so
    /// far plus every raw byte pushed since the last [`SegmentEncoder::flush_block`] (zstd
    /// never expands data by more than a few bytes per block).
    pub fn estimated_bytes(&self) -> u64 {
        self.compressed_bytes() + (self.raw_bytes - self.raw_at_flush)
    }

    /// Makes zstd emit everything it holds as a block, so [`SegmentEncoder::compressed_bytes`]
    /// is exact. Each call ends a block early, which costs a little compression; the rolling
    /// writer only calls it when the estimate says the target may have been reached.
    pub fn flush_block(&mut self) -> Result<(), SegmentError> {
        self.zstd.flush()?;
        self.raw_at_flush = self.raw_bytes;
        Ok(())
    }

    /// Closes the frame, flushes the writer and returns it with the segment's counters.
    pub fn finish(self) -> Result<(W, SegmentStats), SegmentError> {
        let mut counting = self.zstd.finish()?;
        counting.flush()?;
        let stats = SegmentStats {
            records: self.records,
            raw_bytes: self.raw_bytes,
            bytes: counting.count(),
        };
        Ok((counting.into_inner(), stats))
    }
}

/// Encodes `rows` as one segment in memory (tests, small outputs, one-shot uploads).
pub fn encode_segment<'a, T: Serialize + 'a>(
    rows: impl IntoIterator<Item = &'a T>,
) -> Result<Vec<u8>, SegmentError> {
    let mut encoder = SegmentEncoder::new(Vec::new())?;
    for row in rows {
        encoder.push(row)?;
    }
    Ok(encoder.finish()?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_magic_then_a_zstd_frame() {
        let bytes = encode_segment::<u32>(&[1, 2, 3]).unwrap();
        assert_eq!(&bytes[..8], MAGIC);
        // The zstd frame magic, little-endian 0xFD2FB528.
        assert_eq!(&bytes[8..12], &[0x28, 0xB5, 0x2F, 0xFD]);
    }

    #[test]
    fn counts_records_and_bytes() {
        let mut encoder = SegmentEncoder::<String, _>::new(Vec::new()).unwrap();
        encoder.push(&"abc".to_owned()).unwrap();
        encoder.push(&String::new()).unwrap();
        assert_eq!(encoder.records(), 2);
        assert!(encoder.compressed_bytes() >= 8);
        let (bytes, stats) = encoder.finish().unwrap();
        assert_eq!(stats.records, 2);
        // "abc" is postcard varint(3) + 3 bytes = 4, plus its length byte; "" is 1 plus 1.
        assert_eq!(stats.raw_bytes, 5 + 2);
        assert_eq!(stats.bytes, bytes.len() as u64);
    }

    #[test]
    fn refuses_rows_over_the_limit() {
        let mut encoder = SegmentEncoder::<Vec<u8>, _>::with_options(Vec::new(), 1, 10).unwrap();
        encoder.push(&vec![0u8; 5]).unwrap();
        let err = encoder.push(&vec![0u8; 50]).unwrap_err();
        assert!(
            matches!(err, SegmentError::RecordTooLarge { limit: 10, .. }),
            "{err}"
        );
        assert_eq!(encoder.records(), 1, "the refused row was not written");
    }
}
