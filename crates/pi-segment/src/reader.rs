//! Streaming segment reads: one row at a time, memory bounded by the largest row.
//!
//! Errors are classified so the caller can act on them (see [`SegmentError`]): failures of the
//! underlying reader stay [`SegmentError::Io`]; an end inside the magic, the zstd frame or a
//! record is [`SegmentError::Truncated`]; anything else wrong with the bytes is
//! [`SegmentError::Corrupt`]. To keep the source's own errors apart from zstd's, the source is
//! wrapped so its errors carry a marker that survives the trip through the decompressor.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::marker::PhantomData;
use std::path::Path;

use serde::de::DeserializeOwned;

use crate::error::SegmentError;
use crate::format::{DEFAULT_MAX_RECORD_BYTES, MAGIC};
use crate::varint::{self, Decoded};

/// Reads rows of type `T` from a segment in `R`. Iterate it; after the first error it yields
/// nothing more.
pub struct SegmentReader<T, R: Read> {
    payload: BufReader<zstd::stream::read::Decoder<'static, BufReader<Source<R>>>>,
    record: Vec<u8>,
    records: u64,
    max_record_bytes: usize,
    done: bool,
    _row: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> SegmentReader<T, File> {
    /// Opens a segment file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SegmentError> {
        Self::new(File::open(path)?)
    }
}

impl<T: DeserializeOwned, R: Read> SegmentReader<T, R> {
    /// Starts reading a segment: checks the magic and opens the decompressor.
    pub fn new(reader: R) -> Result<Self, SegmentError> {
        Self::with_max_record_bytes(reader, DEFAULT_MAX_RECORD_BYTES)
    }

    /// Like [`SegmentReader::new`] with a different record limit (it must be at least the
    /// writer's).
    pub fn with_max_record_bytes(reader: R, max_record_bytes: usize) -> Result<Self, SegmentError> {
        let mut source = BufReader::with_capacity(128 << 10, Source(reader));
        let mut magic = Vec::with_capacity(MAGIC.len());
        (&mut source)
            .take(MAGIC.len() as u64)
            .read_to_end(&mut magic)
            .map_err(|e| classify(e, 0))?;
        if magic.as_slice() != MAGIC {
            if MAGIC.starts_with(&magic) {
                return Err(SegmentError::Truncated {
                    records: 0,
                    detail: format!("{} of {} magic bytes", magic.len(), MAGIC.len()),
                });
            }
            return Err(SegmentError::BadMagic {
                expected: MAGIC,
                found: magic,
            });
        }
        let decoder =
            zstd::stream::read::Decoder::with_buffer(source).map_err(|e| classify(e, 0))?;
        Ok(Self {
            payload: BufReader::with_capacity(64 << 10, decoder),
            record: Vec::new(),
            records: 0,
            max_record_bytes,
            done: false,
            _row: PhantomData,
        })
    }

    /// Rows read so far.
    pub fn records(&self) -> u64 {
        self.records
    }

    fn next_row(&mut self) -> Result<Option<T>, SegmentError> {
        let records = self.records;
        let len = match varint::decode(&mut self.payload).map_err(|e| classify(e, records))? {
            Decoded::Value(len) => len,
            Decoded::End => return Ok(None),
            Decoded::Truncated => return Err(truncated(records, "inside a record length")),
            Decoded::Overflow => return Err(corrupt(records, "record length overflows")),
        };
        let len = usize::try_from(len)
            .ok()
            .filter(|len| *len <= self.max_record_bytes)
            .ok_or_else(|| {
                corrupt(
                    records,
                    format!(
                        "record length {len} exceeds the limit of {}",
                        self.max_record_bytes
                    ),
                )
            })?;
        // Read through `take` rather than resizing first, so a damaged length cannot make the
        // reader allocate more than the data that is actually there.
        self.record.clear();
        (&mut self.payload)
            .take(len as u64)
            .read_to_end(&mut self.record)
            .map_err(|e| classify(e, records))?;
        if self.record.len() < len {
            return Err(truncated(
                records,
                format!("inside a record: {} of {len} bytes", self.record.len()),
            ));
        }
        let (row, rest) = postcard::take_from_bytes::<T>(&self.record)
            .map_err(|e| corrupt(records, format!("record does not decode: {e}")))?;
        if !rest.is_empty() {
            return Err(corrupt(
                records,
                format!("{} trailing bytes after a record", rest.len()),
            ));
        }
        self.records += 1;
        Ok(Some(row))
    }
}

impl<T: DeserializeOwned, R: Read> Iterator for SegmentReader<T, R> {
    type Item = Result<T, SegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Reads a whole in-memory segment (tests, small inputs).
pub fn decode_segment<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, SegmentError> {
    SegmentReader::new(bytes)?.collect()
}

/// The source reader, marking its errors so they are not mistaken for decompression errors.
struct Source<R>(R);

/// The marker: an error from the source, wrapped to keep its kind and message.
#[derive(Debug)]
struct SourceError(io::Error);

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for SourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl<R: Read> Read for Source<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.0.read(buf) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(io::Error::new(e.kind(), SourceError(e))),
                ok => return ok,
            }
        }
    }
}

/// Sorts an error from the decompression path into source failure, truncation or corruption.
fn classify(err: io::Error, records: u64) -> SegmentError {
    if err
        .get_ref()
        .is_some_and(<dyn std::error::Error + Send + Sync>::is::<SourceError>)
    {
        let source = err
            .into_inner()
            .and_then(|inner| inner.downcast::<SourceError>().ok());
        // The check above makes the fallback unreachable; it keeps the function total.
        return SegmentError::Io(source.map_or_else(|| io::Error::other("source error"), |s| s.0));
    }
    if err.kind() == io::ErrorKind::UnexpectedEof {
        return truncated(records, format!("zstd: {err}"));
    }
    corrupt(records, format!("zstd: {err}"))
}

fn truncated(records: u64, detail: impl Into<String>) -> SegmentError {
    SegmentError::Truncated {
        records,
        detail: detail.into(),
    }
}

fn corrupt(records: u64, detail: impl Into<String>) -> SegmentError {
    SegmentError::Corrupt {
        records,
        detail: detail.into(),
    }
}
