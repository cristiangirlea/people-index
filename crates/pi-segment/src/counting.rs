//! A writer that counts the bytes it passes on, so the encoder knows the compressed size of
//! the segment so far without asking the sink.

use std::io::{self, Write};

/// Passes writes through to `inner` and counts the bytes accepted.
#[derive(Debug)]
pub(crate) struct CountingWriter<W> {
    inner: W,
    count: u64,
}

impl<W: Write> CountingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    /// Bytes accepted by `inner` so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_what_passes() {
        let mut w = CountingWriter::new(Vec::new());
        w.write_all(b"hello").unwrap();
        w.write_all(b" world").unwrap();
        w.flush().unwrap();
        assert_eq!(w.count(), 11);
        assert_eq!(w.into_inner(), b"hello world");
    }
}
