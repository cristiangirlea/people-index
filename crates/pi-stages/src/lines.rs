//! [`LineReader`]: newline-separated lines with a length cap.
//!
//! The cap keeps one bad line (a file with no newlines at all, a runaway record) from taking
//! the task's memory: a line past the cap is reported as too long with only its first
//! `max_line_bytes` bytes kept, and the rest is skipped up to the next newline. The cap counts
//! the line without its `\n` or `\r\n`, so the same line fits with either ending.

use std::io::{self, BufRead};

/// What [`LineReader::read_line`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LineInfo {
    /// 1-based line number in the file.
    pub(crate) number: u64,
    /// The full length of the line, newline excluded (may exceed what the buffer holds).
    pub(crate) len: u64,
    /// True when the line was longer than the cap; the buffer holds its first part only.
    pub(crate) too_long: bool,
}

/// Reads lines from a buffered reader.
#[derive(Debug)]
pub(crate) struct LineReader<R> {
    inner: R,
    max_line_bytes: usize,
    number: u64,
    bytes: u64,
}

impl<R: BufRead> LineReader<R> {
    pub(crate) fn new(inner: R, max_line_bytes: usize) -> Self {
        Self {
            inner,
            max_line_bytes,
            number: 0,
            bytes: 0,
        }
    }

    /// Bytes read so far, newlines included.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Reads the next line into `buf` (cleared first), without its `\n` or `\r\n`. `None` at
    /// the end of the input. A last line without a newline is still a line; an empty input,
    /// or nothing after the last newline, is no line.
    pub(crate) fn read_line(&mut self, buf: &mut Vec<u8>) -> io::Result<Option<LineInfo>> {
        buf.clear();
        let mut len = 0u64;
        let mut too_long = false;
        let mut saw_bytes = false;
        // One byte past the cap, so a `\r` before the newline still fits; the cap itself is
        // checked once that `\r` is dropped below.
        let cap = self.max_line_bytes.saturating_add(1);
        loop {
            let available = match self.inner.fill_buf() {
                Ok(b) => b,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if available.is_empty() {
                break;
            }
            saw_bytes = true;
            let (chunk_len, used, found) = match memchr::memchr(b'\n', available) {
                Some(i) => (i, i + 1, true),
                None => (available.len(), available.len(), false),
            };
            let chunk = &available[..chunk_len];
            if !too_long {
                let room = cap - buf.len();
                if chunk.len() > room {
                    buf.extend_from_slice(&chunk[..room]);
                    too_long = true;
                } else {
                    buf.extend_from_slice(chunk);
                }
            }
            len += chunk_len as u64;
            self.inner.consume(used);
            self.bytes += used as u64;
            if found {
                break;
            }
        }
        if !saw_bytes {
            return Ok(None);
        }
        if !too_long && buf.last() == Some(&b'\r') {
            buf.pop();
            len -= 1;
        }
        if buf.len() > self.max_line_bytes {
            buf.truncate(self.max_line_bytes);
            too_long = true;
        }
        self.number += 1;
        Ok(Some(LineInfo {
            number: self.number,
            len,
            too_long,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;

    use super::*;

    fn all(input: &[u8], max: usize, buffer: usize) -> Vec<(LineInfo, Vec<u8>)> {
        let mut r = LineReader::new(BufReader::with_capacity(buffer, input), max);
        let mut buf = Vec::new();
        let mut out = Vec::new();
        while let Some(info) = r.read_line(&mut buf).unwrap() {
            out.push((info, buf.clone()));
        }
        assert_eq!(r.bytes(), input.len() as u64);
        out
    }

    #[test]
    fn splits_lines_whatever_the_buffer_size() {
        let input = b"one\r\ntwo\n\nlast";
        for buffer in [1, 2, 3, 64] {
            let lines = all(input, 100, buffer);
            let texts: Vec<&[u8]> = lines.iter().map(|(_, b)| b.as_slice()).collect();
            assert_eq!(texts, [&b"one"[..], b"two", b"", b"last"]);
            assert_eq!(lines[3].0.number, 4);
            assert!(lines.iter().all(|(i, _)| !i.too_long));
        }
    }

    #[test]
    fn no_line_after_the_last_newline() {
        assert_eq!(all(b"a\n", 10, 4).len(), 1);
        assert!(all(b"", 10, 4).is_empty());
    }

    #[test]
    fn long_lines_are_cut_and_skipped() {
        let input = b"short\n0123456789abcdef\nnext\n";
        for buffer in [1, 4, 64] {
            let lines = all(input, 8, buffer);
            assert_eq!(lines.len(), 3);
            assert_eq!(lines[1].1, b"01234567");
            assert!(lines[1].0.too_long);
            assert_eq!(lines[1].0.len, 16);
            assert_eq!(lines[2].1, b"next");
            assert_eq!(lines[2].0.number, 3);
        }
    }

    #[test]
    fn a_line_of_exactly_the_cap_fits() {
        let lines = all(b"12345678\n", 8, 3);
        assert!(!lines[0].0.too_long);
        assert_eq!(lines[0].1, b"12345678");
    }

    #[test]
    fn a_crlf_line_of_exactly_the_cap_fits() {
        for buffer in [1, 3, 64] {
            let lines = all(b"12345678\r\nnext\r\n", 8, buffer);
            assert_eq!(lines.len(), 2);
            assert!(!lines[0].0.too_long);
            assert_eq!(lines[0].0.len, 8);
            assert_eq!(lines[0].1, b"12345678");
            assert_eq!(lines[1].1, b"next");
        }
        let lines = all(b"12345678\r", 8, 3);
        assert!(!lines[0].0.too_long);
        assert_eq!(lines[0].1, b"12345678");
    }

    #[test]
    fn one_byte_past_the_cap_is_too_long_with_either_ending() {
        for input in [&b"123456789\n"[..], b"123456789\r\n", b"123456789"] {
            for buffer in [1, 3, 64] {
                let lines = all(input, 8, buffer);
                assert_eq!(lines.len(), 1);
                assert!(lines[0].0.too_long);
                assert_eq!(lines[0].1, b"12345678");
            }
        }
    }

    #[test]
    fn a_cr_not_before_the_newline_is_content() {
        for buffer in [1, 3, 64] {
            let lines = all(b"1234567\rX\n", 8, buffer);
            assert!(lines[0].0.too_long);
            assert_eq!(lines[0].0.len, 9);
            assert_eq!(lines[0].1, b"1234567\r");
            let lines = all(b"123456\rX\n", 8, buffer);
            assert!(!lines[0].0.too_long);
            assert_eq!(lines[0].1, b"123456\rX");
            let lines = all(b"1234567\r\r\n", 8, buffer);
            assert!(!lines[0].0.too_long);
            assert_eq!(lines[0].1, b"1234567\r");
        }
    }
}
