//! LEB128 varints for record lengths: seven bits per byte, low bits first, high bit set on
//! every byte but the last. A length under 128 costs one byte and one under 16 KiB two, so
//! the framing adds almost nothing to a row.

use std::io::{self, BufRead};

/// The longest encoding of a u64.
pub(crate) const MAX_LEN: usize = 10;

/// Encodes `value`, returning the buffer and the number of bytes used.
pub(crate) fn encode(mut value: u64) -> ([u8; MAX_LEN], usize) {
    let mut buf = [0u8; MAX_LEN];
    let mut n = 0;
    loop {
        // The mask keeps seven bits, so the cast cannot truncate anything that matters.
        #[allow(clippy::cast_possible_truncation)]
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            buf[n] = byte;
            return (buf, n + 1);
        }
        buf[n] = byte | 0x80;
        n += 1;
    }
}

/// What reading a varint found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decoded {
    /// A complete value.
    Value(u64),
    /// The input ended before the first byte: a clean end of stream.
    End,
    /// The input ended inside the varint.
    Truncated,
    /// More than ten bytes, or a value past `u64::MAX`.
    Overflow,
}

/// Reads one varint from `reader`.
pub(crate) fn decode(reader: &mut impl BufRead) -> io::Result<Decoded> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut first = true;
    loop {
        let buf = reader.fill_buf()?;
        let Some(&byte) = buf.first() else {
            return Ok(if first {
                Decoded::End
            } else {
                Decoded::Truncated
            });
        };
        reader.consume(1);
        first = false;
        let bits = u64::from(byte & 0x7f);
        // The tenth byte may only carry the single bit left of a u64.
        if shift == 63 && bits > 1 {
            return Ok(Decoded::Overflow);
        }
        value |= bits << shift;
        if byte & 0x80 == 0 {
            return Ok(Decoded::Value(value));
        }
        shift += 7;
        if shift > 63 {
            return Ok(Decoded::Overflow);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: u64) -> u64 {
        let (buf, n) = encode(value);
        match decode(&mut &buf[..n]).unwrap() {
            Decoded::Value(v) => v,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn round_trips_edges() {
        for v in [
            0,
            1,
            127,
            128,
            300,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            assert_eq!(round_trip(v), v);
        }
        assert_eq!(encode(0).1, 1);
        assert_eq!(encode(127).1, 1);
        assert_eq!(encode(128).1, 2);
        assert_eq!(encode(u64::MAX).1, MAX_LEN);
    }

    #[test]
    fn end_truncation_and_overflow() {
        assert_eq!(decode(&mut &[][..]).unwrap(), Decoded::End);
        assert_eq!(decode(&mut &[0x80][..]).unwrap(), Decoded::Truncated);
        assert_eq!(decode(&mut &[0xff; 11][..]).unwrap(), Decoded::Overflow);
        let mut past_max = [0xff_u8; 10];
        past_max[9] = 0x02;
        assert_eq!(decode(&mut &past_max[..]).unwrap(), Decoded::Overflow);
    }

    #[test]
    fn reads_only_its_own_bytes() {
        let mut input = &[0x05, 0x07][..];
        assert_eq!(decode(&mut input).unwrap(), Decoded::Value(5));
        assert_eq!(input, [0x07]);
    }
}
