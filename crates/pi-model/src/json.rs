//! The two JSON writing primitives the document builder needs, kept infallible.
//!
//! Writing into a `Vec<u8>` cannot fail, but `serde_json::to_writer` still returns a `Result`;
//! these small writers make that impossibility visible in the types instead of hiding an
//! `unwrap`.

/// Appends `text` as a JSON string literal, quotes included.
pub(crate) fn write_string(out: &mut Vec<u8>, text: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    let bytes = text.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escape: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x08 => b"\\b",
            0x0c => b"\\f",
            0x00..=0x1f => b"",
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        if escape.is_empty() {
            out.extend_from_slice(b"\\u00");
            out.push(HEX[usize::from(b >> 4)]);
            out.push(HEX[usize::from(b & 0x0f)]);
        } else {
            out.extend_from_slice(escape);
        }
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

/// Appends an unsigned integer in decimal.
pub(crate) fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(value.to_string().as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(text: &str) -> String {
        let mut out = Vec::new();
        write_string(&mut out, text);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn escapes_round_trip_through_serde_json() {
        for text in [
            "",
            "plain",
            "quote \" backslash \\ slash /",
            "\n\r\t\u{8}\u{c}\u{0}\u{1f}\u{7f}",
            "unicode é 漢 🦀",
        ] {
            let written = string(text);
            let back: String = serde_json::from_str(&written).unwrap();
            assert_eq!(back, text, "{written}");
        }
        assert_eq!(string("\u{1}"), "\"\\u0001\"");
    }

    #[test]
    fn writes_integers() {
        let mut out = Vec::new();
        write_u64(&mut out, u64::MAX);
        assert_eq!(out, u64::MAX.to_string().as_bytes());
    }
}
