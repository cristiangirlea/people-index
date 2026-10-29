//! Prediction for the built-in Elasticsearch date format.

/// A mapping format whose rejection rules are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateFormat {
    /// `strict_date_optional_time||strict_year_month||strict_year`.
    Partial,
}

pub(crate) fn certainly_ignored(raw: &str, _format: DateFormat) -> bool {
    if raw == "null" || raw == "[]" {
        return false;
    }
    if raw.starts_with('"') {
        let Some(inner) = raw.strip_prefix('"').and_then(|s| s.strip_suffix('"')) else {
            return false;
        };
        if !inner.contains('\\') {
            return !valid(inner);
        }
        let mut buf = [0_u8; 64];
        let Some(decoded) = decode_ascii(inner, &mut buf) else {
            return false;
        };
        return !valid(decoded);
    }
    if raw == "true" || raw == "false" {
        return true;
    }
    !valid(raw) || raw.contains(['.', 'e', 'E'])
}

// JSON strings have already been validated by the record parser. Unknown or oversized
// decoded strings are left uncounted rather than guessing about Elasticsearch's grammar.
fn decode_ascii<'a>(s: &str, buf: &'a mut [u8; 64]) -> Option<&'a str> {
    let mut input = s.bytes();
    let mut len = 0;
    while let Some(mut c) = input.next() {
        if c == b'\\' {
            c = match input.next()? {
                b'"' => b'"',
                b'\\' => b'\\',
                b'/' => b'/',
                b'b' => 8,
                b'f' => 12,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'u' => {
                    let mut value = 0_u32;
                    for _ in 0..4 {
                        value = value * 16 + char::from(input.next()?).to_digit(16)?;
                    }
                    u8::try_from(value).ok().filter(u8::is_ascii)?
                }
                _ => return None,
            };
        }
        if !c.is_ascii() || len == buf.len() {
            return None;
        }
        buf[len] = c;
        len += 1;
    }
    std::str::from_utf8(&buf[..len]).ok()
}

#[allow(clippy::too_many_lines)]
fn valid(s: &str) -> bool {
    fn digits(b: &[u8], i: &mut usize, n: usize) -> Option<u32> {
        let part = b.get(*i..i.checked_add(n)?)?;
        if !part.iter().all(u8::is_ascii_digit) {
            return None;
        }
        *i += n;
        Some(part.iter().fold(0, |v, c| v * 10 + u32::from(c - b'0')))
    }
    fn take(b: &[u8], i: &mut usize, c: u8) -> bool {
        if b.get(*i) == Some(&c) {
            *i += 1;
            true
        } else {
            false
        }
    }
    let b = s.as_bytes();
    let mut i = usize::from(b.first() == Some(&b'-'));
    let Some(year) = digits(b, &mut i, 4) else {
        return false;
    };
    if i == b.len() {
        return true;
    }
    if !take(b, &mut i, b'-') {
        return false;
    }
    let Some(month @ 1..=12) = digits(b, &mut i, 2) else {
        return false;
    };
    if i == b.len() {
        return true;
    }
    if !take(b, &mut i, b'-') {
        return false;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    let Some(day) = digits(b, &mut i, 2) else {
        return false;
    };
    if day == 0 || day > max {
        return false;
    }
    if i == b.len() {
        return true;
    }
    if !take(b, &mut i, b'T') {
        return false;
    }
    let has_time = b.get(i).is_some_and(u8::is_ascii_digit);
    if has_time {
        if digits(b, &mut i, 2).is_none_or(|v| v > 23) {
            return false;
        }
        if take(b, &mut i, b':') {
            if digits(b, &mut i, 2).is_none_or(|v| v > 59) {
                return false;
            }
            if take(b, &mut i, b':') {
                if digits(b, &mut i, 2).is_none_or(|v| v > 59) {
                    return false;
                }
                if take(b, &mut i, b'.') || take(b, &mut i, b',') {
                    let start = i;
                    while b.get(i).is_some_and(u8::is_ascii_digit) {
                        i += 1;
                    }
                    if !(1..=9).contains(&(i - start)) {
                        return false;
                    }
                }
            }
        }
    }
    if i == b.len() {
        return true;
    }
    if !has_time {
        return false;
    }
    if b.get(i) == Some(&b'Z') && (i + 1 == b.len() || b.get(i + 1) == Some(&b'[')) {
        return i + 1 == b.len();
    }
    // Zone IDs are a large, version-dependent set. Leave letter-led names
    // uncounted when they could be valid instead of guessing the registry.
    if b.get(i).is_some_and(u8::is_ascii_alphabetic) && b.len() - i >= 2 {
        return true;
    }
    if !(take(b, &mut i, b'+') || take(b, &mut i, b'-')) {
        return false;
    }
    let Some(hour) = digits(b, &mut i, 2) else {
        return false;
    };
    let mut minute = 0;
    let mut second = 0;
    if take(b, &mut i, b':') {
        let Some(v) = digits(b, &mut i, 2) else {
            return false;
        };
        minute = v;
        if take(b, &mut i, b':') {
            let Some(v) = digits(b, &mut i, 2) else {
                return false;
            };
            second = v;
        }
    } else if i < b.len() {
        let Some(v) = digits(b, &mut i, 2) else {
            return false;
        };
        minute = v;
    }
    i == b.len()
        && minute <= 59
        && second <= 59
        && (hour < 18 || (hour == 18 && minute == 0 && second == 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escaped_buffer_overflow_is_uncounted() {
        let raw = format!("\"{}\\u0030\"", "0".repeat(64));
        assert!(!certainly_ignored(&raw, DateFormat::Partial));
    }

    #[test]
    #[allow(clippy::too_many_lines, clippy::needless_raw_string_hashes)]
    fn elasticsearch_955_probe() {
        for (raw, ignored) in [
            (r###""2020""###, false),
            (r###""2020-05""###, false),
            (r###""2020-02-30""###, true),
            (r###""2021-02-29""###, true),
            (r###""2024-02-29""###, false),
            (r###""2020-02-29""###, false),
            (r###""1900-02-29""###, true),
            (r###""2000-02-29""###, false),
            (r###""0000-01-01""###, false),
            (r###""0000-02-29""###, false),
            (r###""-0001-01-01""###, false),
            (r###""-0001-02-29""###, true),
            (r###""-0004-02-29""###, false),
            (r###""-0000-01-01""###, false),
            (r###""0001-02-29""###, true),
            (r###""-2020""###, false),
            (r###""-20200""###, true),
            (r###""-202000""###, true),
            (r###""20200""###, true),
            (r###""00000""###, true),
            (r###""0""###, true),
            (r###""000""###, true),
            (r###""99999""###, true),
            (r###""+2020""###, true),
            (r###""+20200""###, true),
            (r###""+10000-01-01""###, true),
            (r###""10000-01-01""###, true),
            (r###""2020-1-1""###, true),
            (r###""2020-01-01""###, false),
            (r###""2020-01-01T12:34:56Z""###, false),
            (r###""2020-01-01T12:34:56+02:00""###, false),
            (r###""2020-01-01T12:34:56-05:30""###, false),
            (r###""2020-01-01T12:34:56.1Z""###, false),
            (r###""2020-01-01T12:34:56.123456789Z""###, false),
            (r###""2020-01-01T12:34:56.1234567891Z""###, true),
            (r###""2020-01-01T12:34:56,123Z""###, false),
            (r###""2020-01-01t12:00:00z""###, true),
            (r###""2020-01-01T12:00:00z""###, true),
            (r###""2020-01-01t12:00:00Z""###, true),
            (r###""2020-01-01T24:00:00Z""###, true),
            (r###""2020-01-01T24:00""###, true),
            (r###""2020-01-01T23:59:60Z""###, true),
            (r###""2020-01-01T12""###, false),
            (r###""2020-01-01T12Z""###, false),
            (r###""2020-01-01T12:00""###, false),
            (r###""2020-01-01T12:00:00""###, false),
            (r###""2020-01-01T12:00:00+0200""###, false),
            (r###""2020-01-01T12:00:00+02""###, false),
            (r###""2020-01-01T12:00:00+2""###, true),
            (r###""2020-01-01T12:00:00+02:00:00""###, false),
            (r###""2020-01-01T12:00:00+18:00""###, false),
            (r###""2020-01-01T12:00:00+19:00""###, true),
            (r###""2020-01-01T12:00:00Z[UTC]""###, true),
            (r###""2020-01-01T""###, false),
            (r###""2020-01-01Tfoo""###, true),
            (r###""2020-01-01TZ""###, true),
            (r###""2020-01-01T+02:00""###, true),
            (r###""2020-01-01T12:00:00UTC""###, false),
            (r###""2020-01-01T12:00:00GMT+02:00""###, false),
            (r###""2020-01-01T12:00:00Europe/Paris""###, false),
            (r###""2020-01-01T12:00:00Nowhere/City""###, true),
            (r###""2020-01-01T12+02""###, false),
            (r###""2020-01-01T12:00UTC""###, false),
            (r###""2020-01-01T12:00:00 UTC""###, true),
            (r###""2020-01-01 12:00:00""###, true),
            (r###""2020-02-29T25:00:00Z""###, true),
            (r###""2019-02-29T12:00:00Z""###, true),
            (r###""2020-05-""###, true),
            (r###""2020-""###, true),
            (r###""2020-W01""###, true),
            (r###""2020-001""###, true),
            (r###""20200101""###, true),
            (r###"" 2020""###, true),
            (r###""2020 ""###, true),
            (r###"" ""###, true),
            (r###""""###, true),
            (r###""n/a""###, true),
            (r###""N/A""###, true),
            (r###""null""###, true),
            (r###""1609459200000""###, true),
            (r###""-1609459200000""###, true),
            (r###""1609459200000.5""###, true),
            (r###""1e3""###, true),
            (r###""9223372036854775807""###, true),
            (r###""9223372036854775808""###, true),
            (r###""99999999999999999999""###, true),
            (r###""2020-13""###, true),
            (r###""2020-00""###, true),
            (r###""2020-12""###, false),
            (r###""-2020-05""###, false),
            (r###""+2020-05""###, true),
            (r###""1-01-01""###, true),
            (r###""12020-01-01""###, true),
            (r###""2020-01-32""###, true),
            (r###""2020-04-31""###, true),
            (r###""2020-1""###, true),
            (r###""2020-01-1""###, true),
            (r###""2020-01-01T1:00:00Z""###, true),
            (r###""2020-01-01T12:0:00Z""###, true),
            (r###""2020-01-01T12:00:00.Z""###, true),
            (r###""2020-01-01T12:00:00.""###, true),
            (r###""-""###, true),
            (r###""--""###, true),
            (r###""\u0662\u0660\u0662\u0660""###, true),
            ("\"\u{0662}\u{0660}\u{0662}\u{0660}\"", true),
            (r###""\u0032020""###, false),
            (r###""\t2020""###, true),
            (r###"2020"###, false),
            (r###"0"###, true),
            (r###"-1"###, true),
            (r###"1609459200000"###, true),
            (r###"-2020"###, false),
            (r###"-20200"###, true),
            (r###"20200"###, true),
            (r###"99999999999999999999"###, true),
            (r###"2020.0"###, true),
            (r###"1e3"###, true),
            (r###"1.5e12"###, true),
            (r###"-0"###, true),
            (r###"0.5"###, true),
            (r###"true"###, true),
            (r###"false"###, true),
            (r###"null"###, false),
            (r###"[]"###, false),
        ] {
            // These observed rejections are deliberately left uncounted.
            let gap = raw == r###""2020-01-01T12:00:00Nowhere/City""###
                || raw == r###""\u0662\u0660\u0662\u0660""###;
            assert_eq!(
                certainly_ignored(raw, DateFormat::Partial),
                ignored && !gap,
                "{raw}"
            );
        }
    }
}
