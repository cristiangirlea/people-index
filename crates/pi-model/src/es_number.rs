//! How Elasticsearch reads one scalar into a `long` or `integer` field, so extraction can count
//! the values a field with `ignore_malformed` skips (and lists in `_ignored`).
//!
//! The rules follow what Elasticsearch 9.5.5 was observed to do (pinned in
//! `shape::tests::counts_what_elasticsearch_9_5_5_skips`):
//!
//! - A JSON boolean is skipped; the empty string is indexed (as no value) and a string of
//!   spaces only is skipped, in both types.
//! - `long`, a string: an exact decimal, `[+-]? (digits [. digits?] | . digits) ([eE] [+-]?
//!   digits)?` with no surrounding whitespace, truncated toward zero and checked against the
//!   range of `i64` without floating point. Anything else is skipped, and so is a decimal whose
//!   exponent is outside `i32`, or whose scale (the digits after the point, as written, minus
//!   the exponent) is outside `i32` or below -19, whatever its value (`"0e20"`, `"0.0e21"`,
//!   `"0.5e-2147483647"` are skipped; `"0e19"`, `"0.0e20"`, `"1e-2147483647"` are not): Java's
//!   `BigDecimal` limits, observed.
//! - `long`, a JSON integer (no fraction, no exponent): exact, against `i64`.
//! - `long`, a JSON number with a fraction or an exponent: rounded to a double and skipped when
//!   it is above `i64::MAX as f64` or below `i64::MIN as f64` (Jackson's comparison), so a value
//!   that rounds to 2^63 passes.
//! - `integer`, a string: read by Java's `Double.parseDouble` ([`crate::java_double`]: trimmed,
//!   `NaN`, `Infinity`, `d`/`f` suffixes and hexadecimal included; those forms observed, the
//!   rest of the grammar derived from Java's); `integer`, a JSON number:
//!   rounded to a double. Either is skipped when outside the range of `i32` before truncation
//!   (`2147483647.5` is skipped); `NaN` is indexed (as 0).
//!
//! One known divergence: Elasticsearch accepts a non-ASCII decimal digit in a `long` string
//! (`"٥"`, U+0665); this grammar does not, so such a value is counted although Elasticsearch
//! indexes it.

use crate::java_double;

/// The numeric types a lenient field can be mapped as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumericType {
    /// `long`: a signed 64-bit integer.
    Long,
    /// `integer`: a signed 32-bit integer.
    Integer,
}

/// What Elasticsearch does with one scalar in a numeric field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reading {
    /// Indexed (or, for `""`, taken as no value): not listed in `_ignored`.
    Indexed,
    /// Skipped: a boolean, or a string the type does not read as a number.
    NotANumber,
    /// Skipped: a number outside the type's range.
    OutOfRange,
}

/// 2^63, `i64::MAX as f64` and `-(i64::MIN as f64)`, exact.
const TWO_TO_THE_63: f64 = 9_223_372_036_854_775_808.0;

/// The lowest scale (digits after the point minus the exponent) a `long` string may have.
const MIN_LONG_SCALE: i64 = -19;

/// How a field of type `ty` reads the raw JSON scalar `text` (not null, not an array or
/// object).
pub(crate) fn read(text: &str, ty: NumericType) -> Reading {
    match text.as_bytes().first() {
        Some(b'"') => match serde_json::from_str::<String>(text) {
            Ok(s) if s.is_empty() => Reading::Indexed,
            Ok(s) => match ty {
                NumericType::Long => read_long_string(&s),
                NumericType::Integer => {
                    java_double::parse(&s).map_or(Reading::NotANumber, integer_range)
                }
            },
            Err(_) => Reading::NotANumber,
        },
        Some(b't' | b'f') => Reading::NotANumber,
        _ => match ty {
            NumericType::Long => read_long_number(text),
            NumericType::Integer => text
                .parse::<f64>()
                .map_or(Reading::NotANumber, integer_range),
        },
    }
}

/// A string in a `long` field: an exact decimal within `BigDecimal`'s exponent and scale
/// limits, truncated.
fn read_long_string(s: &str) -> Reading {
    match Decimal::parse(s) {
        Some(d) if !d.scale_fits_long() => Reading::NotANumber,
        Some(d) if d.truncation_fits_long() => Reading::Indexed,
        Some(_) => Reading::OutOfRange,
        None => Reading::NotANumber,
    }
}

/// A JSON number in a `long` field: an integer exactly, any other number as a double.
fn read_long_number(text: &str) -> Reading {
    if !text.contains(['.', 'e', 'E']) {
        return match Decimal::parse(text) {
            Some(d) if d.truncation_fits_long() => Reading::Indexed,
            Some(_) => Reading::OutOfRange,
            None => Reading::NotANumber,
        };
    }
    match text.parse::<f64>() {
        Ok(d) if d > TWO_TO_THE_63 || d < -TWO_TO_THE_63 => Reading::OutOfRange,
        Ok(_) => Reading::Indexed,
        Err(_) => Reading::NotANumber,
    }
}

/// A double in an `integer` field: checked against `i32` before truncation; `NaN` passes.
fn integer_range(d: f64) -> Reading {
    if d > f64::from(i32::MAX) || d < f64::from(i32::MIN) {
        Reading::OutOfRange
    } else {
        Reading::Indexed
    }
}

/// A decimal literal, split into its parts.
#[derive(Debug, PartialEq, Eq)]
struct Decimal<'a> {
    negative: bool,
    int: &'a str,
    frac: &'a str,
    /// Saturated: an exponent past `i64` is as good as infinite here.
    exponent: i64,
}

impl<'a> Decimal<'a> {
    /// `s` as `[+-]? (digits [. digits?] | . digits) ([eE] [+-]? digits)?`, or `None`.
    fn parse(s: &'a str) -> Option<Self> {
        let b = s.as_bytes();
        let mut i = 0;
        let negative = match b.first() {
            Some(b'-') => {
                i = 1;
                true
            }
            Some(b'+') => {
                i = 1;
                false
            }
            _ => false,
        };
        let int = digits(s, &mut i);
        let mut frac = "";
        if b.get(i) == Some(&b'.') {
            i += 1;
            frac = digits(s, &mut i);
        }
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        let mut exponent = 0i64;
        if matches!(b.get(i), Some(b'e' | b'E')) {
            i += 1;
            let negative_exponent = match b.get(i) {
                Some(b'-') => {
                    i += 1;
                    true
                }
                Some(b'+') => {
                    i += 1;
                    false
                }
                _ => false,
            };
            let exp = digits(s, &mut i);
            if exp.is_empty() {
                return None;
            }
            exponent = exp.bytes().fold(0i64, |e, d| {
                e.saturating_mul(10).saturating_add(i64::from(d - b'0'))
            });
            if negative_exponent {
                exponent = -exponent;
            }
        }
        (i == b.len()).then_some(Self {
            negative,
            int,
            frac,
            exponent,
        })
    }

    /// Whether the exponent and the scale (fraction digits as written minus the exponent) are
    /// within `i32`, and the scale is at least [`MIN_LONG_SCALE`].
    fn scale_fits_long(&self) -> bool {
        let digits = i64::try_from(self.frac.len()).unwrap_or(i64::MAX);
        let scale = digits.saturating_sub(self.exponent);
        i32::try_from(self.exponent).is_ok()
            && i32::try_from(scale).is_ok()
            && scale >= MIN_LONG_SCALE
    }

    /// Whether the value truncated toward zero is within the range of `i64`.
    fn truncation_fits_long(&self) -> bool {
        let limit = if self.negative {
            u128::from(i64::MIN.unsigned_abs())
        } else {
            u128::from(i64::MAX.unsigned_abs())
        };
        let len = self.int.len() + self.frac.len();
        // Where the decimal point falls in `int` followed by `frac`.
        let point = i64::try_from(self.int.len())
            .unwrap_or(i64::MAX)
            .saturating_add(self.exponent);
        let take = usize::try_from(point.max(0)).unwrap_or(usize::MAX).min(len);
        let mut value = 0u128;
        for d in self.int.bytes().chain(self.frac.bytes()).take(take) {
            // `value` stays at most `limit` (below 2^64) before each step: no overflow.
            value = value * 10 + u128::from(d - b'0');
            if value > limit {
                return false;
            }
        }
        if value == 0 {
            return true;
        }
        // Zeros the exponent appends past the last digit; each one at least multiplies a
        // non-zero value by ten, so the loop ends within twenty steps.
        let zeros = point.saturating_sub(i64::try_from(len).unwrap_or(i64::MAX));
        for _ in 0..zeros.max(0) {
            value *= 10;
            if value > limit {
                return false;
            }
        }
        true
    }
}

/// The run of ASCII digits of `s` from `*i`, advancing `*i` past it.
fn digits<'a>(s: &'a str, i: &mut usize) -> &'a str {
    let start = *i;
    while s.as_bytes().get(*i).is_some_and(u8::is_ascii_digit) {
        *i += 1;
    }
    &s[start..*i]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_grammar() {
        for good in [
            "5", "-5", "+5", "5.", ".5", "5.7", "007", "1e3", "1E+3", "1e-3", "-.5e2",
        ] {
            assert!(Decimal::parse(good).is_some(), "{good}");
        }
        for bad in [
            "", ".", "+", "-", " 5", "5 ", "7 8", "n/a", "5e", "e5", "5.5.5", "0x10", "1_000",
            "--5", "5e+", "Infinity", "NaN",
        ] {
            assert!(Decimal::parse(bad).is_none(), "{bad}");
        }
    }

    /// The rules at the edges of each type, derived from them (not observed). The observed
    /// rows are pinned in `shape::tests::counts_what_elasticsearch_9_5_5_skips`.
    #[test]
    fn the_rules_at_the_edges() {
        use NumericType::{Integer, Long};
        use Reading::{Indexed, NotANumber, OutOfRange};
        for (text, ty, reading) in [
            // `long`, JSON integers: exact.
            ("-9223372036854775808", Long, Indexed),
            ("-9223372036854775809", Long, OutOfRange),
            ("0000000000000000000000000001", Long, Indexed),
            // `long`, other JSON numbers: as doubles, against 2^63.
            ("9223372036854775807.999", Long, Indexed),
            ("9223372036854776832.0", Long, Indexed), // a tie: to even, 2^63
            ("9223372036854776833.0", Long, OutOfRange),
            ("-9223372036854777856.0", Long, OutOfRange),
            ("1e18", Long, Indexed),
            ("1e400", Long, OutOfRange),
            ("1e-400", Long, Indexed),
            ("0e99999999999999999999999", Long, Indexed),
            ("1e99999999999999999999999", Long, OutOfRange),
            ("1e-99999999999999999999999", Long, Indexed),
            // `long`, strings: exact decimals, the exponent within `i32`.
            ("\"\\u0035\"", Long, Indexed),
            ("\"922337203685477580.7e1\"", Long, Indexed),
            ("\"922337203685477580.8e1\"", Long, OutOfRange),
            ("\"9223372036854775808.0\"", Long, OutOfRange),
            ("\"0e2147483648\"", Long, NotANumber),
            ("\"1e-2147483648\"", Long, NotANumber),
            ("\"1.5e-2147483646\"", Long, Indexed),
            ("\"1.55e-2147483646\"", Long, NotANumber),
            ("\"1e19\"", Long, OutOfRange),
            ("\"1e20\"", Long, NotANumber),
            ("\"NaN\"", Long, NotANumber),
            // `integer`: doubles, against `i32` before truncation.
            ("-2147483648", Integer, Indexed),
            ("-2147483649", Integer, OutOfRange),
            ("2147483647.0000000001", Integer, Indexed),
            ("-2147483648.0", Integer, Indexed),
            ("1e400", Integer, OutOfRange),
            ("\"\"", Integer, Indexed),
            ("\"+5\"", Integer, Indexed),
            ("\"-NaN\"", Integer, Indexed),
            ("\"0x1p30d\"", Integer, Indexed),
            ("\"0e99999999999\"", Integer, Indexed),
            ("\"yes\"", Integer, NotANumber),
            ("false", Integer, NotANumber),
        ] {
            assert_eq!(read(text, ty), reading, "{text} as {ty:?}");
        }
    }

    #[test]
    fn very_long_strings_read_without_panic() {
        let digits = 100_000;
        let long = format!(r#""0.{}1""#, "0".repeat(digits));
        assert_eq!(read(&long, NumericType::Long), Reading::Indexed);
        let long = format!(r#""9.{}""#, "9".repeat(digits));
        assert_eq!(read(&long, NumericType::Long), Reading::Indexed);
        let long = format!(r#""{}""#, "9".repeat(digits));
        assert_eq!(read(&long, NumericType::Long), Reading::OutOfRange);
        let long = format!(r#""{}e-{}""#, "9".repeat(digits), digits);
        assert_eq!(read(&long, NumericType::Long), Reading::Indexed);
        let number = format!("0.{}1", "0".repeat(digits));
        assert_eq!(read(&number, NumericType::Long), Reading::Indexed);
        assert_eq!(read(&number, NumericType::Integer), Reading::Indexed);
        let hex = format!(r#""0x0.{}p0""#, "f".repeat(digits));
        assert_eq!(read(&hex, NumericType::Integer), Reading::Indexed);
    }
}
