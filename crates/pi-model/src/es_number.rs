//! How Elasticsearch reads one scalar into a `long` or `integer` field, so extraction can count
//! exactly the values a field with `ignore_malformed` skips (and lists in `_ignored`).
//!
//! The rule, observed against Elasticsearch 9.5.5: a JSON boolean is skipped; the empty string
//! is indexed (as no value); any other string must be a decimal number with no surrounding
//! whitespace, `[+-]? (digits [. digits?] | . digits) ([eE] [+-]? digits)?`, or it is skipped;
//! a number (a JSON number or such a string) is truncated toward zero and skipped when that
//! is outside the type's range. The range check is exact (decimal digits, no floating point),
//! so `9223372036854775807` fits a `long` and `9223372036854775808` does not.

/// The numeric types a lenient field can be mapped as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumericType {
    /// `long`: a signed 64-bit integer.
    Long,
    /// `integer`: a signed 32-bit integer.
    Integer,
}

impl NumericType {
    /// The largest magnitude a positive and a negative value can have.
    fn limits(self) -> (u128, u128) {
        match self {
            Self::Long => (
                u128::from(i64::MAX.unsigned_abs()),
                u128::from(i64::MIN.unsigned_abs()),
            ),
            Self::Integer => (
                u128::from(i32::MAX.unsigned_abs()),
                u128::from(i32::MIN.unsigned_abs()),
            ),
        }
    }
}

/// What Elasticsearch does with one scalar in a numeric field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reading {
    /// Indexed (or, for `""`, taken as no value): not listed in `_ignored`.
    Indexed,
    /// Skipped: a boolean, or a string that is not a decimal number.
    NotANumber,
    /// Skipped: a number whose integer part is outside the type's range.
    OutOfRange,
}

/// How a field of type `ty` reads the raw JSON scalar `text` (not null, not an array or
/// object).
pub(crate) fn read(text: &str, ty: NumericType) -> Reading {
    let decoded;
    let number = if text.starts_with('"') {
        decoded = serde_json::from_str::<String>(text).ok();
        match decoded.as_deref() {
            Some("") => return Reading::Indexed,
            Some(s) => s,
            None => return Reading::NotANumber,
        }
    } else {
        // A JSON number always matches the grammar; `true` and `false` never do.
        text
    };
    match Decimal::parse(number) {
        None => Reading::NotANumber,
        Some(d) if d.truncation_fits(ty) => Reading::Indexed,
        Some(_) => Reading::OutOfRange,
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

    /// Whether the value truncated toward zero is within `ty`'s range.
    fn truncation_fits(&self, ty: NumericType) -> bool {
        let (positive, negative) = ty.limits();
        let limit = if self.negative { negative } else { positive };
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

    #[test]
    fn the_range_is_exact_at_the_edges() {
        use NumericType::{Integer, Long};
        for (text, ty, reading) in [
            ("9223372036854775807", Long, Reading::Indexed),
            ("9223372036854775808", Long, Reading::OutOfRange),
            ("-9223372036854775808", Long, Reading::Indexed),
            ("-9223372036854775809", Long, Reading::OutOfRange),
            ("9223372036854775807.999", Long, Reading::Indexed),
            ("922337203685477580.7e1", Long, Reading::Indexed),
            ("922337203685477580.8e1", Long, Reading::OutOfRange),
            ("2147483647", Integer, Reading::Indexed),
            ("-2147483648", Integer, Reading::Indexed),
            ("-2147483649", Integer, Reading::OutOfRange),
            ("1e18", Long, Reading::Indexed),
            ("1e19", Long, Reading::OutOfRange),
            ("1e400", Long, Reading::OutOfRange),
            ("1e-400", Long, Reading::Indexed),
            ("0e99999999999999999999999", Long, Reading::Indexed),
            ("1e99999999999999999999999", Long, Reading::OutOfRange),
            ("1e-99999999999999999999999", Long, Reading::Indexed),
            ("0000000000000000000000000001", Long, Reading::Indexed),
            ("\"\"", Long, Reading::Indexed),
            ("\"+5\"", Integer, Reading::Indexed),
            ("\"3000000000\"", Integer, Reading::OutOfRange),
            ("\"\\u0035\"", Long, Reading::Indexed),
            ("\" 5\"", Long, Reading::NotANumber),
            ("true", Long, Reading::NotANumber),
        ] {
            assert_eq!(read(text, ty), reading, "{text} as {ty:?}");
        }
    }
}
