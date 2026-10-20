//! Java's `Double.parseDouble`, which Elasticsearch 9.5.5 was observed to use when it reads a
//! string into an `integer` field.
//!
//! The grammar: leading and trailing characters up to U+0020 are trimmed; then an optional sign
//! and `NaN`, `Infinity`, a decimal `(digits [. digits?] | . digits) ([eE] [+-]? digits)?` or a
//! hexadecimal `0x hexdigits [. hexdigits] p [+-]? digits` (either with an optional `d`, `D`,
//! `f` or `F` suffix). The value is rounded to the nearest double, as Java does: exactly for
//! decimals (Rust's parser), and for hexadecimal exact outside the subnormal range, which no
//! range check depends on (there `2f64.powi` underflows for powers at or below -1024, and a
//! subnormal result is rounded twice).

/// The double Java reads from `s`, or `None` when Java throws `NumberFormatException`. Exact
/// (rounded once, to nearest even) outside the subnormal range, which no range check depends
/// on. Linear in the length of `s`.
pub(crate) fn parse(s: &str) -> Option<f64> {
    let s = s.trim_matches(|c: char| c <= ' ');
    let (negative, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let magnitude = match body {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        _ => {
            let core = body.strip_suffix(['d', 'D', 'f', 'F']).unwrap_or(body);
            match core.strip_prefix("0x").or_else(|| core.strip_prefix("0X")) {
                Some(hex) => parse_hex(hex)?,
                None if is_decimal(core) => core.parse::<f64>().ok()?,
                None => return None,
            }
        }
    };
    Some(if negative { -magnitude } else { magnitude })
}

/// Whether `s` is `(digits [. digits?] | . digits) ([eE] [+-]? digits)?`, ASCII digits only. Rust
/// parses exactly this grammar to the nearest double, as Java does.
fn is_decimal(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    let int = run(b, &mut i, u8::is_ascii_digit);
    let mut frac = 0;
    if b.get(i) == Some(&b'.') {
        i += 1;
        frac = run(b, &mut i, u8::is_ascii_digit);
    }
    if int + frac == 0 {
        return false;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if run(b, &mut i, u8::is_ascii_digit) == 0 {
            return false;
        }
    }
    i == b.len()
}

/// The value of `hexdigits [. hexdigits] p [+-]? digits` (after `0x`), rounded to the nearest
/// double.
fn parse_hex(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    let mut i = 0;
    let int = run(b, &mut i, u8::is_ascii_hexdigit);
    let mut frac = &b[i..i];
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        run(b, &mut i, u8::is_ascii_hexdigit);
        frac = &b[start..i];
    }
    if int + frac.len() == 0 || !matches!(b.get(i), Some(b'p' | b'P')) {
        return None;
    }
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
    let exp_start = i;
    if run(b, &mut i, u8::is_ascii_digit) == 0 || i != b.len() {
        return None;
    }
    let exponent = b[exp_start..i].iter().fold(0i64, |e, d| {
        e.saturating_mul(10).saturating_add(i64::from(d - b'0'))
    });
    let exponent = if negative_exponent {
        -exponent
    } else {
        exponent
    };

    // The first 16 significant hex digits (64 bits), the binary scale of the last one kept, and
    // whether a dropped digit was non-zero (a sticky bit, so a tie rounds the right way).
    let mut mantissa = 0u64;
    let mut kept = 0;
    let mut scale = 0i64;
    let mut sticky = false;
    let digits = b[..int].iter().chain(frac);
    for (pos, c) in digits.enumerate() {
        let d = u64::from(char::from(*c).to_digit(16).unwrap_or(0));
        let in_frac = pos >= int;
        if mantissa == 0 && d == 0 {
            if in_frac {
                scale -= 4;
            }
        } else if kept < 16 {
            mantissa = mantissa << 4 | d;
            kept += 1;
            if in_frac {
                scale -= 4;
            }
        } else {
            sticky |= d != 0;
            if !in_frac {
                scale += 4;
            }
        }
    }
    if mantissa == 0 {
        return Some(0.0);
    }
    if sticky {
        // Sixteen kept digits hold at least 61 significant bits: the lowest is past rounding.
        mantissa |= 1;
    }
    // Past ±2200 the result is 0 or infinite either way. In between, `powi` of two is exact
    // short of overflow and of underflow (it is 0 at or below -1024, and a subnormal result is
    // rounded a second time): exact outside the subnormal range, which no range check depends
    // on. There the one rounding is `mantissa as f64`'s.
    let power = i32::try_from(exponent.saturating_add(scale).clamp(-2200, 2200)).unwrap_or(0);
    #[allow(
        clippy::cast_precision_loss,
        reason = "the one rounding to 53 bits, to nearest even like Java's"
    )]
    let rounded = mantissa as f64;
    Some(rounded * 2f64.powi(power))
}

/// Advances `*i` past the run of bytes of `b` that `class` accepts; returns its length.
fn run(b: &[u8], i: &mut usize, class: fn(&u8) -> bool) -> usize {
    let start = *i;
    while b.get(*i).is_some_and(class) {
        *i += 1;
    }
    *i - start
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn javas_grammar() {
        for (s, value) in [
            ("5", 5.0),
            (" 5", 5.0),
            ("5 ", 5.0),
            ("\t\n 1999\u{0}", 1999.0),
            ("+5", 5.0),
            ("-5.5", -5.5),
            ("5.", 5.0),
            (".5", 0.5),
            ("5d", 5.0),
            ("5F", 5.0),
            ("1e3", 1000.0),
            ("1E-1", 0.1),
            ("0x1p3", 8.0),
            ("0X.8P1", 1.0),
            ("-0x1.8p1", -3.0),
            ("0x10p-4f", 1.0),
            ("0x0p99999999999999999999", 0.0),
            ("Infinity", f64::INFINITY),
            ("-Infinity", f64::NEG_INFINITY),
            ("1e400", f64::INFINITY),
            ("0x1p99999999999999999999", f64::INFINITY),
            ("1e-400", 0.0),
        ] {
            assert_eq!(parse(s), Some(value), "{s:?}");
        }
        assert!(parse("NaN").is_some_and(f64::is_nan));
        assert!(parse("-NaN").is_some_and(f64::is_nan));
        for s in [
            "", "  ", "+", ".", "e5", "5e", "5e+", "--5", "5dd", "NaNd", "nan", "inf", "infinity",
            "0x", "0x1", "0x1p", "0xp1", "0x1.g1p1", "1_000", "5 5", "\u{0665}",
        ] {
            assert_eq!(parse(s), None, "{s:?}");
        }
    }

    #[test]
    fn hex_rounds_to_nearest_even_with_dropped_digits_sticky() {
        // 1 + 2^-53 is a tie: to even, down.
        assert_eq!(parse("0x1.00000000000008p0"), Some(1.0));
        // Past the tie: up.
        assert_eq!(parse("0x1.000000000000081p0"), Some(1.0 + f64::EPSILON));
        // A tie in the sixteen kept digits, broken by a seventeenth: up.
        assert_eq!(parse("0x1.0000000000000801p0"), Some(1.0 + f64::EPSILON));
        assert_eq!(parse("0x1.0000000000000800p0"), Some(1.0));
        // Integer digits past the sixteenth scale the value.
        assert_eq!(parse("0x10000000000000000p0"), Some(2f64.powi(64)));
        // A dropped seventeenth integer digit sets the sticky bit: the kept 2^60 + 2^7 is a
        // tie on its own (down, to 2^64 once scaled), past the tie with the sticky bit (up).
        assert_eq!(parse("0x10000000000000800p0"), Some(2f64.powi(64)));
        assert_eq!(
            parse("0x10000000000000801p0"),
            Some(2f64.powi(64) + 2f64.powi(12))
        );
        // Sticky but not deciding: far below the tie.
        assert_eq!(parse("0x10000000000000081p0"), Some(2f64.powi(64)));
        // Leading fraction zeros scale the first kept digit.
        assert_eq!(parse("0x0.0001p16"), Some(1.0));
        assert_eq!(parse("0x0.00000000000000000001p80"), Some(1.0));
    }

    #[test]
    fn more_of_javas_grammar() {
        assert_eq!(parse("0x1.p1"), Some(2.0));
        assert_eq!(parse("5.e3"), Some(5000.0));
        assert_eq!(parse("+Infinity"), Some(f64::INFINITY));
        let negative_zero = parse("-0x0p1").unwrap();
        assert_eq!(negative_zero.to_bits(), (-0.0f64).to_bits());
        for s in ["0x.p1", "Infinityd", "+-5", "0x1p1.5", "1e1.5", "5ee3"] {
            assert_eq!(parse(s), None, "{s:?}");
        }
    }

    #[test]
    fn very_long_inputs_parse_without_panic() {
        let digits = 100_000;
        let decimal = format!("0.{}1", "0".repeat(digits));
        assert_eq!(parse(&decimal), Some(0.0));
        let decimal = format!("{}.5", "9".repeat(digits));
        assert_eq!(parse(&decimal), Some(f64::INFINITY));
        let hex = format!("0x0.{}p0", "f".repeat(digits));
        assert_eq!(parse(&hex), Some(1.0));
        let hex = format!("0x{}p-{}", "1".repeat(digits), 4 * digits);
        assert!(parse(&hex).is_some_and(|v| v > 0.0 && v < 1.0));
        assert_eq!(parse(&"5".repeat(digits * 10)), Some(f64::INFINITY));
    }
}
