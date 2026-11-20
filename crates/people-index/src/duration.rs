//! Duration settings use the grammar and numeric limits of humantime 2.4.0.

use std::time::Duration;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

pub(crate) fn parse_duration(input: &str) -> Result<Duration, String> {
    parse(input).ok_or_else(|| {
        format!("invalid duration {input:?}: expected a number with a time unit and supported range and precision")
    })
}

fn parse(input: &str) -> Option<Duration> {
    if input == "0" {
        return Some(Duration::ZERO);
    }

    let mut remaining = input.trim_start();
    let mut total = Duration::ZERO;
    loop {
        let (whole, _) = take_digits(&mut remaining)?;
        let fraction = if let Some(tail) = remaining.strip_prefix('.') {
            remaining = tail;
            let (numerator, digits) = take_digits(&mut remaining)?;
            Some((numerator, 10_u64.checked_pow(digits.try_into().ok()?)?))
        } else {
            None
        };

        let unit_end = remaining
            .find(|ch: char| !ch.is_ascii_alphabetic() && ch != 'µ')
            .unwrap_or(remaining.len());
        let (unit, tail) = remaining.split_at(unit_end);
        let (seconds, nanos): (u64, u64) = match unit {
            "nanos" | "nsec" | "ns" => (0, 1),
            "usec" | "us" | "µs" => (0, 1_000),
            "millis" | "msec" | "ms" => (0, 1_000_000),
            "seconds" | "second" | "secs" | "sec" | "s" => (1, 0),
            "minutes" | "minute" | "min" | "mins" | "m" => (60, 0),
            "hours" | "hour" | "hr" | "hrs" | "h" => (3_600, 0),
            "days" | "day" | "d" => (86_400, 0),
            "weeks" | "week" | "wk" | "wks" | "w" => (604_800, 0),
            "months" | "month" | "M" => (2_630_016, 0),
            "years" | "year" | "yr" | "yrs" | "y" => (31_557_600, 0),
            _ => return None,
        };

        total = add_parts(
            total,
            whole.checked_mul(seconds)?,
            whole.checked_mul(nanos)?,
        )?;
        if let Some((numerator, denominator)) = fraction {
            // Only microseconds through minutes allow subunit values in nanoseconds.
            // Longer units require whole seconds, and nanoseconds reject all fractions.
            let (scale, in_nanos) = match (seconds, nanos) {
                (0, 1) => return None,
                (0, _) => (nanos, true),
                (1 | 60, _) => (seconds * NANOS_PER_SECOND, true),
                _ => (seconds, false),
            };
            let scaled = numerator.checked_mul(scale)?;
            if scaled % denominator != 0 {
                return None;
            }
            let value = scaled / denominator;
            total = if in_nanos {
                add_parts(total, 0, value)?
            } else {
                add_parts(total, value, 0)?
            };
        }

        remaining = tail.trim_start();
        if remaining.is_empty() {
            return Some(total);
        }
    }
}

fn take_digits(remaining: &mut &str) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    let mut digits = 0;
    let mut end = 0;
    for ch in remaining.chars() {
        if let Some(digit) = ch.to_digit(10) {
            value = value.checked_mul(10)?.checked_add(u64::from(digit))?;
            digits += 1;
        } else if !ch.is_whitespace() {
            break;
        }
        end += ch.len_utf8();
    }
    *remaining = &remaining[end..];
    (digits > 0).then_some((value, digits))
}

fn add_parts(total: Duration, seconds: u64, nanos: u64) -> Option<Duration> {
    // Keep the reference parser's u64 nanosecond limit before normalizing the sum.
    let nanos = nanos.checked_add(u64::from(total.subsec_nanos()))?;
    let seconds = total.as_secs().checked_add(seconds)?;
    Duration::from_secs(seconds).checked_add(Duration::from_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Captured from humantime 2.4.0 as (seconds, nanoseconds), or None on error.
    const CASES: &[(&str, Option<(u64, u32)>)] = &[
        ("1nanos", Some((0, 1))),
        ("1nsec", Some((0, 1))),
        ("1ns", Some((0, 1))),
        ("1usec", Some((0, 1_000))),
        ("1us", Some((0, 1_000))),
        ("1µs", Some((0, 1_000))),
        ("1millis", Some((0, 1_000_000))),
        ("1msec", Some((0, 1_000_000))),
        ("1ms", Some((0, 1_000_000))),
        ("1seconds", Some((1, 0))),
        ("1second", Some((1, 0))),
        ("1secs", Some((1, 0))),
        ("1sec", Some((1, 0))),
        ("1s", Some((1, 0))),
        ("1minutes", Some((60, 0))),
        ("1minute", Some((60, 0))),
        ("1min", Some((60, 0))),
        ("1mins", Some((60, 0))),
        ("1m", Some((60, 0))),
        ("1hours", Some((3_600, 0))),
        ("1hour", Some((3_600, 0))),
        ("1hr", Some((3_600, 0))),
        ("1hrs", Some((3_600, 0))),
        ("1h", Some((3_600, 0))),
        ("1days", Some((86_400, 0))),
        ("1day", Some((86_400, 0))),
        ("1d", Some((86_400, 0))),
        ("1weeks", Some((604_800, 0))),
        ("1week", Some((604_800, 0))),
        ("1wk", Some((604_800, 0))),
        ("1wks", Some((604_800, 0))),
        ("1w", Some((604_800, 0))),
        ("1months", Some((2_630_016, 0))),
        ("1month", Some((2_630_016, 0))),
        ("1M", Some((2_630_016, 0))),
        ("1years", Some((31_557_600, 0))),
        ("1year", Some((31_557_600, 0))),
        ("1yr", Some((31_557_600, 0))),
        ("1yrs", Some((31_557_600, 0))),
        ("1y", Some((31_557_600, 0))),
        ("1h 30m", Some((5_400, 0))),
        ("2h30m", Some((9_000, 0))),
        ("1µs2ms", Some((0, 2_001_000))),
        (" \t1 \u{a0}h\n 3 0m\r ", Some((5_400, 0))),
        ("0", Some((0, 0))),
        ("0s", Some((0, 0))),
        (" 0 ", None),
        ("", None),
        ("12", None),
        ("1s 0", None),
        ("1fortnight", None),
        ("1S", None),
        ("1μs", None),
        ("١s", None),
        ("-1s", None),
        ("+1s", None),
        ("1h,30m", None),
        ("1m sec", None),
        (".5s", None),
        ("1.s", None),
        ("1.5s", Some((1, 500_000_000))),
        ("1 . 0 5 s", Some((1, 50_000_000))),
        ("0.001us", Some((0, 1))),
        ("0.000001ms", Some((0, 1))),
        ("0.000000001m", Some((0, 60))),
        ("0.5m", Some((30, 0))),
        ("0.5h", Some((1_800, 0))),
        ("0.0001h", None),
        ("0.0ns", None),
        ("0.0000000001s", None),
        ("0.00000000000000000000s", None),
        ("0.99999999999m", None),
        (
            "18446744073709551615s999999999ns",
            Some((18_446_744_073_709_551_615, 999_999_999)),
        ),
        ("18446744073709551616s", None),
        ("18446744073709551615s1s", None),
        (
            "18446744073709551615ns",
            Some((18_446_744_073, 709_551_615)),
        ),
        ("1ns18446744073709551615ns", None),
        ("18446744073709552us", None),
        ("500ms500ms", Some((1, 0))),
    ];

    #[test]
    fn matches_humantime_2_4_0() {
        for &(input, expected) in CASES {
            let parsed = parse_duration(input);
            assert_eq!(
                parsed
                    .as_ref()
                    .ok()
                    .map(|d| (d.as_secs(), d.subsec_nanos())),
                expected,
                "{input:?}",
            );
            if let Err(error) = parsed {
                assert!(error.contains(&format!("{input:?}")), "{error}");
            }
        }
    }

    #[test]
    fn rejects_overflow_when_nanoseconds_carry_one_second() {
        // The reference panics here when Duration normalizes exactly one second.
        let input = "18446744073709551615s500ms500ms";
        assert!(parse_duration(input).is_err());
    }
}
