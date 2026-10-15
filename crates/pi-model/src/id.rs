//! Identifiers: a non-negative 64-bit integer, written as a JSON number or as a string of
//! decimal digits.
//!
//! Feeds written by different tools disagree on whether a 64-bit id is a number or a string
//! (JavaScript cannot hold every u64 in a number), so both are accepted, and nothing else is:
//! no sign, no fraction, no exponent, no whitespace, no non-ASCII digits, nothing past
//! `u64::MAX`. Ids are compared as integers, so `"007"` and `7` are the same id.

use std::fmt;

use serde::de::{self, Deserialize, Deserializer, Visitor};

/// Parses a string of ASCII decimal digits into a u64; `None` for anything else, including an
/// empty string and a value past `u64::MAX`.
#[must_use]
pub fn parse_decimal_id(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Digits only, so the only possible error left is overflow, which is reported as None.
    text.parse::<u64>().ok()
}

/// An id read from JSON under the rules above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordId(pub u64);

/// An id that may also be JSON `null` (a role's `organization_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OptionalId(pub Option<u64>);

struct IdVisitor;

impl Visitor<'_> for IdVisitor {
    type Value = RecordId;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a non-negative 64-bit integer or a string of decimal digits")
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<RecordId, E> {
        Ok(RecordId(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<RecordId, E> {
        u64::try_from(v)
            .map(RecordId)
            .map_err(|_| E::custom(format_args!("id {v} is negative")))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<RecordId, E> {
        // serde_json hands out an f64 for fractions, exponents and integers past u64::MAX.
        Err(E::custom(format_args!("id {v} is not a 64-bit integer")))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<RecordId, E> {
        parse_decimal_id(v).map(RecordId).ok_or_else(|| {
            E::custom(format_args!(
                "id string {:?} is not a decimal u64",
                truncate(v, 40)
            ))
        })
    }
}

impl<'de> Deserialize<'de> for RecordId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(IdVisitor)
    }
}

struct OptionalIdVisitor;

impl<'de> Visitor<'de> for OptionalIdVisitor {
    type Value = OptionalId;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("null, a non-negative 64-bit integer or a string of decimal digits")
    }

    fn visit_unit<E: de::Error>(self) -> Result<OptionalId, E> {
        Ok(OptionalId(None))
    }

    fn visit_none<E: de::Error>(self) -> Result<OptionalId, E> {
        Ok(OptionalId(None))
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<OptionalId, D::Error> {
        RecordId::deserialize(d).map(|id| OptionalId(Some(id.0)))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<OptionalId, E> {
        IdVisitor.visit_u64(v).map(|id| OptionalId(Some(id.0)))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<OptionalId, E> {
        IdVisitor.visit_i64(v).map(|id| OptionalId(Some(id.0)))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<OptionalId, E> {
        IdVisitor.visit_f64(v).map(|id| OptionalId(Some(id.0)))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<OptionalId, E> {
        IdVisitor.visit_str(v).map(|id| OptionalId(Some(id.0)))
    }
}

impl<'de> Deserialize<'de> for OptionalId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(OptionalIdVisitor)
    }
}

/// The first `max` characters of `text`, so a hostile value cannot bloat an error message.
fn truncate(text: &str, max: usize) -> &str {
    match text.char_indices().nth(max) {
        Some((end, _)) => &text[..end],
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(json: &str) -> Result<u64, serde_json::Error> {
        serde_json::from_str::<RecordId>(json).map(|r| r.0)
    }

    fn opt(json: &str) -> Result<Option<u64>, serde_json::Error> {
        serde_json::from_str::<OptionalId>(json).map(|r| r.0)
    }

    #[test]
    fn numbers_and_digit_strings_are_ids() {
        assert_eq!(id("0").unwrap(), 0);
        assert_eq!(id("18446744073709551615").unwrap(), u64::MAX);
        assert_eq!(id("\"18446744073709551615\"").unwrap(), u64::MAX);
        assert_eq!(id("\"007\"").unwrap(), 7);
        assert_eq!(
            id("\"\\u0031\"").unwrap(),
            1,
            "escaped digits are still digits"
        );
    }

    #[test]
    fn everything_else_is_rejected() {
        for bad in [
            "-1",
            "1.5",
            "1e3",
            "18446744073709551616",
            "\"18446744073709551616\"",
            "\"\"",
            "\"+1\"",
            "\"-1\"",
            "\" 1\"",
            "\"1 \"",
            "\"1.0\"",
            "\"\u{0661}\"",
            "true",
            "null",
            "[1]",
            "{\"id\":1}",
        ] {
            assert!(id(bad).is_err(), "{bad} should not be an id");
        }
    }

    #[test]
    fn optional_ids_accept_null() {
        assert_eq!(opt("null").unwrap(), None);
        assert_eq!(opt("5").unwrap(), Some(5));
        assert_eq!(opt("\"5\"").unwrap(), Some(5));
        assert!(opt("\"x\"").is_err());
        assert!(opt("false").is_err());
        assert!(opt("-3").is_err());
    }

    #[test]
    fn parse_decimal_id_is_strict() {
        assert_eq!(parse_decimal_id("42"), Some(42));
        assert_eq!(parse_decimal_id(""), None);
        assert_eq!(parse_decimal_id("4 2"), None);
        assert_eq!(parse_decimal_id("99999999999999999999"), None);
    }

    #[test]
    fn long_bad_strings_are_truncated_in_errors() {
        let long = format!("\"{}\"", "x".repeat(1000));
        let err = id(&long).unwrap_err().to_string();
        assert!(err.len() < 200, "{err}");
    }
}
