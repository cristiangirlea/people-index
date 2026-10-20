//! The longest number token Elasticsearch will read.
//!
//! Elasticsearch's JSON parser refuses a number token past a length limit while it parses the
//! document, before any mapping applies, so the bulk item fails whatever field holds it: text,
//! `ignore_malformed`, unknown or `enabled: false`. Extraction quarantines such a record instead.

use std::convert::Infallible;
use std::ops::ControlFlow;

use crate::error::{MalformedKind, MalformedRecord};
use crate::scan::{JsonPath, ScanVisitor, scan};

/// The most characters a JSON number token may have, as written (sign, digits, `.`,
/// exponent). Observed against a real Elasticsearch 9.5.5: a number of 1001 digits fails the
/// bulk item in any field, even `text` and even with `ignore_malformed`; 999 digits are
/// indexed. Counting the sign, point and exponent too is deliberately on the strict side: the
/// parser may count only digits, so such a record may be quarantined where it would index.
pub(crate) const MAX_NUMBER_CHARS: usize = 1000;

/// Stops at the first number longer than [`MAX_NUMBER_CHARS`].
struct TooLong;

impl<'a> ScanVisitor<'a> for TooLong {
    type Break = MalformedRecord;

    fn number(&mut self, path: &JsonPath<'a>, token: &'a str) -> ControlFlow<MalformedRecord> {
        // Number tokens are ASCII, so bytes are characters.
        if token.len() <= MAX_NUMBER_CHARS {
            return ControlFlow::Continue(());
        }
        ControlFlow::Break(MalformedRecord::new(
            MalformedKind::Record,
            format!(
                "{path}: a number of {} characters; Elasticsearch accepts at most {MAX_NUMBER_CHARS}",
                token.len()
            ),
        ))
    }
}

/// Checks every number in `record` (valid JSON) against [`MAX_NUMBER_CHARS`], in one scan that
/// `watcher` sees too (`()` for none).
pub(crate) fn check_number_lengths<'a>(
    record: &'a str,
    watcher: impl ScanVisitor<'a, Break = Infallible>,
) -> Result<(), MalformedRecord> {
    match scan(record, &mut (TooLong, watcher)) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_the_token_as_written() {
        let at_limit = format!("-{}.5e+1", "9".repeat(MAX_NUMBER_CHARS - 6));
        assert!(check_number_lengths(&format!(r#"{{"n":{at_limit}}}"#), ()).is_ok());
        let over = format!("-{}.5e+1", "9".repeat(MAX_NUMBER_CHARS - 5));
        let err = check_number_lengths(&format!(r#"{{"a":[0,{{"n":{over}}}]}}"#), ()).unwrap_err();
        assert_eq!(err.kind(), MalformedKind::Record);
        assert_eq!(
            err.reason(),
            "a[1].n: a number of 1001 characters; Elasticsearch accepts at most 1000"
        );
    }

    #[test]
    fn long_digit_strings_are_not_numbers() {
        let digits = "9".repeat(MAX_NUMBER_CHARS * 2);
        assert!(check_number_lengths(&format!(r#"{{"s":"{digits}"}}"#), ()).is_ok());
    }
}
