//! The longest number token Elasticsearch will read.
//!
//! Elasticsearch's JSON parser refuses a number token past a length limit while it parses the
//! document, before any mapping applies, so the bulk item fails whatever field holds it: text,
//! `ignore_malformed`, unknown or `enabled: false`. Extraction quarantines such a record instead,
//! unless every such number is in an occurrence of a repeated key that the record's reduction
//! drops ([`crate::repeated_keys`]): Elasticsearch never sees those.

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

/// Notes every number longer than [`MAX_NUMBER_CHARS`] during a scan; it never stops it, since
/// whether a number reaches Elasticsearch is known only once the scan has seen which keys
/// repeat. [`LongNumbers::check`] decides after the scan.
///
/// Each such number is at least [`MAX_NUMBER_CHARS`] bytes of the record, so the list is at most
/// a thousandth of the record's length. Only the first one's path is written down (paths cost a
/// string each); when that one is dropped and a later one is kept, `check` scans the record a
/// second time for the kept one's path, only on its way to quarantine.
#[derive(Debug, Default)]
pub(crate) struct LongNumbers {
    /// The offset and length of each too-long number, in record order.
    numbers: Vec<(usize, usize)>,
    /// The first one's path.
    first_path: Option<String>,
}

impl<'a> ScanVisitor<'a> for LongNumbers {
    type Break = Infallible;

    fn number(
        &mut self,
        path: &JsonPath<'a>,
        at: usize,
        token: &'a str,
    ) -> ControlFlow<Infallible> {
        // Number tokens are ASCII, so bytes are characters.
        if token.len() > MAX_NUMBER_CHARS {
            if self.first_path.is_none() {
                self.first_path = Some(path.to_string());
            }
            self.numbers.push((at, token.len()));
        }
        ControlFlow::Continue(())
    }
}

impl LongNumbers {
    /// Fails, naming the first one, if a too-long number of the scanned `record` is not
    /// `dropped` (given its offset).
    pub(crate) fn check(
        self,
        record: &str,
        dropped: impl Fn(usize) -> bool,
    ) -> Result<(), MalformedRecord> {
        let Some(index) = self.numbers.iter().position(|&(at, _)| !dropped(at)) else {
            return Ok(());
        };
        let (at, len) = self.numbers[index];
        let path = match self.first_path {
            Some(path) if index == 0 => path,
            _ => path_at(record, at),
        };
        Err(MalformedRecord::new(
            MalformedKind::Record,
            format!(
                "{path}: a number of {len} characters; Elasticsearch accepts at most {MAX_NUMBER_CHARS}"
            ),
        ))
    }
}

/// The path of the number at offset `at` of `record`.
fn path_at(record: &str, at: usize) -> String {
    struct PathAt(usize);
    impl<'a> ScanVisitor<'a> for PathAt {
        type Break = String;
        fn number(&mut self, path: &JsonPath<'a>, at: usize, _: &'a str) -> ControlFlow<String> {
            if at == self.0 {
                return ControlFlow::Break(path.to_string());
            }
            ControlFlow::Continue(())
        }
    }
    match scan(record, &mut PathAt(at)) {
        ControlFlow::Break(path) => path,
        // Not reached: `at` came from a scan of the same record.
        ControlFlow::Continue(()) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(record: &str, dropped: impl Fn(usize) -> bool) -> Result<(), MalformedRecord> {
        let mut long = LongNumbers::default();
        let ControlFlow::Continue(()) = scan(record, &mut long);
        long.check(record, dropped)
    }

    #[test]
    fn counts_the_token_as_written() {
        let at_limit = format!("-{}.5e+1", "9".repeat(MAX_NUMBER_CHARS - 6));
        assert!(check(&format!(r#"{{"n":{at_limit}}}"#), |_| false).is_ok());
        let over = format!("-{}.5e+1", "9".repeat(MAX_NUMBER_CHARS - 5));
        let err = check(&format!(r#"{{"a":[0,{{"n":{over}}}]}}"#), |_| false).unwrap_err();
        assert_eq!(err.kind(), MalformedKind::Record);
        assert_eq!(
            err.reason(),
            "a[1].n: a number of 1001 characters; Elasticsearch accepts at most 1000"
        );
    }

    #[test]
    fn long_digit_strings_are_not_numbers() {
        let digits = "9".repeat(MAX_NUMBER_CHARS * 2);
        assert!(check(&format!(r#"{{"s":"{digits}"}}"#), |_| false).is_ok());
    }

    #[test]
    fn dropped_numbers_do_not_count_and_the_reason_names_a_kept_one() {
        let long = "1".repeat(MAX_NUMBER_CHARS + 1);
        let longer = "2".repeat(MAX_NUMBER_CHARS + 2);
        let record = format!(r#"{{"a":{long},"b":[{longer}],"c":{long}}}"#);
        let b = record.find('2').unwrap();
        let c = record.rfind(":1").unwrap() + 1;
        assert!(check(&record, |_| true).is_ok());
        // The first is dropped: the path of the next kept one is found again.
        let err = check(&record, |at| at < b).unwrap_err();
        assert_eq!(
            err.reason(),
            "b[0]: a number of 1002 characters; Elasticsearch accepts at most 1000"
        );
        let err = check(&record, |at| at != c).unwrap_err();
        assert_eq!(
            err.reason(),
            "c: a number of 1001 characters; Elasticsearch accepts at most 1000"
        );
    }
}
