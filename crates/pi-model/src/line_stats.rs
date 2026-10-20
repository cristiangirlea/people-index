//! What extraction noticed in a line it kept, besides the row.

/// A value of a lenient field ([`crate::JsonKind::LenientLong`],
/// [`crate::JsonKind::LenientInteger`], [`crate::JsonKind::LenientBoolean`]) that Elasticsearch
/// skips (`ignore_malformed`): the malformed value is not indexed and the field is listed in
/// the document's `_ignored`; the record stays searchable by its other fields, and the value
/// stays in `_source`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoredValue {
    /// The field, named as a quarantine reason names one: `followers`, `connections[1]`.
    pub field: String,
    /// The value's JSON kind in words (never the value itself).
    pub found: &'static str,
}

/// What extraction noticed in a line it kept.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LineStats {
    /// Values of lenient fields that Elasticsearch skips (lists in `_ignored`), by the rules
    /// observed on Elasticsearch 9.5.5, every element of an array counted on its own.
    pub ignored_by_index: u64,
    /// The first of them, for a log line.
    pub first_ignored: Option<IgnoredValue>,
}

impl LineStats {
    /// Counts one value the index skips; `field` names it, built only for the first.
    pub(crate) fn ignore(&mut self, field: impl FnOnce() -> String, found: &'static str) {
        self.ignored_by_index += 1;
        if self.first_ignored.is_none() {
            self.first_ignored = Some(IgnoredValue {
                field: field(),
                found,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_every_value_and_keeps_the_first() {
        let mut stats = LineStats::default();
        stats.ignore(|| "followers".to_owned(), "a boolean");
        stats.ignore(|| unreachable!("only the first is named"), "a number");
        assert_eq!(stats.ignored_by_index, 2);
        assert_eq!(
            stats.first_ignored,
            Some(IgnoredValue {
                field: "followers".to_owned(),
                found: "a boolean",
            })
        );
    }
}
