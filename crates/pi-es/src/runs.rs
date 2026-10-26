//! Which `<alias>-*` indexes belong to runs older than the publishing one.
//!
//! Run ids begin with their UTC start second: `YYYYMMDD`, then `T` or `-`, then `HHMMSS`.
//! Generated ids are `20261015T120000Z-a1b2c3` and the Makefile's compose ids are
//! `20261015-120000`; index names are lowercased, so the `T` reads `t`. Whole ids cannot be
//! compared as strings across the two forms (`t` sorts after `-`), so only the stamp counts: a
//! run is older when its stamp is an earlier second. Runs started in the same second, and ids
//! without a stamp (a hand-picked `--run-id`), are never older: they may be loading now.

/// The start second of the run that owns `index` (`<alias>-<run_id>`), as `YYYYMMDDHHMMSS`.
#[must_use]
pub fn run_stamp(alias: &str, index: &str) -> Option<String> {
    let id = index.strip_prefix(alias)?.strip_prefix('-')?;
    let bytes = id.as_bytes();
    let digits = |range: std::ops::Range<usize>| bytes[range].iter().all(u8::is_ascii_digit);
    let stamped = bytes.len() >= 15
        && digits(0..8)
        && matches!(bytes[8], b't' | b'T' | b'-')
        && digits(9..15)
        && !bytes.get(15).is_some_and(u8::is_ascii_digit);
    // The checks above passed on ASCII bytes, so both slices fall on character boundaries.
    stamped.then(|| format!("{}{}", &id[..8], &id[9..15]))
}

/// Whether `index` belongs to a run that started before the run owning `than`.
#[must_use]
pub fn is_older_run(alias: &str, index: &str, than: &str) -> bool {
    match (run_stamp(alias, index), run_stamp(alias, than)) {
        (Some(index), Some(than)) => index < than,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_of_both_id_forms() {
        let stamp = |index| run_stamp("persons", index);
        assert_eq!(
            stamp("persons-20261015t120000z-cdef12").as_deref(),
            Some("20261015120000")
        );
        assert_eq!(
            stamp("persons-20261015-120000").as_deref(),
            Some("20261015120000")
        );
        assert_eq!(
            stamp("persons-20261015-120000-2").as_deref(),
            Some("20261015120000")
        );
        for none in [
            "persons",
            "persons-",
            "persons-nightly-1",
            "persons-eu-20261015-120000",
            "persons-20261015x120000",
            "persons-20261015-1200001",
            "persons-2026101-120000",
            "other-20261015-120000",
            "persons20261015-120000",
        ] {
            assert_eq!(stamp(none), None, "{none}");
        }
    }

    #[test]
    fn older_compares_seconds_across_forms() {
        let older = |index, than| is_older_run("persons", index, than);
        let generated = "persons-20261015t120000z-cdef12";
        let compose = "persons-20261015-120000";
        // A string comparison would call the compose id older (`-` < `t`): same second.
        assert!(!older(compose, generated));
        assert!(!older(generated, compose));
        assert!(!older("persons-20261015-120000-2", compose));
        assert!(older("persons-20261015-115959", generated));
        assert!(older("persons-20261014t235959z-000000", compose));
        assert!(!older("persons-20261015-120001", generated));
        assert!(!older("persons-nightly-1", generated));
        assert!(!older("persons-20261014-000000", "persons-nightly-1"));
    }
}
