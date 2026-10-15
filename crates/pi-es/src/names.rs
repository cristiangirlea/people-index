//! Index and alias name checks. Names end up in request paths, so a name Elasticsearch would
//! reject is refused here, before it can turn into a different path.

use crate::error::{EsError, Result};

/// Accepts what Elasticsearch accepts for an index or alias name: lowercase, at most 255 bytes,
/// not `.` or `..`, not starting with `-`, `_` or `+`, and none of `\ / * ? " < > | , # :` or
/// whitespace.
pub fn validate_index_name(name: &str) -> Result<()> {
    let bad = |why: &str| Err(EsError::Config(format!("index name {name:?} {why}")));
    if name.is_empty() || name.len() > 255 {
        return bad("must be 1 to 255 bytes");
    }
    if name == "." || name == ".." {
        return bad("is reserved");
    }
    if name.starts_with(['-', '_', '+']) {
        return bad("must not start with -, _ or +");
    }
    if name.chars().any(char::is_uppercase) {
        return bad("must be lowercase");
    }
    if name.chars().any(|c| {
        c.is_whitespace()
            || matches!(
                c,
                '\\' | '/' | '*' | '?' | '"' | '<' | '>' | '|' | ',' | '#' | ':'
            )
    }) {
        return bad("holds a character Elasticsearch forbids");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        for good in ["persons", "persons-20261015t120000z-ab12", "a.b_c-d"] {
            assert!(validate_index_name(good).is_ok(), "{good}");
        }
        for bad in [
            "", "Persons", "_x", "-x", "+x", "a/b", "a b", "a,b", "a*", "..", "a#b", "a:b",
        ] {
            assert!(validate_index_name(bad).is_err(), "{bad}");
        }
        assert!(validate_index_name(&"a".repeat(256)).is_err());
    }
}
