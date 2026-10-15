//! `expected.json`: the exact counts the acceptance check compares the index against.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The file name of the expected counts, beside `organizations/` and `persons/`.
pub const EXPECTED_FILE: &str = "expected.json";

/// Exact counts for a generated data set, computed while generating.
///
/// Field order and names are the wire format read by the `pi-es` verifier. Maps are
/// `BTreeMap` so the file is byte-stable for a given seed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expected {
    /// Distinct valid person ids: the number of documents in the index.
    pub persons: u64,
    /// Distinct valid organization ids.
    pub organizations: u64,
    /// Malformed lines per feed.
    pub malformed: MalformedCounts,
    /// Persons whose document has `unresolved_organization_ids` (at least one non-null role
    /// organization id that is not in the organization feed).
    pub persons_with_unresolved: u64,
    /// Per title: persons with at least one role carrying exactly this title (the term query
    /// on `roles.title.keyword`). Common and rare titles, at most 20.
    pub role_title_counts: BTreeMap<String, u64>,
    /// Per name: persons whose `organizations[]` holds an organization with exactly this name
    /// after last-wins deduplication of organization records (the term query on
    /// `organizations.name.keyword`). At most 20, including a renamed organization's old name.
    pub organization_name_counts: BTreeMap<String, u64>,
}

/// Malformed line counts per feed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MalformedCounts {
    pub persons: u64,
    pub organizations: u64,
}

impl Expected {
    /// The pretty-printed JSON written to [`EXPECTED_FILE`], with a trailing newline.
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format_has_the_contract_keys_in_order() {
        let mut e = Expected {
            persons: 3,
            organizations: 2,
            malformed: MalformedCounts {
                persons: 1,
                organizations: 0,
            },
            persons_with_unresolved: 1,
            ..Expected::default()
        };
        e.role_title_counts.insert("Chef".into(), 1);
        e.organization_name_counts.insert("Velmor Labs".into(), 2);
        let text = String::from_utf8(e.to_json_bytes().unwrap()).unwrap();
        let keys = [
            "\"persons\"",
            "\"organizations\"",
            "\"malformed\"",
            "\"persons_with_unresolved\"",
            "\"role_title_counts\"",
            "\"organization_name_counts\"",
        ];
        let positions: Vec<usize> = keys.iter().map(|k| text.find(k).unwrap()).collect();
        assert!(positions.windows(2).all(|w| w[0] < w[1]), "{text}");
        let back: Expected = serde_json::from_str(&text).unwrap();
        assert_eq!(back, e);
    }
}
