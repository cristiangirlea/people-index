//! `expected.json`: the exact counts `pi-datagen` knows about the feeds it generated, and that
//! the published index must reproduce. The field set is a contract with `pi-datagen`; unknown
//! fields are refused so the two sides cannot drift apart unnoticed.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{EsError, Result};

/// Malformed lines per feed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MalformedCounts {
    pub persons: u64,
    pub organizations: u64,
}

/// The acceptance counts for one generated data set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expected {
    /// Distinct valid person ids: the documents in the index.
    pub persons: u64,
    /// Distinct valid organization ids.
    pub organizations: u64,
    pub malformed: MalformedCounts,
    /// Persons whose document has `unresolved_organization_ids`.
    pub persons_with_unresolved: u64,
    /// Title to the number of persons with at least one role of exactly that title (after
    /// last-wins person dedup). Up to 20 titles, rare and common.
    pub role_title_counts: BTreeMap<String, u64>,
    /// Organization name to the number of persons whose `organizations[]` holds an
    /// organization of exactly that name (after last-wins organization dedup).
    pub organization_name_counts: BTreeMap<String, u64>,
}

impl Expected {
    /// Parses the JSON text of an expectations file.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| EsError::Expected(e.to_string()))
    }

    /// Reads and parses an expectations file.
    pub async fn load(path: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| EsError::Expected(format!("{}: {e}", path.display())))?;
        Self::from_json(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "persons": 10,
        "organizations": 4,
        "malformed": {"persons": 1, "organizations": 2},
        "persons_with_unresolved": 3,
        "role_title_counts": {"Engineer": 5, "Chief Llama Officer": 1},
        "organization_name_counts": {"Acme": 2}
    }"#;

    #[test]
    fn parses_the_contract_format() {
        let expected = Expected::from_json(SAMPLE.as_bytes()).expect("parse");
        assert_eq!(expected.persons, 10);
        assert_eq!(expected.malformed.organizations, 2);
        assert_eq!(expected.role_title_counts["Chief Llama Officer"], 1);
        let back = serde_json::to_vec(&expected).expect("serialize");
        assert_eq!(Expected::from_json(&back).ok(), Some(expected));
    }

    #[test]
    fn refuses_drift() {
        let extra = SAMPLE.replace("\"persons\": 10,", "\"persons\": 10, \"surprise\": 1,");
        assert!(Expected::from_json(extra.as_bytes()).is_err());
        let missing = SAMPLE.replace("\"persons_with_unresolved\": 3,", "");
        assert!(Expected::from_json(missing.as_bytes()).is_err());
    }
}
