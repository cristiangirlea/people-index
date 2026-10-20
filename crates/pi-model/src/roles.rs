//! The one part of a person record the join reads: `roles[].organization_id`.
//!
//! Extraction (which decides what is malformed) and the document builder (which writes the
//! output) both go through [`PersonFields::parse`], so a record that passed extraction can
//! never fail differently when its document is built.
//!
//! The rules: `roles` absent or `null` means no roles; anything other than an array is
//! malformed. Each role must be an object. Its `organization_id` absent or `null` is ordinary
//! data (no organization); otherwise it must be an id. Inside the record, duplicate keys are
//! last-wins, as in any common JSON parser.

use serde_json::value::RawValue;

use crate::error::{MalformedKind, MalformedRecord};
use crate::id::OptionalId;
use crate::raw::{RawObject, is_null};

/// One role, read one level deep, with its organization id.
#[derive(Debug)]
pub(crate) struct Role<'a> {
    pub object: RawObject<'a>,
    pub organization_id: Option<u64>,
}

/// A person record read one level deep, with its roles.
#[derive(Debug)]
pub(crate) struct PersonFields<'a> {
    pub object: RawObject<'a>,
    /// `None` when `roles` is absent or `null`.
    pub roles: Option<Vec<Role<'a>>>,
}

impl<'a> PersonFields<'a> {
    /// Reads a person record's raw text.
    pub fn parse(record: &'a str) -> Result<Self, MalformedRecord> {
        let object = RawObject::parse(record)
            .map_err(|e| MalformedRecord::from_json(MalformedKind::Record, &e))?;
        let roles = match object.get("roles") {
            None => None,
            Some(raw) if is_null(raw) => None,
            Some(raw) => Some(parse_roles(raw)?),
        };
        Ok(Self { object, roles })
    }

    /// One entry per role: its organization id, or `None` for a null one.
    pub fn role_organization_ids(&self) -> Vec<Option<u64>> {
        self.roles
            .iter()
            .flatten()
            .map(|r| r.organization_id)
            .collect()
    }
}

fn parse_roles(raw: &RawValue) -> Result<Vec<Role<'_>>, MalformedRecord> {
    let items: Vec<&RawValue> = serde_json::from_str(raw.get()).map_err(|_| {
        MalformedRecord::new(MalformedKind::Record, "`roles` is not an array or null")
    })?;
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            let object = RawObject::parse(item.get()).map_err(|_| {
                MalformedRecord::new(
                    MalformedKind::Record,
                    format!("roles[{index}] is not an object"),
                )
            })?;
            let organization_id = match object.get("organization_id") {
                None => None,
                Some(value) => {
                    serde_json::from_str::<OptionalId>(value.get())
                        .map_err(|e| {
                            MalformedRecord::new(
                                MalformedKind::Record,
                                format!("roles[{index}].organization_id: {e}"),
                            )
                        })?
                        .0
                }
            };
            Ok(Role {
                object,
                organization_id,
            })
        })
        .collect()
}

/// The distinct non-null ids of `ids`, in order of first appearance.
pub(crate) fn distinct_in_order(ids: impl IntoIterator<Item = Option<u64>>) -> Vec<u64> {
    let mut out: Vec<u64> = Vec::new();
    let mut seen: Option<std::collections::HashSet<u64>> = None;
    for id in ids.into_iter().flatten() {
        // Persons hold a handful of roles, where a linear scan beats hashing; a set takes over
        // past that so a hostile record with thousands of roles stays linear.
        let fresh = match &mut seen {
            Some(set) => set.insert(id),
            None if out.len() < 32 => !out.contains(&id),
            None => {
                let mut set: std::collections::HashSet<u64> = out.iter().copied().collect();
                let fresh = set.insert(id);
                seen = Some(set);
                fresh
            }
        };
        if fresh {
            out.push(id);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(record: &str) -> Result<Vec<Option<u64>>, MalformedRecord> {
        PersonFields::parse(record).map(|p| p.role_organization_ids())
    }

    #[test]
    fn reads_ids_and_nulls_in_role_order() {
        let got = ids(r#"{"roles":[{"organization_id":3},{"organization_id":null},{},{"organization_id":"7"}]}"#)
            .unwrap();
        assert_eq!(got, [Some(3), None, None, Some(7)]);
    }

    #[test]
    fn absent_or_null_roles_mean_none() {
        assert_eq!(ids("{}").unwrap(), [] as [std::option::Option<u64>; 0]);
        assert_eq!(
            ids(r#"{"roles":null}"#).unwrap(),
            [] as [std::option::Option<u64>; 0]
        );
        assert_eq!(
            ids(r#"{"roles":[]}"#).unwrap(),
            [] as [std::option::Option<u64>; 0]
        );
    }

    #[test]
    fn duplicate_keys_are_last_wins() {
        let got =
            ids(r#"{"roles":5,"roles":[{"organization_id":1,"organization_id":2}]}"#).unwrap();
        assert_eq!(got, [Some(2)]);
    }

    #[test]
    fn malformed_roles() {
        for (record, needle) in [
            (r#"{"roles":{}}"#, "not an array"),
            (r#"{"roles":"x"}"#, "not an array"),
            (r#"{"roles":[1]}"#, "roles[0] is not an object"),
            (r#"{"roles":[{},[]]}"#, "roles[1] is not an object"),
            (
                r#"{"roles":[{"organization_id":-1}]}"#,
                "roles[0].organization_id",
            ),
            (
                r#"{"roles":[{"organization_id":"abc"}]}"#,
                "roles[0].organization_id",
            ),
            (
                r#"{"roles":[{"organization_id":true}]}"#,
                "roles[0].organization_id",
            ),
        ] {
            let err = ids(record).unwrap_err();
            assert_eq!(err.kind(), MalformedKind::Record, "{record}");
            assert!(err.reason().contains(needle), "{record}: {err}");
        }
    }

    #[test]
    fn distinct_keeps_first_appearance() {
        assert_eq!(
            distinct_in_order([Some(3), None, Some(1), Some(3), Some(2), Some(1)]),
            [3, 1, 2]
        );
    }
}
