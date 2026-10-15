//! `assemble`'s side of the join: joined rows grouped by person, for the winning record only.
//!
//! A person id can appear several times in the feed, each record with its own roles and so its
//! own edges. Only the edges of the winning (last) record may reach the document, so each group
//! keeps the rows of the greatest `Seq` it has seen and drops older ones as soon as a newer one
//! arrives, whatever order the joined segments are read in.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::lookup::{OrgLookup, Resolution};
use crate::rows::JoinedRow;
use crate::seq::Seq;

/// The join results of one person record.
#[derive(Debug, Clone, Default)]
pub struct JoinedGroup {
    seq: Seq,
    orgs: Vec<(u64, Option<Vec<u8>>)>,
}

impl JoinedGroup {
    /// The person record these results belong to.
    #[must_use]
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// The number of organization ids with a result.
    #[must_use]
    pub fn len(&self) -> usize {
        self.orgs.len()
    }

    /// Whether the group holds no results.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.orgs.is_empty()
    }

    fn put(&mut self, org_id: u64, body: Option<Vec<u8>>) {
        match self.orgs.iter_mut().find(|(id, _)| *id == org_id) {
            Some(slot) => slot.1 = body,
            None => self.orgs.push((org_id, body)),
        }
    }
}

impl OrgLookup for JoinedGroup {
    fn resolve(&self, org_id: u64) -> Resolution<'_> {
        match self.orgs.iter().find(|(id, _)| *id == org_id) {
            Some((_, Some(body))) => Resolution::Resolved(body),
            Some((_, None)) => Resolution::Unresolved,
            None => Resolution::Missing,
        }
    }
}

/// A lookup with no results: every id is [`Resolution::Missing`]. Returned for a person whose
/// winning record has no joined rows, so a role that needed one fails loudly.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoJoins;

impl OrgLookup for NoJoins {
    fn resolve(&self, _org_id: u64) -> Resolution<'_> {
        Resolution::Missing
    }
}

/// The lookup for one person: its group, or [`NoJoins`].
#[derive(Debug, Clone, Copy)]
pub enum PersonJoins<'a> {
    /// The joined rows of the winning record.
    Group(&'a JoinedGroup),
    /// No joined rows for the winning record.
    None(NoJoins),
}

impl OrgLookup for PersonJoins<'_> {
    fn resolve(&self, org_id: u64) -> Resolution<'_> {
        match self {
            Self::Group(group) => group.resolve(org_id),
            Self::None(none) => none.resolve(org_id),
        }
    }
}

/// Joined rows of one partition, grouped by person id, newest record only.
#[derive(Debug, Clone, Default)]
pub struct JoinedGroups {
    groups: HashMap<u64, JoinedGroup>,
}

impl JoinedGroups {
    /// An empty set of groups.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one joined row: it replaces the person's group when its record is newer, joins it
    /// when it is the same record, and is dropped when it is older.
    pub fn push(&mut self, row: JoinedRow) {
        match self.groups.entry(row.person_id) {
            Entry::Vacant(slot) => {
                slot.insert(JoinedGroup {
                    seq: row.seq,
                    orgs: vec![(row.org_id, row.body)],
                });
            }
            Entry::Occupied(mut slot) => {
                let group = slot.get_mut();
                if row.seq > group.seq {
                    group.seq = row.seq;
                    group.orgs.clear();
                    group.orgs.push((row.org_id, row.body));
                } else if row.seq == group.seq {
                    group.put(row.org_id, row.body);
                }
            }
        }
    }

    /// The lookup for the person record `person_id` at `seq` (the winning record). Rows of any
    /// other record of that person are never returned.
    #[must_use]
    pub fn for_person(&self, person_id: u64, seq: Seq) -> PersonJoins<'_> {
        match self.groups.get(&person_id) {
            Some(group) if group.seq == seq => PersonJoins::Group(group),
            _ => PersonJoins::None(NoJoins),
        }
    }

    /// The number of persons with joined rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Whether no rows were pushed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(person_id: u64, org_id: u64, line: u64, body: Option<&str>) -> JoinedRow {
        JoinedRow {
            person_id,
            org_id,
            seq: Seq::new(0, line),
            body: body.map(|b| b.as_bytes().to_vec()),
        }
    }

    #[test]
    fn only_the_newest_record_survives_in_any_order() {
        let rows = [
            row(1, 10, 5, Some("{\"old\":1}")),
            row(1, 11, 9, Some("{\"new\":1}")),
            row(1, 12, 9, None),
            row(1, 13, 5, None),
        ];
        for order in [[0, 1, 2, 3], [1, 0, 3, 2], [3, 2, 1, 0], [2, 3, 0, 1]] {
            let mut groups = JoinedGroups::new();
            for i in order {
                groups.push(rows[i].clone());
            }
            let joins = groups.for_person(1, Seq::new(0, 9));
            assert_eq!(joins.resolve(11), Resolution::Resolved(b"{\"new\":1}"));
            assert_eq!(joins.resolve(12), Resolution::Unresolved);
            assert_eq!(
                joins.resolve(10),
                Resolution::Missing,
                "older record's edge"
            );
            assert_eq!(
                joins.resolve(13),
                Resolution::Missing,
                "older record's edge"
            );
            assert_eq!(groups.len(), 1);
        }
    }

    #[test]
    fn a_different_seq_gets_no_joins() {
        let mut groups = JoinedGroups::new();
        groups.push(row(1, 10, 5, None));
        assert!(matches!(
            groups.for_person(1, Seq::new(0, 6)),
            PersonJoins::None(_)
        ));
        assert!(matches!(
            groups.for_person(2, Seq::new(0, 5)),
            PersonJoins::None(_)
        ));
        assert_eq!(NoJoins.resolve(1), Resolution::Missing);
    }

    #[test]
    fn same_record_same_org_keeps_one_entry() {
        let mut groups = JoinedGroups::new();
        groups.push(row(1, 10, 5, None));
        groups.push(row(1, 10, 5, Some("{}")));
        match groups.for_person(1, Seq::new(0, 5)) {
            PersonJoins::Group(g) => {
                assert_eq!(g.len(), 1);
                assert!(!g.is_empty());
                assert_eq!(g.seq(), Seq::new(0, 5));
                assert_eq!(g.resolve(10), Resolution::Resolved(b"{}"));
            }
            PersonJoins::None(_) => panic!("group expected"),
        }
    }
}
