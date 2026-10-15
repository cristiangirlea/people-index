//! How the document builder finds an organization body.
//!
//! A trait rather than a map so `assemble` can hand over its joined rows as they are grouped,
//! tests can use a plain map, and a missing join result (a bug in an earlier stage) is told
//! apart from an id that genuinely did not resolve.

use std::collections::HashMap;
use std::hash::BuildHasher;

/// What the join said about one organization id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution<'a> {
    /// The id resolved to this raw organization record.
    Resolved(&'a [u8]),
    /// The id is not in the organization feed.
    Unresolved,
    /// The join produced nothing for this id: an earlier stage lost a row.
    Missing,
}

/// Resolves organization ids for one person.
pub trait OrgLookup {
    /// What the join said about `org_id`.
    fn resolve(&self, org_id: u64) -> Resolution<'_>;
}

/// A plain organization table: absent ids are unresolved (used by tests and small tools).
impl<S: BuildHasher> OrgLookup for HashMap<u64, Vec<u8>, S> {
    fn resolve(&self, org_id: u64) -> Resolution<'_> {
        match self.get(&org_id) {
            Some(body) => Resolution::Resolved(body),
            None => Resolution::Unresolved,
        }
    }
}

/// Join results: `None` means unresolved, an absent id means the join lost it.
impl<S: BuildHasher> OrgLookup for HashMap<u64, Option<Vec<u8>>, S> {
    fn resolve(&self, org_id: u64) -> Resolution<'_> {
        match self.get(&org_id) {
            Some(Some(body)) => Resolution::Resolved(body),
            Some(None) => Resolution::Unresolved,
            None => Resolution::Missing,
        }
    }
}

impl<T: OrgLookup + ?Sized> OrgLookup for &T {
    fn resolve(&self, org_id: u64) -> Resolution<'_> {
        (**self).resolve(org_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_tables_treat_absent_as_unresolved() {
        let table: HashMap<u64, Vec<u8>> = HashMap::from([(1, b"{}".to_vec())]);
        assert_eq!(table.resolve(1), Resolution::Resolved(b"{}"));
        assert_eq!(table.resolve(2), Resolution::Unresolved);
    }

    #[test]
    fn join_results_treat_absent_as_missing() {
        let joined: HashMap<u64, Option<Vec<u8>>> =
            HashMap::from([(1, Some(b"{}".to_vec())), (2, None)]);
        assert_eq!(joined.resolve(1), Resolution::Resolved(b"{}"));
        assert_eq!(joined.resolve(2), Resolution::Unresolved);
        let by_ref: &dyn OrgLookup = &&joined;
        assert_eq!(by_ref.resolve(3), Resolution::Missing);
    }
}
