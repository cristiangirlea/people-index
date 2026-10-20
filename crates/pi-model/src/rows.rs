//! The rows stages write to segments and read back (docs/ARCHITECTURE.md, "Segment files").
//!
//! They are serialized with postcard. Bodies are raw JSON bytes that are never re-parsed until
//! the document is built (person bodies) or never at all (organization bodies, which are
//! spliced in as they are). `serde_bytes` keeps their encoding a length plus the bytes, decoded
//! as one copy.

use serde::{Deserialize, Serialize};

use crate::seq::Seq;

/// One organization record, keyed for the `orgs/p=<p>/` partition of its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrgRow {
    /// The organization id from the envelope.
    pub org_id: u64,
    /// Where the record sits in feed order, for last-wins deduplication.
    pub seq: Seq,
    /// The raw JSON of the record (an object), repeated top-level keys reduced to the last one.
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

/// One person record, keyed for the `persons/q=<q>/` partition of its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersonRow {
    /// The person id from the envelope, and the document `_id`.
    pub person_id: u64,
    /// Where the record sits in feed order, for last-wins deduplication.
    pub seq: Seq,
    /// The raw JSON of the record (an object).
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
    /// The distinct non-null `roles[].organization_id` values, in role order.
    pub org_ids: Vec<u64>,
}

impl PersonRow {
    /// One edge per distinct organization id, for the `edges/p=<p>/` partitions.
    // The iterator it returns is already `#[must_use]`; clippy before 1.99 still suggests the
    // attribute, which 1.99's `double_must_use` rejects.
    #[allow(clippy::must_use_candidate)]
    pub fn edges(&self) -> impl ExactSizeIterator<Item = EdgeRow> + '_ {
        self.org_ids.iter().map(|&org_id| EdgeRow {
            org_id,
            person_id: self.person_id,
            seq: self.seq,
        })
    }
}

/// "This person record (at `seq`) refers to this organization", routed by organization id so
/// the join can resolve it against one organization partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeRow {
    /// The organization the role refers to; decides the partition `p`.
    pub org_id: u64,
    /// The person the role belongs to; decides the partition `q` of the joined row.
    pub person_id: u64,
    /// The person record's position, so `assemble` keeps only edges of the winning record.
    pub seq: Seq,
}

/// An edge after the join: the organization body when the id resolved, `None` when it did not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinedRow {
    /// The person the role belongs to; decides the partition `q`.
    pub person_id: u64,
    /// The organization the role refers to.
    pub org_id: u64,
    /// The person record's position (copied from the edge).
    pub seq: Seq,
    /// The raw JSON of the winning organization record, or `None` when the id is unresolved.
    #[serde(with = "serde_bytes")]
    pub body: Option<Vec<u8>>,
}

/// The final document for one person, ready for a bulk request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocRow {
    /// The document `_id`.
    pub person_id: u64,
    /// The document JSON.
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let bytes = postcard::to_stdvec(value).unwrap();
        postcard::from_bytes(&bytes).unwrap()
    }

    #[test]
    fn rows_round_trip_through_postcard() {
        let seq = Seq::new(3, 42);
        let org = OrgRow {
            org_id: u64::MAX,
            seq,
            body: br#"{"name":"x"}"#.to_vec(),
        };
        assert_eq!(round_trip(&org), org);
        let person = PersonRow {
            person_id: 7,
            seq,
            body: b"{}".to_vec(),
            org_ids: vec![1, 2],
        };
        assert_eq!(round_trip(&person), person);
        let edge = EdgeRow {
            org_id: 1,
            person_id: 7,
            seq,
        };
        assert_eq!(round_trip(&edge), edge);
        for body in [None, Some(b"{}".to_vec())] {
            let joined = JoinedRow {
                person_id: 7,
                org_id: 1,
                seq,
                body,
            };
            assert_eq!(round_trip(&joined), joined);
        }
        let doc = DocRow {
            person_id: 7,
            body: b"{}".to_vec(),
        };
        assert_eq!(round_trip(&doc), doc);
    }

    #[test]
    fn bodies_encode_as_length_and_bytes() {
        let doc = DocRow {
            person_id: 1,
            body: vec![0xAB; 300],
        };
        let bytes = postcard::to_stdvec(&doc).unwrap();
        // varint(1) + varint(300) (two bytes) + 300 body bytes.
        assert_eq!(bytes.len(), 1 + 2 + 300);
    }

    #[test]
    fn edges_follow_org_ids() {
        let person = PersonRow {
            person_id: 9,
            seq: Seq::new(0, 1),
            body: b"{}".to_vec(),
            org_ids: vec![5, 3],
        };
        let edges: Vec<EdgeRow> = person.edges().collect();
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].org_id, 5);
        assert_eq!(edges[1].org_id, 3);
        assert!(
            edges
                .iter()
                .all(|e| e.person_id == 9 && e.seq == person.seq)
        );
    }
}
