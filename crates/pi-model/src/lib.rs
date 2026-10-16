//! pi-model: the schema and the pure rules of people-index (docs/ARCHITECTURE.md).
//!
//! Nothing here does I/O. The extract stages turn lines into rows with [`parse_org_line`] and
//! [`parse_person_line`] (or quarantine a [`MalformedRecord`], including a mapped field of the
//! wrong JSON kind: [`FieldShape`]); every stage routes ids with a [`Partitioner`]; `join` and
//! `assemble` deduplicate with [`LastWins`], group joined rows with [`JoinedGroups`] and split a
//! partition that exceeds the memory budget into [`Passes`]; `assemble` writes documents with
//! [`DocumentBuilder`].

mod builder;
mod dedup;
mod envelope;
mod error;
mod extract;
mod id;
mod joined;
mod json;
mod lookup;
mod partition;
mod pass;
mod raw;
mod roles;
mod rows;
mod seq;
mod shape;

pub use builder::{
    BuildError, BuiltDocument, DEFAULT_MAX_DOCUMENT_BYTES, DocumentBuilder, DocumentStats,
};
pub use dedup::LastWins;
pub use envelope::{Envelope, parse_envelope};
pub use error::{MalformedKind, MalformedRecord};
pub use extract::{parse_org_line, parse_person_line};
pub use id::parse_decimal_id;
pub use joined::{JoinedGroup, JoinedGroups, NoJoins, PersonJoins};
pub use lookup::{OrgLookup, Resolution};
pub use partition::{PartitionError, Partitioner, partition_count};
pub use pass::{MAX_PASSES, PASS_SEED, Passes};
pub use rows::{DocRow, EdgeRow, JoinedRow, OrgRow, PersonRow};
pub use seq::Seq;
pub use shape::{
    BUILDER_OWNED, FieldShape, JsonKind, ORGANIZATION_FIELDS, PERSON_FIELDS, ROLE_FIELDS,
};
