//! pi-model: the schema and the pure rules of people-index (docs/ARCHITECTURE.md).
//!
//! Nothing here does I/O. The extract stages turn lines into rows with [`parse_org_line_with`]
//! and [`parse_person_line_with`], reading the names a [`FeedSchema`] gives (or quarantine a
//! [`MalformedRecord`], including a mapped field of the
//! wrong JSON kind: [`FieldShape`]; values the index will skip are counted in [`LineStats`]);
//! every stage routes ids with a [`Partitioner`]; `join` and `assemble` deduplicate with
//! [`LastWins`], group joined rows with [`JoinedGroups`] and split a partition that exceeds the
//! memory budget into [`Passes`]; `assemble` writes documents with [`DocumentBuilder`].

mod builder;
mod dedup;
mod envelope;
mod error;
mod es_date;
mod es_number;
mod extract;
mod id;
mod java_double;
mod joined;
mod json;
mod line_stats;
mod lookup;
mod number_length;
mod partition;
mod pass;
mod raw;
mod repeated_keys;
mod roles;
mod rows;
mod scan;
mod schema;
mod seq;
mod shape;

pub use builder::{
    BuildError, BuiltDocument, DEFAULT_MAX_DOCUMENT_BYTES, DocumentBuilder, DocumentStats,
};
pub use dedup::LastWins;
pub use envelope::{Envelope, parse_envelope, parse_envelope_with_names};
pub use error::{MalformedKind, MalformedRecord};
pub use es_date::DateFormat;
pub use extract::{
    log_field_name, parse_org_line, parse_org_line_with, parse_person_line, parse_person_line_with,
};
pub use id::parse_decimal_id;
pub use joined::{JoinedGroup, JoinedGroups, NoJoins, PersonJoins};
pub use line_stats::{IgnoredValue, LineStats};
pub use lookup::{OrgLookup, Resolution};
pub use partition::{PartitionError, Partitioner, partition_count};
pub use pass::{MAX_PASSES, PASS_SEED, Passes};
pub use rows::{DocRow, EdgeRow, JoinedRow, OrgRow, PersonRow};
pub use schema::{FeedSchema, ORGANIZATIONS_FIELD, RESOLVED_FLAG, SchemaError, UNRESOLVED_FIELD};
pub use seq::Seq;
pub use shape::{
    BUILDER_OWNED, FieldShape, JsonKind, Name, ORGANIZATION_DISABLED, ORGANIZATION_FIELDS,
    PERSON_DISABLED, PERSON_FIELDS, ROLE_FIELDS,
};
