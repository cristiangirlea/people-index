//! Counter names the stages commit. Later stages and `summary.json` read them back, so they
//! are constants rather than strings repeated at each use.

/// Lines read from an input file.
pub const LINES: &str = "lines";
/// Records kept by an extract task (lines that parsed).
pub const RECORDS: &str = "records";
/// Malformed lines; summed per feed against `max_malformed`.
pub const MALFORMED: &str = "malformed";
/// Values of lenient fields (`connections`, `followers`, `open_to_work`, `employee_count`,
/// `founded_year`) in kept records that Elasticsearch skips (`ignore_malformed`), following the
/// rules Elasticsearch 9.5.5 was observed to apply (`pi_model::JsonKind`): the malformed values
/// are not indexed and the field is listed in the document's `_ignored`; the record stays
/// searchable by its other fields. Counted per extracted line, before person deduplication; an
/// organization's value counts once however many person documents embed it.
pub const VALUES_IGNORED_BY_INDEX: &str = "values_ignored_by_index";
/// Kept records that repeated a key in some object, at any depth, and were rewritten to each
/// key's last occurrence (Elasticsearch rejects a duplicate key anywhere in a document).
/// Counted per extracted line, like [`VALUES_IGNORED_BY_INDEX`].
pub const RECORDS_WITH_REPEATED_KEYS: &str = "records_with_repeated_keys";
/// Decompressed bytes of an input file.
pub const DECOMPRESSED_BYTES: &str = "decompressed_bytes";
/// Edge rows written by `extract-persons`.
pub const EDGES: &str = "edges";
/// Segments written.
pub const SEGMENTS: &str = "segments";
/// Organization rows read by `join`.
pub const ORG_ROWS: &str = "org_rows";
/// Distinct organization ids in a `join` partition.
pub const ORGANIZATIONS: &str = "organizations";
/// Edges whose organization resolved.
pub const EDGES_RESOLVED: &str = "edges_resolved";
/// Edges whose organization did not resolve.
pub const EDGES_UNRESOLVED: &str = "edges_unresolved";
/// Passes a `join` or `assemble` task ran over its partition (1 unless the partition was
/// estimated larger than `task_memory_bytes`).
pub const PASSES: &str = "passes";
/// Person rows read by `assemble`.
pub const PERSON_ROWS: &str = "person_rows";
/// Joined rows read by `assemble`.
pub const JOINED_ROWS: &str = "joined_rows";
/// Documents written by `assemble`; their sum is the count `publish` checks.
pub const DOCUMENTS: &str = "documents";
/// Person records dropped because a later record of the same id won.
pub const DUPLICATES: &str = "duplicates";
/// Documents quarantined for exceeding the size limit.
pub const DOCUMENTS_TOO_LARGE: &str = "documents_too_large";
/// Documents with at least one unresolved organization id.
pub const DOCUMENTS_WITH_UNRESOLVED: &str = "documents_with_unresolved";
/// Documents acknowledged by Elasticsearch in `load`.
pub const DOCS_INDEXED: &str = "docs_indexed";
