//! Counter names the stages commit. Later stages and `summary.json` read them back, so they
//! are constants rather than strings repeated at each use.

/// Lines read from an input file.
pub const LINES: &str = "lines";
/// Records kept by an extract task (lines that parsed).
pub const RECORDS: &str = "records";
/// Malformed lines; summed per feed against `max_malformed`.
pub const MALFORMED: &str = "malformed";
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
