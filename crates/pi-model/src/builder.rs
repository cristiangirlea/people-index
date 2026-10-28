//! The join's output rules: from a person record and its join results to the final document.
//!
//! The rules (docs/ARCHITECTURE.md, "The output document"):
//!
//! - `organizations[]` holds the full record of each distinct resolved `roles[].organization_id`,
//!   in order of first appearance in `roles[]`; it is always present, possibly empty;
//! - `roles[].organization_resolved` is `true` when the id resolved, `false` when it did not,
//!   and absent when the id is null;
//! - `unresolved_organization_ids[]` lists the distinct unresolved ids in role order, and is
//!   absent when there are none;
//! - every other field is carried through untouched.
//!
//! The document is written by splicing raw JSON: the person record is read one level deep (and
//! each role one level deep), values are copied as raw text, and organization bodies are copied
//! as they are, never parsed. Because the output names `organizations`,
//! `unresolved_organization_ids` and `roles[].organization_resolved` itself, input fields with
//! those names are replaced, and duplicate keys at the levels the builder writes are reduced to
//! their last occurrence so the output never holds a duplicate key. With another feed schema
//! ([`FeedSchema`]), `roles` and `organization_id` are the names it gives the references and
//! their organization id; the three output names stay the same.

use std::collections::HashMap;
use std::ops::AddAssign;
use std::sync::Arc;

use crate::error::{MalformedKind, MalformedRecord};
use crate::json::{write_string, write_u64};
use crate::lookup::{OrgLookup, Resolution};
use crate::raw::RawObject;
use crate::roles::{PersonFields, Role};
use crate::rows::{DocRow, PersonRow};
use crate::schema::{
    FeedSchema, ORGANIZATIONS_FIELD as ORGANIZATIONS, RESOLVED_FLAG, UNRESOLVED_FIELD as UNRESOLVED,
};

/// The default largest document, in bytes: one document must fit a default bulk request.
pub const DEFAULT_MAX_DOCUMENT_BYTES: usize = 8 << 20;

/// Why a document could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    /// The person body does not parse. Extraction validates bodies with the same parser, so
    /// this means a row was corrupted or written by something else.
    #[error("person body is invalid: {0}")]
    InvalidPerson(MalformedRecord),
    /// The join produced no result for an id the person refers to: an earlier stage lost a row.
    #[error("no join result for organization {org_id}")]
    MissingJoin {
        /// The organization id without a result.
        org_id: u64,
    },
    /// The document is larger than the limit. This is a property of the data (a person with
    /// very many or very large organizations), so the caller quarantines the person.
    #[error("document is larger than {limit} bytes (at least {bytes})")]
    TooLarge {
        /// The size reached when the limit was detected (a lower bound of the full size).
        bytes: usize,
        /// The limit.
        limit: usize,
    },
}

impl BuildError {
    /// Whether the error comes from the data (quarantine the person) rather than from a bug or
    /// corruption in an earlier stage (fail the task).
    #[must_use]
    pub fn is_data_error(&self) -> bool {
        matches!(self, Self::TooLarge { .. })
    }
}

/// Counters for one document, or summed over many.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DocumentStats {
    /// Documents counted.
    pub documents: u64,
    /// Roles seen.
    pub roles: u64,
    /// Roles whose `organization_id` is null or absent.
    pub roles_null_organization: u64,
    /// Roles whose id resolved.
    pub roles_resolved: u64,
    /// Roles whose id did not resolve.
    pub roles_unresolved: u64,
    /// Entries written to `organizations[]` (distinct resolved ids).
    pub organizations: u64,
    /// Entries written to `unresolved_organization_ids[]` (distinct unresolved ids).
    pub unresolved_ids: u64,
    /// Documents with at least one unresolved id.
    pub documents_with_unresolved: u64,
}

impl AddAssign for DocumentStats {
    fn add_assign(&mut self, other: Self) {
        self.documents += other.documents;
        self.roles += other.roles;
        self.roles_null_organization += other.roles_null_organization;
        self.roles_resolved += other.roles_resolved;
        self.roles_unresolved += other.roles_unresolved;
        self.organizations += other.organizations;
        self.unresolved_ids += other.unresolved_ids;
        self.documents_with_unresolved += other.documents_with_unresolved;
    }
}

/// A built document and its counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltDocument {
    /// The row `assemble` writes.
    pub doc: DocRow,
    /// What went into it.
    pub stats: DocumentStats,
}

/// Builds documents under a size limit, reading person records as a feed schema names them.
#[derive(Debug, Clone)]
pub struct DocumentBuilder {
    max_document_bytes: usize,
    /// `None` for the built-in schema, which needs no allocation.
    schema: Option<Arc<FeedSchema>>,
}

impl Default for DocumentBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_DOCUMENT_BYTES)
    }
}

/// What the join said about one role, decided before anything is written.
#[derive(Debug, Clone, Copy)]
enum RoleState {
    Null,
    Resolved,
    Unresolved,
}

impl DocumentBuilder {
    /// A builder for the built-in schema that refuses documents larger than
    /// `max_document_bytes`.
    #[must_use]
    pub fn new(max_document_bytes: usize) -> Self {
        Self {
            max_document_bytes,
            schema: None,
        }
    }

    /// The builder for person records named as `schema` says.
    #[must_use]
    pub fn with_schema(mut self, schema: Arc<FeedSchema>) -> Self {
        self.schema = (!schema.is_builtin()).then_some(schema);
        self
    }

    /// The schema person records are read with.
    #[must_use]
    pub fn schema(&self) -> &FeedSchema {
        self.schema.as_deref().unwrap_or(FeedSchema::builtin())
    }

    /// The size limit.
    #[must_use]
    pub fn max_document_bytes(&self) -> usize {
        self.max_document_bytes
    }

    /// Builds the document of a person row.
    pub fn build_row(
        &self,
        person: &PersonRow,
        lookup: &impl OrgLookup,
    ) -> Result<BuiltDocument, BuildError> {
        self.build(person.person_id, &person.body, lookup)
    }

    /// Builds the document of person `person_id` from its raw record `body`.
    pub fn build(
        &self,
        person_id: u64,
        body: &[u8],
        lookup: &impl OrgLookup,
    ) -> Result<BuiltDocument, BuildError> {
        let text = std::str::from_utf8(body).map_err(|e| {
            BuildError::InvalidPerson(MalformedRecord::new(
                MalformedKind::Syntax,
                format!("person body is not UTF-8: {e}"),
            ))
        })?;
        let schema = self.schema();
        let person = PersonFields::parse(text, schema).map_err(BuildError::InvalidPerson)?;
        let roles: &[Role<'_>] = person.roles.as_deref().unwrap_or(&[]);
        // Decide every role first, so the size check happens before any copying.
        let decisions = self.decide(roles, body.len(), lookup)?;
        let out = write_document(schema.references(), &person, roles, &decisions, body.len());
        if out.len() > self.max_document_bytes {
            return Err(BuildError::TooLarge {
                bytes: out.len(),
                limit: self.max_document_bytes,
            });
        }
        Ok(BuiltDocument {
            doc: DocRow {
                person_id,
                body: out,
            },
            stats: decisions.stats,
        })
    }

    /// Resolves every role in order, collecting the output lists and the counters.
    fn decide<'o>(
        &self,
        roles: &[Role<'_>],
        body_len: usize,
        lookup: &'o impl OrgLookup,
    ) -> Result<Decisions<'o>, BuildError> {
        let mut d = Decisions {
            flags: Vec::with_capacity(roles.len()),
            organizations: Vec::new(),
            unresolved: Vec::new(),
            organization_bytes: 0,
            stats: DocumentStats {
                documents: 1,
                ..DocumentStats::default()
            },
        };
        // Ids already decided. Linear scans over the two output lists are cheapest for the
        // usual handful of roles; a map takes over for long role lists so a hostile record
        // cannot make the build quadratic.
        let mut decided: Option<HashMap<u64, RoleState>> =
            (roles.len() > 32).then(|| HashMap::with_capacity(roles.len()));
        for role in roles {
            let flag = match role.organization_id {
                None => RoleState::Null,
                Some(id) => {
                    let known = match &decided {
                        Some(map) => map.get(&id).copied(),
                        None => d.known(id),
                    };
                    let flag = match known {
                        Some(flag) => flag,
                        None => self.resolve_new(&mut d, id, body_len, lookup)?,
                    };
                    if let Some(map) = &mut decided {
                        map.insert(id, flag);
                    }
                    flag
                }
            };
            d.stats.roles += 1;
            match flag {
                RoleState::Null => d.stats.roles_null_organization += 1,
                RoleState::Resolved => d.stats.roles_resolved += 1,
                RoleState::Unresolved => d.stats.roles_unresolved += 1,
            }
            d.flags.push(flag);
        }
        d.stats.organizations = d.organizations.len() as u64;
        d.stats.unresolved_ids = d.unresolved.len() as u64;
        d.stats.documents_with_unresolved = u64::from(!d.unresolved.is_empty());
        Ok(d)
    }

    /// Looks up an id seen for the first time and records the answer.
    fn resolve_new<'o>(
        &self,
        d: &mut Decisions<'o>,
        id: u64,
        body_len: usize,
        lookup: &'o impl OrgLookup,
    ) -> Result<RoleState, BuildError> {
        match lookup.resolve(id) {
            Resolution::Resolved(org) => {
                d.organization_bytes = d.organization_bytes.saturating_add(org.len() + 1);
                let floor = body_len.saturating_add(d.organization_bytes);
                if floor > self.max_document_bytes {
                    return Err(BuildError::TooLarge {
                        bytes: floor,
                        limit: self.max_document_bytes,
                    });
                }
                d.organizations.push((id, org));
                Ok(RoleState::Resolved)
            }
            Resolution::Unresolved => {
                d.unresolved.push(id);
                Ok(RoleState::Unresolved)
            }
            Resolution::Missing => Err(BuildError::MissingJoin { org_id: id }),
        }
    }
}

/// What [`DocumentBuilder::decide`] found, borrowing organization bodies from the lookup.
struct Decisions<'o> {
    /// One per role.
    flags: Vec<RoleState>,
    /// Distinct resolved ids and their bodies, in first-appearance order.
    organizations: Vec<(u64, &'o [u8])>,
    /// Distinct unresolved ids, in first-appearance order.
    unresolved: Vec<u64>,
    /// Bytes the organization bodies add, commas included.
    organization_bytes: usize,
    stats: DocumentStats,
}

impl Decisions<'_> {
    /// The earlier answer for `id`, by scanning the output lists.
    fn known(&self, id: u64) -> Option<RoleState> {
        if self.organizations.iter().any(|(o, _)| *o == id) {
            Some(RoleState::Resolved)
        } else if self.unresolved.contains(&id) {
            Some(RoleState::Unresolved)
        } else {
            None
        }
    }
}

/// Writes the document: the person's fields in input order (the references field, `roles`,
/// rewritten), then `organizations` and, when needed, `unresolved_organization_ids`.
fn write_document(
    references: &str,
    person: &PersonFields<'_>,
    roles: &[Role<'_>],
    decisions: &Decisions<'_>,
    body_len: usize,
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(body_len + decisions.organization_bytes + 32 * roles.len() + 64);
    out.push(b'{');
    let mut first = true;
    let keep = person.object.last_occurrences();
    for ((key, value), kept) in person.object.entries.iter().zip(keep) {
        let key = key.0.as_ref();
        if !kept || key == ORGANIZATIONS || key == UNRESOLVED {
            continue;
        }
        write_key(&mut out, &mut first, key);
        if key == references && person.roles.is_some() {
            write_roles(&mut out, roles, &decisions.flags);
        } else {
            out.extend_from_slice(value.get().as_bytes());
        }
    }
    write_key(&mut out, &mut first, ORGANIZATIONS);
    out.push(b'[');
    for (i, (_, org)) in decisions.organizations.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(org);
    }
    out.push(b']');
    if !decisions.unresolved.is_empty() {
        write_key(&mut out, &mut first, UNRESOLVED);
        out.push(b'[');
        for (i, id) in decisions.unresolved.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            write_u64(&mut out, *id);
        }
        out.push(b']');
    }
    out.push(b'}');
    out
}

/// Writes `"key":`, preceded by a comma unless it is the object's first entry.
fn write_key(out: &mut Vec<u8>, first: &mut bool, key: &str) {
    if !std::mem::take(first) {
        out.push(b',');
    }
    write_string(out, key);
    out.push(b':');
}

fn write_roles(out: &mut Vec<u8>, roles: &[Role<'_>], flags: &[RoleState]) {
    out.push(b'[');
    for (i, (role, flag)) in roles.iter().zip(flags).enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write_role(out, &role.object, *flag);
    }
    out.push(b']');
}

fn write_role(out: &mut Vec<u8>, role: &RawObject<'_>, flag: RoleState) {
    out.push(b'{');
    let mut first = true;
    let keep = role.last_occurrences();
    for ((key, value), kept) in role.entries.iter().zip(keep) {
        let key = key.0.as_ref();
        if !kept || key == RESOLVED_FLAG {
            continue;
        }
        write_key(out, &mut first, key);
        out.extend_from_slice(value.get().as_bytes());
    }
    let literal: &[u8] = match flag {
        RoleState::Null => b"",
        RoleState::Resolved => b"true",
        RoleState::Unresolved => b"false",
    };
    if !literal.is_empty() {
        write_key(out, &mut first, RESOLVED_FLAG);
        out.extend_from_slice(literal);
    }
    out.push(b'}');
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::{Value, json};

    use super::*;

    fn orgs(entries: &[(u64, &str)]) -> HashMap<u64, Vec<u8>> {
        entries
            .iter()
            .map(|(id, body)| (*id, body.as_bytes().to_vec()))
            .collect()
    }

    fn build(record: &str, table: &HashMap<u64, Vec<u8>>) -> (Value, DocumentStats) {
        let built = DocumentBuilder::default()
            .build(1, record.as_bytes(), table)
            .unwrap();
        (
            serde_json::from_slice(&built.doc.body).unwrap(),
            built.stats,
        )
    }

    #[test]
    fn applies_every_output_rule() {
        let table = orgs(&[
            (10, r#"{"id":10,"name":"Ten"}"#),
            (30, r#"{"id":30, "n": 1.50}"#),
        ]);
        let record = r#"{"id":1,"first_name":"A","extra":{"k":[1,2]},"roles":[
            {"title":"a","organization_id":30},
            {"title":"b","organization_id":null},
            {"title":"c","organization_id":20},
            {"title":"d","organization_id":10},
            {"title":"e","organization_id":"30"},
            {"title":"f"},
            {"title":"g","organization_id":20},
            {"title":"h","organization_id":40}
        ]}"#;
        let (doc, stats) = build(record, &table);
        assert_eq!(
            doc["organizations"],
            json!([{"id":30,"n":1.50},{"id":10,"name":"Ten"}])
        );
        let flags: Vec<Value> = doc["roles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r.get("organization_resolved")
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();
        assert_eq!(
            flags,
            [
                json!(true),
                Value::Null,
                json!(false),
                json!(true),
                json!(true),
                Value::Null,
                json!(false),
                json!(false)
            ]
        );
        assert!(doc["roles"][1].get("organization_resolved").is_none());
        assert_eq!(doc["unresolved_organization_ids"], json!([20, 40]));
        assert_eq!(doc["extra"], json!({"k":[1,2]}));
        assert_eq!(doc["first_name"], "A");
        assert_eq!(
            stats,
            DocumentStats {
                documents: 1,
                roles: 8,
                roles_null_organization: 2,
                roles_resolved: 3,
                roles_unresolved: 3,
                organizations: 2,
                unresolved_ids: 2,
                documents_with_unresolved: 1,
            }
        );
    }

    #[test]
    fn a_schema_names_the_references_the_builder_rewrites() {
        let schema = FeedSchema::from_json(
            r#"{"person": {"references": "jobs", "reference_id": "org",
                "fields": [{"name": "jobs", "kind": "object_array"}]}}"#,
        )
        .unwrap();
        let builder = DocumentBuilder::default().with_schema(Arc::new(schema));
        assert_eq!(builder.schema().references(), "jobs");
        let table = orgs(&[(10, r#"{"id":10}"#)]);
        let built = builder
            .build(
                1,
                br#"{"roles":[{"organization_id":10}],"jobs":[{"org":10,"organization_resolved":"x"},{"org":7},{}],"organizations":1}"#,
                &table,
            )
            .unwrap();
        let doc: Value = serde_json::from_slice(&built.doc.body).unwrap();
        assert_eq!(
            doc,
            json!({
                "roles": [{"organization_id": 10}],
                "jobs": [{"org": 10, "organization_resolved": true},
                         {"org": 7, "organization_resolved": false}, {}],
                "organizations": [{"id": 10}],
                "unresolved_organization_ids": [7],
            })
        );
        assert_eq!(built.stats.roles, 3);
        // The built-in schema is held as nothing at all.
        let builtin = DocumentBuilder::default().with_schema(Arc::new(FeedSchema::default()));
        assert!(builtin.schema.is_none());
    }

    #[test]
    fn organization_bodies_are_spliced_byte_for_byte() {
        let body =
            r#"{"name":"X",  "n":1.000000000000000000001,"big":123456789012345678901234567890}"#;
        let table = orgs(&[(5, body)]);
        let built = DocumentBuilder::default()
            .build(1, br#"{"roles":[{"organization_id":5}]}"#, &table)
            .unwrap();
        let text = String::from_utf8(built.doc.body).unwrap();
        assert!(text.contains(body), "{text}");
    }

    #[test]
    fn no_roles_gives_empty_organizations_and_no_unresolved() {
        for record in ["{}", r#"{"roles":null}"#, r#"{"roles":[]}"#] {
            let (doc, stats) = build(record, &HashMap::new());
            assert_eq!(doc["organizations"], json!([]), "{record}");
            assert!(doc.get("unresolved_organization_ids").is_none());
            assert_eq!(stats.roles, 0);
        }
        let (doc, _) = build(r#"{"roles":null}"#, &HashMap::new());
        assert!(doc["roles"].is_null(), "null roles stay null");
    }

    #[test]
    fn input_fields_with_output_names_are_replaced() {
        let record = r#"{"organizations":["member of X"],"unresolved_organization_ids":[1],
            "roles":[{"organization_id":7,"organization_resolved":"stale"},{"organization_resolved":true}]}"#;
        let (doc, _) = build(record, &orgs(&[(7, "{}")]));
        assert_eq!(doc["organizations"], json!([{}]));
        assert!(doc.get("unresolved_organization_ids").is_none());
        assert_eq!(doc["roles"][0]["organization_resolved"], json!(true));
        assert!(doc["roles"][1].get("organization_resolved").is_none());
    }

    #[test]
    fn duplicate_keys_are_reduced_to_the_last() {
        let record = r#"{"a":1,"a":2,"roles":[{"t":1,"t":2,"organization_id":3}]}"#;
        let built = DocumentBuilder::default()
            .build(1, record.as_bytes(), &HashMap::<u64, Vec<u8>>::new())
            .unwrap();
        let text = String::from_utf8(built.doc.body).unwrap();
        assert_eq!(
            text,
            r#"{"a":2,"roles":[{"t":2,"organization_id":3,"organization_resolved":false}],"organizations":[],"unresolved_organization_ids":[3]}"#
        );
    }

    #[test]
    fn missing_join_results_are_errors() {
        let joined: HashMap<u64, Option<Vec<u8>>> = HashMap::from([(1, None)]);
        let err = DocumentBuilder::default()
            .build(
                1,
                br#"{"roles":[{"organization_id":1},{"organization_id":2}]}"#,
                &joined,
            )
            .unwrap_err();
        assert_eq!(err, BuildError::MissingJoin { org_id: 2 });
        assert!(!err.is_data_error());
    }

    #[test]
    fn size_limit_is_enforced() {
        let big = format!(r#"{{"blob":"{}"}}"#, "x".repeat(1000));
        let table = orgs(&[(1, &big)]);
        let record = br#"{"roles":[{"organization_id":1}]}"#;
        let err = DocumentBuilder::new(500)
            .build(1, record, &table)
            .unwrap_err();
        assert!(
            matches!(err, BuildError::TooLarge { limit: 500, .. }),
            "{err}"
        );
        assert!(err.is_data_error());
        // Without organizations the final check still applies.
        let err = DocumentBuilder::new(10)
            .build(1, b"{\"a\":\"0123456789\"}", &table)
            .unwrap_err();
        assert!(matches!(err, BuildError::TooLarge { limit: 10, .. }));
        assert!(DocumentBuilder::new(2000).build(1, record, &table).is_ok());
    }

    #[test]
    fn invalid_bodies_are_reported() {
        let table = HashMap::<u64, Vec<u8>>::new();
        for body in [&b"[]"[..], b"{\"roles\":[1]}", b"\xff", b"{"] {
            let err = DocumentBuilder::default()
                .build(1, body, &table)
                .unwrap_err();
            assert!(matches!(err, BuildError::InvalidPerson(_)), "{err}");
        }
    }

    #[test]
    fn build_row_uses_the_row_id() {
        let row = PersonRow {
            person_id: 42,
            seq: crate::Seq::default(),
            body: b"{}".to_vec(),
            org_ids: vec![],
        };
        let built = DocumentBuilder::default()
            .build_row(&row, &HashMap::<u64, Vec<u8>>::new())
            .unwrap();
        assert_eq!(built.doc.person_id, 42);
        assert_eq!(
            DocumentBuilder::default().max_document_bytes(),
            DEFAULT_MAX_DOCUMENT_BYTES
        );
    }

    #[test]
    fn stats_add_up() {
        let mut total = DocumentStats::default();
        let one = DocumentStats {
            documents: 1,
            roles: 2,
            ..DocumentStats::default()
        };
        total += one;
        total += one;
        assert_eq!(total.documents, 2);
        assert_eq!(total.roles, 4);
    }
}
