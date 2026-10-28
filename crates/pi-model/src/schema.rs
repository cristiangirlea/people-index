//! [`FeedSchema`]: the names the pipeline reads in the two feeds.
//!
//! The envelope keys (`id`, `record`), the person's list of references to organizations
//! (`roles`) and each reference's organization id (`organization_id`), and the tables of mapped
//! fields extraction checks ([`crate::shape`]) are compiled in as the built-in schema
//! ([`FeedSchema::builtin`], a static that borrows the compiled tables). A feed that names them
//! differently is described by a schema file (`PI_SCHEMA_FILE`, docs/OPERATIONS.md "Feed
//! schema"), read once at startup by [`FeedSchema::from_json`]. Every name is resolved there
//! into this struct, so parsing a line compares keys against the same borrowed strings as with
//! the compiled names: no lookup by name, no allocation per record.
//!
//! The names the document builder writes (`organizations`, `unresolved_organization_ids` and
//! each reference's `organization_resolved`) belong to the output, not to the feed: they are
//! fixed, and a schema that uses one of them as an input name is refused.

use std::borrow::Cow;
use std::fmt;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::shape::{
    FieldShape, JsonKind, Name, ORGANIZATION_DISABLED, ORGANIZATION_FIELDS, PERSON_DISABLED,
    PERSON_FIELDS, ROLE_FIELDS,
};

/// The output field holding the resolved organizations.
pub const ORGANIZATIONS_FIELD: &str = "organizations";
/// The output field holding the organization ids that did not resolve.
pub const UNRESOLVED_FIELD: &str = "unresolved_organization_ids";
/// The flag the builder writes into each reference with an organization id.
pub const RESOLVED_FLAG: &str = "organization_resolved";

/// The names of the input feeds, resolved once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedSchema {
    id: Name,
    record: Name,
    references: Name,
    reference_id: Name,
    person_fields: Cow<'static, [FieldShape]>,
    reference_fields: Cow<'static, [FieldShape]>,
    person_disabled: Cow<'static, [Name]>,
    organization_fields: Cow<'static, [FieldShape]>,
    organization_disabled: Cow<'static, [Name]>,
    /// `<references>.organization_resolved`, the builder-owned path inside the references.
    resolved_path: Name,
}

static BUILTIN: FeedSchema = FeedSchema {
    id: Cow::Borrowed("id"),
    record: Cow::Borrowed("record"),
    references: Cow::Borrowed("roles"),
    reference_id: Cow::Borrowed("organization_id"),
    person_fields: Cow::Borrowed(PERSON_FIELDS),
    reference_fields: Cow::Borrowed(ROLE_FIELDS),
    person_disabled: Cow::Borrowed(PERSON_DISABLED),
    organization_fields: Cow::Borrowed(ORGANIZATION_FIELDS),
    organization_disabled: Cow::Borrowed(ORGANIZATION_DISABLED),
    resolved_path: Cow::Borrowed("roles.organization_resolved"),
};

impl Default for FeedSchema {
    /// The built-in schema (a copy of borrowed names: no allocation).
    fn default() -> Self {
        BUILTIN.clone()
    }
}

/// Why a schema file was refused: the field of the file and the problem, or the JSON error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SchemaError(String);

impl SchemaError {
    fn at(path: &str, problem: impl fmt::Display) -> Self {
        Self(format!("{path}: {problem}"))
    }
}

impl FeedSchema {
    /// The built-in schema: today's names, what runs without a schema file use.
    #[must_use]
    pub fn builtin() -> &'static Self {
        &BUILTIN
    }

    /// Whether this is the built-in schema (by content: a file that spells out every built-in
    /// name, its tables in the built-in order, is the built-in schema too).
    #[must_use]
    pub fn is_builtin(&self) -> bool {
        *self == BUILTIN
    }

    /// Reads a schema file. Every key is optional and an absent one keeps the built-in value;
    /// a table given replaces the built-in table whole. Unknown keys, empty names, names with
    /// a `.`, a name listed twice in one object's tables, names the document builder writes,
    /// and a references field not listed as an `object_array` are refused, naming the field.
    pub fn from_json(text: &str) -> Result<Self, SchemaError> {
        let value: Value = serde_json::from_str(text).map_err(|e| SchemaError(e.to_string()))?;
        require_objects(&value)?;
        // Parsed again from the text, so an error carries its line and column.
        let file: SchemaFile =
            serde_json::from_str(text).map_err(|e| SchemaError(e.to_string()))?;
        file.resolve()
    }

    /// Resolve keyword limits once for extraction, leaving the schema's serialized form unchanged.
    #[must_use]
    pub fn with_keyword_limits(&self, mapping: &Value) -> Self {
        fn field_at<'a>(mapping: &'a Value, path: &str) -> &'a Value {
            path.split('.')
                .fold(mapping, |node, part| &node["properties"][part])
        }
        fn limit(node: &Value) -> Option<usize> {
            let own = (node["type"] == "keyword")
                .then(|| {
                    node["ignore_above"]
                        .as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                })
                .flatten();
            node["fields"]
                .as_object()
                .into_iter()
                .flat_map(|m| m.values())
                .filter_map(limit)
                .chain(own)
                .min()
        }
        fn apply(fields: &mut Cow<'static, [FieldShape]>, mapping: &Value, prefix: &str) {
            for shape in fields.to_mut() {
                let path = format!("{prefix}{}", shape.name);
                let node = field_at(mapping, &path);
                let targets: Vec<&str> = match &node["copy_to"] {
                    Value::String(s) => vec![s],
                    Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
                    _ => Vec::new(),
                };
                shape.keyword_limit = targets
                    .into_iter()
                    .filter_map(|p| limit(field_at(mapping, p)))
                    .chain(limit(node))
                    .min();
            }
        }
        let mut result = self.clone();
        apply(&mut result.person_fields, mapping, "");
        apply(
            &mut result.reference_fields,
            mapping,
            &format!("{}.", self.references),
        );
        apply(&mut result.organization_fields, mapping, "organizations.");
        result
    }

    /// The schema as a complete schema file (every key present), the canonical form the plan
    /// pins by its digest. Resolved keyword limits are omitted and must be resolved again from the mapping.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "envelope": {"id": self.id, "record": self.record},
            "person": {
                "references": self.references,
                "reference_id": self.reference_id,
                "fields": fields_json(&self.person_fields),
                "reference_fields": fields_json(&self.reference_fields),
                "disabled": self.person_disabled,
            },
            "organization": {
                "fields": fields_json(&self.organization_fields),
                "disabled": self.organization_disabled,
            },
        })
    }

    /// The envelope key of the person or organization id.
    #[must_use]
    pub fn envelope_id(&self) -> &str {
        &self.id
    }

    /// The envelope key of the record.
    #[must_use]
    pub fn envelope_record(&self) -> &str {
        &self.record
    }

    /// The person field holding the list of references to organizations.
    #[must_use]
    pub fn references(&self) -> &str {
        &self.references
    }

    /// The field of a reference holding the organization id.
    #[must_use]
    pub fn reference_id(&self) -> &str {
        &self.reference_id
    }

    /// The checked top-level fields of a person record.
    #[must_use]
    pub fn person_fields(&self) -> &[FieldShape] {
        &self.person_fields
    }

    /// The checked fields of each reference.
    #[must_use]
    pub fn reference_fields(&self) -> &[FieldShape] {
        &self.reference_fields
    }

    /// The person fields the mapping disables.
    #[must_use]
    pub fn person_disabled(&self) -> &[Name] {
        &self.person_disabled
    }

    /// The checked fields of an organization record.
    #[must_use]
    pub fn organization_fields(&self) -> &[FieldShape] {
        &self.organization_fields
    }

    /// The organization fields the mapping disables.
    #[must_use]
    pub fn organization_disabled(&self) -> &[Name] {
        &self.organization_disabled
    }

    /// The mapped paths the document builder writes: `organizations`,
    /// `unresolved_organization_ids` and `<references>.organization_resolved`.
    #[must_use]
    pub fn builder_owned(&self) -> [&str; 3] {
        [ORGANIZATIONS_FIELD, UNRESOLVED_FIELD, &self.resolved_path]
    }
}

/// serde reads a struct from an array too, by position; a schema file spells every section
/// and every field entry as an object.
fn require_objects(file: &Value) -> Result<(), SchemaError> {
    let object = |path: &str, value: &Value| {
        if value.is_object() {
            Ok(())
        } else {
            Err(SchemaError::at(path, "expected an object"))
        }
    };
    object("the schema", file)?;
    for section in ["envelope", "person", "organization"] {
        let Some(value) = file.get(section) else {
            continue;
        };
        object(section, value)?;
        for table in ["fields", "reference_fields"] {
            let Some(Value::Array(entries)) = value.get(table) else {
                continue;
            };
            for (i, entry) in entries.iter().enumerate() {
                object(&format!("{section}.{table}[{i}]"), entry)?;
            }
        }
    }
    Ok(())
}

fn fields_json(fields: &[FieldShape]) -> Value {
    fields
        .iter()
        .map(|f| json!({"name": f.name, "kind": f.kind.as_str()}))
        .collect()
}

/// A schema file as written: every key optional.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaFile {
    #[serde(default)]
    envelope: EnvelopeFile,
    #[serde(default)]
    person: PersonFile,
    #[serde(default)]
    organization: OrganizationFile,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeFile {
    id: Option<String>,
    record: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonFile {
    references: Option<String>,
    reference_id: Option<String>,
    fields: Option<Vec<FieldFile>>,
    reference_fields: Option<Vec<FieldFile>>,
    disabled: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrganizationFile {
    fields: Option<Vec<FieldFile>>,
    disabled: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldFile {
    name: String,
    kind: JsonKind,
}

impl SchemaFile {
    fn resolve(self) -> Result<FeedSchema, SchemaError> {
        let Self {
            envelope,
            person,
            organization,
        } = self;
        let id = key(envelope.id, &BUILTIN.id, "envelope.id")?;
        let record = key(envelope.record, &BUILTIN.record, "envelope.record")?;
        if id == record {
            return Err(SchemaError::at(
                "envelope.record",
                format!("{record:?} is also envelope.id"),
            ));
        }
        let references = field_name(person.references, &BUILTIN.references, "person.references")?;
        let reference_id = field_name(
            person.reference_id,
            &BUILTIN.reference_id,
            "person.reference_id",
        )?;
        let schema = FeedSchema {
            resolved_path: format!("{references}.{RESOLVED_FLAG}").into(),
            id,
            record,
            references,
            reference_id,
            person_fields: table(person.fields, &BUILTIN.person_fields, "person.fields")?,
            reference_fields: table(
                person.reference_fields,
                &BUILTIN.reference_fields,
                "person.reference_fields",
            )?,
            person_disabled: names(person.disabled, &BUILTIN.person_disabled, "person.disabled")?,
            organization_fields: table(
                organization.fields,
                &BUILTIN.organization_fields,
                "organization.fields",
            )?,
            organization_disabled: names(
                organization.disabled,
                &BUILTIN.organization_disabled,
                "organization.disabled",
            )?,
        };
        schema.check()?;
        Ok(schema)
    }
}

impl FeedSchema {
    /// The rules across names: no name twice in one object's tables, no builder-owned name,
    /// and the references field listed as the one `object_array`.
    fn check(&self) -> Result<(), SchemaError> {
        let person = tagged(
            "person.fields",
            &self.person_fields,
            "person.disabled",
            &self.person_disabled,
        );
        unique(&person)?;
        for (path, name) in person
            .iter()
            .map(|(path, name)| (path.as_str(), *name))
            .chain([("person.references", &*self.references)])
        {
            if [ORGANIZATIONS_FIELD, UNRESOLVED_FIELD].contains(&name) {
                return Err(builder_owned(path, name));
            }
        }
        let references = self
            .person_fields
            .iter()
            .position(|f| f.name == self.references)
            .ok_or_else(|| {
                SchemaError::at(
                    "person.references",
                    format!(
                        "{:?} is not listed in person.fields (with kind \"object_array\")",
                        self.references
                    ),
                )
            })?;
        for (i, f) in self.person_fields.iter().enumerate() {
            let is_references = i == references;
            if is_references != (f.kind == JsonKind::ObjectArray) {
                return Err(SchemaError::at(
                    &format!("person.fields[{i}].kind"),
                    if is_references {
                        format!("the references field {:?} must be \"object_array\"", f.name)
                    } else {
                        ONLY_REFERENCES.to_owned()
                    },
                ));
            }
        }
        let reference = tagged("person.reference_fields", &self.reference_fields, "", &[]);
        unique(&reference)?;
        for (path, name) in reference
            .iter()
            .map(|(path, name)| (path.as_str(), *name))
            .chain([("person.reference_id", &*self.reference_id)])
        {
            if name == RESOLVED_FLAG {
                return Err(builder_owned(path, name));
            }
        }
        // The join reads the organization id by the id rule (a number, or a string of decimal
        // digits); a table entry for it may only say as much.
        if let Some((i, f)) = self
            .reference_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == self.reference_id)
            && !matches!(f.kind, JsonKind::Number | JsonKind::String)
        {
            return Err(SchemaError::at(
                &format!("person.reference_fields[{i}].kind"),
                format!(
                    "the organization id {:?} (person.reference_id) is read as an id: its kind \
                     must be \"number\" or \"string\"",
                    f.name
                ),
            ));
        }
        unique(&tagged(
            "organization.fields",
            &self.organization_fields,
            "organization.disabled",
            &self.organization_disabled,
        ))?;
        for (path, fields) in [
            ("person.reference_fields", &self.reference_fields),
            ("organization.fields", &self.organization_fields),
        ] {
            if let Some(i) = fields.iter().position(|f| f.kind == JsonKind::ObjectArray) {
                return Err(SchemaError::at(
                    &format!("{path}[{i}].kind"),
                    ONLY_REFERENCES,
                ));
            }
        }
        Ok(())
    }
}

const ONLY_REFERENCES: &str =
    "\"object_array\" is only for the references field (person.references)";

/// Each name of a table and of a name list, with the file field it came from
/// (`person.fields[2].name`, `person.disabled[0]`).
fn tagged<'a>(
    fields_path: &str,
    fields: &'a [FieldShape],
    names_path: &str,
    names: &'a [Name],
) -> Vec<(String, &'a str)> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| (format!("{fields_path}[{i}].name"), &*f.name))
        .chain(
            names
                .iter()
                .enumerate()
                .map(|(i, n)| (format!("{names_path}[{i}]"), &**n)),
        )
        .collect()
}

/// Refuses a name listed twice, naming both places. Tables are short: a scan is enough.
fn unique(names: &[(String, &str)]) -> Result<(), SchemaError> {
    for (i, (path, name)) in names.iter().enumerate() {
        if let Some((first, _)) = names[..i].iter().find(|(_, n)| n == name) {
            return Err(SchemaError::at(
                path,
                format!("{name:?} is listed twice (also at {first})"),
            ));
        }
    }
    Ok(())
}

fn builder_owned(path: &str, name: &str) -> SchemaError {
    SchemaError::at(
        path,
        format!("{name:?} is written by the document builder; it cannot be an input name"),
    )
}

/// An envelope key: any non-empty string (it is never a mapping path). Absent keeps `default`.
fn key(value: Option<String>, default: &Name, path: &str) -> Result<Name, SchemaError> {
    match value {
        None => Ok(default.clone()),
        Some(name) if name.is_empty() => Err(SchemaError::at(path, "empty name")),
        Some(name) => Ok(Cow::Owned(name)),
    }
}

/// A record field name, absent keeping `default`.
fn field_name(value: Option<String>, default: &Name, path: &str) -> Result<Name, SchemaError> {
    value.map_or_else(|| Ok(default.clone()), |name| record_name(name, path))
}

/// A record field name: non-empty and without a `.`, which Elasticsearch reads as an object
/// path, so the mapping check could not tell `a.b` from `b` inside `a`.
fn record_name(name: String, path: &str) -> Result<Name, SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::at(path, "empty name"));
    }
    if name.contains('.') {
        return Err(SchemaError::at(
            path,
            format!("{name:?} contains '.', which Elasticsearch reads as an object path"),
        ));
    }
    Ok(Cow::Owned(name))
}

fn table(
    value: Option<Vec<FieldFile>>,
    default: &'static [FieldShape],
    path: &str,
) -> Result<Cow<'static, [FieldShape]>, SchemaError> {
    let Some(fields) = value else {
        return Ok(Cow::Borrowed(default));
    };
    fields
        .into_iter()
        .enumerate()
        .map(|(i, f)| {
            let name = record_name(f.name, &format!("{path}[{i}].name"))?;
            Ok(FieldShape::new(name, f.kind))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Cow::Owned)
}

fn names(
    value: Option<Vec<String>>,
    default: &'static [Name],
    path: &str,
) -> Result<Cow<'static, [Name]>, SchemaError> {
    let Some(list) = value else {
        return Ok(Cow::Borrowed(default));
    };
    list.into_iter()
        .enumerate()
        .map(|(i, name)| record_name(name, &format!("{path}[{i}]")))
        .collect::<Result<Vec<_>, _>>()
        .map(Cow::Owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(text: &str) -> String {
        FeedSchema::from_json(text).unwrap_err().to_string()
    }

    #[test]
    fn the_builtin_schema_round_trips_and_an_empty_file_is_builtin() {
        let builtin = FeedSchema::builtin();
        assert!(builtin.is_builtin());
        assert_eq!(builtin.envelope_id(), "id");
        assert_eq!(builtin.envelope_record(), "record");
        assert_eq!(builtin.references(), "roles");
        assert_eq!(builtin.reference_id(), "organization_id");
        assert_eq!(builtin.builder_owned(), crate::shape::BUILDER_OWNED);
        let back = FeedSchema::from_json(&builtin.to_json().to_string()).unwrap();
        assert_eq!(&back, builtin);
        assert!(back.is_builtin());
        assert!(FeedSchema::from_json("{}").unwrap().is_builtin());
        assert!(FeedSchema::default().is_builtin());
    }

    #[test]
    fn a_file_renames_and_replaces_whole_tables() {
        let schema = FeedSchema::from_json(
            r#"{
                "envelope": {"id": "key", "record": "payload"},
                "person": {
                    "references": "positions",
                    "reference_id": "company_ref",
                    "fields": [
                        {"name": "given_name", "kind": "string"},
                        {"name": "positions", "kind": "object_array"}
                    ],
                    "reference_fields": [{"name": "company_ref", "kind": "number"}],
                    "disabled": []
                },
                "organization": {"disabled": ["offices"]}
            }"#,
        )
        .unwrap();
        assert!(!schema.is_builtin());
        assert_eq!(schema.envelope_id(), "key");
        assert_eq!(schema.envelope_record(), "payload");
        assert_eq!(schema.references(), "positions");
        assert_eq!(schema.reference_id(), "company_ref");
        assert_eq!(schema.person_fields().len(), 2);
        assert_eq!(schema.person_disabled(), [] as [Name; 0]);
        assert_eq!(schema.organization_fields(), ORGANIZATION_FIELDS);
        assert_eq!(schema.organization_disabled(), ["offices"]);
        assert_eq!(
            schema.builder_owned(),
            [
                "organizations",
                "unresolved_organization_ids",
                "positions.organization_resolved"
            ]
        );
        let back = FeedSchema::from_json(&schema.to_json().to_string()).unwrap();
        assert_eq!(back, schema);
    }

    /// Schema files that are refused, with a part of the error that names the field.
    const BAD_FILES: &[(&str, &str)] = &[
        ("[]", "the schema: expected an object"),
        (
            r#"{"envelope": ["id", "record"]}"#,
            "envelope: expected an object",
        ),
        (
            r#"{"person": {"fields": [["roles", "object_array"]]}}"#,
            "person.fields[0]: expected an object",
        ),
        (r#"{"person": {"fields": {}}}"#, "invalid type: map"),
        (r#"{"person": {"references": 3}}"#, "invalid type: integer"),
        ("{", "EOF while parsing"),
        (r#"{"envelope": {"idd": "x"}}"#, "unknown field `idd`"),
        (r#"{"people": {}}"#, "unknown field `people`"),
        (
            r#"{"person": {"fields": [{"name": "a", "kind": "float"}]}}"#,
            "unknown variant `float`",
        ),
        (
            r#"{"person": {"fields": [{"name": "a", "kind": "string", "x": 1}]}}"#,
            "unknown field `x`",
        ),
        (r#"{"envelope": {"id": ""}}"#, "envelope.id: empty name"),
        (
            r#"{"envelope": {"id": "record"}}"#,
            "envelope.record: \"record\" is also envelope.id",
        ),
        (
            r#"{"person": {"references": ""}}"#,
            "person.references: empty name",
        ),
        (
            r#"{"person": {"fields": [{"name": "", "kind": "string"}]}}"#,
            "person.fields[0].name: empty name",
        ),
        (
            r#"{"person": {"fields": [{"name": "a.b", "kind": "string"}]}}"#,
            "person.fields[0].name: \"a.b\" contains '.'",
        ),
        (
            r#"{"person": {"fields": [{"name": "roles", "kind": "object_array"},
                {"name": "x", "kind": "string"}, {"name": "x", "kind": "number"}]}}"#,
            "person.fields[2].name: \"x\" is listed twice (also at person.fields[1].name)",
        ),
        (
            r#"{"person": {"fields": [{"name": "roles", "kind": "object_array"}],
                "disabled": ["roles"]}}"#,
            "person.disabled[0]: \"roles\" is listed twice (also at person.fields[0].name)",
        ),
        (
            r#"{"person": {"disabled": ["organizations"]}}"#,
            "person.disabled[0]: \"organizations\" is written by the document builder",
        ),
        (
            r#"{"person": {"references": "unresolved_organization_ids"}}"#,
            "person.references: \"unresolved_organization_ids\" is written by the document builder",
        ),
        (
            r#"{"person": {"fields": [{"name": "roles", "kind": "object_array"},
                {"name": "organizations", "kind": "string"}]}}"#,
            "person.fields[1].name: \"organizations\" is written by the document builder",
        ),
        (
            r#"{"person": {"reference_fields": [{"name": "organization_resolved", "kind": "boolean"}]}}"#,
            "person.reference_fields[0].name: \"organization_resolved\" is written by the document builder",
        ),
        (
            r#"{"person": {"reference_id": "organization_resolved"}}"#,
            "person.reference_id: \"organization_resolved\" is written",
        ),
        (
            r#"{"person": {"references": "positions"}}"#,
            "person.references: \"positions\" is not listed in person.fields",
        ),
        (
            r#"{"person": {"fields": [{"name": "roles", "kind": "string"}]}}"#,
            "person.fields[0].kind: the references field \"roles\" must be \"object_array\"",
        ),
        (
            r#"{"person": {"fields": [{"name": "roles", "kind": "object_array"},
                {"name": "jobs", "kind": "object_array"}]}}"#,
            "person.fields[1].kind: \"object_array\" is only for the references field",
        ),
        (
            r#"{"organization": {"fields": [{"name": "offices", "kind": "object_array"}]}}"#,
            "organization.fields[0].kind: \"object_array\" is only for the references field",
        ),
        (
            r#"{"organization": {"fields": [{"name": "name", "kind": "string"}], "disabled": ["name"]}}"#,
            "organization.disabled[0]: \"name\" is listed twice (also at organization.fields[0].name)",
        ),
        (
            r#"{"organization": {"disabled": ["a", ""]}}"#,
            "organization.disabled[1]: empty name",
        ),
        (
            r#"{"person": {"reference_fields": [{"name": "title", "kind": "string"},
                {"name": "organization_id", "kind": "lenient_long"}]}}"#,
            "person.reference_fields[1].kind: the organization id \"organization_id\" \
             (person.reference_id) is read as an id: its kind must be \"number\" or \"string\"",
        ),
        (
            r#"{"person": {"reference_id": "org",
                "reference_fields": [{"name": "org", "kind": "string_array"}]}}"#,
            "person.reference_fields[0].kind: the organization id \"org\"",
        ),
    ];

    #[test]
    fn the_organization_id_may_be_listed_as_a_number_or_a_string() {
        for kind in ["number", "string"] {
            let text = format!(
                r#"{{"person": {{"reference_fields": [{{"name": "organization_id", "kind": "{kind}"}}]}}}}"#
            );
            FeedSchema::from_json(&text).unwrap_or_else(|e| panic!("{kind}: {e}"));
        }
        // Another field of the same name elsewhere is not the organization id.
        FeedSchema::from_json(
            r#"{"organization": {"fields": [{"name": "organization_id", "kind": "lenient_long"}]}}"#,
        )
        .unwrap();
    }

    #[test]
    fn bad_files_are_refused_naming_the_field() {
        for (text, want) in BAD_FILES {
            let got = err(text);
            assert!(got.contains(want), "{text}\n  got: {got}\n want: {want}");
        }
        // Builder names are input names on the organization side, like any unknown field.
        FeedSchema::from_json(
            r#"{"organization": {"fields": [{"name": "organizations", "kind": "string"}]}}"#,
        )
        .unwrap();
    }
}
