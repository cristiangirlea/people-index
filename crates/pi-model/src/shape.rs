//! The JSON kind each mapped field must have, checked at extract time.
//!
//! The index mapping (`crates/pi-es/src/mapping.json`) gives a type to some of the record's
//! fields. A value Elasticsearch cannot read as that type (`"yes"` for a boolean, a string where
//! an object is mapped) makes the bulk item fail with a mapper error, which would fail the whole
//! load. So extraction checks those fields first and quarantines the record as `record`, naming
//! the field: one bad record costs that record, not the run.
//!
//! The tables below list every mapped field Elasticsearch parses, with the JSON kind it expects.
//! A test in `pi-es` walks `mapping.json` and checks that the two agree, so they cannot drift.
//! Fields the mapping disables (`enabled: false`) accept any value and are not listed; neither
//! are the fields the document builder writes itself ([`BUILDER_OWNED`]), since an input value
//! of that name is replaced. `null` is always allowed, at any level: Elasticsearch skips it.
//! When a key repeats in a person, role or organization object, only its last occurrence is
//! checked, the one the document keeps.

use std::fmt;

use serde_json::value::RawValue;

use crate::error::{MalformedKind, MalformedRecord};
use crate::raw::{RawObject, is_null};

/// The JSON kind a mapped field expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    /// A number, or a string holding a JSON number (Elasticsearch coerces it): `long`, `integer`.
    Number,
    /// `true` or `false`, or the strings `"true"`, `"false"` and `""` (Elasticsearch reads the
    /// empty string as false): `boolean`.
    Boolean,
    /// One value: `text`, `keyword`, `date`. Elasticsearch takes any scalar (it reads a number
    /// or a boolean as text, and a date field ignores what it cannot parse) or an array of
    /// scalars; only an object fails it, alone or inside an array.
    String,
    /// A `keyword` field that holds a list; it takes the same values as [`Self::String`].
    StringArray,
    /// An array of objects (and nulls): an `object` field that holds a list.
    ObjectArray,
}

impl JsonKind {
    /// The kind in words, for a quarantine reason.
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            Self::Number => "a number",
            Self::Boolean => "a boolean",
            Self::String | Self::StringArray => "a string, number or boolean",
            Self::ObjectArray => "an array of objects",
        }
    }
}

impl fmt::Display for JsonKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.describe())
    }
}

/// One mapped field and the JSON kind it expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldShape {
    /// The field name inside its object.
    pub name: &'static str,
    /// The JSON kind it expects.
    pub kind: JsonKind,
}

const fn field(name: &'static str, kind: JsonKind) -> FieldShape {
    FieldShape { name, kind }
}

/// The checked top-level fields of a person record (mapping paths without a prefix).
pub const PERSON_FIELDS: &[FieldShape] = &[
    field("id", JsonKind::Number),
    field("first_name", JsonKind::String),
    field("last_name", JsonKind::String),
    field("full_name", JsonKind::String),
    field("headline", JsonKind::String),
    field("summary", JsonKind::String),
    field("country", JsonKind::String),
    field("region", JsonKind::String),
    field("city", JsonKind::String),
    field("industry", JsonKind::String),
    field("profile_url", JsonKind::String),
    field("skills", JsonKind::StringArray),
    field("connections", JsonKind::Number),
    field("followers", JsonKind::Number),
    field("open_to_work", JsonKind::Boolean),
    field("roles", JsonKind::ObjectArray),
];

/// The checked fields of each `roles[]` entry (mapping paths under `roles.`).
pub const ROLE_FIELDS: &[FieldShape] = &[
    field("id", JsonKind::Number),
    field("title", JsonKind::String),
    field("organization_id", JsonKind::Number),
    field("organization_name", JsonKind::String),
    field("organization_domain", JsonKind::String),
    field("employment_type", JsonKind::String),
    field("start_date", JsonKind::String),
    field("end_date", JsonKind::String),
    field("location", JsonKind::String),
    field("description", JsonKind::String),
];

/// The checked fields of an organization record (mapping paths under `organizations.`).
pub const ORGANIZATION_FIELDS: &[FieldShape] = &[
    field("id", JsonKind::Number),
    field("name", JsonKind::String),
    field("domain", JsonKind::String),
    field("industry", JsonKind::String),
    field("country", JsonKind::String),
    field("city", JsonKind::String),
    field("size_range", JsonKind::String),
    field("employee_count", JsonKind::Number),
    field("founded_year", JsonKind::Number),
    field("keywords", JsonKind::StringArray),
    field("technologies", JsonKind::StringArray),
    field("description", JsonKind::String),
];

/// The mapped paths the document builder writes itself; input values of these names are
/// replaced, so they are never checked.
pub const BUILDER_OWNED: &[&str] = &[
    "organizations",
    "unresolved_organization_ids",
    "roles.organization_resolved",
];

/// Checks the fields of `object` listed in `fields`; `role` is the index of the role `object`
/// is (`None` for a whole record), so the reason names the field as `roles[3].title`.
pub(crate) fn check_fields(
    object: &RawObject<'_>,
    fields: &[FieldShape],
    role: Option<usize>,
) -> Result<(), MalformedRecord> {
    for shape in fields {
        if let Some(value) = object.get(shape.name) {
            check_value(value, shape.kind)
                .map_err(|(at, found)| mismatch(role, *shape, at.as_deref(), found))?;
        }
    }
    Ok(())
}

/// What a value was, in words: the element index (for an array) and the kind found.
type Mismatch = (Option<String>, &'static str);

fn check_value(value: &RawValue, kind: JsonKind) -> Result<(), Mismatch> {
    if is_null(value) {
        return Ok(());
    }
    let text = value.get();
    let ok = match kind {
        JsonKind::Number => is_number(text) || string_value(text).is_some_and(|s| is_number(&s)),
        JsonKind::Boolean => {
            matches!(text, "true" | "false")
                || string_value(text).is_some_and(|s| matches!(s.as_str(), "true" | "false" | ""))
        }
        JsonKind::String | JsonKind::StringArray => return check_no_object(text),
        JsonKind::ObjectArray => return check_elements(text, '{'),
    };
    if ok {
        Ok(())
    } else {
        Err((None, found(text, kind)))
    }
}

/// `text` is a scalar or an array of them (nested arrays included): no object at any depth.
fn check_no_object(text: &str) -> Result<(), Mismatch> {
    if text.starts_with('{') {
        return Err((None, "an object"));
    }
    if !text.starts_with('[') {
        return Ok(());
    }
    let Ok(items) = serde_json::from_str::<Vec<&RawValue>>(text) else {
        return Err((None, "an array"));
    };
    for (index, item) in items.iter().enumerate() {
        check_no_object(item.get())
            .map_err(|(at, found)| (Some(format!("[{index}]{}", at.unwrap_or_default())), found))?;
    }
    Ok(())
}

/// Every element of the array `text` is null or starts with `first`.
fn check_elements(text: &str, first: char) -> Result<(), Mismatch> {
    let Ok(items) = serde_json::from_str::<Vec<&RawValue>>(text) else {
        return Err((None, found(text, JsonKind::String)));
    };
    for (index, item) in items.iter().enumerate() {
        if !is_null(item) && !item.get().starts_with(first) {
            return Err((
                Some(format!("[{index}]")),
                found(item.get(), JsonKind::String),
            ));
        }
    }
    Ok(())
}

/// The kind of a raw JSON value in words; `expected` refines the description of a string.
fn found(text: &str, expected: JsonKind) -> &'static str {
    match text.as_bytes().first() {
        Some(b'"') if expected == JsonKind::Number => "a string that is not a number",
        Some(b'"') if expected == JsonKind::Boolean => "a string other than \"true\" or \"false\"",
        Some(b'"') => "a string",
        Some(b'{') => "an object",
        Some(b'[') => "an array",
        Some(b't' | b'f') => "a boolean",
        _ => "a number",
    }
}

/// Whether `text` is exactly one JSON number literal (any size: it is not converted).
fn is_number(text: &str) -> bool {
    text.starts_with(|c: char| c == '-' || c.is_ascii_digit())
        && serde_json::from_str::<&RawValue>(text).is_ok_and(|v| v.get().len() == text.len())
}

/// The decoded string, when `text` is a JSON string.
fn string_value(text: &str) -> Option<String> {
    if text.starts_with('"') {
        serde_json::from_str::<String>(text).ok()
    } else {
        None
    }
}

fn mismatch(
    role: Option<usize>,
    shape: FieldShape,
    at: Option<&str>,
    found: &str,
) -> MalformedRecord {
    let path = role.map(|i| format!("roles[{i}].")).unwrap_or_default();
    let at = at.unwrap_or("");
    let expected = match (at.is_empty(), shape.kind) {
        (false, JsonKind::ObjectArray) => "an object",
        (_, kind) => kind.describe(),
    };
    MalformedRecord::new(
        MalformedKind::Record,
        format!(
            "{path}{}{at}: expected {expected}, found {found}",
            shape.name
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(record: &str, fields: &[FieldShape]) -> Result<(), MalformedRecord> {
        check_fields(&RawObject::parse(record).unwrap(), fields, None)
    }

    #[test]
    fn valid_values_and_nulls_pass() {
        for record in [
            r#"{"id":5,"connections":-3,"followers":1.5e3,"open_to_work":true}"#,
            r#"{"id":"12","followers":"-7.5","connections":1e400,"open_to_work":"false"}"#,
            r#"{"open_to_work":""}"#,
            r#"{"first_name":"A","skills":["x",null,"y"],"roles":[{},null]}"#,
            r#"{"skills":[],"roles":[]}"#,
            r#"{"id":null,"first_name":null,"skills":null,"open_to_work":null,"roles":null}"#,
            r#"{"unknown":{"deep":[1]},"memberships":"anything"}"#,
            r#"{"id":"abc","id":5}"#,
            // Elasticsearch reads a scalar of any kind as text, and takes an array anywhere.
            r#"{"city":3,"headline":true,"summary":1.5,"first_name":["A","B"]}"#,
            r#"{"skills":"rust","industry":[1,[false,null]]}"#,
            r#"{"skills":["a",7,true,null,["b"]]}"#,
        ] {
            check(record, PERSON_FIELDS).unwrap_or_else(|e| panic!("{record}: {e}"));
        }
    }

    #[test]
    fn mismatches_name_the_field() {
        for (record, reason) in [
            (
                r#"{"open_to_work":"yes"}"#,
                "open_to_work: expected a boolean, found a string other than \"true\" or \"false\"",
            ),
            (
                r#"{"open_to_work":1}"#,
                "open_to_work: expected a boolean, found a number",
            ),
            (
                r#"{"id":"abc"}"#,
                "id: expected a number, found a string that is not a number",
            ),
            (r#"{"id":true}"#, "id: expected a number, found a boolean"),
            (
                r#"{"followers":" 7"}"#,
                "followers: expected a number, found a string that is not a number",
            ),
            (
                r#"{"followers":"7 8"}"#,
                "followers: expected a number, found a string that is not a number",
            ),
            (
                r#"{"id":5,"id":[5]}"#,
                "id: expected a number, found an array",
            ),
            (
                r#"{"first_name":{"given":"A"}}"#,
                "first_name: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"city":["x",{"y":1}]}"#,
                "city[1]: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"skills":{"b":1}}"#,
                "skills: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"skills":["a",{"b":1}]}"#,
                "skills[1]: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"skills":["a",[null,{"b":1}]]}"#,
                "skills[1][1]: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"roles":"engineer"}"#,
                "roles: expected an array of objects, found a string",
            ),
            (
                r#"{"roles":[{},"x"]}"#,
                "roles[1]: expected an object, found a string",
            ),
        ] {
            let err = check(record, PERSON_FIELDS).unwrap_err();
            assert_eq!(err.kind(), MalformedKind::Record, "{record}");
            assert_eq!(err.reason(), reason, "{record}");
        }
    }

    #[test]
    fn the_path_prefixes_the_reason() {
        let object = RawObject::parse(r#"{"title":{"x":1}}"#).unwrap();
        let err = check_fields(&object, ROLE_FIELDS, Some(2)).unwrap_err();
        assert_eq!(
            err.reason(),
            "roles[2].title: expected a string, number or boolean, found an object"
        );
    }

    #[test]
    fn organization_fields() {
        check(
            r#"{"id":1,"name":"A","employee_count":"40","keywords":["k"],"addresses":"x"}"#,
            ORGANIZATION_FIELDS,
        )
        .unwrap();
        let err = check(r#"{"founded_year":"long ago"}"#, ORGANIZATION_FIELDS).unwrap_err();
        assert!(err.reason().starts_with("founded_year:"), "{err}");
    }

    #[test]
    fn tables_have_no_duplicates_or_builder_fields() {
        for (fields, prefix) in [
            (PERSON_FIELDS, ""),
            (ROLE_FIELDS, "roles."),
            (ORGANIZATION_FIELDS, "organizations."),
        ] {
            for (i, a) in fields.iter().enumerate() {
                assert!(
                    fields[i + 1..].iter().all(|b| b.name != a.name),
                    "{}",
                    a.name
                );
                let path = format!("{prefix}{}", a.name);
                assert!(!BUILDER_OWNED.contains(&path.as_str()), "{path}");
            }
        }
    }
}
