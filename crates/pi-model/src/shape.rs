//! The JSON kind each mapped field must have, checked at extract time.
//!
//! The index mapping (`crates/pi-es/src/mapping.json`) gives a type to some of the record's
//! fields. A value Elasticsearch cannot read as that type (an object where a number or a string
//! is mapped, a string where `roles` expects objects) makes the bulk item fail with a mapper
//! error, which would fail the whole load. So extraction checks those fields first and
//! quarantines the record as `record`, naming the field: one bad record costs that record, not
//! the run.
//!
//! Identifiers are strict ([`JsonKind::Number`]): an `id` or `organization_id` that is not a
//! number, or a string the id rule accepts ([`crate::parse_decimal_id`]), is quarantined. Measures are lenient ([`JsonKind::LenientLong`],
//! [`JsonKind::LenientInteger`], [`JsonKind::LenientBoolean`]): their mapping has
//! `ignore_malformed`, so Elasticsearch skips a scalar it cannot read as the field's type (the
//! value is not indexed and the field is listed in the document's `_ignored`) and keeps the
//! document, searchable by its other fields. Extraction lets the record through and counts
//! the values Elasticsearch skips ([`LineStats`]), following the rules Elasticsearch 9.5.5 was
//! observed to apply ([`crate::es_number`] for numbers: `long` strings as exact decimals,
//! `integer` values as Java doubles with the range checked before truncation, JSON floats in a
//! `long` compared as doubles), with one known divergence: a non-ASCII decimal digit in a
//! `long` string is counted, though Elasticsearch indexes it. An object still fails those
//! fields, `ignore_malformed` or not, so it is quarantined there too.
//!
//! The tables below list every mapped field Elasticsearch parses, with the JSON kind it expects.
//! A test in `pi-es` walks `mapping.json` and checks that the two agree, so they cannot drift.
//! Fields the mapping disables (`enabled: false`) accept any value and are not listed; neither
//! are the fields the document builder writes itself ([`BUILDER_OWNED`]), since an input value
//! of that name is replaced. `null` is always allowed, at any level: Elasticsearch skips it.
//! When a key repeats in a person, role or organization object, only its last occurrence is
//! checked, the one the document keeps.

use std::fmt::{self, Write as _};

use serde_json::value::RawValue;

use crate::error::{MalformedKind, MalformedRecord};
use crate::es_number::{self, NumericType, Reading};
use crate::id::parse_decimal_id;
use crate::line_stats::LineStats;
use crate::raw::{RawObject, is_null};

/// The JSON kind a mapped field expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonKind {
    /// A number, or a string of ASCII decimal digits within `u64` (the id rule,
    /// [`crate::parse_decimal_id`]; Elasticsearch coerces it): `long`, `integer`. Strict, for
    /// identifiers: anything else is quarantined.
    Number,
    /// `true` or `false`, or the strings `"true"`, `"false"` and `""` (Elasticsearch reads the
    /// empty string as false): `boolean`. Strict: anything else is quarantined.
    Boolean,
    /// A `long` field with `ignore_malformed`: any scalar or an array of scalars (nested arrays
    /// included). Each scalar Elasticsearch skips is counted ([`LineStats`]) by the rules of
    /// `es_number`: a boolean, a string other than `""` that is not an exact decimal
    /// or whose integer part is outside `i64`, a JSON integer outside `i64`, a JSON float
    /// beyond `±2^63` as a double. Only an object, alone or inside an array, is quarantined.
    LenientLong,
    /// An `integer` field with `ignore_malformed`: as [`Self::LenientLong`], but a value is
    /// read as a double (a string by Java's `Double.parseDouble`) and skipped when outside
    /// `i32` before truncation.
    LenientInteger,
    /// A `boolean` field with `ignore_malformed`: as [`Self::LenientLong`], with the values
    /// [`Self::Boolean`] accepts as the only ones Elasticsearch indexes.
    LenientBoolean,
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
            Self::LenientLong
            | Self::LenientInteger
            | Self::LenientBoolean
            | Self::String
            | Self::StringArray => "a string, number or boolean",
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
    field("connections", JsonKind::LenientLong),
    field("followers", JsonKind::LenientLong),
    field("open_to_work", JsonKind::LenientBoolean),
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
    field("employee_count", JsonKind::LenientLong),
    field("founded_year", JsonKind::LenientInteger),
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
/// is (`None` for a whole record), so the reason names the field as `roles[3].title`. The
/// values of lenient fields that Elasticsearch will skip are counted in `stats`.
pub(crate) fn check_fields(
    object: &RawObject<'_>,
    fields: &[FieldShape],
    role: Option<usize>,
    stats: &mut LineStats,
) -> Result<(), MalformedRecord> {
    let prefix = role.map(|i| format!("roles[{i}].")).unwrap_or_default();
    for shape in fields {
        if let Some(value) = object.get(shape.name) {
            let mut ignored = |at: &[usize], found| {
                stats.ignore(|| format!("{prefix}{}{}", shape.name, indices(at)), found);
            };
            check_value(value, shape.kind, &mut ignored)
                .map_err(|(at, found)| mismatch(&prefix, *shape, &at, found))?;
        }
    }
    Ok(())
}

/// What a value was, in words: the element indices (for an array) and the kind found.
type Mismatch = (String, &'static str);

/// Told of each scalar of a lenient field the index skips: its element indices and kind.
type OnIgnored<'a> = dyn FnMut(&[usize], &'static str) + 'a;

fn check_value(
    value: &RawValue,
    kind: JsonKind,
    ignored: &mut OnIgnored<'_>,
) -> Result<(), Mismatch> {
    if is_null(value) {
        return Ok(());
    }
    let text = value.get();
    match kind {
        JsonKind::Number | JsonKind::Boolean if parses(text, kind) => Ok(()),
        JsonKind::Number | JsonKind::Boolean => Err((String::new(), found(text, kind))),
        JsonKind::LenientLong | JsonKind::LenientInteger | JsonKind::LenientBoolean => {
            walk_scalars(text, &mut Vec::new(), &mut |scalar, at| {
                if let Some(found) = skipped(scalar, kind) {
                    ignored(at, found);
                }
            })
        }
        JsonKind::String | JsonKind::StringArray => {
            walk_scalars(text, &mut Vec::new(), &mut |_, _| {})
        }
        JsonKind::ObjectArray => check_elements(text, '{'),
    }
}

/// Walks `text`, a scalar or an array of them (nested arrays included), calling `scalar` with
/// each non-null scalar and the element indices that lead to it; an object at any depth is a
/// mismatch.
fn walk_scalars(
    text: &str,
    at: &mut Vec<usize>,
    scalar: &mut dyn FnMut(&str, &[usize]),
) -> Result<(), Mismatch> {
    if text.starts_with('{') {
        return Err((indices(at), "an object"));
    }
    if !text.starts_with('[') {
        if text != "null" {
            scalar(text, at);
        }
        return Ok(());
    }
    let Ok(items) = serde_json::from_str::<Vec<&RawValue>>(text) else {
        return Err((indices(at), "an array"));
    };
    for (index, item) in items.iter().enumerate() {
        at.push(index);
        walk_scalars(item.get(), at, scalar)?;
        at.pop();
    }
    Ok(())
}

/// Element indices as a reason writes them: `[1][0]`, or nothing for the value itself.
fn indices(at: &[usize]) -> String {
    let mut out = String::new();
    for i in at {
        // Writing to a String cannot fail.
        let _ = write!(out, "[{i}]");
    }
    out
}

/// Whether the scalar `text` passes a strict kind (true for every other kind).
fn parses(text: &str, kind: JsonKind) -> bool {
    match kind {
        JsonKind::Number => {
            is_number(text) || string_value(text).is_some_and(|s| parse_decimal_id(&s).is_some())
        }
        JsonKind::Boolean | JsonKind::LenientBoolean => {
            matches!(text, "true" | "false")
                || string_value(text).is_some_and(|s| matches!(s.as_str(), "true" | "false" | ""))
        }
        _ => true,
    }
}

/// What the scalar `text` is, in words, when Elasticsearch skips it in a lenient field of
/// `kind`; `None` when it indexes it.
fn skipped(text: &str, kind: JsonKind) -> Option<&'static str> {
    let ty = match kind {
        JsonKind::LenientLong => NumericType::Long,
        JsonKind::LenientInteger => NumericType::Integer,
        JsonKind::LenientBoolean => return (!parses(text, kind)).then(|| found(text, kind)),
        _ => return None,
    };
    let string = text.starts_with('"');
    match es_number::read(text, ty) {
        Reading::Indexed => None,
        Reading::NotANumber => Some(found(text, kind)),
        Reading::OutOfRange if string => Some("a string holding a number out of range"),
        Reading::OutOfRange => Some("a number out of range"),
    }
}

/// Every element of the array `text` is null or starts with `first`.
fn check_elements(text: &str, first: char) -> Result<(), Mismatch> {
    let Ok(items) = serde_json::from_str::<Vec<&RawValue>>(text) else {
        return Err((String::new(), found(text, JsonKind::String)));
    };
    for (index, item) in items.iter().enumerate() {
        if !is_null(item) && !item.get().starts_with(first) {
            return Err((indices(&[index]), found(item.get(), JsonKind::String)));
        }
    }
    Ok(())
}

/// The kind of a raw JSON value in words; `expected` refines the description of a string.
fn found(text: &str, expected: JsonKind) -> &'static str {
    let number = matches!(
        expected,
        JsonKind::Number | JsonKind::LenientLong | JsonKind::LenientInteger
    );
    let boolean = matches!(expected, JsonKind::Boolean | JsonKind::LenientBoolean);
    match text.as_bytes().first() {
        Some(b'"') if number => "a string that is not a number",
        Some(b'"') if boolean => "a string other than \"true\" or \"false\"",
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

fn mismatch(prefix: &str, shape: FieldShape, at: &str, found: &str) -> MalformedRecord {
    let expected = match (at.is_empty(), shape.kind) {
        (false, JsonKind::ObjectArray) => "an object",
        (_, kind) => kind.describe(),
    };
    MalformedRecord::new(
        MalformedKind::Record,
        format!(
            "{prefix}{}{at}: expected {expected}, found {found}",
            shape.name
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::line_stats::IgnoredValue;

    fn check(record: &str, fields: &[FieldShape]) -> Result<LineStats, MalformedRecord> {
        let mut stats = LineStats::default();
        check_fields(&RawObject::parse(record).unwrap(), fields, None, &mut stats)?;
        Ok(stats)
    }

    #[test]
    fn valid_values_and_nulls_pass() {
        for record in [
            r#"{"id":5,"connections":-3,"followers":1.5e3,"open_to_work":true}"#,
            r#"{"id":"12","followers":"-7.5","connections":1e18,"open_to_work":"false"}"#,
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
            // Measures take arrays too; values that parse are not counted.
            r#"{"followers":[5,"6",[null]],"connections":[],"open_to_work":[true,"false"]}"#,
        ] {
            let stats = check(record, PERSON_FIELDS).unwrap_or_else(|e| panic!("{record}: {e}"));
            assert_eq!(stats, LineStats::default(), "{record}");
        }
    }

    #[test]
    fn lenient_fields_count_the_values_the_index_skips() {
        for (record, count, first, kind) in [
            (
                r#"{"followers":"n/a"}"#,
                1,
                "followers",
                "a string that is not a number",
            ),
            (r#"{"followers":[5]}"#, 0, "", ""),
            (
                r#"{"followers":["x",3]}"#,
                1,
                "followers[0]",
                "a string that is not a number",
            ),
            (
                r#"{"open_to_work":"yes"}"#,
                1,
                "open_to_work",
                "a string other than \"true\" or \"false\"",
            ),
            (r#"{"open_to_work":[true]}"#, 0, "", ""),
            (r#"{"open_to_work":null,"followers":null}"#, 0, "", ""),
            (r#"{"open_to_work":1}"#, 1, "open_to_work", "a number"),
            (r#"{"connections":true}"#, 1, "connections", "a boolean"),
            // Every element counts; the first in table order is the one named.
            (
                r#"{"open_to_work":"no","followers":" 7","connections":[1,["7 8",[false]]]}"#,
                4,
                "connections[1][0]",
                "a string that is not a number",
            ),
        ] {
            let stats = check(record, PERSON_FIELDS).unwrap_or_else(|e| panic!("{record}: {e}"));
            assert_eq!(stats.ignored_by_index, count, "{record}");
            let want = (count > 0).then(|| IgnoredValue {
                field: first.to_owned(),
                found: kind,
            });
            assert_eq!(stats.first_ignored, want, "{record}");
        }
    }

    // The observed rows of `counts_what_elasticsearch_9_5_5_skips`: (JSON value, skipped).
    const LONG: &[(&str, bool)] = &[
        ("5", false),
        ("5.7", false),
        (r#""5""#, false),
        (r#""5.0""#, false),
        (r#""1e3""#, false),
        (r#""+5""#, false),
        (r#"".5""#, false),
        (r#""5.""#, false),
        (r#""007""#, false),
        (r#""""#, false),
        ("-0", false),
        ("[]", false),
        ("9223372036854775807", false),
        (r#"" 5""#, true),
        (r#""5 ""#, true),
        (r#""7 8""#, true),
        (r#""n/a""#, true),
        ("true", true),
        ("9223372036854775808", true),
        ("1e19", true),
        (r#"[5,"x"]"#, true),
        // JSON numbers with a fraction or exponent compare as doubles against 2^63.
        ("9223372036854775808.0", false),
        ("922337203685477580.8e1", false),
        ("-9223372036854775809.0", false),
        ("9223372036854775807.9", false),
        // Strings are exact decimals, truncated; the exponent must fit an `int`.
        (r#""9223372036854775807.9""#, false),
        (r#""-9223372036854775808.9""#, false),
        (r#""0e99999999999""#, true),
        (r#""1e-3000000000""#, true),
        // Stored as 9223372036854775807: both round to 2^63 as doubles.
        ("9.223372036854775807e18", false),
        ("9.2233720368547758e18", false),
        ("9.223372036854777e18", true),
        // Unlike `integer`, no trimming and no Java suffix.
        (r#""   ""#, true),
        (r#""5d""#, true),
        // `BigDecimal`'s limits: the exponent and the scale (fraction digits as written
        // minus the exponent) within `int`, and the scale at least -19, whatever the value.
        (r#""0.5e-2147483647""#, true),
        (r#""0.0e-2147483647""#, true),
        (r#""0e2147483647""#, true),
        (r#""0e20""#, true),
        (r#""0e100""#, true),
        (r#""0e999""#, true),
        (r#""0e1000""#, true),
        (r#""0e1000000""#, true),
        (r#""0e100000000""#, true),
        (r#""0e600000000""#, true),
        (r#""0e646456992""#, true),
        (r#""0e646456993""#, true),
        (r#""0e700000000""#, true),
        (r#""0e1500000000""#, true),
        (r#""0.0e1000""#, true),
        (r#""0.00e2147483647""#, true),
        (r#""0.0e21""#, true),
        (r#""00e20""#, true),
        (r#""-0e20""#, true),
        (r#""0E20""#, true),
        // Stored values in brackets.
        (r#""1e-2147483647""#, false), // [0]
        (r#""1e-600000000""#, false),  // [0]
        (r#""1e-500000000""#, false),  // [0]
        (r#""1e-100000000""#, false),  // [0]
        (r#""5e-1""#, false),          // [0]
        (r#""0e0""#, false),           // [0]
        (r#""0e1""#, false),           // [0]
        (r#""0e2""#, false),           // [0]
        (r#""0e18""#, false),          // [0]
        (r#""0e19""#, false),          // [0]
        (r#""0.0e1""#, false),         // [0]
        (r#""0.0e5""#, false),         // [0]
        (r#""00e5""#, false),          // [0]
        (r#""-0e5""#, false),          // [0]
        (r#""0e-5""#, false),          // [0]
        (r#""1e18""#, false),          // [1000000000000000000]
        (r#""5e0""#, false),           // [5]
        (r#""1e2""#, false),           // [100]
        (r#""1.5e2""#, false),         // [150]
        (r#""0.5e1""#, false),         // [5]
        (r#""10e17""#, false),         // [1000000000000000000]
        (r#""9.2e18""#, false),        // [9200000000000000000]
        (r#""0.0e20""#, false),        // [0]
        (r#""0.00e21""#, false),       // [0]
        (r#""0.000e22""#, false),      // [0]
        (r#""0.5e19""#, false),        // [5000000000000000000]
        (r#""0.05e20""#, false),       // [5000000000000000000]
    ];
    const INTEGER: &[(&str, bool)] = &[
        ("2147483647", false),
        ("1.9", false),
        ("2147483648", true),
        ("3000000000", true),
        (r#""3000000000""#, true),
        // Strings go through Java's `Double.parseDouble`.
        (r#"" 5""#, false),
        (r#""5 ""#, false),
        (r#"" 1999""#, false),
        (r#""1999""#, false),
        (r#""NaN""#, false),
        (r#""5d""#, false),
        (r#""5f""#, false),
        (r#""0x1p3""#, false),
        (r#""Infinity""#, true),
        // The range is checked before truncation, for strings and JSON numbers alike.
        (r#""2147483647.5""#, true),
        ("2147483647.5", true),
        ("-2147483648.5", true),
        (r#""-2147483648.9""#, true),
        ("2147483647.9", true),
        // Further Java-double forms; the stored value in brackets.
        (r#""0x1p30""#, false),          // [1073741824]
        (r#""0x1.fffffffcp30""#, false), // [2147483647]
        (r#""1e1d""#, false),            // [10]
        (r#""5.5e0f""#, false),          // [5]
        (r#""\t7""#, false),             // a tab before 7 [7]
        (r#""+NaN""#, false),            // [0], as "NaN"
        (r#""1e9""#, false),             // [1000000000]
        (r#""   ""#, true),              // trimmed to nothing: not a number
        (r#""-Infinity""#, true),
        (r#""0x1p31""#, true),
        (r#""0x""#, true),
        (r#""1_000""#, true),
        (r#""2147483647.4""#, true),
        ("-2147483648.4", true),
        (r#""1e10""#, true),
    ];
    const BOOLEAN: &[(&str, bool)] = &[
        ("true", false),
        ("false", false),
        (r#""true""#, false),
        (r#""false""#, false),
        (r#""""#, false),
        (r#""yes""#, true),
        (r#""TRUE""#, true),
        ("1", true),
        ("0", true),
        (r#""1""#, true),
        (r#"[true,"x"]"#, true),
    ];

    /// What Elasticsearch 9.5.5 did with each value, observed against a real 9.5.5 instance
    /// (fields mapped `long`, `integer` and `boolean` with `ignore_malformed: true`, `_ignored`
    /// read back per document): `false` means indexed (not in `_ignored`), `true` skipped (the
    /// field listed in `_ignored`). The count must match every row; the one known divergence
    /// is pinned apart below.
    #[test]
    fn counts_what_elasticsearch_9_5_5_skips() {
        for (field, fields, table) in [
            ("followers", PERSON_FIELDS, LONG),
            ("employee_count", ORGANIZATION_FIELDS, LONG),
            ("founded_year", ORGANIZATION_FIELDS, INTEGER),
            ("open_to_work", PERSON_FIELDS, BOOLEAN),
        ] {
            for (value, skipped) in table {
                let record = format!(r#"{{"{field}":{value}}}"#);
                let stats = check(&record, fields).unwrap_or_else(|e| panic!("{record}: {e}"));
                assert_eq!(stats.ignored_by_index, u64::from(*skipped), "{record}");
            }
        }
        // The known divergence: Elasticsearch 9.5.5 indexes a `long` string of a non-ASCII
        // decimal digit (U+0665, ARABIC-INDIC DIGIT FIVE); the grammar here is ASCII only, so
        // the counter counts it.
        let stats = check("{\"followers\":\"\u{0665}\"}", PERSON_FIELDS).unwrap();
        assert_eq!(stats.ignored_by_index, 1);
    }

    #[test]
    fn a_repeated_lenient_key_counts_its_last_occurrence() {
        for (record, count) in [
            (r#"{"followers":{"n":1},"followers":5}"#, 0),
            (r#"{"followers":5,"followers":"n/a"}"#, 1),
            (r#"{"followers":"n/a","followers":5}"#, 0),
        ] {
            let stats = check(record, PERSON_FIELDS).unwrap_or_else(|e| panic!("{record}: {e}"));
            assert_eq!(stats.ignored_by_index, count, "{record}");
        }
        let err = check(r#"{"followers":5,"followers":{"n":1}}"#, PERSON_FIELDS).unwrap_err();
        assert!(err.reason().starts_with("followers:"), "{err}");
    }

    #[test]
    fn mismatches_name_the_field() {
        for (record, reason) in [
            (
                r#"{"id":"abc"}"#,
                "id: expected a number, found a string that is not a number",
            ),
            (r#"{"id":true}"#, "id: expected a number, found a boolean"),
            (
                r#"{"id":5,"id":[5]}"#,
                "id: expected a number, found an array",
            ),
            (
                r#"{"followers":{"n":1}}"#,
                "followers: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"connections":[1,[2,{"n":3}]]}"#,
                "connections[1][1]: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"open_to_work":{"value":true}}"#,
                "open_to_work: expected a string, number or boolean, found an object",
            ),
            (
                r#"{"open_to_work":["yes",{}]}"#,
                "open_to_work[1]: expected a string, number or boolean, found an object",
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
        let mut stats = LineStats::default();
        let object = RawObject::parse(r#"{"title":{"x":1}}"#).unwrap();
        let err = check_fields(&object, ROLE_FIELDS, Some(2), &mut stats).unwrap_err();
        assert_eq!(
            err.reason(),
            "roles[2].title: expected a string, number or boolean, found an object"
        );
        let object = RawObject::parse(r#"{"id":"abc"}"#).unwrap();
        let err = check_fields(&object, ROLE_FIELDS, Some(0), &mut stats).unwrap_err();
        assert_eq!(
            err.reason(),
            "roles[0].id: expected a number, found a string that is not a number"
        );
    }

    #[test]
    fn organization_fields() {
        let stats = check(
            r#"{"id":1,"name":"A","employee_count":"40","keywords":["k"],"addresses":"x"}"#,
            ORGANIZATION_FIELDS,
        )
        .unwrap();
        assert_eq!(stats.ignored_by_index, 0);
        let stats = check(
            r#"{"founded_year":"long ago","employee_count":["n/a",12]}"#,
            ORGANIZATION_FIELDS,
        )
        .unwrap();
        assert_eq!(stats.ignored_by_index, 2);
        let first = stats.first_ignored.unwrap();
        assert_eq!(first.field, "employee_count[0]");
        let err = check(r#"{"founded_year":{"year":1999}}"#, ORGANIZATION_FIELDS).unwrap_err();
        assert!(err.reason().starts_with("founded_year:"), "{err}");
        let err = check(r#"{"id":"abc"}"#, ORGANIZATION_FIELDS).unwrap_err();
        assert_eq!(
            err.reason(),
            "id: expected a number, found a string that is not a number"
        );
    }

    #[test]
    fn id_strings_follow_the_id_rule() {
        let cases = [
            (PERSON_FIELDS, "id", None),
            (ORGANIZATION_FIELDS, "id", None),
            (ROLE_FIELDS, "id", Some(0)),
            (ROLE_FIELDS, "organization_id", Some(0)),
        ];
        for (fields, name, role) in cases {
            let prefix = role.map_or(String::new(), |i| format!("roles[{i}]."));
            for value in [r#""007""#, r#""7""#, "7"] {
                let record = format!(r#"{{"{name}":{value}}}"#);
                let object = RawObject::parse(&record).unwrap();
                check_fields(&object, fields, role, &mut LineStats::default())
                    .unwrap_or_else(|e| panic!("{prefix}{name}={value}: {e}"));
            }
            for (value, found) in [
                (r#""7a""#, "a string that is not a number"),
                (r#"" 7""#, "a string that is not a number"),
                (r#""-1""#, "a string that is not a number"),
                (r#""""#, "a string that is not a number"),
                ("{}", "an object"),
            ] {
                let record = format!(r#"{{"{name}":{value}}}"#);
                let object = RawObject::parse(&record).unwrap();
                let err =
                    check_fields(&object, fields, role, &mut LineStats::default()).unwrap_err();
                assert_eq!(err.kind(), MalformedKind::Record, "{prefix}{name}={value}");
                assert_eq!(
                    err.reason(),
                    format!("{prefix}{name}: expected a number, found {found}"),
                );
            }
        }
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
