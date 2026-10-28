//! The explicit mapping of the output document. The reasons for each choice are in
//! `mapping.md` next to `mapping.json`.
//!
//! The built-in mapping is compiled in; a run may use another one (`PI_MAPPING_FILE`, read by
//! [`parse_mapping`]). Either way [`check_mapping`] holds it against the feed schema's tables
//! (`pi_model::FeedSchema`) at startup, so extraction checks exactly the fields Elasticsearch
//! parses, with the kinds their types expect.

use std::collections::{BTreeMap, BTreeSet};

use pi_model::{FeedSchema, JsonKind, ORGANIZATIONS_FIELD, UNRESOLVED_FIELD};
use serde_json::Value;

use crate::error::EsError;

/// `mapping.json`, embedded at compile time so the binary needs no file next to it.
pub const MAPPING_JSON: &str = include_str!("mapping.json");

/// The mapping as JSON, ready for the `mappings` key of a create-index request.
#[must_use]
pub fn mapping() -> Value {
    // The file is part of the crate and its parse is covered by `mapping_parses` below, so a
    // malformed mapping fails the test suite, never a run.
    serde_json::from_str(MAPPING_JSON).unwrap_or(Value::Null)
}

/// Reads a mapping file: the `mappings` body of a create-index request, as `mapping.json` is.
pub fn parse_mapping(text: &str) -> Result<Value, EsError> {
    let value: Value =
        serde_json::from_str(text).map_err(|e| EsError::Config(format!("not JSON: {e}")))?;
    if !value.is_object() {
        return Err(EsError::Config("expected a JSON object".into()));
    }
    Ok(value)
}

/// Checks that `mapping` and the tables of `schema` agree, the way extraction relies on:
///
/// - `dynamic` is `false` at the top (unknown fields stay in `_source`, unindexed) and nowhere
///   anything else;
/// - every field Elasticsearch parses (not disabled, not written by the document builder) is
///   in the schema's tables with a JSON kind its type accepts (`kind_fits`), and every table
///   entry is mapped; every `long`, `integer` and `date` field has `ignore_malformed: true`;
/// - every multi-field takes what its field lets through (`multi_field_fits`), and every
///   `copy_to` target is a `text` or `keyword` field;
/// - the fields disabled in the mapping (`enabled: false`) are exactly the schema's disabled
///   lists;
/// - the builder's fields are mapped with the types it writes: `organizations` an `object`,
///   `unresolved_organization_ids` a `long` with `ignore_malformed: true`,
///   `<references>.organization_resolved` a `boolean`.
///
/// Every problem is listed in the error, each naming the mapping path.
pub fn check_mapping(mapping: &Value, schema: &FeedSchema) -> Result<(), EsError> {
    let problems = mapping_problems(mapping, schema);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(EsError::Config(format!(
            "the index mapping does not fit the feed schema: {}",
            problems.join("; ")
        )))
    }
}

fn mapping_problems(mapping: &Value, schema: &FeedSchema) -> Vec<String> {
    let mut problems = Vec::new();
    if !mapping.is_object() {
        problems.push("the mapping is not a JSON object".to_owned());
        return problems;
    }
    if mapping.get("dynamic") != Some(&Value::Bool(false)) {
        problems.push(
            "\"dynamic\" must be false: unknown record fields stay in _source, unindexed".into(),
        );
    }
    let table = shape_table(schema);
    let disabled: BTreeSet<String> = schema
        .person_disabled()
        .iter()
        .map(ToString::to_string)
        .chain(
            schema
                .organization_disabled()
                .iter()
                .map(|name| format!("organizations.{name}")),
        )
        .collect();
    let owned = schema.builder_owned();
    let mut seen = BTreeSet::new();
    visit(mapping, "", &mut |path, node| {
        if node
            .get("dynamic")
            .is_some_and(|d| d != &Value::Bool(false))
        {
            problems.push(format!("{path}: \"dynamic\" must be false or absent"));
        }
        let is_disabled = node.get("enabled") == Some(&Value::Bool(false));
        let listed_disabled = disabled.contains(path);
        if is_disabled != listed_disabled {
            problems.push(if is_disabled {
                format!(
                    "{path} is disabled in the mapping but not listed as disabled in the schema"
                )
            } else {
                format!("{path} is listed as disabled in the schema but the mapping indexes it")
            });
        }
        if !is_disabled {
            problems.extend(copy_to_problems(mapping, path, node));
        }
        if let Some((kind, list)) = table.get(path) {
            seen.insert(path.to_owned());
            if !is_disabled {
                problems.extend(kind_problem(path, node, *kind, list));
                problems.extend(multi_field_problems(path, node, *kind, list));
            }
        } else if !is_disabled && !owned.contains(&path) {
            problems.push(format!(
                "{path} ({}) is mapped but the schema does not check it",
                describe(node)
            ));
        }
    });
    for (path, (_, list)) in &table {
        if !seen.contains(path) {
            problems.push(format!("{path} is in {list} but not mapped"));
        }
    }
    for path in &disabled {
        if field(mapping, path).is_null() {
            problems.push(format!(
                "{path} is listed as disabled in the schema but not mapped"
            ));
        }
    }
    // The unresolved ids are u64: one above `i64::MAX` must be skipped, not fail the document.
    for (path, ty, lenient, kind) in [
        (ORGANIZATIONS_FIELD, "object", false, JsonKind::ObjectArray),
        (UNRESOLVED_FIELD, "long", true, JsonKind::Number),
        (owned[2], "boolean", false, JsonKind::Boolean),
    ] {
        let node = field(mapping, path);
        problems.extend(multi_field_problems(
            path,
            node,
            kind,
            "the document builder",
        ));
        let ignores_malformed = node["ignore_malformed"] == Value::Bool(true);
        if node_type(node) != Some(ty) || (lenient && !ignores_malformed) {
            let want = if lenient {
                format!("{ty} with ignore_malformed")
            } else {
                ty.to_owned()
            };
            problems.push(format!(
                "{path} is written by the document builder and must be mapped as {want}, not {}",
                describe(node)
            ));
        }
    }
    problems
}

/// The schema's checked fields as mapping paths (references under `<references>.`, the
/// organization's under `organizations.`), each with its kind and the schema table it is in.
fn shape_table(schema: &FeedSchema) -> BTreeMap<String, (JsonKind, &'static str)> {
    let references = format!("{}.", schema.references());
    let mut table = BTreeMap::new();
    for (fields, prefix, list) in [
        (schema.person_fields(), "", "person.fields"),
        (
            schema.reference_fields(),
            references.as_str(),
            "person.reference_fields",
        ),
        (
            schema.organization_fields(),
            "organizations.",
            "organization.fields",
        ),
    ] {
        for shape in fields {
            table.insert(format!("{prefix}{}", shape.name), (shape.kind, list));
        }
    }
    table
}

// The definition of the field at `path` (`Null` when it is not mapped).
fn field<'a>(mapping: &'a Value, path: &str) -> &'a Value {
    let mut node = mapping;
    for part in path.split('.') {
        node = &node["properties"][part];
    }
    node
}

/// Calls `f` with the path and definition of every mapped field, depth first.
fn visit(node: &Value, prefix: &str, f: &mut dyn FnMut(&str, &Value)) {
    let Some(properties) = node.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (name, child) in properties {
        let path = format!("{prefix}{name}");
        f(&path, child);
        visit(child, &format!("{path}."), f);
    }
}

/// A field's type: as written, or `object` for one with `properties` and no type, as
/// Elasticsearch reads it.
fn node_type(node: &Value) -> Option<&str> {
    node.get("type")
        .and_then(Value::as_str)
        .or_else(|| node.get("properties").map(|_| "object"))
}

/// The type in words for a problem: `long with ignore_malformed`, `nothing` when unmapped.
fn describe(node: &Value) -> String {
    match node_type(node) {
        None if node.is_null() => "nothing".to_owned(),
        None => "no type".to_owned(),
        Some(ty) if node["ignore_malformed"] == Value::Bool(true) => {
            format!("{ty} with ignore_malformed")
        }
        Some(ty) => ty.to_owned(),
    }
}

/// The problem with mapping a field of `kind` (from the schema table `list`) as `node`, if any.
fn kind_problem(path: &str, node: &Value, kind: JsonKind, list: &str) -> Option<String> {
    fit_problem(path, node, kind, list, kind_fits)
}

/// The problem when `fits` refuses `node` for `kind`. The `ignore_malformed` message only when
/// adding it would make the field fit, so a field of the wrong type hears about its type first.
fn fit_problem(
    path: &str,
    node: &Value,
    kind: JsonKind,
    list: &str,
    fits: fn(&Value, JsonKind) -> bool,
) -> Option<String> {
    if fits(node, kind) {
        return None;
    }
    let mut lenient = node.clone();
    if let Some(definition) = lenient.as_object_mut() {
        definition.insert("ignore_malformed".into(), Value::Bool(true));
    }
    Some(if fits(&lenient, kind) {
        format!(
            "{path}: mapped as {} without \"ignore_malformed\": true, which every number and \
             date field needs (a value out of its range or format would fail the document)",
            describe(node)
        )
    } else {
        format!(
            "{path}: mapped as {}, which does not take the schema's kind {:?} ({list})",
            describe(node),
            kind.as_str()
        )
    })
}

/// The multi-fields of a field of `kind` (`"fields"`: each indexes the same value under
/// another type) that cannot take that value ([`multi_field_fits`]).
fn multi_field_problems(path: &str, node: &Value, kind: JsonKind, list: &str) -> Vec<String> {
    let Some(fields) = node.get("fields").and_then(Value::as_object) else {
        return Vec::new();
    };
    fields
        .iter()
        .filter_map(|(name, sub)| {
            fit_problem(&format!("{path}.{name}"), sub, kind, list, multi_field_fits)
        })
        .collect()
}

/// Whether a multi-field takes every value a field of `kind` lets through: a `text` or
/// `keyword` one takes any scalar, a `long`, `integer`, `date` or `boolean` one with
/// `ignore_malformed: true` skips what it cannot read, and any other must fit `kind` as the
/// field itself does.
fn multi_field_fits(sub: &Value, kind: JsonKind) -> bool {
    let ignores_malformed = sub["ignore_malformed"] == Value::Bool(true);
    match node_type(sub) {
        Some("text" | "keyword") => true,
        Some("long" | "integer" | "date" | "boolean") if ignores_malformed => true,
        _ => kind_fits(sub, kind),
    }
}

/// The `copy_to` targets of `node` that are not `text` or `keyword` fields: a copied value is
/// whatever the source field took, which only those types read whatever it is.
fn copy_to_problems(mapping: &Value, path: &str, node: &Value) -> Vec<String> {
    let targets: Vec<&str> = match node.get("copy_to") {
        None => return Vec::new(),
        Some(Value::String(target)) => vec![target.as_str()],
        Some(Value::Array(targets)) => targets.iter().filter_map(Value::as_str).collect(),
        Some(other) => return vec![format!("{path}: copy_to {other} is not a field name")],
    };
    targets
        .into_iter()
        .filter_map(|target| {
            let to = field(mapping, target);
            (!matches!(node_type(to), Some("text" | "keyword"))).then(|| {
                format!(
                    "{path}: copy_to {target}, which is mapped as {}; a copy_to target must be \
                     text or keyword",
                    describe(to)
                )
            })
        })
        .collect()
}

/// Whether extraction's `kind` fits the field mapped as `node`. A lenient kind lets a value
/// Elasticsearch cannot parse through, so its field must skip that value
/// (`ignore_malformed`) rather than fail the document, and it counts by the range of one
/// type, so the type must match. Number and date fields need `ignore_malformed` for every kind
/// (below). Other types (`nested`, `float`, ...) have no rule here and are refused.
fn kind_fits(node: &Value, kind: JsonKind) -> bool {
    let ignores_malformed = node["ignore_malformed"] == Value::Bool(true);
    match node_type(node) {
        // Every number and date field needs `ignore_malformed`, whatever the kind: a strict
        // `number` still lets an id above `i64::MAX` (or above `i32` in an `integer`) through,
        // and a `string` any text, which such a field would otherwise refuse, failing the
        // bulk item and the load.
        Some(ty @ ("long" | "integer")) => {
            ignores_malformed
                && match kind {
                    JsonKind::Number => true,
                    JsonKind::LenientLong => ty == "long",
                    JsonKind::LenientInteger => ty == "integer",
                    _ => false,
                }
        }
        Some("date") => {
            ignores_malformed && matches!(kind, JsonKind::String | JsonKind::StringArray)
        }
        Some("boolean") => match kind {
            JsonKind::Boolean => true,
            JsonKind::LenientBoolean => ignores_malformed,
            _ => false,
        },
        Some("text" | "keyword") => matches!(kind, JsonKind::String | JsonKind::StringArray),
        Some("object") => kind == JsonKind::ObjectArray,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn mapping_parses_and_is_strict() {
        let mapping = mapping();
        assert!(mapping.is_object());
        assert_eq!(mapping["dynamic"], Value::Bool(false));
        assert_eq!(parse_mapping(MAPPING_JSON).unwrap(), mapping);
        assert!(parse_mapping("[]").is_err());
        assert!(parse_mapping("{").is_err());
    }

    #[test]
    fn acceptance_fields_have_keywords() {
        let mapping = mapping();
        for path in [
            "roles.title",
            "organizations.name",
            "first_name",
            "last_name",
            "roles.organization_name",
        ] {
            let node = field(&mapping, path);
            assert_eq!(node["type"], "text", "{path}");
            assert_eq!(node["fields"]["keyword"]["type"], "keyword", "{path}");
        }
        assert_eq!(
            field(&mapping, "unresolved_organization_ids")["type"],
            "long"
        );
        assert_eq!(
            field(&mapping, "roles.organization_resolved")["type"],
            "boolean"
        );
    }

    #[test]
    fn numbers_and_dates_ignore_malformed_values() {
        let mut checked = 0;
        visit(&mapping(), "", &mut |path, node| {
            if matches!(node["type"].as_str(), Some("long" | "integer" | "date")) {
                assert_eq!(node["ignore_malformed"], Value::Bool(true), "{path}");
                checked += 1;
            }
        });
        assert!(checked >= 10, "{checked}");
    }

    #[test]
    fn lenient_kinds_need_ignore_malformed() {
        for (ty, kind, other) in [
            ("long", JsonKind::LenientLong, JsonKind::LenientInteger),
            ("integer", JsonKind::LenientInteger, JsonKind::LenientLong),
        ] {
            let lenient = json!({"type": ty, "ignore_malformed": true});
            let strict = json!({"type": ty});
            assert!(kind_fits(&lenient, kind), "{ty}");
            assert!(kind_fits(&lenient, JsonKind::Number), "{ty}");
            // Even a strict id: one past the type's range would fail the document.
            assert!(!kind_fits(&strict, JsonKind::Number), "{ty}");
            assert!(!kind_fits(&strict, kind), "{ty}");
            // The range a lenient number counts by must be the mapped type's.
            assert!(!kind_fits(&lenient, other), "{ty}");
            assert!(!kind_fits(&lenient, JsonKind::LenientBoolean), "{ty}");
        }

        let lenient = json!({"type": "boolean", "ignore_malformed": true});
        let strict = json!({"type": "boolean"});
        assert!(kind_fits(&lenient, JsonKind::LenientBoolean));
        assert!(kind_fits(&strict, JsonKind::Boolean));
        assert!(!kind_fits(&strict, JsonKind::LenientBoolean));
        assert!(!kind_fits(&lenient, JsonKind::LenientLong));
        let date = json!({"type": "date", "ignore_malformed": true});
        assert!(kind_fits(&date, JsonKind::String));
        assert!(kind_fits(&date, JsonKind::StringArray));
        assert!(!kind_fits(&json!({"type": "date"}), JsonKind::String));
        assert!(kind_fits(&json!({"type": "keyword"}), JsonKind::String));
        assert!(kind_fits(&json!({"properties": {}}), JsonKind::ObjectArray));
        assert!(!kind_fits(
            &json!({"type": "nested"}),
            JsonKind::ObjectArray
        ));
        assert!(!kind_fits(&json!({"type": "float"}), JsonKind::Number));
    }

    /// Extraction's type check and the built-in mapping agree: every field Elasticsearch
    /// parses (not disabled, not written by the document builder) has a table entry of the
    /// kind its type expects, and every table entry is such a field.
    #[test]
    fn extract_shapes_agree_with_the_mapping() {
        check_mapping(&mapping(), FeedSchema::builtin()).unwrap();
        // Its keyword multi-fields and the copy_to into full_name are what the check accepts.
        let mapping = mapping();
        assert_eq!(
            field(&mapping, "first_name")["fields"]["keyword"]["type"],
            "keyword"
        );
        assert_eq!(field(&mapping, "first_name")["copy_to"], "full_name");
        // A number multi-field with ignore_malformed is fine under a string field too.
        let mut with_number = mapping.clone();
        with_number["properties"]["city"]["fields"] =
            json!({"n": {"type": "long", "ignore_malformed": true}, "t": {"type": "text"}});
        check_mapping(&with_number, FeedSchema::builtin()).unwrap();
    }

    /// Runs the check on the built-in mapping changed by `change`, against `schema`.
    fn problems_after(schema: &FeedSchema, change: impl FnOnce(&mut Value)) -> String {
        let mut mapping = mapping();
        change(&mut mapping);
        match check_mapping(&mapping, schema) {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        }
    }

    /// A change to the built-in mapping and a part of the problem the check reports.
    type Drift = (Box<dyn FnOnce(&mut Value)>, &'static str);

    /// Ways the built-in mapping can drift from the built-in schema.
    #[allow(clippy::too_many_lines, reason = "one table of cases")]
    fn drifts() -> Vec<Drift> {
        vec![
            (
                Box::new(|m: &mut Value| m["dynamic"] = json!(true)),
                "\"dynamic\" must be false",
            ),
            (
                Box::new(|m: &mut Value| m["properties"]["roles"]["dynamic"] = json!("true")),
                "roles: \"dynamic\" must be false or absent",
            ),
            (
                Box::new(|m: &mut Value| m["properties"]["nickname"] = json!({"type": "keyword"})),
                "nickname (keyword) is mapped but the schema does not check it",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"].as_object_mut().unwrap().remove("headline");
                }),
                "headline is in person.fields but not mapped",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["followers"] = json!({"type": "long"});
                }),
                "followers: mapped as long without \"ignore_malformed\": true",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["roles"]["properties"]["title"] = json!({"type": "float"});
                }),
                "roles.title: mapped as float",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["languages"] = json!({"type": "keyword"});
                }),
                "languages is listed as disabled in the schema but the mapping indexes it",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["organizations"]["properties"]["name"] =
                        json!({"type": "object", "enabled": false});
                }),
                "organizations.name is disabled in the mapping but not listed as disabled",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["organizations"]["properties"]
                        .as_object_mut()
                        .unwrap()
                        .remove("addresses");
                }),
                "organizations.addresses is listed as disabled in the schema but not mapped",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["roles"]["properties"]
                        .as_object_mut()
                        .unwrap()
                        .remove("organization_resolved");
                }),
                "roles.organization_resolved is written by the document builder and must be mapped as boolean, not nothing",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["unresolved_organization_ids"] = json!({"type": "keyword"});
                }),
                "unresolved_organization_ids is written by the document builder and must be mapped as long with ignore_malformed, not keyword",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["unresolved_organization_ids"] = json!({"type": "long"});
                }),
                "unresolved_organization_ids is written by the document builder and must be mapped as long with ignore_malformed, not long",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["id"] = json!({"type": "long"});
                }),
                "id: mapped as long without \"ignore_malformed\": true, which every number and date field needs",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["organizations"]["properties"]["founded_year"] =
                        json!({"type": "integer"});
                }),
                "organizations.founded_year: mapped as integer without \"ignore_malformed\"",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["roles"]["properties"]["start_date"] = json!({"type": "date"});
                }),
                "roles.start_date: mapped as date without \"ignore_malformed\"",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["country"] = json!({"type": "long"});
                }),
                "country: mapped as long, which does not take the schema's kind \"string\" (person.fields)",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["city"] =
                        json!({"type": "keyword", "fields": {"n": {"type": "long"}}});
                }),
                "city.n: mapped as long without \"ignore_malformed\": true",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["followers"]["fields"] = json!({"b": {"type": "boolean"}});
                }),
                "followers.b: mapped as boolean without \"ignore_malformed\": true",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["city"]["fields"] = json!({"f": {"type": "float"}});
                }),
                "city.f: mapped as float, which does not take the schema's kind \"string\" (person.fields)",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["unresolved_organization_ids"]["fields"] =
                        json!({"i": {"type": "integer"}});
                }),
                "unresolved_organization_ids.i: mapped as integer without \"ignore_malformed\"",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["headline"]["copy_to"] = json!(["full_name", "followers"]);
                }),
                "headline: copy_to followers, which is mapped as long with ignore_malformed; a copy_to target must be text or keyword",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["headline"]["copy_to"] = json!("nowhere");
                }),
                "headline: copy_to nowhere, which is mapped as nothing",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["id"] = json!({"type": "integer", "ignore_malformed": true});
                    m["properties"]["followers"] =
                        json!({"type": "integer", "ignore_malformed": true});
                }),
                "followers: mapped as integer with ignore_malformed, which does not take the schema's kind \"lenient_long\" (person.fields)",
            ),
            (
                Box::new(|m: &mut Value| {
                    m["properties"]["roles"]["type"] = json!("nested");
                }),
                "roles: mapped as nested",
            ),
        ]
    }

    #[test]
    fn a_mapping_that_drifts_from_the_schema_is_refused() {
        let builtin = FeedSchema::builtin();
        for (change, want) in drifts() {
            let got = problems_after(builtin, change);
            assert!(got.contains(want), "want {want}\n got {got}");
        }
        let err = check_mapping(&json!([]), builtin).unwrap_err().to_string();
        assert!(err.contains("not a JSON object"), "{err}");
    }

    #[test]
    fn a_renamed_schema_needs_the_renamed_mapping() {
        let schema = FeedSchema::from_json(
            r#"{"person": {"references": "jobs", "reference_id": "org",
                "fields": [{"name": "name", "kind": "string"}, {"name": "jobs", "kind": "object_array"}],
                "reference_fields": [{"name": "org", "kind": "number"}],
                "disabled": []},
               "organization": {"fields": [{"name": "label", "kind": "string"}], "disabled": []}}"#,
        )
        .unwrap();
        let renamed = json!({
            "dynamic": false,
            "properties": {
                "name": {"type": "text"},
                "jobs": {"properties": {
                    "org": {"type": "long", "ignore_malformed": true},
                    "organization_resolved": {"type": "boolean"},
                }},
                "organizations": {"type": "object", "properties": {"label": {"type": "keyword"}}},
                "unresolved_organization_ids": {"type": "long", "ignore_malformed": true},
            }
        });
        check_mapping(&renamed, &schema).unwrap();
        let err = check_mapping(&mapping(), &schema).unwrap_err().to_string();
        for want in [
            "jobs is in person.fields but not mapped",
            "roles (object) is mapped but the schema does not check it",
            "jobs.organization_resolved is written by the document builder",
        ] {
            assert!(err.contains(want), "want {want}\n got {err}");
        }
        let err = check_mapping(&renamed, FeedSchema::builtin())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("first_name is in person.fields but not mapped"),
            "{err}"
        );
    }

    #[test]
    fn roles_and_organizations_are_objects_not_nested() {
        let mapping = mapping();
        assert_eq!(field(&mapping, "roles")["type"], "object");
        assert_eq!(field(&mapping, "organizations")["type"], "object");
    }

    #[test]
    fn heavy_structures_are_disabled() {
        let mapping = mapping();
        for path in [
            "memberships",
            "educations",
            "certifications",
            "languages",
            "organizations.addresses",
            "organizations.locations",
            "organizations.funding_rounds",
        ] {
            assert_eq!(
                field(&mapping, path)["enabled"],
                Value::Bool(false),
                "{path}"
            );
        }
        for path in [
            "summary",
            "roles.description",
            "organizations.description",
            "profile_url",
        ] {
            assert_eq!(field(&mapping, path)["index"], Value::Bool(false), "{path}");
        }
    }
}
