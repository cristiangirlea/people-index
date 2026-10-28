//! The explicit mapping of the output document. The reasons for each choice are in
//! `mapping.md` next to `mapping.json`.

use serde_json::Value;

/// `mapping.json`, embedded at compile time so the binary needs no file next to it.
pub const MAPPING_JSON: &str = include_str!("mapping.json");

/// The mapping as JSON, ready for the `mappings` key of a create-index request.
#[must_use]
pub fn mapping() -> Value {
    // The file is part of the crate and its parse is covered by `mapping_parses` below, so a
    // malformed mapping fails the test suite, never a run.
    serde_json::from_str(MAPPING_JSON).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use pi_model::{
        BUILDER_OWNED, JsonKind, ORGANIZATION_DISABLED, ORGANIZATION_FIELDS, PERSON_DISABLED,
        PERSON_FIELDS, ROLE_FIELDS,
    };

    use super::*;

    fn field<'a>(mapping: &'a Value, path: &str) -> &'a Value {
        let mut node = mapping;
        for part in path.split('.') {
            node = &node["properties"][part];
        }
        node
    }

    #[test]
    fn mapping_parses_and_is_strict() {
        let mapping = mapping();
        assert!(mapping.is_object());
        assert_eq!(mapping["dynamic"], Value::Bool(false));
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

    /// Calls `f` with the path and definition of every mapped field, depth first.
    fn visit(node: &Value, prefix: &str, f: &mut dyn FnMut(&str, &Value)) {
        let Some(properties) = node["properties"].as_object() else {
            return;
        };
        for (name, child) in properties {
            let path = format!("{prefix}{name}");
            f(&path, child);
            visit(child, &format!("{path}."), f);
        }
    }

    /// The JSON-kind table extraction checks (`pi_model::*_FIELDS`), as mapping paths.
    fn shape_table() -> std::collections::BTreeMap<String, JsonKind> {
        let mut table = std::collections::BTreeMap::new();
        for (fields, prefix) in [
            (PERSON_FIELDS, ""),
            (ROLE_FIELDS, "roles."),
            (ORGANIZATION_FIELDS, "organizations."),
        ] {
            for shape in fields {
                let old = table.insert(format!("{prefix}{}", shape.name), shape.kind);
                assert!(old.is_none(), "{prefix}{} listed twice", shape.name);
            }
        }
        table
    }

    /// Whether extraction's `kind` fits the field mapped as `node`. A lenient kind lets a value
    /// Elasticsearch cannot parse through, so its field must skip that value
    /// (`ignore_malformed`) rather than fail the document, and it counts by the range of one
    /// type, so the type must match; a strict one may have `ignore_malformed` too.
    fn kind_fits(path: &str, node: &Value, kind: JsonKind) -> bool {
        let ignores_malformed = node["ignore_malformed"] == Value::Bool(true);
        match node["type"].as_str() {
            Some(ty @ ("long" | "integer")) => match kind {
                JsonKind::Number => true,
                JsonKind::LenientLong => ty == "long" && ignores_malformed,
                JsonKind::LenientInteger => ty == "integer" && ignores_malformed,
                _ => false,
            },
            Some("boolean") => match kind {
                JsonKind::Boolean => true,
                JsonKind::LenientBoolean => ignores_malformed,
                _ => false,
            },
            Some("text" | "keyword" | "date") => {
                matches!(kind, JsonKind::String | JsonKind::StringArray)
            }
            Some("object") => kind == JsonKind::ObjectArray,
            other => panic!("{path}: no rule for mapping type {other:?}"),
        }
    }

    #[test]
    fn lenient_kinds_need_ignore_malformed() {
        for (ty, kind, other) in [
            ("long", JsonKind::LenientLong, JsonKind::LenientInteger),
            ("integer", JsonKind::LenientInteger, JsonKind::LenientLong),
        ] {
            let lenient = serde_json::json!({"type": ty, "ignore_malformed": true});
            let strict = serde_json::json!({"type": ty});
            assert!(kind_fits("n", &lenient, kind), "{ty}");
            assert!(kind_fits("n", &lenient, JsonKind::Number), "{ty}");
            assert!(kind_fits("n", &strict, JsonKind::Number), "{ty}");
            assert!(!kind_fits("n", &strict, kind), "{ty}");
            // The range a lenient number counts by must be the mapped type's.
            assert!(!kind_fits("n", &lenient, other), "{ty}");
            assert!(!kind_fits("n", &lenient, JsonKind::LenientBoolean), "{ty}");
        }

        let lenient = serde_json::json!({"type": "boolean", "ignore_malformed": true});
        let strict = serde_json::json!({"type": "boolean"});
        assert!(kind_fits("b", &lenient, JsonKind::LenientBoolean));
        assert!(kind_fits("b", &strict, JsonKind::Boolean));
        assert!(!kind_fits("b", &strict, JsonKind::LenientBoolean));
        assert!(!kind_fits("b", &lenient, JsonKind::LenientLong));
    }

    /// Extraction's type check and the mapping cannot drift: every field Elasticsearch parses
    /// (not disabled, not written by the document builder) has a table entry of the kind its
    /// type expects, and every table entry is such a field.
    #[test]
    fn extract_shapes_agree_with_the_mapping() {
        let table = shape_table();
        let mut seen = Vec::new();
        visit(&mapping(), "", &mut |path, node| {
            // `organizations` itself is builder-owned; its fields are the organization
            // record's, checked at extract-orgs.
            let owned = BUILDER_OWNED.contains(&path);
            let disabled = node["enabled"] == Value::Bool(false);
            let disabled_table = PERSON_DISABLED.contains(&path)
                || path
                    .strip_prefix("organizations.")
                    .is_some_and(|name| ORGANIZATION_DISABLED.contains(&name));
            assert_eq!(disabled, disabled_table, "{path}: disabled table mismatch");
            assert_eq!(
                usize::from(owned)
                    + usize::from(disabled_table)
                    + usize::from(table.contains_key(path)),
                1,
                "{path}: must occur in exactly one table"
            );
            let Some(kind) = table.get(path) else {
                assert!(
                    disabled || owned,
                    "{path} ({}) is mapped but extraction does not check it",
                    node["type"]
                );
                return;
            };
            assert!(!disabled, "{path} is disabled: any value is fine, drop it");
            assert!(
                !owned,
                "{path} is builder-owned: the input value is replaced"
            );
            assert!(
                kind_fits(path, node, *kind),
                "{path}: mapped {node}, checked as {kind:?}"
            );
            seen.push(path.to_owned());
        });
        let missing: Vec<&String> = table.keys().filter(|k| !seen.contains(k)).collect();
        assert!(missing.is_empty(), "checked but not mapped: {missing:?}");
        for path in PERSON_DISABLED {
            assert_eq!(field(&mapping(), path)["enabled"], false, "{path}");
        }
        for name in ORGANIZATION_DISABLED {
            let path = format!("organizations.{name}");
            assert_eq!(field(&mapping(), &path)["enabled"], false, "{path}");
        }
        for owned in BUILDER_OWNED {
            assert!(!field(&mapping(), owned).is_null(), "{owned} is not mapped");
        }
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
