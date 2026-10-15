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
