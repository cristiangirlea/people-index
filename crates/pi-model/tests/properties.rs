//! Property tests for the partitioner and the document builder.

use std::collections::{BTreeMap, HashMap};

use pi_model::{DocumentBuilder, Partitioner, Seq, parse_person_line};
use proptest::prelude::*;
use serde_json::{Map, Value, json};

/// An arbitrary JSON value without floats (float text is checked byte for byte elsewhere).
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        any::<u64>().prop_map(|n| json!(n)),
        ".{0,12}".prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map("[a-z\"\\\\é ]{0,6}", inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect::<Map<_, _>>())),
        ]
    })
}

/// How a role writes its organization id.
#[derive(Debug, Clone)]
enum OrgRef {
    Absent,
    Null,
    Number(u64),
    Text(u64),
}

impl OrgRef {
    fn id(&self) -> Option<u64> {
        match self {
            Self::Absent | Self::Null => None,
            Self::Number(id) | Self::Text(id) => Some(*id),
        }
    }
}

fn org_ref() -> impl Strategy<Value = OrgRef> {
    prop_oneof![
        1 => Just(OrgRef::Absent),
        2 => Just(OrgRef::Null),
        4 => (0u64..12).prop_map(OrgRef::Number),
        2 => (0u64..12).prop_map(OrgRef::Text),
    ]
}

#[derive(Debug, Clone)]
struct Case {
    unknown: BTreeMap<String, Value>,
    roles: Vec<(OrgRef, BTreeMap<String, Value>)>,
    orgs: BTreeMap<u64, Value>,
}

const RESERVED: [&str; 3] = ["roles", "organizations", "unresolved_organization_ids"];

fn case() -> impl Strategy<Value = Case> {
    let unknown =
        prop::collection::btree_map("[a-z_\"\\\\é]{1,8}", json_value(), 0..6).prop_map(|m| {
            m.into_iter()
                .filter(|(k, _)| !RESERVED.contains(&k.as_str()))
                .collect()
        });
    let role_fields =
        prop::collection::btree_map("[a-z_]{1,6}", json_value(), 0..3).prop_map(|m| {
            m.into_iter()
                .filter(|(k, _)| k != "organization_id" && k != "organization_resolved")
                .collect()
        });
    let roles = prop::collection::vec((org_ref(), role_fields), 0..10);
    let orgs = prop::collection::btree_map(0u64..12, json_value(), 0..8).prop_map(|m| {
        m.into_iter()
            .map(|(id, extra)| {
                (
                    id,
                    json!({"id": id, "name": format!("org {id}"), "extra": extra}),
                )
            })
            .collect()
    });
    (unknown, roles, orgs).prop_map(|(unknown, roles, orgs)| Case {
        unknown,
        roles,
        orgs,
    })
}

impl Case {
    fn record(&self) -> Value {
        let mut record: Map<String, Value> = self.unknown.clone().into_iter().collect();
        let roles = self
            .roles
            .iter()
            .map(|(org, fields)| {
                let mut role: Map<String, Value> = fields.clone().into_iter().collect();
                match org {
                    OrgRef::Absent => {}
                    OrgRef::Null => {
                        role.insert("organization_id".into(), Value::Null);
                    }
                    OrgRef::Number(id) => {
                        role.insert("organization_id".into(), json!(id));
                    }
                    OrgRef::Text(id) => {
                        role.insert("organization_id".into(), json!(id.to_string()));
                    }
                }
                Value::Object(role)
            })
            .collect();
        record.insert("roles".into(), Value::Array(roles));
        Value::Object(record)
    }

    fn table(&self) -> HashMap<u64, Vec<u8>> {
        self.orgs
            .iter()
            .map(|(id, body)| (*id, serde_json::to_vec(body).unwrap()))
            .collect()
    }
}

proptest! {
    #[test]
    fn partition_is_in_range_and_refines(id in any::<u64>(), log in 0u32..31) {
        let small = Partitioner::new(1 << log).unwrap();
        let large = Partitioner::new(1 << (log + 1)).unwrap();
        prop_assert!(small.partition(id) < small.count());
        prop_assert!(large.partition(id) < large.count());
        // Doubling P splits each partition in two: the low bits stay the same.
        prop_assert_eq!(large.partition(id) & (small.count() - 1), small.partition(id));
        prop_assert_eq!(small.partition(id), small.partition(id));
    }

    #[test]
    fn documents_follow_the_output_rules(case in case()) {
        let record = case.record();
        let line = serde_json::to_vec(&json!({"id": 1, "updated_at": "t", "record": record})).unwrap();
        let row = parse_person_line(&line, Seq::new(0, 1)).unwrap();
        let table = case.table();
        let built = DocumentBuilder::default().build_row(&row, &table).unwrap();
        let doc: Value = serde_json::from_slice(&built.doc.body).unwrap();
        let doc = doc.as_object().unwrap();

        // Unknown fields survive untouched.
        for (key, value) in &case.unknown {
            prop_assert_eq!(doc.get(key), Some(value), "field {}", key);
        }

        // Expected organizations and unresolved ids, in first-appearance order.
        let mut expected_orgs: Vec<u64> = Vec::new();
        let mut expected_unresolved: Vec<u64> = Vec::new();
        for (org, _) in &case.roles {
            if let Some(id) = org.id() {
                let list = if case.orgs.contains_key(&id) { &mut expected_orgs } else { &mut expected_unresolved };
                if !list.contains(&id) {
                    list.push(id);
                }
            }
        }
        let orgs: Vec<Value> = expected_orgs.iter().map(|id| case.orgs[id].clone()).collect();
        prop_assert_eq!(doc.get("organizations"), Some(&Value::Array(orgs)));
        if expected_unresolved.is_empty() {
            prop_assert!(!doc.contains_key("unresolved_organization_ids"));
        } else {
            prop_assert_eq!(doc.get("unresolved_organization_ids"), Some(&json!(expected_unresolved)));
        }
        let distinct: Vec<u64> = expected_orgs.iter().chain(&expected_unresolved).copied().collect();
        let mut row_ids = row.org_ids.clone();
        let mut distinct_sorted = distinct.clone();
        row_ids.sort_unstable();
        distinct_sorted.sort_unstable();
        prop_assert_eq!(row_ids, distinct_sorted);

        // Every role keeps its fields and gets the right flag.
        let roles = doc["roles"].as_array().unwrap();
        prop_assert_eq!(roles.len(), case.roles.len());
        let (mut null, mut resolved, mut unresolved) = (0u64, 0u64, 0u64);
        for ((org, fields), role) in case.roles.iter().zip(roles) {
            let role = role.as_object().unwrap();
            for (key, value) in fields {
                prop_assert_eq!(role.get(key), Some(value));
            }
            match (org.id(), role.get("organization_resolved")) {
                (None, None) => null += 1,
                (Some(id), Some(Value::Bool(true))) if case.orgs.contains_key(&id) => resolved += 1,
                (Some(id), Some(Value::Bool(false))) if !case.orgs.contains_key(&id) => unresolved += 1,
                other => prop_assert!(false, "wrong flag {:?}", other),
            }
        }
        prop_assert_eq!(null + resolved + unresolved, case.roles.len() as u64);
        let stats = built.stats;
        prop_assert_eq!(stats.roles, case.roles.len() as u64);
        prop_assert_eq!(stats.roles_null_organization + stats.roles_resolved + stats.roles_unresolved, stats.roles);
        prop_assert_eq!((stats.roles_null_organization, stats.roles_resolved, stats.roles_unresolved), (null, resolved, unresolved));
        prop_assert_eq!(stats.organizations, expected_orgs.len() as u64);
        prop_assert_eq!(stats.unresolved_ids, expected_unresolved.len() as u64);
    }

    #[test]
    fn builder_never_panics_on_arbitrary_records(record in json_value(), table in prop::collection::hash_map(0u64..4, json_value(), 0..4)) {
        let table: HashMap<u64, Vec<u8>> = table.into_iter().map(|(k, v)| (k, serde_json::to_vec(&v).unwrap())).collect();
        let body = serde_json::to_vec(&record).unwrap();
        if let Ok(built) = DocumentBuilder::default().build(1, &body, &table) {
            prop_assert!(serde_json::from_slice::<Value>(&built.doc.body).is_ok());
        }
    }
}
