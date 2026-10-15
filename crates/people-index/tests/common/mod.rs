//! Shared helpers for the pipeline tests: generate a dataset into a local store, run stages
//! with the local executor, read the documents back, and an independent reference join.

#![allow(
    dead_code,
    reason = "each test binary uses a different part of this module"
)]

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use flate2::read::MultiGzDecoder;
use people_index::store_sink::StoreSink;
use pi_coord::{
    CommitLog, LeaseConfig, LocalExecutor, RetryPolicy, RunContext, StageName, StageReport,
    StoreCoordination,
};
use pi_datagen::{Expected, GenConfig};
use pi_model::DocRow;
use pi_segment::{SegmentConfig, decode_segment};
use pi_stages::StagesConfig;
use pi_store::{Dataset, Store};
use serde_json::{Map, Value};
use tempfile::TempDir;

/// The stages up to the documents (no Elasticsearch).
pub const OFFLINE: [StageName; 5] = [
    StageName::Plan,
    StageName::ExtractOrgs,
    StageName::ExtractPersons,
    StageName::Join,
    StageName::Assemble,
];

/// A generated dataset in a local store.
pub struct Generated {
    pub dir: TempDir,
    pub store: Store,
    pub expected: Expected,
}

/// A small dataset with every kind of trouble turned up: malformed lines, duplicate ids in
/// both feeds, unresolved and null organization ids, string ids, unknown fields.
pub fn small_config() -> GenConfig {
    GenConfig {
        persons: 3000,
        organizations: 1000,
        files_per_feed: 4,
        seed: 7,
        malformed_rate: 0.01,
        duplicate_org_rate: 0.05,
        duplicate_person_rate: 0.05,
        unresolved_rate: 0.05,
        ..GenConfig::default()
    }
}

/// Generates `cfg` under `raw/` of a fresh local store, through the CLI's store sink.
pub async fn generate(cfg: GenConfig) -> Generated {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::local(dir.path()).expect("local store");
    let handle = tokio::runtime::Handle::current();
    let sink_store = store.clone();
    let expected = tokio::task::spawn_blocking(move || {
        let mut sink = StoreSink::new(sink_store, "raw", handle);
        pi_datagen::generate(&cfg, &mut sink)
    })
    .await
    .expect("join")
    .expect("generate");
    Generated {
        dir,
        store,
        expected,
    }
}

/// Stage settings giving exactly `partitions` partitions, with tiny segments and buffers so
/// rolling and partial flushes happen even on a small dataset.
pub fn config(partitions: u32) -> StagesConfig {
    StagesConfig {
        partition_target_bytes: u64::MAX / 4,
        min_partitions: partitions,
        segment: SegmentConfig {
            target_bytes: 8 << 10,
            ..SegmentConfig::default()
        },
        buffer_bytes: 32 << 10,
        ..StagesConfig::default()
    }
}

/// A local executor that does not retry, so a failure shows at once.
pub fn executor(worker: &str) -> LocalExecutor {
    LocalExecutor::new(Arc::new(StoreCoordination::new(
        worker,
        LeaseConfig::default(),
    )))
    .with_concurrency(4)
    .with_retry(RetryPolicy::none())
}

/// Runs `stages` of run `run_id` in order.
pub async fn run_stages(
    store: &Store,
    run_id: &str,
    config: StagesConfig,
    stages: &[StageName],
) -> anyhow::Result<Vec<StageReport>> {
    let registry = pi_stages::build_registry(config)?;
    let ctx = RunContext::new(run_id, store.clone())?;
    let exec = executor("test");
    let mut reports = Vec::new();
    for name in stages {
        let stage = registry.get(*name).expect("registered");
        reports.push(pi_coord::Executor::run_stage(&exec, stage, &ctx).await?);
    }
    Ok(reports)
}

/// Every committed document of a run, by person id. Fails on a duplicate id.
pub async fn read_docs(store: &Store, run_id: &str) -> BTreeMap<u64, Vec<u8>> {
    let ctx = RunContext::new(run_id, store.clone()).unwrap();
    let commits = CommitLog::new(store.clone(), ctx.layout.clone())
        .list(StageName::Assemble)
        .await
        .unwrap();
    let docs_dir = format!(
        "{}/",
        ctx.layout
            .dataset_dir(StageName::Assemble.as_str(), Dataset::Docs)
            .unwrap()
    );
    let mut docs = BTreeMap::new();
    for key in commits
        .iter()
        .flat_map(|c| c.report.outputs.iter())
        .filter(|k| k.starts_with(&docs_dir))
    {
        let bytes = store.get(key).await.unwrap().bytes;
        for doc in decode_segment::<DocRow>(&bytes).unwrap() {
            assert!(
                docs.insert(doc.person_id, doc.body).is_none(),
                "person {} has two documents",
                doc.person_id
            );
        }
    }
    docs
}

/// The summed counters of a stage's commits.
pub async fn counters(store: &Store, run_id: &str, stage: StageName) -> BTreeMap<String, u64> {
    let ctx = RunContext::new(run_id, store.clone()).unwrap();
    let commits = CommitLog::new(store.clone(), ctx.layout)
        .list(stage)
        .await
        .unwrap();
    pi_coord::sum_counters(&commits)
}

/// The winning records of both feeds, parsed independently of pi-model.
pub struct Reference {
    pub orgs: HashMap<u64, Value>,
    pub persons: HashMap<u64, Value>,
    pub malformed_orgs: u64,
    pub malformed_persons: u64,
}

impl Reference {
    /// Reads `<root>/raw/{organizations,persons}/*.ndjson.gz` in feed order.
    pub fn read(root: &Path) -> Self {
        let (orgs, malformed_orgs) = read_feed(&root.join("raw").join("organizations"));
        let (persons, malformed_persons) = read_feed(&root.join("raw").join("persons"));
        Self {
            orgs,
            persons,
            malformed_orgs,
            malformed_persons,
        }
    }

    /// The document the contract says person `id` gets.
    pub fn expected_document(&self, id: u64) -> Value {
        let person = &self.persons[&id];
        let mut doc: Map<String, Value> = person.as_object().expect("object").clone();
        let mut resolved_ids: Vec<u64> = Vec::new();
        let mut unresolved_ids: Vec<u64> = Vec::new();
        if let Some(Value::Array(roles)) = doc.get_mut("roles") {
            for role in roles.iter_mut() {
                let role = role.as_object_mut().expect("role object");
                let Some(org_id) = role.get("organization_id").and_then(id_of) else {
                    continue;
                };
                let resolved = self.orgs.contains_key(&org_id);
                role.insert("organization_resolved".into(), Value::Bool(resolved));
                let list = if resolved {
                    &mut resolved_ids
                } else {
                    &mut unresolved_ids
                };
                if !list.contains(&org_id) {
                    list.push(org_id);
                }
            }
        }
        doc.insert(
            "organizations".into(),
            Value::Array(resolved_ids.iter().map(|i| self.orgs[i].clone()).collect()),
        );
        if !unresolved_ids.is_empty() {
            doc.insert(
                "unresolved_organization_ids".into(),
                Value::Array(unresolved_ids.into_iter().map(Value::from).collect()),
            );
        }
        Value::Object(doc)
    }
}

/// An id: a u64 number or a string of decimal digits that is one.
fn id_of(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse().ok()
        }
        _ => None,
    }
}

fn read_feed(dir: &Path) -> (HashMap<u64, Value>, u64) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".ndjson.gz"))
        .collect();
    files.sort();
    let mut out = HashMap::new();
    let mut malformed = 0;
    for path in files {
        let reader = BufReader::new(MultiGzDecoder::new(std::fs::File::open(&path).unwrap()));
        for line in reader.lines() {
            let line = line.unwrap();
            let parsed: Option<(u64, Value)> =
                serde_json::from_str::<Value>(&line).ok().and_then(|v| {
                    let id = v.get("id").and_then(id_of)?;
                    let record = v.get("record").filter(|r| r.is_object())?.clone();
                    Some((id, record))
                });
            match parsed {
                Some((id, record)) => {
                    out.insert(id, record);
                }
                None => malformed += 1,
            }
        }
    }
    (out, malformed)
}

/// Documents with at least one role titled `title`.
pub fn count_title(docs: &[Value], title: &str) -> u64 {
    docs.iter()
        .filter(|d| {
            d["roles"]
                .as_array()
                .is_some_and(|roles| roles.iter().any(|r| r["title"] == title))
        })
        .count() as u64
}

/// Documents with at least one organization named `name`.
pub fn count_org_name(docs: &[Value], name: &str) -> u64 {
    docs.iter()
        .filter(|d| {
            d["organizations"]
                .as_array()
                .is_some_and(|orgs| orgs.iter().any(|o| o["name"] == name))
        })
        .count() as u64
}

/// Checks a run's documents against `expected.json` and the reference join.
pub async fn check_run(
    data: &Generated,
    run_id: &str,
    reference: &Reference,
) -> BTreeMap<u64, Vec<u8>> {
    let docs = read_docs(&data.store, run_id).await;
    let e = &data.expected;
    assert_eq!(docs.len() as u64, e.persons, "{run_id}: document count");
    let values: Vec<Value> = docs
        .values()
        .map(|b| serde_json::from_slice(b).expect("document is JSON"))
        .collect();
    let with_unresolved = values
        .iter()
        .filter(|d| d.get("unresolved_organization_ids").is_some())
        .count() as u64;
    assert_eq!(
        with_unresolved, e.persons_with_unresolved,
        "{run_id}: persons_with_unresolved"
    );
    for (title, n) in &e.role_title_counts {
        assert_eq!(count_title(&values, title), *n, "{run_id}: title {title}");
    }
    for (name, n) in &e.organization_name_counts {
        assert_eq!(
            count_org_name(&values, name),
            *n,
            "{run_id}: organization {name}"
        );
    }

    let persons = counters(&data.store, run_id, StageName::ExtractPersons).await;
    let orgs = counters(&data.store, run_id, StageName::ExtractOrgs).await;
    assert_eq!(
        persons["malformed"], e.malformed.persons,
        "{run_id}: malformed persons"
    );
    assert_eq!(
        orgs["malformed"], e.malformed.organizations,
        "{run_id}: malformed orgs"
    );
    let join = counters(&data.store, run_id, StageName::Join).await;
    assert_eq!(
        join["organizations"], e.organizations,
        "{run_id}: distinct organizations"
    );
    let assemble = counters(&data.store, run_id, StageName::Assemble).await;
    assert_eq!(assemble["documents"], e.persons);
    assert_eq!(
        assemble["documents_with_unresolved"],
        e.persons_with_unresolved
    );

    assert_eq!(
        reference.persons.len() as u64,
        e.persons,
        "reference persons"
    );
    assert_eq!(
        reference.malformed_persons, e.malformed.persons,
        "reference malformed"
    );
    for (id, body) in &docs {
        let got: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(
            got,
            reference.expected_document(*id),
            "{run_id}: document {id}"
        );
    }
    docs
}
