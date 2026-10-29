//! A run on a feed with other field names (docs/OPERATIONS.md, "Feed schema"): the generated
//! dataset rewritten with renamed envelope keys, references, reference id and fields, read
//! through a schema file and a mapping file, gives the same documents (renamed) and the same
//! counters as the run on the original feed, creates the index with that mapping, loads and
//! publishes; and its plan refuses to go on with the built-in schema.

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser as _;
use common::{Generated, OFFLINE, config, counters, generate, read_docs, run_stages, small_config};
use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use people_index::cli::{Cli, Command};
use people_index::settings;
use pi_coord::{RunContext, StageName};
use pi_model::FeedSchema;
use pi_stages::{Plan, SchemaPin, StagesConfig, Summary};
use pi_store::Store;
use serde_json::{Map, Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Envelope keys: built-in, renamed.
const ENVELOPE: [(&str, &str); 2] = [("id", "key"), ("record", "payload")];
/// Top-level person fields.
const PERSON: [(&str, &str); 3] = [
    ("first_name", "given_name"),
    ("last_name", "family_name"),
    ("roles", "positions"),
];
/// Fields of each reference.
const REFERENCE: [(&str, &str); 2] = [
    ("organization_id", "company_ref"),
    ("title", "position_title"),
];
/// Top-level organization fields (`addresses` is a disabled one).
const ORGANIZATION: [(&str, &str); 2] = [("name", "label"), ("addresses", "offices")];

type Renames = [(&'static str, &'static str)];

/// Renames the keys of `object` from the first name of each pair to the second, or back.
fn rename_keys(object: &mut Map<String, Value>, renames: &Renames, back: bool) {
    for (builtin, renamed) in renames {
        let (from, to) = if back {
            (renamed, builtin)
        } else {
            (builtin, renamed)
        };
        if let Some(value) = object.remove(*from) {
            object.insert((*to).to_owned(), value);
        }
    }
}

/// A person record (or document) with its fields and its references' fields renamed.
fn rename_person(person: &mut Map<String, Value>, back: bool) {
    rename_keys(person, &PERSON, back);
    let references = if back { "roles" } else { "positions" };
    if let Some(Value::Array(roles)) = person.get_mut(references) {
        for role in roles.iter_mut().filter_map(Value::as_object_mut) {
            rename_keys(role, &REFERENCE, back);
        }
    }
}

/// One feed line in the renamed schema. A line that is not a JSON object stays as it is (it is
/// malformed either way); a malformed envelope stays malformed under the new keys.
fn rename_line(line: &str, person: bool) -> String {
    let Ok(Value::Object(mut envelope)) = serde_json::from_str::<Value>(line) else {
        return line.to_owned();
    };
    rename_keys(&mut envelope, &ENVELOPE, false);
    if let Some(Value::Object(record)) = envelope.get_mut("payload") {
        if person {
            rename_person(record, false);
        } else {
            rename_keys(record, &ORGANIZATION, false);
        }
    }
    Value::Object(envelope).to_string()
}

/// Copies `<from>/raw/<feed>/*.ndjson.gz` into `to`, every line renamed.
async fn copy_renamed(from: &Path, to: &Store) {
    for feed in ["organizations", "persons"] {
        for entry in std::fs::read_dir(from.join("raw").join(feed)).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if !name.ends_with(".ndjson.gz") {
                continue;
            }
            let reader = BufReader::new(MultiGzDecoder::new(std::fs::File::open(&path).unwrap()));
            let mut out = GzEncoder::new(Vec::new(), Compression::fast());
            for line in reader.lines() {
                writeln!(out, "{}", rename_line(&line.unwrap(), feed == "persons")).unwrap();
            }
            to.put(&format!("raw/{feed}/{name}"), out.finish().unwrap())
                .await
                .unwrap();
        }
    }
}

/// Renames the entries named `from` to `to` in a table of the schema file.
fn rename_table(table: &mut Value, renames: &Renames) {
    for entry in table.as_array_mut().unwrap() {
        let name = if entry.is_object() {
            &mut entry["name"]
        } else {
            entry
        };
        if let Some((_, to)) = renames.iter().find(|(from, _)| name == from) {
            *name = json!(to);
        }
    }
}

/// The complete schema file of docs/OPERATIONS.md ("Feed schema"), which describes this feed.
fn documented_schema_file() -> &'static str {
    let doc = include_str!("../../../docs/OPERATIONS.md");
    let marker = "<!-- feed-schema-example -->\n```json\n";
    let start = doc.find(marker).expect("the example in docs/OPERATIONS.md") + marker.len();
    let end = doc[start..].find("\n```").expect("the end of the example");
    &doc[start..=start + end]
}

/// The schema of the renamed feed: the built-in schema, spelled out, renamed.
fn schema_file() -> Value {
    let mut schema = FeedSchema::builtin().to_json();
    schema["envelope"] = json!({"id": "key", "record": "payload"});
    let person = &mut schema["person"];
    person["references"] = json!("positions");
    person["reference_id"] = json!("company_ref");
    rename_table(&mut person["fields"], &PERSON);
    rename_table(&mut person["reference_fields"], &REFERENCE);
    rename_table(&mut schema["organization"]["fields"], &ORGANIZATION);
    rename_table(&mut schema["organization"]["disabled"], &ORGANIZATION);
    schema
}

/// The mapping of the renamed feed: the built-in mapping with the same renames.
fn mapping_file() -> Value {
    let mut mapping = pi_es::mapping();
    let properties = mapping["properties"].as_object_mut().unwrap();
    rename_keys(properties, &PERSON, false);
    rename_keys(
        properties["positions"]["properties"]
            .as_object_mut()
            .unwrap(),
        &REFERENCE,
        false,
    );
    rename_keys(
        properties["organizations"]["properties"]
            .as_object_mut()
            .unwrap(),
        &ORGANIZATION,
        false,
    );
    mapping
}

/// A document of the renamed run with the built-in names put back.
fn renamed_back(body: &[u8]) -> Value {
    let mut doc: Map<String, Value> = serde_json::from_slice(body).unwrap();
    rename_person(&mut doc, true);
    if let Some(Value::Array(orgs)) = doc.get_mut("organizations") {
        for org in orgs.iter_mut().filter_map(Value::as_object_mut) {
            rename_keys(org, &ORGANIZATION, true);
        }
    }
    Value::Object(doc)
}

/// Accepts every bulk item.
struct Bulk;

impl Respond for Bulk {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let text = std::str::from_utf8(&request.body).unwrap();
        let items = vec![json!({"index": {"status": 201}}); text.lines().count() / 2];
        ResponseTemplate::new(200).set_body_json(json!({"errors": false, "items": items}))
    }
}

/// The documents of every bulk request `server` received, by id.
async fn bulk_documents(server: &MockServer) -> BTreeMap<u64, Value> {
    let mut docs = BTreeMap::new();
    for request in server.received_requests().await.unwrap() {
        if !request.url.path().ends_with("/_bulk") {
            continue;
        }
        let text = String::from_utf8(request.body).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        for pair in lines.chunks(2) {
            let action: Value = serde_json::from_str(pair[0]).unwrap();
            let id = action["index"]["_id"].as_str().unwrap().parse().unwrap();
            docs.insert(id, serde_json::from_str(pair[1]).unwrap());
        }
    }
    docs
}

fn shards() -> Value {
    json!({"total": 2, "successful": 2, "skipped": 0, "failed": 0})
}

/// A first publish of `index` under alias `persons`, with nothing to replace.
async fn mount_cluster(server: &MockServer, index: &str, documents: u64, bulk: Bulk) {
    let ok = |body: Value| ResponseTemplate::new(200).set_body_json(body);
    Mock::given(method("GET"))
        .and(path("/_nodes/settings"))
        .respond_with(ok(json!({
            "_nodes": {"total": 1, "successful": 1, "failed": 0},
            "nodes": {"a": {"roles": ["data"]}}
        })))
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/{index}")))
        .respond_with(ok(
            json!({"acknowledged": true, "shards_acknowledged": true}),
        ))
        .expect(1)
        .mount(server)
        .await;
    let mut mapping = mapping_file();
    mapping["_meta"] = json!({"people_index": {"run_id": "renamed"}});
    Mock::given(method("GET"))
        .and(path(format!("/{index}/_mapping")))
        .respond_with(ok(json!({index: {"mappings": mapping}})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{index}/_settings")))
        .respond_with(ok(json!({index: {"settings": {"index": {}}}})))
        .mount(server)
        .await;
    Mock::given(method("HEAD"))
        .and(path(format!("/{index}")))
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_bulk")))
        .respond_with(bulk)
        .mount(server)
        .await;
    for op in ["_refresh", "_flush"] {
        Mock::given(method("POST"))
            .and(path(format!("/{index}/{op}")))
            .respond_with(ok(json!({"_shards": shards()})))
            .mount(server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path(format!("/{index}/_count")))
        .respond_with(ok(json!({"count": documents, "_shards": shards()})))
        .mount(server)
        .await;
    Mock::given(method("PUT"))
        .and(path(format!("/{index}/_settings")))
        .respond_with(ok(json!({"acknowledged": true})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/_cluster/health/{index}")))
        .respond_with(ok(json!({"status": "green", "timed_out": false})))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"status": 404})))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_aliases"))
        .respond_with(ok(json!({"acknowledged": true})))
        .expect(1)
        .mount(server)
        .await;
}

/// The schema and mapping files of the renamed feed, written to `dir` and read back through
/// the command line's settings (`--schema-file`, `--mapping-file`), as `run` reads them. The
/// schema file is the documented example.
fn settings_from_files(dir: &Path) -> (FeedSchema, Value) {
    let schema_path = dir.join("schema.json");
    let mapping_path = dir.join("mapping.json");
    std::fs::write(&schema_path, documented_schema_file()).unwrap();
    std::fs::write(
        &mapping_path,
        serde_json::to_string_pretty(&mapping_file()).unwrap(),
    )
    .unwrap();
    let cli = Cli::try_parse_from([
        "people-index".as_ref(),
        "run".as_ref(),
        "--schema-file".as_ref(),
        schema_path.as_os_str(),
        "--mapping-file".as_ref(),
        mapping_path.as_os_str(),
    ])
    .unwrap();
    let Command::Run(args) = cli.command else {
        unreachable!("parsed run")
    };
    let schema = settings::feed_schema(&args.pipeline).unwrap().unwrap();
    let mapping = settings::index_mapping(&args.pipeline).unwrap().unwrap();
    assert_eq!(
        schema,
        FeedSchema::from_json(&schema_file().to_string()).unwrap(),
        "the documented example renames what this test renames"
    );
    // Either file alone does not fit the other half.
    assert!(pi_es::check_mapping(&mapping, FeedSchema::builtin()).is_err());
    assert!(pi_es::check_mapping(&pi_es::mapping(), &schema).is_err());
    (schema, mapping)
}

/// The renamed run's documents and counters are the built-in run's, renamed.
async fn assert_renamed_twin(data: &Generated, store: &Store, docs: &BTreeMap<u64, Vec<u8>>) {
    let builtin_docs = read_docs(&data.store, "builtin").await;
    assert_eq!(docs.len(), builtin_docs.len());
    assert_eq!(docs.len() as u64, data.expected.persons);
    for (id, body) in &builtin_docs {
        let want: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(renamed_back(&docs[id]), want, "document {id}");
    }
    // Under the renamed names the documents carry the new names and the builder's own.
    let resolved = docs
        .values()
        .map(|b| serde_json::from_slice::<Value>(b).unwrap())
        .find(|d| {
            d["positions"]
                .as_array()
                .is_some_and(|p| p.iter().any(|r| r["organization_resolved"] == true))
        });
    let resolved = resolved.expect("a person with a resolved organization");
    assert!(resolved.get("roles").is_none(), "{resolved}");
    assert!(
        resolved["organizations"][0].get("label").is_some(),
        "{resolved}"
    );
    for stage in [
        StageName::ExtractOrgs,
        StageName::ExtractPersons,
        StageName::Join,
        StageName::Assemble,
    ] {
        let mut want = counters(&data.store, "builtin", stage).await;
        let mut got = counters(store, "renamed", stage).await;
        // The rewritten files are compressed differently.
        for c in [&mut want, &mut got] {
            c.remove("decompressed_bytes");
            c.remove("segments");
        }
        assert_eq!(got, want, "{stage:?} counters");
    }
}

/// Runs `prepare-index`, `load` and `publish` of the renamed run against a mock cluster and
/// checks the index's mapping, the loaded documents and the summary's pin.
async fn publish_renamed(
    store: &Store,
    renamed: &StagesConfig,
    plan: &Plan,
    docs: &BTreeMap<u64, Vec<u8>>,
) {
    let server = MockServer::start().await;
    mount_cluster(&server, &plan.index, docs.len() as u64, Bulk).await;
    let mut es = renamed.clone();
    es.es.client = pi_es::EsConfig::new(server.uri());
    es.es.client.retry = pi_es::RetryPolicy {
        max_attempts: 3,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
    };
    es.es.bulk.max_bytes = 64 << 10;
    es.es.health_timeout = Duration::from_secs(5);
    es.es.publish_min_share = 0.0;
    run_stages(
        store,
        "renamed",
        es,
        &[StageName::PrepareIndex, StageName::Load, StageName::Publish],
    )
    .await
    .unwrap();
    let requests = server.received_requests().await.unwrap();
    let create = requests
        .iter()
        .find(|r| r.method.as_str() == "PUT" && r.url.path() == format!("/{}", plan.index))
        .unwrap();
    let body: Value = serde_json::from_slice(&create.body).unwrap();
    let mut sent = body["mappings"].clone();
    assert_eq!(
        sent.as_object_mut().unwrap().remove("_meta"),
        Some(json!({"people_index": {"run_id": "renamed"}}))
    );
    assert_eq!(&sent, &*renamed.mapping);
    let loaded = bulk_documents(&server).await;
    assert_eq!(loaded.len(), docs.len());
    for (id, body) in docs {
        assert_eq!(loaded[id], serde_json::from_slice::<Value>(body).unwrap());
    }
    let ctx = RunContext::new("renamed", store.clone()).unwrap();
    let (summary, _) = store
        .get_json::<Summary>(&ctx.layout.summary())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(summary.schema, Some(renamed.schema_pin()));
    assert!(!summary.builtin_schema());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feed_with_other_names_runs_end_to_end_through_a_schema_file() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "builtin", config(4), &OFFLINE)
        .await
        .unwrap();

    // The same data under other names, with its schema and mapping in files.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::local(dir.path()).unwrap();
    copy_renamed(data.dir.path(), &store).await;
    let files = tempfile::tempdir().unwrap();
    let (schema, mapping) = settings_from_files(files.path());
    let renamed = StagesConfig {
        schema: Arc::new(schema),
        mapping: Arc::new(mapping),
        ..config(4)
    };
    renamed.validate().unwrap();
    assert_ne!(renamed.schema_pin(), SchemaPin::builtin());

    run_stages(&store, "renamed", renamed.clone(), &OFFLINE)
        .await
        .unwrap();
    let docs = read_docs(&store, "renamed").await;
    assert_renamed_twin(&data, &store, &docs).await;

    // The plan pins the files' digests; the built-in schema cannot pick the run up.
    let ctx = RunContext::new("renamed", store.clone()).unwrap();
    let plan = Plan::load(&ctx).await.unwrap();
    assert_eq!(plan.schema, Some(renamed.schema_pin()));
    let err = run_stages(&store, "renamed", config(4), &[StageName::PrepareIndex])
        .await
        .unwrap_err();
    let text = format!("{err:#}");
    assert!(text.contains("planned with another feed schema"), "{text}");
    assert!(text.contains("PI_SCHEMA_FILE"), "{text}");

    // The index is created with the file's mapping, loaded and published.
    publish_renamed(&store, &renamed, &plan, &docs).await;
}
