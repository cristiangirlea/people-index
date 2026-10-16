//! Inputs that must fail a run, and fail it for good (no retry can fix the bytes).

mod common;

use std::io::Write as _;

use bytes::Bytes;
use common::{OFFLINE, config, generate, run_stages, small_config};
use flate2::Compression;
use flate2::write::GzEncoder;
use pi_coord::{StageName, is_permanent};
use pi_store::Store;

fn gzip(text: &str) -> Vec<u8> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::fast());
    gz.write_all(text.as_bytes()).unwrap();
    gz.finish().unwrap()
}

async fn store_with(persons: Vec<u8>) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::local(dir.path()).unwrap();
    store
        .put(
            "raw/organizations/part-00000.ndjson.gz",
            gzip("{\"id\":1,\"record\":{\"name\":\"A\"}}\n"),
        )
        .await
        .unwrap();
    store
        .put("raw/persons/part-00000.ndjson.gz", Bytes::from(persons))
        .await
        .unwrap();
    (dir, store)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn too_many_malformed_lines_fail_the_run() {
    let data = generate(small_config()).await;
    assert!(data.expected.malformed.persons > 2);
    let mut cfg = config(2);
    cfg.max_malformed = 1;
    let err = run_stages(&data.store, "run-m", cfg, &OFFLINE)
        .await
        .unwrap_err();
    assert!(is_permanent(&err), "{err:#}");
    assert!(format!("{err:#}").contains("malformed"), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_lines_up_to_the_limit_pass() {
    let text = "{\"id\":5,\"record\":{\"roles\":[{\"title\":\"x\",\"organization_id\":1}]}}\n\
                not json\n\
                {\"id\":6,\"record\":[]}\n";
    let (_dir, store) = store_with(gzip(text)).await;
    let mut cfg = config(1);
    cfg.max_malformed = 2;
    run_stages(&store, "run-ok", cfg.clone(), &OFFLINE)
        .await
        .unwrap();
    let docs = common::read_docs(&store, "run-ok").await;
    assert_eq!(docs.len(), 1);
    let doc: serde_json::Value = serde_json::from_slice(&docs[&5]).unwrap();
    assert_eq!(doc["organizations"][0]["name"], "A");
    assert_eq!(doc["roles"][0]["organization_resolved"], true);

    cfg.max_malformed = 1;
    let err = run_stages(&store, "run-strict", cfg, &OFFLINE)
        .await
        .unwrap_err();
    assert!(is_permanent(&err), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncated_gzip_file_fails_for_good() {
    let mut bytes = gzip(&"{\"id\":5,\"record\":{}}\n".repeat(200));
    bytes.truncate(bytes.len() - 6);
    let (_dir, store) = store_with(bytes).await;
    let err = run_stages(&store, "run-t", config(1), &OFFLINE)
        .await
        .unwrap_err();
    assert!(is_permanent(&err), "{err:#}");
    assert!(format!("{err:#}").contains("not a valid gzip"), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_crc_fails_for_good() {
    let mut bytes = gzip("{\"id\":5,\"record\":{}}\n");
    let n = bytes.len();
    bytes[n - 8] ^= 0xff; // the CRC32 in the gzip trailer
    let (_dir, store) = store_with(bytes).await;
    let err = run_stages(&store, "run-c", config(1), &OFFLINE)
        .await
        .unwrap_err();
    assert!(is_permanent(&err), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_file_fails_and_concatenated_members_pass() {
    let (_dir, store) = store_with(Vec::new()).await;
    let err = run_stages(&store, "run-e", config(1), &OFFLINE)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("empty"), "{err:#}");

    // Two gzip members back to back are one valid file (`cat a.gz b.gz`).
    let mut two = gzip("{\"id\":5,\"record\":{}}\n");
    two.extend(gzip("{\"id\":6,\"record\":{}}\n"));
    let (_dir, store) = store_with(two).await;
    run_stages(&store, "run-2", config(1), &OFFLINE)
        .await
        .unwrap();
    assert_eq!(common::read_docs(&store, "run-2").await.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_person_files_fail_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::local(dir.path()).unwrap();
    let err = run_stages(&store, "run-n", config(1), &[StageName::Plan])
        .await
        .unwrap_err();
    assert!(is_permanent(&err), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_input_replaced_after_the_plan_fails_for_good() {
    let text = "{\"id\":5,\"record\":{}}\n";
    let (_dir, store) = store_with(gzip(text)).await;
    run_stages(&store, "run-v", config(1), &[StageName::Plan])
        .await
        .unwrap();
    let (plan, _) = store
        .get_json::<serde_json::Value>("runs/run-v/plan.json")
        .await
        .unwrap()
        .unwrap();
    let planned = &plan["persons"][0]["version"]["e_tag"];
    assert!(planned.is_string(), "plan.json records the etag: {plan}");

    // Same records, new bytes: the run must not read them under its old plan.
    let key = "raw/persons/part-00000.ndjson.gz";
    store
        .put(key, Bytes::from(gzip(&text.repeat(2))))
        .await
        .unwrap();
    let err = run_stages(
        &store,
        "run-v",
        config(1),
        &[StageName::ExtractOrgs, StageName::ExtractPersons],
    )
    .await
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(is_permanent(&err), "{msg}");
    assert!(msg.contains(key), "{msg}");
    assert!(msg.contains("changed since the run was planned"), "{msg}");
    assert!(msg.contains(planned.as_str().unwrap()), "{msg}");

    // A new run plans the new bytes and goes through.
    run_stages(&store, "run-v2", config(1), &OFFLINE)
        .await
        .unwrap();
    assert_eq!(common::read_docs(&store, "run-v2").await.len(), 1);
}
