//! Inputs that must fail a run, and fail it for good (no retry can fix the bytes).

mod common;

use std::io::Write as _;

use bytes::Bytes;
use common::{OFFLINE, config, counters, generate, run_stages, small_config};
use flate2::Compression;
use flate2::write::GzEncoder;
use pi_coord::{CommitLog, RunContext, StageName, is_permanent};
use pi_stages::StageSummary;
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
async fn mapped_fields_of_the_wrong_type_are_quarantined_not_fatal() {
    let orgs = "{\"id\":1,\"record\":{\"name\":\"A\",\"founded_year\":1999}}\n\
                {\"id\":2,\"record\":{\"name\":\"B\",\"founded_year\":{\"year\":1999}}}\n\
                {\"id\":3,\"record\":{\"name\":\"C\",\"founded_year\":\"long ago\"}}\n";
    let persons = "{\"id\":5,\"record\":{\"open_to_work\":true,\"roles\":[{\"organization_id\":1}]}}\n\
         {\"id\":6,\"record\":{\"open_to_work\":{\"value\":true}}}\n\
         {\"id\":7,\"record\":{\"id\":\"abc\"}}\n\
         {\"id\":8,\"record\":{\"roles\":\"engineer\"}}\n\
         {\"id\":9,\"record\":{\"roles\":[{\"organization_id\":2}]}}\n\
         {\"id\":10,\"record\":{\"followers\":\"n/a\",\"open_to_work\":[\"yes\",true],\"roles\":[{\"organization_id\":3}]}}\n";
    let (_dir, store) = store_with(gzip(persons)).await;
    store
        .put("raw/organizations/part-00000.ndjson.gz", gzip(orgs))
        .await
        .unwrap();
    let mut cfg = config(1);
    cfg.max_malformed = 3;
    run_stages(&store, "run-types", cfg, &OFFLINE)
        .await
        .unwrap();

    let docs = common::read_docs(&store, "run-types").await;
    assert_eq!(docs.keys().copied().collect::<Vec<_>>(), [5, 9, 10]);
    // Organization 2 was quarantined, so person 9's role no longer resolves.
    let doc: serde_json::Value = serde_json::from_slice(&docs[&9]).unwrap();
    assert_eq!(doc["unresolved_organization_ids"], serde_json::json!([2]));
    // Values Elasticsearch skips (`ignore_malformed`) keep the record, as they are, and count.
    let doc: serde_json::Value = serde_json::from_slice(&docs[&10]).unwrap();
    assert_eq!(doc["followers"], "n/a");
    assert_eq!(doc["open_to_work"], serde_json::json!(["yes", true]));
    assert_eq!(doc["organizations"][0]["founded_year"], "long ago");
    let ctx = RunContext::new("run-types", store.clone()).unwrap();
    for (stage, ignored, malformed) in [
        (StageName::ExtractOrgs, 1, 1),
        (StageName::ExtractPersons, 2, 3),
    ] {
        let counted = counters(&store, "run-types", stage).await;
        assert_eq!(
            counted.get("values_ignored_by_index"),
            Some(&ignored),
            "{stage:?}"
        );
        assert_eq!(counted.get("malformed"), Some(&malformed), "{stage:?}");
        // summary.json sums the same commits.
        let commits = CommitLog::new(store.clone(), ctx.layout.clone())
            .list(stage)
            .await
            .unwrap();
        let summary = StageSummary::from_commits(stage, &commits);
        assert_eq!(
            summary.counters["values_ignored_by_index"], ignored,
            "{stage:?}"
        );
    }

    // Each quarantined line is a `record` entry whose reason names the field.
    let mut entries = Vec::new();
    for key in store.list_keys("runs/run-types/quarantine").await.unwrap() {
        let bytes = store.get(&key).await.unwrap().bytes;
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let entry: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert_eq!(entry["kind"], "record", "{entry}");
            entries.push((
                entry["feed"].as_str().unwrap().to_owned(),
                entry["line"].as_u64().unwrap(),
                entry["reason"].as_str().unwrap().to_owned(),
            ));
        }
    }
    entries.sort();
    let expected = [
        (
            "organizations",
            2,
            "founded_year: expected a string, number or boolean, found an object",
        ),
        (
            "persons",
            2,
            "open_to_work: expected a string, number or boolean, found an object",
        ),
        ("persons", 3, "id: expected a number"),
        ("persons", 4, "`roles` is not an array"),
    ];
    assert_eq!(entries.len(), expected.len(), "{entries:?}");
    for ((feed, line, reason), (want_feed, want_line, needle)) in entries.iter().zip(expected) {
        assert_eq!((feed.as_str(), *line), (want_feed, want_line), "{reason}");
        assert!(reason.starts_with(needle), "{reason}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn values_the_index_skips_do_not_count_as_malformed() {
    let orgs = "{\"id\":1,\"record\":{\"name\":\"A\",\"founded_year\":\"long ago\",\"employee_count\":3000000000}}\n";
    let persons = "{\"id\":5,\"record\":{\"followers\":\"n/a\",\"connections\":[5,\"x\"],\"open_to_work\":\"yes\",\"roles\":[{\"organization_id\":1}]}}\n\
         {\"id\":6,\"record\":{\"followers\":1e19,\"open_to_work\":1}}\n";
    let (_dir, store) = store_with(gzip(persons)).await;
    store
        .put("raw/organizations/part-00000.ndjson.gz", gzip(orgs))
        .await
        .unwrap();
    let mut cfg = config(1);
    cfg.max_malformed = 0;
    run_stages(&store, "run-lenient", cfg, &OFFLINE)
        .await
        .unwrap();

    let docs = common::read_docs(&store, "run-lenient").await;
    assert_eq!(docs.keys().copied().collect::<Vec<_>>(), [5, 6]);
    // `employee_count` 3000000000 fits a `long`: only `founded_year` is skipped.
    for (stage, ignored) in [(StageName::ExtractOrgs, 1), (StageName::ExtractPersons, 5)] {
        let counted = counters(&store, "run-lenient", stage).await;
        assert_eq!(counted.get("malformed"), Some(&0), "{stage:?}");
        assert_eq!(
            counted.get("values_ignored_by_index"),
            Some(&ignored),
            "{stage:?}"
        );
    }
    assert!(
        store
            .list_keys("runs/run-lenient/quarantine")
            .await
            .unwrap()
            .is_empty()
    );
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
