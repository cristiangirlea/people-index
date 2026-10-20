//! The offline half of a run (plan to assemble) on a generated dataset in a local store:
//! documents checked against `expected.json` and an independent reference join, identical for
//! every partition count and every pass count of `join` and `assemble`, idempotent, and
//! unaffected by a duplicate attempt.

mod common;

use std::collections::BTreeMap;

use common::{
    OFFLINE, Reference, check_run, config, counters, executor, generate, read_docs, run_stages,
    small_config,
};
use pi_coord::{
    AttemptId, CommitLog, CommitOutcome, CommitRecord, Executor, RunContext, StageName,
};
use pi_stages::StagesConfig;
use pi_store::Store;
use time::OffsetDateTime;

/// The commits of one stage of a run.
async fn commits(store: &Store, run_id: &str, stage: StageName) -> Vec<CommitRecord> {
    let ctx = RunContext::new(run_id, store.clone()).unwrap();
    CommitLog::new(store.clone(), ctx.layout)
        .list(stage)
        .await
        .unwrap()
}

/// The fewest raw bytes any partition of `orgs/` (what `join` holds) or `joined/` (what
/// `assemble` holds) has in run `run_id`, from the sizes its commits recorded.
async fn smallest_held_partition(store: &Store, run_id: &str, partitions: u32) -> u64 {
    let mut sizes = BTreeMap::<String, u64>::new();
    for (stage, dataset) in [
        (StageName::ExtractOrgs, "orgs/"),
        (StageName::Join, "joined/"),
    ] {
        for commit in commits(store, run_id, stage).await {
            for (name, size) in &commit.report.partitions {
                if name.starts_with(dataset) {
                    *sizes.entry(name.clone()).or_default() += size.raw_bytes;
                }
            }
        }
    }
    assert_eq!(sizes.len(), 2 * partitions as usize, "{run_id}: {sizes:?}");
    *sizes.values().min().unwrap()
}

/// A stage's summed counters without the ones that depend on the pass count.
async fn pass_independent_counters(
    store: &Store,
    run_id: &str,
    stage: StageName,
) -> BTreeMap<String, u64> {
    let mut c = counters(store, run_id, stage).await;
    c.remove("passes");
    c.remove("segments");
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn documents_match_expected_counts_and_the_reference_for_every_partition_count() {
    let data = generate(small_config()).await;
    let reference = Reference::read(data.dir.path());
    let mut runs = Vec::new();
    for partitions in [1u32, 4, 16] {
        let run_id = format!("run-p{partitions}");
        let reports = run_stages(&data.store, &run_id, config(partitions), &OFFLINE)
            .await
            .unwrap();
        assert_eq!(
            reports[3].tasks(),
            partitions as usize,
            "one join task per partition"
        );
        assert_eq!(
            reports[4].tasks(),
            partitions as usize,
            "one assemble task per partition"
        );
        runs.push(check_run(&data, &run_id, &reference).await);
    }
    // Byte-identical documents whatever the partition count.
    assert_eq!(runs[0], runs[1]);
    assert_eq!(runs[1], runs[2]);

    // And whatever the pass count: a task memory budget of a third of the smallest partition's
    // raw bytes makes every join and assemble task run at least three passes. Documents are
    // compared by person id (their order inside a segment depends on the passes).
    for partitions in [1u32, 4] {
        let single = format!("run-p{partitions}");
        let budget = smallest_held_partition(&data.store, &single, partitions).await / 3;
        let run_id = format!("run-p{partitions}-passes");
        let cfg = StagesConfig {
            task_memory_bytes: budget,
            ..config(partitions)
        };
        run_stages(&data.store, &run_id, cfg, &OFFLINE)
            .await
            .unwrap();
        for stage in [StageName::Join, StageName::Assemble] {
            let tasks = commits(&data.store, &run_id, stage).await;
            assert_eq!(tasks.len(), partitions as usize);
            for c in &tasks {
                assert!(
                    c.report.counter("passes") >= 3,
                    "{run_id} {stage:?} {}: {} passes",
                    c.task_id,
                    c.report.counter("passes")
                );
            }
            for c in commits(&data.store, &single, stage).await {
                assert_eq!(
                    c.report.counter("passes"),
                    1,
                    "{single}: one pass by default"
                );
            }
            assert_eq!(
                pass_independent_counters(&data.store, &run_id, stage).await,
                pass_independent_counters(&data.store, &single, stage).await,
                "{run_id} {stage:?}"
            );
        }
        assert_eq!(check_run(&data, &run_id, &reference).await, runs[0]);
    }

    // The quarantine names the origin of every malformed line.
    let ctx = RunContext::new("run-p4", data.store.clone()).unwrap();
    let quarantined: u64 = {
        let mut n = 0;
        for key in data
            .store
            .list_keys(&ctx.layout.quarantine_dir())
            .await
            .unwrap()
        {
            let bytes = data.store.get(&key).await.unwrap().bytes;
            for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                let entry: serde_json::Value = serde_json::from_slice(line).unwrap();
                assert!(entry["file"].as_str().unwrap().starts_with("raw/"));
                assert!(entry["line"].as_u64().unwrap() >= 1);
                assert!(entry["kind"].is_string() && entry["reason"].is_string());
                n += 1;
            }
        }
        n
    };
    assert_eq!(
        quarantined,
        data.expected.malformed.persons + data.expected.malformed.organizations
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assemble_twice_is_idempotent() {
    let data = generate(small_config()).await;
    run_stages(&data.store, "run-a", config(4), &OFFLINE)
        .await
        .unwrap();
    let before = read_docs(&data.store, "run-a").await;

    // Through the executor: every task is committed, so nothing runs.
    let again = run_stages(&data.store, "run-a", config(4), &[StageName::Assemble])
        .await
        .unwrap();
    assert_eq!((again[0].ran, again[0].skipped), (0, 4));
    assert_eq!(read_docs(&data.store, "run-a").await, before);

    // Straight to the stage with a fresh attempt: the same documents, under new keys that no
    // commit lists, so readers still see exactly one copy.
    let registry = pi_stages::build_registry(config(4)).unwrap();
    let stage = registry.get(StageName::Assemble).unwrap();
    let ctx = RunContext::new("run-a", data.store.clone()).unwrap();
    let tasks = stage.tasks(&ctx).await.unwrap();
    let mut rerun = std::collections::BTreeMap::new();
    for task in &tasks {
        let report = stage.run(&ctx, task, &AttemptId::new()).await.unwrap();
        for key in report.outputs.iter().filter(|k| k.contains("/data/docs/")) {
            let bytes = data.store.get(key).await.unwrap().bytes;
            for doc in pi_segment::decode_segment::<pi_model::DocRow>(&bytes).unwrap() {
                rerun.insert(doc.person_id, doc.body);
            }
        }
    }
    assert_eq!(rerun, before);
    assert_eq!(read_docs(&data.store, "run-a").await, before);
    let orphans = CommitLog::new(data.store.clone(), ctx.layout.clone())
        .orphans(StageName::Assemble)
        .await
        .unwrap();
    assert_ne!(orphans, [] as [std::string::String; 0]);
    let (deleted, left) = pi_stages::delete_orphans(&ctx).await.unwrap();
    assert_eq!((deleted, left), (orphans.len() as u64, 0));
    assert_eq!(read_docs(&data.store, "run-a").await, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_attempt_is_invisible() {
    let data = generate(small_config()).await;
    let reference = Reference::read(data.dir.path());
    let run_id = "run-dup";
    run_stages(
        &data.store,
        run_id,
        config(4),
        &[StageName::Plan, StageName::ExtractOrgs],
    )
    .await
    .unwrap();

    // Two attempts of the same extract-persons task finish; only the first commit counts.
    let registry = pi_stages::build_registry(config(4)).unwrap();
    let stage = registry.get(StageName::ExtractPersons).unwrap();
    let ctx = RunContext::new(run_id, data.store.clone()).unwrap();
    let task = stage.tasks(&ctx).await.unwrap().remove(0);
    let (first, second) = (AttemptId::new(), AttemptId::new());
    let report_1 = stage.run(&ctx, &task, &first).await.unwrap();
    let report_2 = stage.run(&ctx, &task, &second).await.unwrap();
    assert_eq!(report_1.counters, report_2.counters);
    let log = CommitLog::new(data.store.clone(), ctx.layout.clone());
    let record = |attempt, report| CommitRecord {
        stage: StageName::ExtractPersons,
        task_id: task.task_id.clone(),
        attempt,
        worker: "test".into(),
        committed_at: OffsetDateTime::now_utc(),
        report,
    };
    assert_eq!(
        log.commit(&record(first, report_1.clone())).await.unwrap(),
        CommitOutcome::Committed
    );
    assert!(matches!(
        log.commit(&record(second, report_2.clone())).await.unwrap(),
        CommitOutcome::AlreadyCommitted(winner) if winner.attempt == first
    ));

    // The rest of the stage and the run go on as usual.
    let exec = executor("test");
    let report = exec.run_stage(stage, &ctx).await.unwrap();
    assert_eq!(report.skipped, 1);
    run_stages(
        &data.store,
        run_id,
        config(4),
        &[StageName::Join, StageName::Assemble],
    )
    .await
    .unwrap();
    check_run(&data, run_id, &reference).await;

    // The loser's files are exactly the orphans.
    let mut orphans = log.orphans(StageName::ExtractPersons).await.unwrap();
    orphans.sort();
    let mut losers: Vec<String> = report_2
        .outputs
        .iter()
        .filter(|k| !k.contains("/quarantine/"))
        .cloned()
        .collect();
    losers.sort();
    assert_eq!(orphans, losers);
}
