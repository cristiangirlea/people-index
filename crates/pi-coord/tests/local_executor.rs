//! The local executor with fake stages on the in-memory store.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{Behavior, FakeStage, coordination, ctx, fast_retry};
use pi_coord::{
    CommitLog, Executor, LocalExecutor, RunBy, StageName, StageSpec, is_cancelled, is_permanent,
};
use pi_store::Store;

fn executor(concurrency: usize, attempts: u32) -> LocalExecutor {
    LocalExecutor::new(coordination("local", 60_000))
        .with_concurrency(concurrency)
        .with_retry(fast_retry(attempts))
}

#[tokio::test]
async fn rerunning_a_stage_runs_nothing_twice() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(FakeStage::new(StageName::Join, 6));
    let exec = executor(3, 3);

    let first = exec.run_stage(stage.clone(), &ctx).await.unwrap();
    assert_eq!((first.ran, first.skipped, first.tasks()), (6, 0, 6));
    assert_eq!(first.counter("records"), 6);
    assert_eq!(first.bytes_out(), 60);

    let second = exec.run_stage(stage.clone(), &ctx).await.unwrap();
    assert_eq!((second.ran, second.skipped), (0, 6));
    assert_eq!(
        second.commits, first.commits,
        "the report is the same commits"
    );
    for id in stage.task_ids() {
        assert_eq!(stage.runs_of(&id), 1, "{id} ran once");
    }
    let spec = StageSpec::load(&store, &ctx.layout, StageName::Join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec.run_by, RunBy::Coordinator);
    assert_eq!(spec.tasks.len(), 6);
}

#[tokio::test]
async fn concurrency_is_bounded() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(FakeStage::new(StageName::Join, 10).with_delay(Duration::from_millis(30)));
    executor(3, 1).run_stage(stage.clone(), &ctx).await.unwrap();
    let max = stage.max_in_flight.load(Ordering::SeqCst);
    assert!(max <= 3, "at most 3 tasks at once, saw {max}");
    assert!(max >= 2, "tasks do run in parallel, saw {max}");
}

#[tokio::test]
async fn a_transient_failure_is_retried_and_its_outputs_are_orphans() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(
        FakeStage::new(StageName::Join, 4).with_script(|task, attempt| {
            if task.partition == Some(2) && attempt < 3 {
                Behavior::Fail
            } else {
                Behavior::Succeed
            }
        }),
    );
    let report = executor(2, 3).run_stage(stage.clone(), &ctx).await.unwrap();
    assert_eq!(report.ran, 4);
    assert_eq!(stage.runs_of("part-00002"), 3);
    let log = CommitLog::new(store.clone(), ctx.layout.clone());
    let orphans = log.orphans(StageName::Join).await.unwrap();
    assert_eq!(
        orphans.len(),
        2,
        "the two failed attempts' files: {orphans:?}"
    );
    assert!(orphans.iter().all(|k| k.contains("part-00002")));
    assert_eq!(report.outputs().count(), 4);
}

#[tokio::test]
async fn a_task_that_keeps_failing_fails_the_stage() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(FakeStage::new(StageName::Join, 5).with_script(|task, _| {
        if task.partition == Some(1) {
            Behavior::Fail
        } else {
            Behavior::Succeed
        }
    }));
    let err = executor(1, 3)
        .run_stage(stage.clone(), &ctx)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("part-00001") && msg.contains("3 attempt"),
        "{msg}"
    );
    assert!(!is_cancelled(&err));
    assert_eq!(stage.runs_of("part-00001"), 3);
    // With one task at a time, the stage stops at the failure: later tasks never start.
    assert_eq!(stage.runs_of("part-00004"), 0, "the stage stopped");
    let log = CommitLog::new(store, ctx.layout.clone());
    assert!(
        !log.is_committed(StageName::Join, "part-00001")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_permanent_failure_is_not_retried_and_cancels_siblings() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage =
        Arc::new(
            FakeStage::new(StageName::Join, 3).with_script(|task, _| match task.partition {
                Some(0) => Behavior::FailPermanently,
                _ => Behavior::Hang,
            }),
        );
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        executor(3, 5).run_stage(stage.clone(), &ctx),
    )
    .await
    .expect("a permanent failure stops hanging siblings")
    .unwrap_err();
    assert!(is_permanent(&err));
    assert_eq!(stage.runs_of("part-00000"), 1);
    assert_eq!(
        stage.in_flight.load(Ordering::SeqCst),
        0,
        "the hanging siblings were stopped"
    );
}

#[tokio::test]
async fn a_panicking_task_fails_the_stage() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(FakeStage::new(StageName::Join, 2).with_script(|task, _| {
        if task.partition == Some(1) {
            Behavior::Panic
        } else {
            Behavior::Succeed
        }
    }));
    let err = executor(2, 3).run_stage(stage, &ctx).await.unwrap_err();
    assert!(format!("{err:#}").contains("panicked"), "{err:#}");
}

#[tokio::test]
async fn cancelling_the_run_stops_the_stage_without_failing_it() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(FakeStage::new(StageName::Join, 2).with_script(|_, _| Behavior::Hang));
    let cancel = ctx.cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });
    let err = executor(2, 3).run_stage(stage, &ctx).await.unwrap_err();
    assert!(is_cancelled(&err), "{err:#}");
}

#[tokio::test]
async fn single_task_stages_use_the_run_id() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let stage = Arc::new(FakeStage::new(StageName::Plan, 0));
    let report = executor(4, 1).run_stage(stage.clone(), &ctx).await.unwrap();
    assert_eq!(report.commits[0].task_id, "run");
    assert_eq!(report.commits[0].worker, "local");
    assert!(
        store
            .head(&ctx.layout.commit("plan", "run").unwrap())
            .await
            .unwrap()
            .is_some()
    );
}
