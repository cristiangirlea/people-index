//! Resuming a run with the same id: after a crash, after a failure, after it was published.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{Behavior, FakeStage, coordination, ctx, eventually, fast_retry};
use pi_coord::{
    CommitLog, Coordination, LocalExecutor, RunDriver, RunState, RunStatus, StageName,
    StageRegistry,
};
use pi_store::Store;

fn driver(registry: &Arc<StageRegistry>) -> RunDriver {
    let coord = coordination("coordinator", 60_000);
    let exec = LocalExecutor::new(coord.clone())
        .with_concurrency(2)
        .with_retry(fast_retry(2));
    RunDriver::new(Arc::new(exec), Arc::clone(registry), coord)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_coordinator_resumes_without_redoing_committed_work() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let plan = Arc::new(FakeStage::new(StageName::Plan, 0));
    let extract =
        Arc::new(FakeStage::new(StageName::ExtractOrgs, 8).with_delay(Duration::from_millis(40)));
    let join = Arc::new(FakeStage::new(StageName::Join, 3));
    let registry = Arc::new(
        StageRegistry::new()
            .with(plan.clone())
            .with(extract.clone())
            .with(join.clone()),
    );

    // Crash the coordinator in the middle of extract-orgs.
    let first = {
        let d = driver(&registry);
        let ctx = ctx.clone();
        tokio::spawn(async move { d.run(&ctx).await })
    };
    let log = CommitLog::new(store.clone(), ctx.layout.clone());
    assert!(
        eventually(Duration::from_secs(5), || async {
            log.committed_task_ids(StageName::ExtractOrgs)
                .await
                .unwrap()
                .len()
                >= 3
        })
        .await
    );
    first.abort();
    let _ = first.await;
    let committed_before = log
        .committed_task_ids(StageName::ExtractOrgs)
        .await
        .unwrap();
    assert!(committed_before.len() < 8, "the crash happened mid-stage");
    let state = RunState::load(&store, &ctx.layout).await.unwrap().unwrap();
    assert_eq!(state.status, RunStatus::Running);
    assert_eq!(state.stage, Some(StageName::ExtractOrgs));
    assert_eq!(state.completed, vec![StageName::Plan]);

    let outcome = driver(&registry).run(&ctx).await.unwrap();
    assert_eq!(outcome.status, RunStatus::Published);
    assert_eq!(outcome.skipped, vec![StageName::Plan]);
    assert_eq!(outcome.stages[0].skipped, committed_before.len());
    assert_eq!(outcome.stages[0].tasks(), 8);
    assert_eq!(plan.runs_of("run"), 1, "plan was not run again");
    for id in &committed_before {
        assert_eq!(
            extract.runs_of(id),
            1,
            "{id} was committed before the crash"
        );
    }
    for id in join.task_ids() {
        assert_eq!(join.runs_of(&id), 1);
    }
}

#[tokio::test]
async fn a_failed_run_resumes_from_the_failed_stage() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let broken = Arc::new(AtomicBool::new(true));
    let extract = Arc::new(FakeStage::new(StageName::ExtractOrgs, 3));
    let join = Arc::new(FakeStage::new(StageName::Join, 4).with_script({
        let broken = Arc::clone(&broken);
        move |task, _| {
            if task.partition == Some(2) && broken.load(Ordering::SeqCst) {
                Behavior::FailPermanently
            } else {
                Behavior::Succeed
            }
        }
    }));
    let registry = Arc::new(
        StageRegistry::new()
            .with(extract.clone())
            .with(join.clone()),
    );
    let coord = coordination("coordinator", 60_000);

    let err = driver(&registry).run(&ctx).await.unwrap_err();
    assert!(format!("{err:#}").contains("part-00002"), "{err:#}");
    let state = RunState::load(&store, &ctx.layout).await.unwrap().unwrap();
    assert_eq!(state.status, RunStatus::Failed);
    assert!(
        state
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("part-00002")
    );
    assert!(
        coord.failure(&ctx).await.unwrap().is_some(),
        "workers would see the marker"
    );

    broken.store(false, Ordering::SeqCst);
    let outcome = driver(&registry).run(&ctx).await.unwrap();
    assert_eq!(outcome.status, RunStatus::Published);
    assert_eq!(outcome.skipped, vec![StageName::ExtractOrgs]);
    assert!(
        coord.failure(&ctx).await.unwrap().is_none(),
        "the marker was cleared"
    );
    assert_eq!(extract.total_runs(), 3, "extract-orgs ran once in total");
    assert_eq!(join.runs_of("part-00002"), 2);
    for id in ["part-00000", "part-00001", "part-00003"] {
        // With two tasks at a time, siblings committed before the failure stay committed;
        // a sibling cancelled by the failure runs again.
        assert!(join.runs_of(id) >= 1);
    }
    let state = RunState::load(&store, &ctx.layout).await.unwrap().unwrap();
    assert_eq!(state.status, RunStatus::Published);
    assert_eq!(state.error, None);
}

#[tokio::test]
async fn running_a_published_run_again_does_nothing() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let plan = Arc::new(FakeStage::new(StageName::Plan, 0));
    let registry = Arc::new(StageRegistry::new().with(plan.clone()));
    driver(&registry).run(&ctx).await.unwrap();
    let again = driver(&registry).run(&ctx).await.unwrap();
    assert_eq!(again.status, RunStatus::Published);
    assert_eq!(again.stages, [] as [pi_coord::StageReport; 0]);
    assert_eq!(again.skipped, vec![StageName::Plan]);
    assert_eq!(plan.runs_of("run"), 1);
}
