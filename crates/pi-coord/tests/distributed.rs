//! The distributed executor and workers with fake stages on the in-memory store.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{
    Behavior, CountingStore, FakeStage, Op, coordination, ctx, eventually, fast_retry, short_leases,
};
use pi_coord::{
    CommitLog, Coordination, DistributedExecutor, Executor, Lease, LeaseManager, RunBy, RunContext,
    RunDriver, RunFailed, RunState, RunStatus, StageName, StageRegistry, StageSpec, TaskSpec,
    Worker, WorkerExit,
};
use pi_store::Store;
use time::OffsetDateTime;

const POLL: Duration = Duration::from_millis(15);

fn worker(name: &str, registry: &Arc<StageRegistry>, ttl_ms: u64) -> Worker {
    Worker::new(coordination(name, ttl_ms), Arc::clone(registry))
        .with_concurrency(2)
        .with_retry(fast_retry(2))
        .with_poll_interval(POLL)
}

fn spawn_worker(
    w: Worker,
    ctx: &RunContext,
) -> tokio::task::JoinHandle<anyhow::Result<pi_coord::WorkerReport>> {
    let ctx = ctx.clone();
    tokio::spawn(async move { w.run(&ctx).await })
}

/// Puts a run in `stage` with a hand-written task list, as a coordinator would.
async fn hand_stage_to_workers(ctx: &RunContext, stage: StageName, tasks: Vec<TaskSpec>) {
    let mut run = RunState::new(ctx.run_id.clone());
    run.stage = Some(stage);
    run.save(&ctx.store, &ctx.layout).await.unwrap();
    StageSpec::new(stage, RunBy::Workers, tasks)
        .save(&ctx.store, &ctx.layout)
        .await
        .unwrap();
}

async fn finish_run(ctx: &RunContext) {
    let mut state = RunState::load(&ctx.store, &ctx.layout)
        .await
        .unwrap()
        .unwrap();
    state.status = RunStatus::Published;
    state.save(&ctx.store, &ctx.layout).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_completes_across_three_workers() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let plan = Arc::new(FakeStage::new(StageName::Plan, 0));
    let join = Arc::new(FakeStage::new(StageName::Join, 9).with_delay(Duration::from_millis(20)));
    let publish = Arc::new(FakeStage::new(StageName::Publish, 0));
    let registry = Arc::new(
        StageRegistry::new()
            .with(plan.clone())
            .with(join.clone())
            .with(publish.clone()),
    );

    let workers: Vec<_> = (0..3)
        .map(|i| spawn_worker(worker(&format!("w{i}"), &registry, 2_000), &ctx))
        .collect();
    let coord = coordination("coordinator", 2_000);
    let exec = DistributedExecutor::new(coord.clone()).with_poll_interval(POLL);
    let driver = RunDriver::new(Arc::new(exec), Arc::clone(&registry), coord);
    let outcome = tokio::time::timeout(Duration::from_secs(20), driver.run(&ctx))
        .await
        .expect("the run finishes")
        .unwrap();
    assert_eq!(outcome.status, RunStatus::Published);
    assert_eq!(outcome.stages.len(), 3);
    assert_eq!(outcome.stages[1].tasks(), 9);
    assert_eq!(outcome.stages[1].counter("records"), 9);

    let mut committed = 0;
    for w in workers {
        let report = tokio::time::timeout(Duration::from_secs(5), w)
            .await
            .expect("workers exit once the run is published")
            .unwrap()
            .unwrap();
        assert_eq!(report.exit, WorkerExit::Published);
        committed += report.committed;
    }
    assert_eq!(committed, 9, "workers committed every join task");
    for id in join.task_ids() {
        assert_eq!(join.runs_of(&id), 1, "{id} ran once");
    }
    assert_eq!(plan.runs_of("run"), 1, "the coordinator ran plan itself");
    assert_eq!(publish.runs_of("run"), 1);
    let spec = StageSpec::load(&store, &ctx.layout, StageName::Join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec.run_by, RunBy::Workers);
    let plan_spec = StageSpec::load(&store, &ctx.layout, StageName::Plan)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plan_spec.run_by, RunBy::Coordinator);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_racing_for_one_task_commit_it_once() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let join = Arc::new(FakeStage::new(StageName::Join, 1).with_delay(Duration::from_millis(100)));
    let registry = Arc::new(StageRegistry::new().with(join.clone()));
    hand_stage_to_workers(
        &ctx,
        StageName::Join,
        vec![TaskSpec::for_partition(StageName::Join, 0)],
    )
    .await;

    let a = spawn_worker(worker("a", &registry, 2_000), &ctx);
    let b = spawn_worker(worker("b", &registry, 2_000), &ctx);
    let log = CommitLog::new(store.clone(), ctx.layout.clone());
    assert!(
        eventually(Duration::from_secs(5), || async {
            log.is_committed(StageName::Join, "part-00000")
                .await
                .unwrap()
        })
        .await
    );
    finish_run(&ctx).await;
    let ra = a.await.unwrap().unwrap();
    let rb = b.await.unwrap().unwrap();
    assert_eq!(ra.committed + rb.committed, 1, "exactly one commit");
    assert_eq!(
        join.runs_of("part-00000"),
        1,
        "the lease kept the loser from running it"
    );
    assert_eq!(log.list(StageName::Join).await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_failing_on_a_worker_fails_the_run() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let join = Arc::new(FakeStage::new(StageName::Join, 4).with_script(|task, _| {
        if task.partition == Some(3) {
            Behavior::FailPermanently
        } else {
            Behavior::Succeed
        }
    }));
    let registry = Arc::new(StageRegistry::new().with(join.clone()));
    let w = spawn_worker(worker("w", &registry, 2_000), &ctx);
    let coord = coordination("coordinator", 2_000);
    let exec = DistributedExecutor::new(coord.clone()).with_poll_interval(POLL);
    let driver = RunDriver::new(Arc::new(exec), Arc::clone(&registry), coord.clone());
    let err = tokio::time::timeout(Duration::from_secs(10), driver.run(&ctx))
        .await
        .unwrap()
        .unwrap_err();
    let failed = err.downcast_ref::<RunFailed>().expect("a RunFailed error");
    assert_eq!(failed.stage, Some(StageName::Join));
    assert!(failed.reason.contains("part-00003"), "{}", failed.reason);

    let report = w.await.unwrap().unwrap();
    assert!(matches!(report.exit, WorkerExit::Failed(_)), "{report:?}");
    let state = RunState::load(&store, &ctx.layout).await.unwrap().unwrap();
    assert_eq!(state.status, RunStatus::Failed);
    let failure = coord
        .failure(&ctx)
        .await
        .unwrap()
        .expect("a failure marker");
    assert_eq!(failure.task_id.as_deref(), Some("part-00003"));
    assert_eq!(failure.worker, "w");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_lease_cancels_the_running_task() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let join = Arc::new(FakeStage::new(StageName::Join, 1).with_script(|_, _| Behavior::Hang));
    let registry = Arc::new(StageRegistry::new().with(join.clone()));
    hand_stage_to_workers(
        &ctx,
        StageName::Join,
        vec![TaskSpec::for_partition(StageName::Join, 0)],
    )
    .await;
    let w = spawn_worker(worker("w", &registry, 150), &ctx);
    assert!(
        eventually(Duration::from_secs(5), || async {
            join.runs_of("part-00000") == 1
        })
        .await
    );

    // Another worker takes the lease over (as after a long pause of this one).
    let key = ctx.layout.lease("join", "part-00000").unwrap();
    let now = OffsetDateTime::now_utc();
    store
        .put_json(
            &key,
            &Lease {
                worker: "other".into(),
                holder: "other".into(),
                expires_at: now + time::Duration::minutes(5),
                acquired_at: now,
                renewals: 0,
                released: false,
            },
        )
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(3), || async {
            join.in_flight.load(Ordering::SeqCst) == 0
        })
        .await,
        "the running attempt was stopped"
    );
    finish_run(&ctx).await;
    let report = w.await.unwrap().unwrap();
    assert_eq!(report.lost, 1);
    assert_eq!(report.committed, 0);
    assert_eq!(
        join.runs_of("part-00000"),
        1,
        "the worker did not reclaim a live lease"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_workers_task_is_taken_over_after_its_lease_expires() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    // The first attempt hangs (the worker running it will crash); later ones succeed.
    let join = Arc::new(
        FakeStage::new(StageName::Join, 1).with_script(|_, attempt| {
            if attempt == 1 {
                Behavior::Hang
            } else {
                Behavior::Succeed
            }
        }),
    );
    let registry = Arc::new(StageRegistry::new().with(join.clone()));
    hand_stage_to_workers(
        &ctx,
        StageName::Join,
        vec![TaskSpec::for_partition(StageName::Join, 0)],
    )
    .await;

    let doomed = spawn_worker(worker("doomed", &registry, 200), &ctx);
    assert!(
        eventually(Duration::from_secs(5), || async {
            join.runs_of("part-00000") == 1
        })
        .await
    );
    doomed.abort();
    let _ = doomed.await;

    let survivor = spawn_worker(worker("survivor", &registry, 200), &ctx);
    let log = CommitLog::new(store.clone(), ctx.layout.clone());
    assert!(
        eventually(Duration::from_secs(5), || async {
            log.is_committed(StageName::Join, "part-00000")
                .await
                .unwrap()
        })
        .await,
        "the survivor took the task over"
    );
    let record = log
        .get(StageName::Join, "part-00000")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.worker, "survivor");
    finish_run(&ctx).await;
    assert_eq!(survivor.await.unwrap().unwrap().committed, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_polling_worker_skips_live_leases_and_takes_over_an_expired_one() {
    const N: u32 = 16;
    let (store, ops) = CountingStore::store();
    let ctx = ctx(&store, "r1");
    let join = Arc::new(FakeStage::new(StageName::Join, N));
    let registry = Arc::new(StageRegistry::new().with(join.clone()));
    hand_stage_to_workers(&ctx, StageName::Join, {
        (0..N)
            .map(|p| TaskSpec::for_partition(StageName::Join, p))
            .collect()
    })
    .await;
    // Another worker holds every task but the first, and keeps holding them.
    let other = LeaseManager::new(
        store.clone(),
        ctx.layout.clone(),
        "other",
        short_leases(60_000),
    );
    let mut held = Vec::new();
    for p in 1..N {
        let id = TaskSpec::for_partition(StageName::Join, p).task_id;
        held.push(
            other
                .try_claim(StageName::Join, &id)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    // The first task's holder crashed: its lease lapses after 150 ms.
    drop(
        LeaseManager::new(
            store.clone(),
            ctx.layout.clone(),
            "crashed",
            short_leases(150),
        )
        .try_claim(StageName::Join, "part-00000")
        .await
        .unwrap()
        .unwrap(),
    );

    ops.clear();
    let w = spawn_worker(worker("w", &registry, 2_000), &ctx);
    let log = CommitLog::new(store.clone(), ctx.layout.clone());
    assert!(
        eventually(Duration::from_secs(5), || async {
            log.is_committed(StageName::Join, "part-00000")
                .await
                .unwrap()
        })
        .await,
        "the expired lease was taken over"
    );
    // A few more polls with nothing claimable.
    tokio::time::sleep(POLL * 6).await;
    finish_run(&ctx).await;
    let report = w.await.unwrap().unwrap();
    assert_eq!(report.committed, 1);
    assert_eq!(join.total_runs(), 1, "no live lease was run");

    let held_lease = |path: &str| path.contains("/join/_lease/") && !path.contains("part-00000");
    let polls = ops.count(Op::List, |p| p.ends_with("/join/_lease"));
    assert!(polls >= 6, "the worker polled the leases {polls} times");
    assert_eq!(
        ops.count(Op::List, |p| p.ends_with("/join/_commit")),
        polls,
        "one lease listing per poll, next to the commit listing"
    );
    assert_eq!(
        ops.count(Op::Create, held_lease) + ops.count(Op::Update, held_lease),
        0,
        "no claim attempt on a live lease"
    );
    assert_eq!(
        ops.count(Op::Get, held_lease),
        (N - 1) as usize,
        "each live lease read once, on the first poll"
    );
    drop(held);
}

#[tokio::test]
async fn the_distributed_executor_refuses_a_local_directory() {
    let dir = std::env::temp_dir().join(format!("pi-coord-{}", uuid::Uuid::now_v7().simple()));
    let store = Store::local(&dir).unwrap();
    let ctx = ctx(&store, "r1");
    let exec = DistributedExecutor::new(coordination("c", 1_000));
    let err = exec
        .run_stage(Arc::new(FakeStage::new(StageName::Join, 2)), &ctx)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("conditional updates"), "{err}");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_worker_without_a_run_idles_out() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let registry = Arc::new(StageRegistry::new());
    let report = worker("w", &registry, 1_000)
        .with_idle_timeout(Some(Duration::from_millis(60)))
        .run(&ctx)
        .await
        .unwrap();
    assert_eq!(report.exit, WorkerExit::IdleTimeout);
}

#[tokio::test]
async fn a_stage_that_never_commits_times_out() {
    let store = Store::memory();
    let ctx = ctx(&store, "r1");
    let exec = DistributedExecutor::new(coordination("c", 1_000))
        .with_poll_interval(POLL)
        .with_stage_timeout(Some(Duration::from_millis(80)));
    let err = exec
        .run_stage(Arc::new(FakeStage::new(StageName::Join, 3)), &ctx)
        .await
        .unwrap_err();
    let timeout = err
        .downcast_ref::<pi_coord::StageTimeout>()
        .expect("a StageTimeout");
    assert_eq!((timeout.missing, timeout.total), (3, 3));
}
