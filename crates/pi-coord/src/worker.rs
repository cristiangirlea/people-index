//! [`Worker`]: the worker side of a multi-node run.
//!
//! A worker follows one run: it reads the current stage from the run state, claims that
//! stage's uncommitted tasks with leases, runs them through the stage registry, commits, and
//! exits when the run is published or failed. Any number of workers can follow the same run;
//! leases keep them from duplicating work and commits keep a duplicate harmless.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;

use crate::context::RunContext;
use crate::coordination::{Coordination, TaskLease};
use crate::error::{is_cancelled, permanent};
use crate::failure::RunFailure;
use crate::registry::StageRegistry;
use crate::retry::RetryPolicy;
use crate::run_state::{RunState, RunStatus};
use crate::runner::{TaskOutcome, run_task};
use crate::stage::Stage;
use crate::stage_name::StageName;
use crate::stage_spec::{RunBy, StageSpec};
use crate::task::TaskSpec;

/// Consecutive failed polls (store unreachable) after which the worker gives up.
const MAX_POLL_ERRORS: u32 = 30;

/// Why a worker stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerExit {
    /// The run was published.
    Published,
    /// The run failed (by this worker or another participant); the recorded error.
    Failed(String),
    /// The worker's context was cancelled (shutdown).
    Cancelled,
    /// Nothing to do for longer than the idle timeout.
    IdleTimeout,
}

/// What a worker did before it stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerReport {
    /// Why it stopped.
    pub exit: WorkerExit,
    /// Tasks it committed.
    pub committed: usize,
    /// Tasks it ran but another attempt committed first.
    pub duplicates: usize,
    /// Tasks it abandoned because their lease was lost.
    pub lost: usize,
}

/// Follows a run and works on its tasks.
#[derive(Clone)]
pub struct Worker {
    coordination: Arc<dyn Coordination>,
    registry: Arc<StageRegistry>,
    concurrency: usize,
    retry: RetryPolicy,
    poll_interval: Duration,
    idle_timeout: Option<Duration>,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("worker", &self.coordination.worker_id())
            .field("concurrency", &self.concurrency)
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

/// One finished task, as the worker's join set returns it.
struct Finished {
    stage: StageName,
    task_id: String,
    result: anyhow::Result<TaskOutcome>,
    lease_lost: bool,
}

impl Worker {
    /// A worker running one task per CPU, polling every 2 s, never idling out.
    #[must_use]
    pub fn new(coordination: Arc<dyn Coordination>, registry: Arc<StageRegistry>) -> Self {
        Self {
            coordination,
            registry,
            concurrency: std::thread::available_parallelism().map_or(4, std::num::NonZero::get),
            retry: RetryPolicy::default(),
            poll_interval: Duration::from_secs(2),
            idle_timeout: None,
        }
    }

    /// Sets how many tasks run at once (at least 1).
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Sets the retry policy.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Sets how often the run state and the task list are read.
    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval.max(Duration::from_millis(1));
        self
    }

    /// Exits after this long with nothing to do (no run state yet, or a stage the coordinator
    /// runs itself). `None` waits for the run to finish.
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Works on the run until it is published or failed, the context is cancelled, or the
    /// idle timeout passes.
    pub async fn run(&self, ctx: &RunContext) -> anyhow::Result<WorkerReport> {
        if !self.coordination.supports_leases(ctx) {
            return Err(permanent(anyhow::anyhow!(
                "workers need a store with conditional updates (S3, MinIO); {} has none",
                ctx.store.url()
            )));
        }
        let mut state = Loop {
            worker: self,
            ctx,
            tasks_ctx: ctx.child(),
            running: JoinSet::new(),
            task_of: HashMap::new(),
            in_flight: HashSet::new(),
            report: WorkerReport {
                exit: WorkerExit::Cancelled,
                committed: 0,
                duplicates: 0,
                lost: 0,
            },
        };
        let exit = state.drive().await;
        state.tasks_ctx.cancel.cancel();
        while let Some(joined) = state.running.join_next_with_id().await {
            state.settle(joined, false).await;
        }
        state.report.exit = exit?;
        tracing::info!(worker = %self.coordination.worker_id(), report = ?state.report, "worker stopped");
        Ok(state.report)
    }
}

/// The mutable state of one `Worker::run`.
struct Loop<'a> {
    worker: &'a Worker,
    ctx: &'a RunContext,
    /// Parent of every task's context; cancelled when the worker stops.
    tasks_ctx: RunContext,
    running: JoinSet<Finished>,
    task_of: HashMap<tokio::task::Id, (StageName, String)>,
    in_flight: HashSet<(StageName, String)>,
    report: WorkerReport,
}

impl Loop<'_> {
    async fn drive(&mut self) -> anyhow::Result<WorkerExit> {
        let mut poll_errors = 0u32;
        let mut idle_since = Instant::now();
        loop {
            while let Some(joined) = self.running.try_join_next_with_id() {
                self.settle(joined, true).await;
            }
            if self.ctx.is_cancelled() {
                return Ok(WorkerExit::Cancelled);
            }
            match self.poll().await {
                Ok(Some(exit)) => return Ok(exit),
                Ok(None) => poll_errors = 0,
                Err(e) => {
                    poll_errors += 1;
                    if poll_errors >= MAX_POLL_ERRORS {
                        return Err(
                            e.context(format!("worker gave up after {poll_errors} failed polls"))
                        );
                    }
                    tracing::warn!(error = %format!("{e:#}"), poll_errors, "worker poll failed");
                }
            }
            if self.running.is_empty() {
                if let Some(timeout) = self.worker.idle_timeout
                    && idle_since.elapsed() >= timeout
                {
                    return Ok(WorkerExit::IdleTimeout);
                }
            } else {
                idle_since = Instant::now();
            }
            tokio::select! {
                biased;
                () = self.ctx.cancel.cancelled() => {}
                Some(joined) = self.running.join_next_with_id(), if !self.running.is_empty() => {
                    self.settle(joined, true).await;
                }
                () = tokio::time::sleep(self.worker.poll_interval) => {}
            }
        }
    }

    /// One look at the run: `Some` when the worker should exit, otherwise claims what it can.
    async fn poll(&mut self) -> anyhow::Result<Option<WorkerExit>> {
        let coord = &self.worker.coordination;
        if let Some(f) = coord.failure(self.ctx).await? {
            return Ok(Some(WorkerExit::Failed(f.error)));
        }
        let Some(state) = RunState::load(&self.ctx.store, &self.ctx.layout).await? else {
            return Ok(None);
        };
        match state.status {
            RunStatus::Published => return Ok(Some(WorkerExit::Published)),
            RunStatus::Failed => {
                return Ok(Some(WorkerExit::Failed(state.error.unwrap_or_default())));
            }
            RunStatus::Running => {}
        }
        let Some(stage_name) = state.stage else {
            return Ok(None);
        };
        if self.running.len() >= self.worker.concurrency {
            return Ok(None);
        }
        let Some(spec) = StageSpec::load(&self.ctx.store, &self.ctx.layout, stage_name).await?
        else {
            return Ok(None);
        };
        if spec.run_by != RunBy::Workers {
            return Ok(None);
        }
        let Some(stage_impl) = self.worker.registry.get(stage_name) else {
            return Err(permanent(anyhow::anyhow!(
                "this worker has no stage {stage_name}; is it running an older build than the coordinator?"
            )));
        };
        let committed = coord.committed_task_ids(self.ctx, stage_name).await?;
        let mut candidates: Vec<TaskSpec> = spec
            .tasks
            .into_iter()
            .filter(|t| {
                !committed.contains(&t.task_id)
                    && !self.in_flight.contains(&(stage_name, t.task_id.clone()))
            })
            .collect();
        // Workers start their scan at different offsets, so a fleet starting together does
        // not race for the same first tasks.
        if !candidates.is_empty() {
            let offset = scan_offset(coord.worker_id(), candidates.len());
            candidates.rotate_left(offset);
        }
        for task in candidates {
            if self.running.len() >= self.worker.concurrency || self.ctx.is_cancelled() {
                break;
            }
            if let Some(lease) = coord.try_claim(self.ctx, stage_name, &task.task_id).await? {
                self.spawn(Arc::clone(&stage_impl), task, lease);
            }
        }
        Ok(None)
    }

    fn spawn(&mut self, stage: Arc<dyn Stage>, task: TaskSpec, lease: Box<dyn TaskLease>) {
        let stage_name = stage.name();
        let task_id = task.task_id.clone();
        let task_ctx = self.tasks_ctx.child();
        let coord = Arc::clone(&self.worker.coordination);
        let retry = self.worker.retry;
        let handle = self.running.spawn(async move {
            // A lost lease cancels the task's context: the stage sees it and the runner drops
            // the attempt.
            let lost = lease.lost_token();
            let task_token = task_ctx.cancel.clone();
            let watcher = tokio::spawn(async move {
                tokio::select! {
                    () = lost.cancelled() => task_token.cancel(),
                    () = task_token.cancelled() => {}
                }
            });
            let id = task.task_id.clone();
            let result = run_task(stage, coord, retry, task_ctx.clone(), task).await;
            let lease_lost = lease.is_lost();
            task_ctx.cancel.cancel();
            let _ = watcher.await;
            if let Err(e) = lease.release().await {
                tracing::warn!(stage = %stage_name, task = %id, error = %format!("{e:#}"), "releasing a lease failed; it will expire");
            }
            Finished {
                stage: stage_name,
                task_id: id,
                result,
                lease_lost,
            }
        });
        self.task_of
            .insert(handle.id(), (stage_name, task_id.clone()));
        self.in_flight.insert((stage_name, task_id));
    }

    /// Books a finished task. With `record_failures`, a task that failed for good fails the run.
    async fn settle(
        &mut self,
        joined: Result<(tokio::task::Id, Finished), JoinError>,
        record_failures: bool,
    ) {
        let (stage, task_id, error) = match joined {
            Ok((id, done)) => {
                self.task_of.remove(&id);
                self.in_flight.remove(&(done.stage, done.task_id.clone()));
                match done.result {
                    Ok(TaskOutcome::Committed) => {
                        self.report.committed += 1;
                        return;
                    }
                    Ok(TaskOutcome::AlreadyCommitted) => {
                        self.report.duplicates += 1;
                        return;
                    }
                    Err(e)
                        if done.lease_lost
                            || (is_cancelled(&e) && self.tasks_ctx.is_cancelled()) =>
                    {
                        self.report.lost += 1;
                        return;
                    }
                    Err(e) => (done.stage, done.task_id, e),
                }
            }
            Err(join) => {
                let Some((stage, task_id)) = self.task_of.remove(&join.id()) else {
                    return;
                };
                self.in_flight.remove(&(stage, task_id.clone()));
                (stage, task_id, anyhow::anyhow!("task panicked: {join}"))
            }
        };
        if !record_failures {
            return;
        }
        tracing::error!(stage = %stage, task = %task_id, error = %format!("{error:#}"), "task failed for good; failing the run");
        let failure = RunFailure::new(
            Some(stage),
            Some(task_id),
            self.worker.coordination.worker_id(),
            &error,
        );
        if let Err(e) = self
            .worker
            .coordination
            .mark_failed(self.ctx, &failure)
            .await
        {
            // The task stays uncommitted and its lease is released, so it is claimed and tried
            // again; the failure surfaces on a later attempt or at the coordinator's timeout.
            tracing::error!(error = %format!("{e:#}"), "could not record the run failure");
        }
    }
}

fn scan_offset(worker: &str, len: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    worker.hash(&mut h);
    // Truncation is fine: only the remainder matters.
    #[allow(clippy::cast_possible_truncation)]
    let v = h.finish() as usize;
    v % len.max(1)
}
