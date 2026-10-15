//! [`LocalExecutor`]: every task of a stage in this process, a bounded number at a time.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::context::RunContext;
use crate::coordination::Coordination;
use crate::error::{Cancelled, permanent};
use crate::executor::{Executor, StageReport};
use crate::retry::RetryPolicy;
use crate::runner::{TaskOutcome, list_tasks, run_task, validate_tasks};
use crate::stage::Stage;
use crate::stage_name::StageName;
use crate::stage_spec::{RunBy, StageSpec};
use crate::task::TaskSpec;

/// Runs a stage's tasks in one process with bounded concurrency. Committed tasks are skipped,
/// a failing task is retried per the [`RetryPolicy`], and a task that fails for good cancels
/// its siblings and fails the stage.
#[derive(Clone)]
pub struct LocalExecutor {
    coordination: Arc<dyn Coordination>,
    concurrency: usize,
    retry: RetryPolicy,
}

impl std::fmt::Debug for LocalExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutor")
            .field("worker", &self.coordination.worker_id())
            .field("concurrency", &self.concurrency)
            .field("retry", &self.retry)
            .finish()
    }
}

impl LocalExecutor {
    /// An executor running as many tasks at once as there are CPUs (`PI_CONCURRENCY`'s
    /// default), with the default retry policy.
    #[must_use]
    pub fn new(coordination: Arc<dyn Coordination>) -> Self {
        Self {
            coordination,
            concurrency: std::thread::available_parallelism().map_or(4, std::num::NonZero::get),
            retry: RetryPolicy::default(),
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

    /// The retry policy in use.
    #[must_use]
    pub fn retry(&self) -> RetryPolicy {
        self.retry
    }

    /// Runs an explicit task list of `stage` and reports its commits. The list is written to
    /// `stages/<stage>.json` first, so operators (and `people-index stage`) can see it.
    pub async fn run_tasks(
        &self,
        stage: Arc<dyn Stage>,
        ctx: &RunContext,
        tasks: Vec<TaskSpec>,
    ) -> anyhow::Result<StageReport> {
        let name = stage.name();
        let started = Instant::now();
        validate_tasks(name, &tasks)?;
        StageSpec::new(name, RunBy::Coordinator, tasks.clone())
            .save(&ctx.store, &ctx.layout)
            .await
            .with_context(|| format!("writing the task list of {name}"))?;

        let committed = self.coordination.committed_task_ids(ctx, name).await?;
        let ids: BTreeSet<String> = tasks.iter().map(|t| t.task_id.clone()).collect();
        let skipped = ids.intersection(&committed).count();
        let mut pending: VecDeque<TaskSpec> = tasks
            .into_iter()
            .filter(|t| !committed.contains(&t.task_id))
            .collect();
        tracing::info!(stage = %name, tasks = ids.len(), skipped, "running stage locally");

        let stage_ctx = ctx.child();
        let mut running = JoinSet::new();
        let mut task_of = HashMap::new();
        let mut first_error: Option<anyhow::Error> = None;
        let mut ran = 0usize;
        loop {
            while running.len() < self.concurrency
                && first_error.is_none()
                && !stage_ctx.is_cancelled()
            {
                let Some(task) = pending.pop_front() else {
                    break;
                };
                let id = task.task_id.clone();
                let handle = running.spawn(run_task(
                    Arc::clone(&stage),
                    Arc::clone(&self.coordination),
                    self.retry,
                    stage_ctx.child(),
                    task,
                ));
                task_of.insert(handle.id(), id);
            }
            let Some(joined) = running.join_next_with_id().await else {
                break;
            };
            let failure = match joined {
                Ok((_, Ok(TaskOutcome::Committed))) => {
                    ran += 1;
                    None
                }
                Ok((_, Ok(TaskOutcome::AlreadyCommitted))) => None,
                Ok((_, Err(e))) => Some(e),
                Err(join) => {
                    let id = task_of.get(&join.id()).cloned().unwrap_or_default();
                    Some(permanent(anyhow::anyhow!(
                        "task {name}/{id} panicked: {join}"
                    )))
                }
            };
            if let Some(e) = failure
                && first_error.is_none()
            {
                // The first failure is the one to report; the siblings it cancels come back
                // as `Cancelled` and are not failures of their own.
                first_error = Some(e);
                stage_ctx.cancel.cancel();
            }
        }

        if let Some(e) = first_error {
            return Err(e.context(format!("stage {name} failed")));
        }
        if ctx.is_cancelled() {
            return Err(anyhow::Error::new(Cancelled).context(format!("stage {name} cancelled")));
        }
        let report = collect_report(
            self.coordination.as_ref(),
            ctx,
            name,
            &ids,
            ran,
            skipped,
            started,
        )
        .await?;
        tracing::info!(stage = %name, ran, skipped, duration = ?report.duration, "stage committed");
        Ok(report)
    }
}

#[async_trait]
impl Executor for LocalExecutor {
    async fn run_stage(
        &self,
        stage: Arc<dyn Stage>,
        ctx: &RunContext,
    ) -> anyhow::Result<StageReport> {
        let tasks = list_tasks(&stage, ctx, self.retry).await?;
        self.run_tasks(stage, ctx, tasks).await
    }
}

/// The commits of exactly the listed tasks, or an error naming the first one missing.
pub(crate) async fn collect_report(
    coord: &dyn Coordination,
    ctx: &RunContext,
    stage: StageName,
    ids: &BTreeSet<String>,
    ran: usize,
    skipped: usize,
    started: Instant,
) -> anyhow::Result<StageReport> {
    let commits: Vec<_> = coord
        .commits(ctx, stage)
        .await?
        .into_iter()
        .filter(|c| ids.contains(&c.task_id))
        .collect();
    if commits.len() != ids.len() {
        let have: BTreeSet<&str> = commits.iter().map(|c| c.task_id.as_str()).collect();
        let missing = ids.iter().find(|id| !have.contains(id.as_str()));
        anyhow::bail!("stage {stage}: task {missing:?} has no commit after the stage finished");
    }
    Ok(StageReport {
        stage,
        commits,
        ran,
        skipped,
        duration: started.elapsed(),
    })
}
