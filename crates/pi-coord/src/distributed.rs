//! [`DistributedExecutor`]: the coordinator side of a multi-node run.
//!
//! It writes each stage's task list for the workers, runs single-task stages itself (handing
//! one task to a worker would only add a poll interval), and waits for every commit while
//! watching the failure marker. The worker side is [`crate::Worker`].

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use tokio::time::Instant;

use crate::context::RunContext;
use crate::coordination::Coordination;
use crate::error::{Cancelled, RunFailed, StageTimeout, permanent};
use crate::executor::{Executor, StageReport};
use crate::local::{LocalExecutor, collect_report};
use crate::runner::{list_tasks, validate_tasks};
use crate::stage::Stage;
use crate::stage_spec::{RunBy, StageSpec};

/// Coordinates a stage across workers that claim tasks with leases.
#[derive(Clone)]
pub struct DistributedExecutor {
    coordination: Arc<dyn Coordination>,
    local: LocalExecutor,
    poll_interval: Duration,
    stage_timeout: Option<Duration>,
}

impl std::fmt::Debug for DistributedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedExecutor")
            .field("worker", &self.coordination.worker_id())
            .field("poll_interval", &self.poll_interval)
            .field("stage_timeout", &self.stage_timeout)
            .finish_non_exhaustive()
    }
}

impl DistributedExecutor {
    /// A coordinator polling every 2 s with no stage timeout.
    #[must_use]
    pub fn new(coordination: Arc<dyn Coordination>) -> Self {
        Self {
            local: LocalExecutor::new(Arc::clone(&coordination)),
            coordination,
            poll_interval: Duration::from_secs(2),
            stage_timeout: None,
        }
    }

    /// Sets how often commits and the failure marker are checked.
    #[must_use]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval.max(Duration::from_millis(1));
        self
    }

    /// Fails a stage whose commits are not all in after this long.
    #[must_use]
    pub fn with_stage_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.stage_timeout = timeout;
        self
    }

    /// The executor used for single-task stages (to set its retry policy).
    #[must_use]
    pub fn with_local(mut self, local: LocalExecutor) -> Self {
        self.local = local;
        self
    }

    async fn check_failure(&self, ctx: &RunContext) -> anyhow::Result<()> {
        if let Some(f) = self.coordination.failure(ctx).await? {
            return Err(anyhow::Error::new(RunFailed {
                run_id: ctx.run_id.clone(),
                stage: f.stage,
                reason: match f.task_id {
                    Some(task) => format!("task {task} on {}: {}", f.worker, f.error),
                    None => format!("{}: {}", f.worker, f.error),
                },
            }));
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for DistributedExecutor {
    async fn run_stage(
        &self,
        stage: Arc<dyn Stage>,
        ctx: &RunContext,
    ) -> anyhow::Result<StageReport> {
        let name = stage.name();
        if !self.coordination.supports_leases(ctx) {
            return Err(permanent(anyhow::anyhow!(
                "the distributed executor needs a store with conditional updates (S3, MinIO); {} has none: use the local executor",
                ctx.store.url()
            )));
        }
        let tasks = list_tasks(&stage, ctx, self.local.retry()).await?;
        if tasks.len() <= 1 {
            return self.local.run_tasks(stage, ctx, tasks).await;
        }
        validate_tasks(name, &tasks)?;
        let started = Instant::now();
        let ids: BTreeSet<String> = tasks.iter().map(|t| t.task_id.clone()).collect();
        StageSpec::new(name, RunBy::Workers, tasks)
            .save(&ctx.store, &ctx.layout)
            .await
            .with_context(|| format!("writing the task list of {name}"))?;
        let before = self.coordination.committed_task_ids(ctx, name).await?;
        let skipped = ids.intersection(&before).count();
        tracing::info!(stage = %name, tasks = ids.len(), skipped, "stage handed to workers");

        let mut last_done = skipped;
        loop {
            self.check_failure(ctx).await?;
            let committed = self.coordination.committed_task_ids(ctx, name).await?;
            let done = ids.intersection(&committed).count();
            if done == ids.len() {
                break;
            }
            if done != last_done {
                tracing::info!(stage = %name, done, total = ids.len(), "stage progress");
                last_done = done;
            }
            if let Some(timeout) = self.stage_timeout
                && started.elapsed() >= timeout
            {
                return Err(anyhow::Error::new(StageTimeout {
                    stage: name,
                    waited: started.elapsed(),
                    missing: ids.len() - done,
                    total: ids.len(),
                }));
            }
            tokio::select! {
                () = ctx.cancel.cancelled() => {
                    return Err(anyhow::Error::new(Cancelled).context(format!("waiting for {name}")));
                }
                () = tokio::time::sleep(self.poll_interval) => {}
            }
        }
        let ran = ids.len() - skipped;
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
        tracing::info!(stage = %name, ran, skipped, duration = ?report.duration, "stage committed by workers");
        Ok(report)
    }
}
