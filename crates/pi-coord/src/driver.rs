//! [`RunDriver`]: runs the registered stages of a run in order, recording progress in
//! `state.json` so a restarted coordinator resumes where the last one stopped.

use std::sync::Arc;

use anyhow::Context as _;

use crate::context::RunContext;
use crate::coordination::Coordination;
use crate::error::is_cancelled;
use crate::executor::{Executor, StageReport};
use crate::failure::RunFailure;
use crate::registry::StageRegistry;
use crate::run_state::{RunState, RunStatus};
use crate::stage_name::StageName;

/// What a call to [`RunDriver::run`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    /// The run.
    pub run_id: String,
    /// Where the run ended up (always `Published` on `Ok`).
    pub status: RunStatus,
    /// Reports of the stages run by this call, in order.
    pub stages: Vec<StageReport>,
    /// Stages skipped because an earlier call completed them.
    pub skipped: Vec<StageName>,
}

/// The coordinator's main loop, independent of how stages are executed.
#[derive(Clone)]
pub struct RunDriver {
    executor: Arc<dyn Executor>,
    registry: Arc<StageRegistry>,
    coordination: Arc<dyn Coordination>,
}

impl std::fmt::Debug for RunDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunDriver")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl RunDriver {
    /// A driver running `registry`'s stages with `executor`.
    #[must_use]
    pub fn new(
        executor: Arc<dyn Executor>,
        registry: Arc<StageRegistry>,
        coordination: Arc<dyn Coordination>,
    ) -> Self {
        Self {
            executor,
            registry,
            coordination,
        }
    }

    /// Runs (or resumes) the run: completed stages are skipped, and within a stage committed
    /// tasks are skipped. A run that failed before is retried from where it failed. A
    /// cancelled call leaves the run `running`, so the next call resumes it.
    pub async fn run(&self, ctx: &RunContext) -> anyhow::Result<RunOutcome> {
        let (store, layout) = (&ctx.store, &ctx.layout);
        let mut state = RunState::load(store, layout)
            .await
            .context("reading the run state")?
            .unwrap_or_else(|| RunState::new(ctx.run_id.clone()));
        let mut outcome = RunOutcome {
            run_id: ctx.run_id.clone(),
            status: state.status,
            stages: Vec::new(),
            skipped: Vec::new(),
        };
        if state.status == RunStatus::Published {
            tracing::info!(run = %ctx.run_id, "run already published; nothing to do");
            outcome.skipped.clone_from(&state.completed);
            return Ok(outcome);
        }
        if state.status == RunStatus::Failed || self.coordination.failure(ctx).await?.is_some() {
            tracing::warn!(run = %ctx.run_id, error = ?state.error, "resuming a failed run");
            self.coordination.clear_failure(ctx).await?;
        }
        state.status = RunStatus::Running;
        state.error = None;
        state
            .save(store, layout)
            .await
            .context("writing the run state")?;

        for stage in self.registry.ordered() {
            let name = stage.name();
            if state.is_completed(name) {
                tracing::info!(stage = %name, "stage completed earlier; skipped");
                outcome.skipped.push(name);
                continue;
            }
            state.stage = Some(name);
            state
                .save(store, layout)
                .await
                .context("writing the run state")?;
            match self.executor.run_stage(stage, ctx).await {
                Ok(report) => {
                    state.completed.push(name);
                    state
                        .save(store, layout)
                        .await
                        .context("writing the run state")?;
                    outcome.stages.push(report);
                }
                Err(e) if is_cancelled(&e) => {
                    // Shutdown, not failure: leave the run resumable.
                    return Err(e.context(format!("run {} interrupted in {name}", ctx.run_id)));
                }
                Err(e) => {
                    self.fail(ctx, &mut state, name, &e).await;
                    return Err(e.context(format!("run {} failed in {name}", ctx.run_id)));
                }
            }
        }
        state.status = RunStatus::Published;
        state
            .save(store, layout)
            .await
            .context("writing the run state")?;
        outcome.status = RunStatus::Published;
        tracing::info!(run = %ctx.run_id, "run finished");
        Ok(outcome)
    }

    /// Marks the run failed in the state and the failure marker, so workers stop. Errors here
    /// are logged, not returned: the stage's error is the one the caller needs.
    async fn fail(
        &self,
        ctx: &RunContext,
        state: &mut RunState,
        failed_stage: StageName,
        err: &anyhow::Error,
    ) {
        state.status = RunStatus::Failed;
        state.error = Some(format!("{err:#}"));
        if let Err(e) = state.save(&ctx.store, &ctx.layout).await {
            tracing::error!(error = %e, "could not record the failed run state");
        }
        let failure = RunFailure::new(Some(failed_stage), None, self.coordination.worker_id(), err);
        if let Err(e) = self.coordination.mark_failed(ctx, &failure).await {
            tracing::error!(error = %format!("{e:#}"), "could not write the failure marker");
        }
    }
}
