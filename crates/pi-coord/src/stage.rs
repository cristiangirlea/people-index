//! The [`Stage`] trait: what one stage of the pipeline must provide.

use async_trait::async_trait;

use crate::context::RunContext;
use crate::stage_name::StageName;
use crate::task::{AttemptId, TaskReport, TaskSpec};

/// One stage: a deterministic task list and a way to run one task.
///
/// The executor, not the stage, writes the commit: `run` writes its outputs under keys that
/// carry the attempt id and returns them in the report. That keeps the protocol (commit with
/// create-if-absent, skip committed tasks, retry, lease) in one place for every stage.
#[async_trait]
pub trait Stage: Send + Sync {
    /// The stage's name.
    fn name(&self) -> StageName;

    /// The tasks of this stage, from the plan and the previous stages' commits.
    ///
    /// Must be deterministic for a given run: a restarted coordinator recomputes it and the
    /// workers match tasks by id.
    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>>;

    /// Runs one task to completion and returns what to commit. Must be idempotent: it may run
    /// more than once, even at the same time on two workers; only the first commit counts.
    ///
    /// Errors are retried by the executor unless wrapped with [`crate::permanent`].
    /// `ctx.cancel` fires when the task must stop (lost lease, failed sibling, shutdown).
    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport>;
}
