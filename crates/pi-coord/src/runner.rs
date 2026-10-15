//! One task to a commit: skip if committed, run, retry with backoff, commit. Shared by the
//! local executor and the workers so both follow exactly the same protocol.

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Context as _;
use time::OffsetDateTime;
use tokio::time::Instant;
use tracing::Instrument as _;

use crate::commit::{CommitOutcome, CommitRecord};
use crate::context::RunContext;
use crate::coordination::Coordination;
use crate::error::{Cancelled, is_cancelled, is_permanent, permanent};
use crate::retry::RetryPolicy;
use crate::stage::Stage;
use crate::stage_name::StageName;
use crate::task::{AttemptId, TaskSpec};

/// How a task ended up committed.
#[derive(Debug, Clone)]
pub(crate) enum TaskOutcome {
    /// This call ran the task and its commit is the one that counts.
    Committed,
    /// The task was committed before (or by another attempt while this one ran).
    AlreadyCommitted,
}

/// Runs one task to a commit. `ctx` must be the task's own child context: cancelling it
/// drops the running attempt.
pub(crate) async fn run_task(
    stage: Arc<dyn Stage>,
    coord: Arc<dyn Coordination>,
    retry: RetryPolicy,
    ctx: RunContext,
    task: TaskSpec,
) -> anyhow::Result<TaskOutcome> {
    let name = stage.name();
    if coord.committed(&ctx, name, &task.task_id).await?.is_some() {
        return Ok(TaskOutcome::AlreadyCommitted);
    }
    let mut attempt_no: u32 = 0;
    loop {
        attempt_no += 1;
        let attempt = AttemptId::new();
        let started = Instant::now();
        let span = tracing::info_span!("task", stage = %name, task = %task.task_id, attempt = %attempt, try_no = attempt_no);
        let result = tokio::select! {
            biased;
            () = ctx.cancel.cancelled() => Err(anyhow::Error::new(Cancelled)),
            r = stage.run(&ctx, &task, &attempt).instrument(span) => r,
        };
        match result {
            Ok(mut report) => {
                if report.duration.is_zero() {
                    report.duration = started.elapsed();
                }
                let record = CommitRecord {
                    stage: name,
                    task_id: task.task_id.clone(),
                    attempt,
                    worker: coord.worker_id().to_owned(),
                    committed_at: OffsetDateTime::now_utc(),
                    report,
                };
                return commit_with_retries(coord.as_ref(), &ctx, &record, retry).await;
            }
            Err(e) if is_cancelled(&e) || ctx.is_cancelled() => {
                return Err(e.context(format!("task {name}/{} cancelled", task.task_id)));
            }
            Err(e) => {
                if is_permanent(&e) || !retry.allows_retry_after(attempt_no) {
                    metrics::counter!("pi_tasks_failed_total", "stage" => name.as_str())
                        .increment(1);
                    return Err(e.context(format!(
                        "task {name}/{} failed after {attempt_no} attempt(s)",
                        task.task_id
                    )));
                }
                metrics::counter!("pi_task_retries_total", "stage" => name.as_str()).increment(1);
                let delay = retry.delay_after(attempt_no);
                tracing::warn!(stage = %name, task = %task.task_id, try_no = attempt_no, ?delay, error = %format!("{e:#}"), "task attempt failed; retrying");
                tokio::select! {
                    () = ctx.cancel.cancelled() => return Err(anyhow::Error::new(Cancelled)),
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

/// The work is done by the time we commit; a transient store error here should cost a retry
/// of the commit, not of the task.
async fn commit_with_retries(
    coord: &dyn Coordination,
    ctx: &RunContext,
    record: &CommitRecord,
    retry: RetryPolicy,
) -> anyhow::Result<TaskOutcome> {
    let mut try_no = 0;
    loop {
        try_no += 1;
        match coord.commit(ctx, record).await {
            Ok(CommitOutcome::Committed) => {
                metrics::counter!("pi_tasks_committed_total", "stage" => record.stage.as_str())
                    .increment(1);
                tracing::info!(stage = %record.stage, task = %record.task_id, attempt = %record.attempt, "task committed");
                return Ok(TaskOutcome::Committed);
            }
            Ok(CommitOutcome::AlreadyCommitted(existing)) => {
                tracing::info!(stage = %record.stage, task = %record.task_id, winner = %existing.attempt, "another attempt committed first; outputs left as orphans");
                return Ok(TaskOutcome::AlreadyCommitted);
            }
            Err(e) if retry.allows_retry_after(try_no) && !ctx.is_cancelled() => {
                tracing::warn!(stage = %record.stage, task = %record.task_id, error = %format!("{e:#}"), "commit failed; retrying");
                tokio::time::sleep(retry.delay_after(try_no)).await;
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("committing {}/{}", record.stage, record.task_id));
            }
        }
    }
}

/// Lists a stage's tasks, retrying transient errors (the listing reads the plan and earlier
/// commits from the store, which can blip) so a hiccup does not fail the whole run.
pub(crate) async fn list_tasks(
    stage: &Arc<dyn Stage>,
    ctx: &RunContext,
    retry: RetryPolicy,
) -> anyhow::Result<Vec<TaskSpec>> {
    let name = stage.name();
    let mut try_no = 0;
    loop {
        try_no += 1;
        match stage.tasks(ctx).await {
            Ok(tasks) => return Ok(tasks),
            Err(e)
                if !is_permanent(&e) && retry.allows_retry_after(try_no) && !ctx.is_cancelled() =>
            {
                tracing::warn!(stage = %name, error = %format!("{e:#}"), "listing tasks failed; retrying");
                tokio::time::sleep(retry.delay_after(try_no)).await;
            }
            Err(e) => return Err(e.context(format!("listing the tasks of {name}"))),
        }
    }
}

/// Checks a stage's task list: every task names this stage and ids are unique.
pub(crate) fn validate_tasks(stage: StageName, tasks: &[TaskSpec]) -> anyhow::Result<()> {
    let mut seen = BTreeSet::new();
    for t in tasks {
        if t.stage != stage {
            return Err(permanent(anyhow::anyhow!(
                "stage {stage} listed task {} of stage {}",
                t.task_id,
                t.stage
            )));
        }
        pi_store::validate_id("task id", &t.task_id).map_err(permanent)?;
        if !seen.insert(t.task_id.as_str()) {
            return Err(permanent(anyhow::anyhow!(
                "stage {stage} listed task {} twice",
                t.task_id
            )));
        }
    }
    Ok(())
}
