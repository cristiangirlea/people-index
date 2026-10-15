//! [`Metered`]: any stage, with Prometheus metrics for every task it runs.

use std::time::Instant;

use async_trait::async_trait;
use pi_coord::{AttemptId, RunContext, Stage, StageName, TaskReport, TaskSpec, is_cancelled};

/// Wraps a stage so each task attempt records its duration, bytes and counters. The pi-coord
/// executors already count commits, retries and failures; this adds what the task itself did.
#[derive(Debug)]
pub struct Metered<S>(pub S);

#[async_trait]
impl<S: Stage> Stage for Metered<S> {
    fn name(&self) -> StageName {
        self.0.name()
    }

    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        self.0.tasks(ctx).await
    }

    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let stage = self.0.name().as_str();
        let started = Instant::now();
        let result = self.0.run(ctx, task, attempt).await;
        let outcome = match &result {
            Ok(_) => "ok",
            Err(e) if is_cancelled(e) => "cancelled",
            Err(_) => "error",
        };
        metrics::histogram!("pi_task_duration_seconds", "stage" => stage, "outcome" => outcome)
            .record(started.elapsed().as_secs_f64());
        if let Ok(report) = &result {
            metrics::counter!("pi_task_bytes_in_total", "stage" => stage)
                .increment(report.bytes_in);
            metrics::counter!("pi_task_bytes_out_total", "stage" => stage)
                .increment(report.bytes_out);
            for (name, value) in &report.counters {
                metrics::counter!("pi_task_counter_total", "stage" => stage, "counter" => name.clone())
                    .increment(*value);
            }
        }
        result
    }
}
