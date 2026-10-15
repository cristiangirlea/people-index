//! `people-index run`: the coordinator.

use std::sync::Arc;

use pi_coord::{
    CancellationToken, DistributedExecutor, Executor, RunDriver, RunOutcome, is_cancelled,
};

use crate::cli::{ExecutorKind, RunArgs};
use crate::run_id::new_run_id;
use crate::settings::{Stores, coordination, local_executor, stages_config};
use crate::telemetry::record_stage;

/// Runs (or resumes) a run until it is published. With the distributed executor, the
/// multi-task stages are claimed by `people-index worker` processes.
pub async fn run(args: RunArgs, cancel: CancellationToken) -> anyhow::Result<RunOutcome> {
    let stores = Stores::open(&args.store)?;
    let run_id = args.run_id.clone().unwrap_or_else(new_run_id);
    let ctx = stores.context(&run_id, cancel)?;
    let registry = Arc::new(pi_stages::build_registry(stages_config(
        &args.pipeline,
        &args.es,
        &stores,
    )?)?);
    let coord = coordination(&args.exec);
    let local = local_executor(&args.exec, coord.clone());
    let executor: Arc<dyn Executor> = match args.executor {
        ExecutorKind::Local => Arc::new(local),
        ExecutorKind::Distributed => {
            if !stores.runs.supports_conditional_update() {
                anyhow::bail!(
                    "the distributed executor needs leases, which {} cannot hold (use s3:// or PI_EXECUTOR=local)",
                    stores.runs.url()
                );
            }
            Arc::new(
                DistributedExecutor::new(coord.clone())
                    .with_local(local)
                    .with_stage_timeout(args.stage_timeout),
            )
        }
    };
    tracing::info!(run = %run_id, store = %stores.runs.url(), executor = ?args.executor, "run starting");
    let outcome = RunDriver::new(executor, registry, coord).run(&ctx).await;
    match &outcome {
        Ok(done) => {
            for stage in &done.stages {
                record_stage(stage);
            }
            tracing::info!(run = %run_id, status = ?done.status, ran = done.stages.len(), skipped = done.skipped.len(), "run finished");
        }
        Err(e) if is_cancelled(e) => {
            tracing::warn!(run = %run_id, "run interrupted; start it again with PI_RUN_ID={run_id} to resume");
        }
        Err(e) => tracing::error!(run = %run_id, error = %format!("{e:#}"), "run failed"),
    }
    outcome
}
