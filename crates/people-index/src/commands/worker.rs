//! `people-index worker`: claims tasks of a run until it is published or failed.

use std::sync::Arc;
use std::time::Duration;

use pi_coord::{CancellationToken, Cancelled, Worker, WorkerReport, latest_run_id};
use pi_store::Store;
use tokio::time::Instant;

use crate::cli::WorkerArgs;
use crate::settings::{Stores, concurrency, coordination, retry_policy, stages_config};

/// Works on `--run-id`, or on the newest run (waiting for one to appear, so workers can start
/// before the coordinator).
pub async fn run(args: WorkerArgs, cancel: CancellationToken) -> anyhow::Result<WorkerReport> {
    let stores = Stores::open(&args.store)?;
    let run_id = match (args.run_id.clone(), args.run_id_from_job_name.as_deref()) {
        (_, Some(name)) => crate::run_id::from_job_name(name)?,
        (Some(id), None) => id,
        (None, None) => {
            wait_for_run(&stores.runs, args.poll_interval, args.idle_timeout, &cancel).await?
        }
    };
    let ctx = stores.context(&run_id, cancel)?;
    let registry = Arc::new(pi_stages::build_registry(stages_config(
        &args.pipeline,
        &args.es,
        &stores,
    )?)?);
    let coord = coordination(&args.exec);
    tracing::info!(run = %run_id, worker = %pi_coord::Coordination::worker_id(coord.as_ref()), "worker starting");
    Worker::new(coord, registry)
        .with_concurrency(concurrency(&args.exec))
        .with_retry(retry_policy(&args.exec))
        .with_poll_interval(args.poll_interval)
        .with_idle_timeout(args.idle_timeout)
        .run(&ctx)
        .await
}

/// The newest run id in the store, polling until one exists.
async fn wait_for_run(
    store: &Store,
    poll: Duration,
    idle_timeout: Option<Duration>,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    let started = Instant::now();
    loop {
        match latest_run_id(store).await {
            Ok(Some(id)) => return Ok(id),
            Ok(None) => tracing::info!("no run yet; waiting for the coordinator"),
            Err(e) => tracing::warn!(error = %e, "listing runs failed"),
        }
        if idle_timeout.is_some_and(|t| started.elapsed() >= t) {
            anyhow::bail!("no run appeared in {:?}", started.elapsed());
        }
        tokio::select! {
            () = cancel.cancelled() => return Err(anyhow::Error::new(Cancelled)),
            () = tokio::time::sleep(poll) => {}
        }
    }
}
