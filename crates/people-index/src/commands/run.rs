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
    let run_id = match &args.run_id_from_job_name {
        Some(name) => crate::run_id::from_job_name(name)?,
        None => args.run_id.clone().unwrap_or_else(new_run_id),
    };
    let ctx = stores.context(&run_id, cancel.child_token())?;
    let registry = Arc::new(pi_stages::build_registry(stages_config(
        &args.pipeline,
        &args.es,
        &args.exec,
        &stores,
    )?)?);
    let coord = coordination(&args.exec);
    let local = local_executor(&args.exec, coord.clone());
    let executor: Arc<dyn Executor> = match args.executor {
        ExecutorKind::Local => Arc::new(local),
        ExecutorKind::Distributed => {
            if !stores.runs.supports_conditional_update() {
                anyhow::bail!(
                    "the distributed executor needs leases, which {} cannot hold (use a store with conditional updates or PI_EXECUTOR=local)",
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
    let driver = RunDriver::new(executor, registry, coord);
    let outcome = with_run_lock(
        &stores.runs,
        &run_id,
        &args.pipeline.alias,
        args.exec.lease_ttl,
        &ctx.cancel,
        driver.run(&ctx),
    )
    .await;
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

async fn with_run_lock<T>(
    store: &pi_store::Store,
    run_id: &str,
    alias: &str,
    ttl: std::time::Duration,
    cancel: &CancellationToken,
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    if !store.supports_conditional_update() {
        tracing::warn!("no run lock on this store");
        return future.await;
    }
    let manager = pi_coord::LeaseManager::new(
        store.clone(),
        pi_store::RunLayout::new(run_id)?,
        run_id,
        pi_coord::LeaseConfig::new(ttl),
    );
    let guard = manager.claim_run(alias, run_id).await?.ok_or_else(|| {
        pi_coord::permanent(anyhow::anyhow!(
            "a run is already in progress for alias {alias}"
        ))
    })?;
    let lost = guard.lost_token();
    tokio::pin!(future);
    let result = tokio::select! {
        biased;
        result = &mut future => result,
        () = lost.cancelled() => {
            cancel.cancel();
            match future.await {
                Ok(done) => Ok(done),
                Err(_) => Err(pi_coord::permanent(anyhow::anyhow!("run lease lost for alias {alias}"))),
            }
        },
    };
    if let Err(e) = guard.release().await {
        tracing::warn!(error = %e, "could not release run lease; it will expire");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn lock_released_after_success_and_failure_and_excludes_other_run() {
        let store = pi_store::Store::memory();
        let cancel = CancellationToken::new();
        let ttl = Duration::from_secs(1);
        for fail in [false, true] {
            let outcome = with_run_lock(&store, "one", "persons", ttl, &cancel, async {
                let error = with_run_lock(&store, "two", "persons", ttl, &cancel, async { Ok(()) })
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("already in progress"));
                if fail {
                    anyhow::bail!("driver failed");
                }
                Ok(())
            })
            .await;
            assert_eq!(outcome.is_err(), fail);
            with_run_lock(&store, "two", "persons", ttl, &cancel, async { Ok(()) })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn loss_cancels_child_and_awaits_driver() {
        let store = pi_store::Store::memory();
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let ttl = Duration::from_millis(150);
        let result = with_run_lock(&store, "one", "persons", ttl, &child, async {
            let manager = pi_coord::LeaseManager::new(
                store.clone(),
                pi_store::RunLayout::new("one").unwrap(),
                "one",
                pi_coord::LeaseConfig::new(ttl),
            );
            let replacement = manager.claim_run("persons", "one").await.unwrap().unwrap();
            child.cancelled().await;
            replacement.release().await.unwrap();
            Err::<(), _>(anyhow::anyhow!("driver cancelled"))
        })
        .await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("run lease lost"));
        assert!(!is_cancelled(&error));
        assert!(!parent.is_cancelled());
        let manager = pi_coord::LeaseManager::new(
            store,
            pi_store::RunLayout::new("two").unwrap(),
            "two",
            pi_coord::LeaseConfig::new(ttl),
        );
        manager
            .claim_run("persons", "two")
            .await
            .unwrap()
            .unwrap()
            .release()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn loss_after_driver_success_returns_success() {
        let store = pi_store::Store::memory();
        let child = CancellationToken::new();
        let ttl = Duration::from_millis(150);
        let result = with_run_lock(&store, "one", "persons", ttl, &child, async {
            let manager = pi_coord::LeaseManager::new(
                store.clone(),
                pi_store::RunLayout::new("one").unwrap(),
                "one",
                pi_coord::LeaseConfig::new(ttl),
            );
            let replacement = manager.claim_run("persons", "one").await.unwrap().unwrap();
            child.cancelled().await;
            replacement.release().await.unwrap();
            Ok(42)
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn local_store_runs_without_lock() {
        let dir = tempfile::tempdir().unwrap();
        let store = pi_store::Store::open(
            dir.path().to_str().unwrap(),
            &pi_store::StoreOptions::default(),
        )
        .unwrap();
        let token = CancellationToken::new();
        with_run_lock(
            &store,
            "one",
            "persons",
            Duration::from_secs(1),
            &token,
            async { Ok(()) },
        )
        .await
        .unwrap();
    }
}
