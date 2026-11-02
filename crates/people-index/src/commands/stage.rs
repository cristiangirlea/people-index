//! `people-index stage`: one task (or every task) of one stage, for debugging.
//!
//! It goes through the local executor, so it follows the commit protocol: a committed task is
//! skipped, and a successful one commits. The stage's task list file is rewritten with the
//! tasks run, so do not point it at a run that distributed workers are following.

use anyhow::Context as _;
use pi_coord::{CancellationToken, StageName, StageReport};

use crate::cli::StageArgs;
use crate::settings::{Stores, coordination, local_executor, stages_config};

/// Runs the selected tasks of the stage and returns their report.
pub async fn run(args: StageArgs, cancel: CancellationToken) -> anyhow::Result<StageReport> {
    let stores = Stores::open(&args.store)?;
    run_with(&args, stores, cancel).await
}

/// [`run`] on stores already opened.
pub async fn run_with(
    args: &StageArgs,
    mut stores: Stores,
    cancel: CancellationToken,
) -> anyhow::Result<StageReport> {
    let name: StageName = args.name.parse()?;
    if stores.follow_latest {
        stores.delivery = recorded_delivery(&stores.runs, &args.run_id).await?;
    }
    let ctx = stores.context(&args.run_id, cancel.child_token())?;
    let registry = pi_stages::build_registry(stages_config(
        &args.pipeline,
        &args.es,
        &args.exec,
        &stores,
    )?)?;
    let stage = registry
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("stage {name} is not registered"))?;
    let work = async {
        let mut tasks = stage.tasks(&ctx).await?;
        if let Some(id) = &args.task {
            tasks.retain(|t| &t.task_id == id);
            if tasks.is_empty() {
                anyhow::bail!("stage {name} of run {} has no task {id}", args.run_id);
            }
        }
        let executor = local_executor(&args.exec, coordination(&args.exec));
        executor.run_tasks(stage, &ctx, tasks).await
    };
    with_stage_lock(
        &stores.runs,
        &args.run_id,
        name,
        &args.pipeline.alias,
        args.exec.lease_ttl,
        args.ignore_run_lock,
        &ctx.cancel,
        work,
    )
    .await
}

/// With `PI_DELIVERY=latest`, the delivery the run's plan recorded: a hand-run stage never
/// chooses one, so a run without a plan is refused.
async fn recorded_delivery(runs: &pi_store::Store, run_id: &str) -> anyhow::Result<Option<String>> {
    let key = pi_store::RunLayout::new(run_id)?.plan();
    match runs
        .get_json::<pi_stages::Plan>(&key)
        .await
        .with_context(|| format!("reading {key}"))?
    {
        Some((plan, _)) => Ok(plan.delivery),
        None => anyhow::bail!(
            "PI_DELIVERY=latest: run {run_id} has no plan yet, and stage does not choose a \
             delivery; let run plan it, or set PI_DELIVERY to the delivery's name"
        ),
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // The lock owns the full lifecycle.
async fn with_stage_lock<T>(
    store: &pi_store::Store,
    run_id: &str,
    name: StageName,
    alias: &str,
    ttl: std::time::Duration,
    ignore: bool,
    cancel: &CancellationToken,
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    if !store.supports_conditional_update() {
        tracing::warn!("no run lock on this store");
        return future.await;
    }
    let config = pi_coord::LeaseConfig::new(ttl);
    let layout = pi_store::RunLayout::new(run_id)?;
    let manager = pi_coord::LeaseManager::new(store.clone(), layout.clone(), run_id, config);
    let initial = manager.claim_stage_run(alias, run_id).await?;
    let mut guard = match initial {
        pi_coord::RunLease::Held(guard) => Some(guard),
        pi_coord::RunLease::Shared => {
            if !ignore && check_shared_stage(store, &layout, name).await? {
                return Err(shared_stage_refusal(name));
            }
            None
        }
        pi_coord::RunLease::Busy if ignore => {
            let key = pi_store::alias_lease(alias)?;
            let holder = store
                .get_json::<pi_coord::Lease>(&key)
                .await?
                .map(|(lease, _)| lease.worker);
            tracing::warn!(alias, holder_run = ?holder, "bypassing run lock for stage");
            return future.await;
        }
        pi_coord::RunLease::Busy => {
            return Err(pi_coord::permanent(anyhow::anyhow!(
                "a run is already in progress for alias {alias}"
            )));
        }
    };
    tokio::pin!(future);
    let mut failures_since = None;
    let result = loop {
        let was_held = guard.is_some();
        if let Some(current) = guard.take() {
            let lost = current.lost_token();
            tokio::select! {
                biased;
                result = &mut future => {
                    guard = Some(current);
                    break result;
                }
                () = lost.cancelled() => {}
            }
            if let Err(e) = current.release().await {
                tracing::warn!(error = %e, "could not release run lease; it will expire");
            }
        } else {
            tokio::select! {
                biased;
                result = &mut future => break result,
                () = tokio::time::sleep(config.renew_interval()) => {}
            }
        }
        let mut shared_read_failed = false;
        let claim = match manager.claim_stage_run(alias, run_id).await {
            Ok(pi_coord::RunLease::Shared) if !ignore => {
                match check_shared_stage(store, &layout, name).await {
                    Ok(true) => {
                        cancel.cancel();
                        let _ = future.await;
                        break Err(shared_stage_refusal(name));
                    }
                    Ok(false) => Ok(pi_coord::RunLease::Shared),
                    Err(e) => {
                        shared_read_failed = true;
                        Err(e)
                    }
                }
            }
            result => result.map_err(anyhow::Error::from),
        };
        match claim {
            Ok(pi_coord::RunLease::Held(current)) => {
                failures_since = None;
                guard = Some(current);
            }
            Ok(pi_coord::RunLease::Shared) => {
                failures_since = None;
            }
            Ok(pi_coord::RunLease::Busy) => {
                cancel.cancel();
                let _ = future.await;
                break Err(pi_coord::permanent(anyhow::anyhow!(
                    "run lease lost for alias {alias}"
                )));
            }
            Err(e) => {
                let since = failures_since.get_or_insert_with(tokio::time::Instant::now);
                tracing::warn!(error = %e, "could not check run lease; retrying");
                if (was_held && !shared_read_failed)
                    || since.elapsed() + config.renew_interval() >= ttl
                {
                    cancel.cancel();
                    let _ = future.await;
                    break Err(pi_coord::permanent(anyhow::anyhow!(
                        "could not check run lease for alias {alias}: {e}"
                    )));
                }
            }
        }
    };
    if let Some(current) = guard
        && let Err(e) = current.release().await
    {
        tracing::warn!(error = %e, "could not release run lease; it will expire");
    }
    result
}

async fn check_shared_stage(
    store: &pi_store::Store,
    layout: &pi_store::RunLayout,
    name: StageName,
) -> anyhow::Result<bool> {
    let state = pi_coord::RunState::load(store, layout).await?;
    let spec = pi_coord::StageSpec::load(store, layout, name).await?;
    Ok(
        state.is_some_and(|s| s.status == pi_coord::RunStatus::Running && s.stage == Some(name))
            && !spec.is_some_and(|s| s.run_by == pi_coord::RunBy::Coordinator),
    )
}

fn shared_stage_refusal(name: StageName) -> anyhow::Error {
    pi_coord::permanent(anyhow::anyhow!(
        "stage {name} may be run by workers; use --ignore-run-lock only for recovery"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_coord::{LeaseConfig, LeaseManager, RunLease};
    use std::time::Duration;

    #[tokio::test]
    async fn with_latest_a_stage_uses_the_recorded_delivery_and_never_chooses_one() {
        let store = pi_store::Store::memory();
        let err = recorded_delivery(&store, "r1")
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "PI_DELIVERY=latest: run r1 has no plan yet, and stage does not choose a delivery; \
             let run plan it, or set PI_DELIVERY to the delivery's name"
        );
        for (run_id, delivery) in [
            ("r1", serde_json::json!("b1")),
            ("r2", serde_json::Value::Null),
        ] {
            store
                .put_json(
                    &pi_store::RunLayout::new(run_id).unwrap().plan(),
                    &serde_json::json!({"run_id": run_id, "delivery": delivery, "partitions": 16,
                        "alias": "persons", "index": "persons-r1", "organizations": [],
                        "persons": [], "total_bytes": 1, "created_at": "2026-11-01T02:00:00Z"}),
                )
                .await
                .unwrap();
            assert_eq!(
                recorded_delivery(&store, run_id).await.unwrap(),
                delivery.as_str().map(str::to_owned)
            );
        }
    }

    #[tokio::test]
    async fn with_latest_the_stage_command_runs_a_planned_run_and_refuses_an_unplanned_one() {
        use clap::Parser as _;
        let store = pi_store::Store::memory();
        store
            .put("raw/b1/persons/a.ndjson.gz", vec![1])
            .await
            .unwrap();
        let plan = |run_id: &str| {
            serde_json::json!({"run_id": run_id, "delivery": "b1", "partitions": 16,
                "alias": "persons", "index": format!("persons-{run_id}"), "organizations": [],
                "persons": [], "total_bytes": 1, "created_at": "2026-11-01T02:00:00Z"})
        };
        store
            .put_json(&pi_store::RunLayout::new("r1").unwrap().plan(), &plan("r1"))
            .await
            .unwrap();
        let stage = |run_id: &str| {
            let crate::cli::Command::Stage(args) = crate::cli::Cli::try_parse_from([
                "people-index",
                "stage",
                "plan",
                "--run-id",
                run_id,
                "--store-url",
                "memory://",
                "--delivery",
                "latest",
                "--metrics-addr",
                "off",
            ])
            .unwrap()
            .command
            else {
                unreachable!()
            };
            args
        };
        let on = |args: &StageArgs| Stores {
            runs: store.clone(),
            ..Stores::open(&args.store).unwrap()
        };
        let planned = stage("r1");
        let report = run_with(&planned, on(&planned), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.stage, StageName::Plan);
        // Nothing read or wrote a delivery named latest.
        assert_eq!(
            store.list_keys("raw/latest").await.unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            recorded_delivery(&store, "r1").await.unwrap().as_deref(),
            Some("b1")
        );

        let unplanned = stage("r2");
        let err = run_with(&unplanned, on(&unplanned), CancellationToken::new())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "PI_DELIVERY=latest: run r2 has no plan yet, and stage does not choose a delivery; \
             let run plan it, or set PI_DELIVERY to the delivery's name"
        );
        assert_eq!(
            store.list_keys("runs/r2").await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn stage_lock_excludes_shares_releases_and_overrides() {
        let store = pi_store::Store::memory();
        let ttl = Duration::from_secs(60);
        let token = CancellationToken::new();
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig::new(ttl),
        );
        let first = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let key = pi_store::alias_lease("persons").unwrap();
        let before = store.get_opt(&key).await.unwrap().unwrap();
        assert!(
            with_stage_lock(
                &store,
                "two",
                StageName::Join,
                "persons",
                ttl,
                false,
                &token,
                async { Ok(()) }
            )
            .await
            .is_err()
        );
        with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            ttl,
            false,
            &token,
            async { Ok(()) },
        )
        .await
        .unwrap();
        let after = store.get_opt(&key).await.unwrap().unwrap();
        assert_eq!(before.bytes, after.bytes);
        assert_eq!(before.version, after.version);
        with_stage_lock(
            &store,
            "two",
            StageName::Join,
            "persons",
            ttl,
            true,
            &token,
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert_eq!(
            after.version,
            store.get_opt(&key).await.unwrap().unwrap().version
        );
        first.release().await.unwrap();
        with_stage_lock(
            &store,
            "two",
            StageName::Join,
            "persons",
            ttl,
            false,
            &token,
            async {
                assert!(matches!(
                    manager.claim_stage_run("persons", "one").await.unwrap(),
                    RunLease::Busy
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            manager.claim_stage_run("persons", "one").await.unwrap(),
            RunLease::Held(_)
        ));
    }

    #[tokio::test]
    async fn stage_takes_expired_lease() {
        let store = pi_store::Store::memory();
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig {
                ttl: Duration::ZERO,
                clock_skew: Duration::ZERO,
            },
        );
        let old = manager.claim_run("persons", "one").await.unwrap().unwrap();
        drop(old);
        tokio::time::sleep(Duration::from_millis(100)).await;
        with_stage_lock(
            &store,
            "two",
            StageName::Join,
            "persons",
            Duration::from_millis(4),
            false,
            &CancellationToken::new(),
            async { Ok(()) },
        )
        .await
        .unwrap();
        let lease: (pi_coord::Lease, _) = store
            .get_json(&pi_store::alias_lease("persons").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.0.worker, "two");
        assert!(lease.0.released);
    }
    #[tokio::test]
    async fn override_free_and_error_release() {
        let store = pi_store::Store::memory();
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig::new(Duration::from_secs(1)),
        );
        let result: anyhow::Result<()> = with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            Duration::from_secs(1),
            true,
            &CancellationToken::new(),
            async {
                assert!(matches!(
                    manager.claim_stage_run("persons", "two").await.unwrap(),
                    RunLease::Busy
                ));
                anyhow::bail!("failure")
            },
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(
            manager.claim_stage_run("persons", "two").await.unwrap(),
            RunLease::Held(_)
        ));
    }

    #[tokio::test]
    async fn refuses_active_workers() {
        let store = pi_store::Store::memory();
        let layout = pi_store::RunLayout::new("one").unwrap();
        let manager = LeaseManager::new(
            store.clone(),
            layout.clone(),
            "one",
            LeaseConfig::new(Duration::from_secs(1)),
        );
        let guard = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let mut state = pi_coord::RunState::new("one");
        state.stage = Some(StageName::Join);
        state.save(&store, &layout).await.unwrap();
        let missing = with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            Duration::from_secs(1),
            false,
            &CancellationToken::new(),
            async { Ok(()) },
        )
        .await;
        assert!(missing.is_err());
        with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            Duration::from_secs(1),
            true,
            &CancellationToken::new(),
            async { Ok(()) },
        )
        .await
        .unwrap();
        pi_coord::StageSpec::new(StageName::Join, pi_coord::RunBy::Workers, vec![])
            .save(&store, &layout)
            .await
            .unwrap();
        let err = with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            Duration::from_secs(1),
            false,
            &CancellationToken::new(),
            async { Ok(()) },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--ignore-run-lock"));
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn shared_adopts_and_hands_over_to_same_run() {
        let store = pi_store::Store::memory();
        let ttl = Duration::from_millis(150);
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig::new(ttl),
        );
        let first = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let work = async {
            first.release().await.unwrap();
            tokio::time::sleep(Duration::from_millis(120)).await;
            assert!(matches!(
                manager.claim_stage_run("persons", "two").await.unwrap(),
                RunLease::Busy
            ));
            let replacement = manager.claim_run("persons", "one").await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
            assert!(!child.is_cancelled());
            assert!(matches!(
                manager.claim_stage_run("persons", "one").await.unwrap(),
                RunLease::Shared
            ));
            assert!(!replacement.is_lost());
            replacement.release().await.unwrap();
            Ok::<(), anyhow::Error>(())
        };
        tokio::time::timeout(
            Duration::from_secs(3),
            with_stage_lock(
                &store,
                "one",
                StageName::Join,
                "persons",
                ttl,
                false,
                &child,
                work,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!parent.is_cancelled());
    }

    #[tokio::test]
    async fn shared_stops_when_different_run_takes_lease() {
        let store = pi_store::Store::memory();
        let ttl = Duration::from_millis(150);
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig::new(ttl),
        );
        let layout = pi_store::RunLayout::new("one").unwrap();
        let mut state = pi_coord::RunState::new("one");
        state.stage = Some(StageName::Join);
        state.save(&store, &layout).await.unwrap();
        let first = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let child = CancellationToken::new();
        let work = async {
            first.release().await.unwrap();
            let other = manager.claim_run("persons", "two").await.unwrap().unwrap();
            child.cancelled().await;
            other.release().await.unwrap();
            Err::<(), _>(anyhow::anyhow!("cancelled"))
        };
        let err = tokio::time::timeout(
            Duration::from_secs(3),
            with_stage_lock(
                &store,
                "one",
                StageName::Join,
                "persons",
                ttl,
                true,
                &child,
                work,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(err.to_string().contains("run lease lost"));
        assert!(!pi_coord::is_cancelled(&err));
    }

    #[tokio::test]
    async fn sharing_retries_read_errors_then_cancels_after_ttl() {
        let store = pi_store::Store::memory();
        let ttl = Duration::from_millis(300);
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig::new(ttl),
        );
        let first = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let key = pi_store::alias_lease("persons").unwrap();
        let original = store.get_opt(&key).await.unwrap().unwrap().bytes;
        let child = CancellationToken::new();
        let work = async {
            store.put(&key, b"bad".to_vec()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!child.is_cancelled());
            store.put(&key, original.to_vec()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!child.is_cancelled());
            store.put(&key, b"bad".to_vec()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(150), child.cancelled())
                    .await
                    .is_err()
            );
            child.cancelled().await;
            Err::<(), _>(anyhow::anyhow!("cancelled"))
        };
        let err = tokio::time::timeout(
            Duration::from_secs(3),
            with_stage_lock(
                &store,
                "one",
                StageName::Join,
                "persons",
                ttl,
                false,
                &child,
                work,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(err.to_string().contains("could not check run lease"));
        first.release().await.unwrap();
    }

    #[tokio::test]
    async fn sharing_retries_state_read_errors_then_stops_after_ttl() {
        let store = pi_store::Store::memory();
        let layout = pi_store::RunLayout::new("one").unwrap();
        let ttl = Duration::from_millis(300);
        let manager =
            LeaseManager::new(store.clone(), layout.clone(), "one", LeaseConfig::new(ttl));
        let guard = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let child = CancellationToken::new();
        let key = layout.state();
        let work = async {
            store.put(&key, b"bad".to_vec()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!child.is_cancelled());
            pi_coord::RunState::new("one")
                .save(&store, &layout)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(!child.is_cancelled());
            store.put(&key, b"bad".to_vec()).await.unwrap();
            let start = tokio::time::Instant::now();
            child.cancelled().await;
            assert!(start.elapsed() >= Duration::from_millis(200));
            Err::<(), _>(anyhow::anyhow!("cancelled"))
        };
        let err = tokio::time::timeout(
            Duration::from_secs(3),
            with_stage_lock(
                &store,
                "one",
                StageName::Join,
                "persons",
                ttl,
                false,
                &child,
                work,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(err.to_string().contains("could not check run lease"));
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn held_stage_stops_for_different_run() {
        let store = pi_store::Store::memory();
        let ttl = Duration::from_millis(300);
        let manager = LeaseManager::new(
            store.clone(),
            pi_store::RunLayout::new("one").unwrap(),
            "one",
            LeaseConfig::new(ttl),
        );
        let child = CancellationToken::new();
        let work = async {
            let replacement = manager.claim_run("persons", "one").await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(350)).await;
            replacement.release().await.unwrap();
            let other = manager.claim_run("persons", "two").await.unwrap().unwrap();
            child.cancelled().await;
            other.release().await.unwrap();
            Err::<(), _>(anyhow::anyhow!("cancelled"))
        };
        let err = tokio::time::timeout(
            Duration::from_secs(3),
            with_stage_lock(
                &store,
                "one",
                StageName::Join,
                "persons",
                ttl,
                false,
                &child,
                work,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(err.to_string().contains("run lease lost"));
        assert!(!pi_coord::is_cancelled(&err));
    }

    #[tokio::test]
    async fn held_lease_read_error_stops_immediately_after_loss() {
        let store = pi_store::Store::memory();
        let child = CancellationToken::new();
        let work = async {
            store
                .put(&pi_store::alias_lease("persons").unwrap(), b"bad".to_vec())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_millis(220), child.cancelled())
                .await
                .unwrap();
            Err::<(), _>(anyhow::anyhow!("cancelled"))
        };
        let err = with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            Duration::from_millis(300),
            false,
            &child,
            work,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("could not check run lease"));
    }

    #[tokio::test]
    async fn sharing_detects_coordinator_reaching_stage() {
        let store = pi_store::Store::memory();
        let layout = pi_store::RunLayout::new("one").unwrap();
        let ttl = Duration::from_millis(300);
        let manager =
            LeaseManager::new(store.clone(), layout.clone(), "one", LeaseConfig::new(ttl));
        let guard = manager.claim_run("persons", "one").await.unwrap().unwrap();
        let child = CancellationToken::new();
        let work = async {
            let mut state = pi_coord::RunState::new("one");
            state.stage = Some(StageName::Join);
            state.save(&store, &layout).await.unwrap();
            tokio::time::timeout(Duration::from_millis(250), child.cancelled())
                .await
                .unwrap();
            Err::<(), _>(anyhow::anyhow!("cancelled"))
        };
        let err = with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            ttl,
            false,
            &child,
            work,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("may be run by workers"));
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn local_store_writes_no_lease() {
        let dir = tempfile::tempdir().unwrap();
        let store = pi_store::Store::open(
            dir.path().to_str().unwrap(),
            &pi_store::StoreOptions::default(),
        )
        .unwrap();
        with_stage_lock(
            &store,
            "one",
            StageName::Join,
            "persons",
            Duration::from_secs(1),
            false,
            &CancellationToken::new(),
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert!(
            store
                .get_opt(&pi_store::alias_lease("persons").unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
}
