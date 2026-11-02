//! `people-index run`: the coordinator.

use std::sync::Arc;

use pi_coord::{
    CancellationToken, DistributedExecutor, Executor, RunDriver, RunOutcome, RunSkipped,
    is_cancelled,
};
use pi_es::{EsClient, IndexAdmin};
use pi_store::RunLayout;

use crate::cli::{ExecutorKind, RunArgs};
use crate::delivery::{Choice, Follow, choose, serving};
use crate::run_id::new_run_id;
use crate::settings::{Stores, coordination, local_executor, stages_config};
use crate::telemetry::{record_skip, record_stage};

/// How a call to [`run`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunEnd {
    /// The run is published (by this call or an earlier one).
    Published(RunOutcome),
    /// The run did nothing: with `PI_DELIVERY=latest` the alias already serves the newest
    /// complete delivery (`runs/<id>/skipped.json`).
    Skipped(RunSkipped),
}

/// Runs (or resumes) a run until it is published. With the distributed executor, the
/// multi-task stages are claimed by `people-index worker` processes.
pub async fn run(args: RunArgs, cancel: CancellationToken) -> anyhow::Result<RunEnd> {
    let stores = Stores::open(&args.store)?;
    Box::pin(run_with(&args, stores, cancel)).await
}

/// [`run`] on stores already opened.
pub async fn run_with(
    args: &RunArgs,
    mut stores: Stores,
    cancel: CancellationToken,
) -> anyhow::Result<RunEnd> {
    let run_id = match &args.run_id_from_job_name {
        Some(name) => crate::run_id::from_job_name(name)?,
        None => args.run_id.clone().unwrap_or_else(new_run_id),
    };
    let layout = RunLayout::new(&run_id)?;
    let mut config = stages_config(&args.pipeline, &args.es, &args.exec, &stores)?;
    // Built now so a bad setting fails before the lease is taken; built again below once the
    // run's delivery is chosen.
    let mut registry = Arc::new(pi_stages::build_registry(config.clone())?);
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
    let lock_store = stores.runs.clone();
    let run_cancel = cancel.child_token();
    let alias = &args.pipeline.alias;
    // Under the alias run lease, before any run state exists: which delivery, or none.
    let work = async {
        let follow = stores.follow_latest.then(|| Follow {
            inputs: stores.input_store(),
            root: stores.deliveries_root(),
            republish_unchanged: args.republish_unchanged_delivery,
        });
        let es = config.es.client.clone();
        let runs = &stores.runs;
        let serving_now = async move {
            let client =
                EsClient::new(&es).map_err(|e| anyhow::anyhow!("Elasticsearch client: {e}"))?;
            serving(&IndexAdmin::new(client), runs, alias).await
        };
        let delivery = match Box::pin(choose(&stores.runs, &layout, follow, serving_now)).await? {
            Choice::Configured => None,
            Choice::Recorded(delivery) => {
                tracing::info!(run = %run_id, delivery = ?delivery, "resuming with the delivery the plan recorded");
                Some(delivery)
            }
            Choice::Newest(delivery) => {
                tracing::info!(run = %run_id, delivery = %delivery, "planning the newest complete delivery");
                config.require_complete_marker = true;
                Some(Some(delivery))
            }
            Choice::Skip(skipped) => return Ok(RunEnd::Skipped(skipped)),
        };
        if let Some(delivery) = delivery {
            stores.delivery.clone_from(&delivery);
            config.delivery = delivery;
            registry = Arc::new(pi_stages::build_registry(config)?);
        }
        let ctx = stores.context(&run_id, run_cancel.clone())?;
        let driver = RunDriver::new(executor, registry, coord);
        Box::pin(driver.run(&ctx)).await.map(RunEnd::Published)
    };
    let outcome = with_run_lock(
        &lock_store,
        &run_id,
        alias,
        args.exec.lease_ttl,
        &run_cancel,
        work,
    )
    .await;
    match &outcome {
        Ok(RunEnd::Published(done)) => {
            for stage in &done.stages {
                record_stage(stage);
            }
            tracing::info!(run = %run_id, status = ?done.status, ran = done.stages.len(), skipped = done.skipped.len(), "run finished");
        }
        Ok(RunEnd::Skipped(skipped)) => {
            record_skip();
            tracing::info!(run = %run_id, delivery = %skipped.delivery, serving_run = ?skipped.serving_run, reason = %skipped.reason, "no new delivery; run skipped");
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

    use clap::Parser as _;
    use pi_store::Store;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::cli::{Cli, Command};

    fn args(es: &str, run_id: &str, extra: &[&str]) -> RunArgs {
        let mut argv = vec![
            "people-index",
            "run",
            "--metrics-addr",
            "off",
            "--store-url",
            "memory://",
            "--es-url",
            es,
            "--run-id",
            run_id,
            "--task-attempts",
            "1",
        ];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Run(r) => r,
            _ => unreachable!(),
        }
    }

    /// The parsed stores, on `store` instead of the fresh memory store `memory://` opens.
    fn on(store: &Store, store_args: &crate::cli::StoreArgs) -> Stores {
        Stores {
            runs: store.clone(),
            ..Stores::open(store_args).unwrap()
        }
    }

    async fn complete_delivery(store: &Store, name: &str) {
        // Not gzip: a run that plans it fails in extract, before Elasticsearch.
        store
            .put(&format!("raw/{name}/persons/a.ndjson.gz"), vec![1])
            .await
            .unwrap();
        store
            .put(&format!("raw/{name}/_COMPLETE"), Vec::new())
            .await
            .unwrap();
    }

    async fn planned_delivery(store: &Store, run_id: &str) -> Option<Option<String>> {
        store
            .get_json::<pi_stages::Plan>(&RunLayout::new(run_id).unwrap().plan())
            .await
            .unwrap()
            .map(|(plan, _)| plan.delivery)
    }

    async fn skip_record(store: &Store, run_id: &str) -> Option<RunSkipped> {
        RunSkipped::load(store, &RunLayout::new(run_id).unwrap())
            .await
            .unwrap()
    }

    async fn lease_is_free(store: &Store) -> bool {
        let manager = pi_coord::LeaseManager::new(
            store.clone(),
            RunLayout::new("probe").unwrap(),
            "probe",
            pi_coord::LeaseConfig::new(Duration::from_secs(60)),
        );
        match manager.claim_run("persons", "probe").await.unwrap() {
            Some(guard) => {
                guard.release().await.unwrap();
                true
            }
            None => false,
        }
    }

    /// Elasticsearch whose alias serves run `r0`'s index.
    async fn serving_r0() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_alias/persons"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r0": {"aliases": {"persons": {}}}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/persons-r0/_mapping"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"persons-r0": {"mappings": {"_meta": {"people_index": {"run_id": "r0"}}}}}),
            ))
            .mount(&server)
            .await;
        server
    }

    /// Elasticsearch that must not be asked anything (checked when it drops).
    async fn untouched() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        server
    }

    /// Delivery `b1`, complete, and run `r0`'s plan, which read it.
    async fn store_serving_b1() -> Store {
        let store = Store::memory();
        complete_delivery(&store, "b1").await;
        store
            .put_json(
                &RunLayout::new("r0").unwrap().plan(),
                &json!({"run_id": "r0", "delivery": "b1", "partitions": 16, "alias": "persons",
                    "index": "persons-r0", "organizations": [], "persons": [],
                    "total_bytes": 1, "created_at": "2026-11-01T02:00:00Z"}),
            )
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn an_unchanged_delivery_skips_the_run_under_the_lease_and_its_workers_exit() {
        let store = store_serving_b1().await;
        let es = serving_r0().await;
        let run = args(&es.uri(), "r1", &["--delivery", "latest"]);
        let end = run_with(&run, on(&store, &run.store), CancellationToken::new())
            .await
            .unwrap();
        let RunEnd::Skipped(skipped) = end else {
            panic!("expected a skip: {end:?}")
        };
        assert_eq!(skipped.delivery, "b1");
        assert_eq!(skipped.serving_run.as_deref(), Some("r0"));
        let layout = RunLayout::new("r1").unwrap();
        assert_eq!(
            store.list_keys("runs/r1").await.unwrap(),
            vec![layout.skipped()],
            "no plan, no state"
        );
        assert!(lease_is_free(&store).await, "the alias lease is released");

        // The run's workers, started with its id as the scheduled Job's are, exit as done.
        let Command::Worker(worker) = Cli::try_parse_from([
            "people-index",
            "worker",
            "--metrics-addr",
            "off",
            "--store-url",
            "memory://",
            "--delivery",
            "latest",
            "--run-id",
            "r1",
            "--poll-interval",
            "10ms",
        ])
        .unwrap()
        .command
        else {
            unreachable!()
        };
        let report = tokio::time::timeout(
            Duration::from_secs(5),
            crate::commands::worker::run_with(
                &worker,
                &on(&store, &worker.store),
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.exit, pi_coord::WorkerExit::Skipped);

        // The same run id started again stays skipped, even once a newer delivery arrived.
        complete_delivery(&store, "b2").await;
        let end = run_with(&run, on(&store, &run.store), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(end, RunEnd::Skipped(skipped));
        assert_eq!(planned_delivery(&store, "r1").await, None);
    }

    #[tokio::test]
    async fn a_new_or_republished_delivery_is_planned_and_recorded() {
        let store = store_serving_b1().await;
        // Asked to republish, the run reads no alias and plans the unchanged delivery.
        let es = untouched().await;
        let run = args(
            &es.uri(),
            "r1",
            &["--delivery", "latest", "--republish-unchanged-delivery"],
        );
        let err = run_with(&run, on(&store, &run.store), CancellationToken::new())
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("extract-persons"), "{err:#}");
        assert_eq!(
            planned_delivery(&store, "r1").await,
            Some(Some("b1".into()))
        );
        assert_eq!(skip_record(&store, "r1").await, None);
        assert!(lease_is_free(&store).await);

        // A newer complete delivery is planned without the flag.
        complete_delivery(&store, "b2").await;
        let es = serving_r0().await;
        let run = args(&es.uri(), "r2", &["--delivery", "latest"]);
        assert!(
            run_with(&run, on(&store, &run.store), CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(
            planned_delivery(&store, "r2").await,
            Some(Some("b2".into()))
        );
        assert_eq!(skip_record(&store, "r2").await, None);

        // Resumed after an even newer one arrived, the run keeps the delivery it recorded.
        complete_delivery(&store, "b3").await;
        let es = untouched().await;
        let run = args(&es.uri(), "r2", &["--delivery", "latest"]);
        assert!(
            run_with(&run, on(&store, &run.store), CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(
            planned_delivery(&store, "r2").await,
            Some(Some("b2".into()))
        );
    }

    #[tokio::test]
    async fn without_latest_a_run_plans_as_before() {
        let store = store_serving_b1().await;
        store.put("raw/persons/a.ndjson.gz", vec![1]).await.unwrap();
        let es = untouched().await;
        for (run_id, extra, delivery) in [
            ("r1", &[][..], None),
            ("r2", &["--delivery", "b1"][..], Some("b1".to_owned())),
        ] {
            let run = args(&es.uri(), run_id, extra);
            assert!(
                run_with(&run, on(&store, &run.store), CancellationToken::new())
                    .await
                    .is_err()
            );
            assert_eq!(planned_delivery(&store, run_id).await, Some(delivery));
            assert_eq!(skip_record(&store, run_id).await, None);
        }
    }

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
