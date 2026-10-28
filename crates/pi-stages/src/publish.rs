//! `publish`: restores serving settings, checks the count, records the indexes the alias
//! serves in `publish.json`, moves the alias, deletes those indexes, writes `summary.json` and
//! removes the files of attempts that never committed.

use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt;
use pi_coord::{
    AttemptId, CommitLog, Lease, RunContext, RunState, RunStatus, Stage, StageName, TaskReport,
    TaskSpec,
};
use pi_es::{EsClient, EsError, IndexAdmin, PublishSpec, Publisher, ServingSettings};
use pi_store::RunLayout;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::time::Instant;

use crate::commits::CommitCache;
use crate::config::StagesConfig;
use crate::es_error::es_error;
use crate::plan_file::Plan;
use crate::summary::{PublishSummary, StageSummary, Summary, millis};

/// Deletes in flight at once while cleaning up orphans.
const DELETE_CONCURRENCY: usize = 16;

/// The stages whose data directories hold segments (and so may hold orphans).
const DATA_STAGES: [StageName; 4] = [
    StageName::ExtractOrgs,
    StageName::ExtractPersons,
    StageName::Join,
    StageName::Assemble,
];

/// The content of `runs/<id>/publish.json`: what the alias served before this run's index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishRecord {
    /// The alias.
    pub alias: String,
    /// The run's index.
    pub index: String,
    /// The indexes the alias pointed at when the first attempt was about to move it.
    pub previous: Vec<String>,
    #[serde(default)]
    pub previous_docs: Option<u64>,
}

/// Records `on_alias` as the indexes to delete after the swap, create-if-absent. When an
/// earlier attempt recorded them already, its list wins: the alias may have moved since, and
/// then only the record knows what it served before.
async fn record_previous(
    ctx: &RunContext,
    alias: &str,
    index: &str,
    on_alias: Vec<String>,
    previous_docs: Option<u64>,
) -> anyhow::Result<(Vec<String>, Option<u64>)> {
    let key = ctx.layout.publish_record();
    let record = PublishRecord {
        alias: alias.to_owned(),
        index: index.to_owned(),
        previous: on_alias,
        previous_docs,
    };
    if ctx
        .store
        .put_json_create_if_absent(&key, &record)
        .await?
        .is_created()
    {
        return Ok((record.previous, record.previous_docs));
    }
    match ctx.store.get_json::<PublishRecord>(&key).await? {
        Some((earlier, _)) => Ok((earlier.previous, earlier.previous_docs)),
        None => anyhow::bail!("{key} existed and then did not"),
    }
}

/// The `publish` stage: one task per run.
#[derive(Debug, Clone)]
pub struct PublishStage {
    config: Arc<StagesConfig>,
    cache: Arc<CommitCache>,
    client: EsClient,
}

impl PublishStage {
    /// The stage with the given settings, reading earlier commits through `cache` and
    /// talking to the cluster through `client`.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>, cache: Arc<CommitCache>, client: EsClient) -> Self {
        Self {
            config,
            cache,
            client,
        }
    }

    /// The summaries of every stage before `publish`, from their commits.
    async fn stage_summaries(&self, ctx: &RunContext) -> anyhow::Result<Vec<StageSummary>> {
        let mut out = Vec::new();
        for stage in StageName::ALL {
            if stage == StageName::Publish {
                break;
            }
            let commits = self.cache.finished(ctx, stage).await?;
            out.push(StageSummary::from_commits(stage, &commits));
        }
        Ok(out)
    }
}

/// Checks feed quality before any alias changes.
#[allow(clippy::cast_precision_loss, reason = "ratios of committed counters")]
fn check_feed_bounds(
    resolved: u64,
    unresolved: u64,
    organizations: u64,
    config: &crate::config::EsStageConfig,
) -> anyhow::Result<()> {
    if organizations == 0 && !config.publish_allow_empty_organizations {
        return Err(pi_coord::permanent(anyhow::anyhow!(
            "publish refused: indexed organizations {organizations}, minimum 1 (allow empty is false)"
        )));
    }
    let total = resolved as f64 + unresolved as f64;
    if total > 0.0 && unresolved as f64 / total > config.publish_max_unresolved {
        return Err(pi_coord::permanent(anyhow::anyhow!(
            "publish refused: unresolved roles {unresolved}, resolved roles {resolved}, maximum unresolved share {}",
            config.publish_max_unresolved
        )));
    }
    Ok(())
}

/// Removes every object under the data stages and the quarantine that no commit lists.
/// Returns (deleted, left). Failures are logged, not fatal: the alias has moved already, and
/// an orphan is never read.
pub async fn delete_orphans(ctx: &RunContext) -> anyhow::Result<(u64, u64)> {
    let log = CommitLog::new(ctx.store.clone(), ctx.layout.clone());
    let mut orphans = Vec::new();
    let mut committed = BTreeSet::new();
    for stage in DATA_STAGES {
        orphans.extend(log.orphans(stage).await?);
        for c in log.list(stage).await? {
            committed.extend(c.report.outputs);
        }
    }
    orphans.extend(
        ctx.store
            .list_keys(&ctx.layout.quarantine_dir())
            .await?
            .into_iter()
            .filter(|k| !committed.contains(k)),
    );
    let results: Vec<bool> = futures::stream::iter(orphans)
        .map(|key| {
            let store = ctx.store.clone();
            async move {
                match store.delete(&key).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(%key, error = %e, "could not delete an orphan");
                        false
                    }
                }
            }
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect()
        .await;
    let deleted = results.iter().filter(|ok| **ok).count() as u64;
    Ok((deleted, results.len() as u64 - deleted))
}

/// Returns the layout if the run has a publish record and published state.
async fn published_layout(ctx: &RunContext, id: &str) -> anyhow::Result<Option<RunLayout>> {
    let layout = RunLayout::new(id)?;
    if ctx.store.get_opt(&layout.publish_record()).await?.is_none() {
        return Ok(None);
    }
    Ok(RunState::load(&ctx.store, &layout)
        .await?
        .filter(|s| s.status == RunStatus::Published)
        .map(|_| layout))
}

async fn clean_run(
    ctx: &RunContext,
    layout: &RunLayout,
    skew: std::time::Duration,
) -> anyhow::Result<u64> {
    let mut keys = Vec::new();
    for stage in DATA_STAGES {
        keys.extend(
            ctx.store
                .list_keys(&layout.data_dir(stage.as_str())?)
                .await?,
        );
    }
    if keys.is_empty() {
        return Ok(0);
    }
    for stage in StageName::ALL {
        for key in ctx
            .store
            .list_keys(&layout.leases_dir(stage.as_str())?)
            .await?
        {
            match ctx.store.get_json::<Lease>(&key).await? {
                Some((lease, _)) if lease.is_expired(OffsetDateTime::now_utc(), skew) => {}
                _ => return Ok(0),
            }
        }
    }
    let failures = futures::stream::iter(keys)
        .map(|key| {
            let store = ctx.store.clone();
            async move {
                if let Err(e) = store.delete(&key).await {
                    tracing::warn!(%key, error = %e, "segment delete failed");
                    1u64
                } else {
                    0
                }
            }
        })
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    Ok(failures.into_iter().sum())
}

/// Remove only stage data from older, completed stamped runs. Conservatively leave runs with
/// missing state or a live lease alone. The current run is always counted and never touched.
async fn retain_segments(
    ctx: &RunContext,
    retain: usize,
    skew: std::time::Duration,
) -> anyhow::Result<u64> {
    if retain == 0 {
        return Ok(0);
    }
    let stamp = |id: &str| pi_es::run_stamp("run", &format!("run-{id}"));
    let Some(current_stamp) = stamp(ctx.layout.run_id()) else {
        tracing::info!(run = %ctx.layout.run_id(), "segment retention skipped: run id has no stamp");
        return Ok(0);
    };
    let mut failures = 0;
    let mut runs = Vec::new();
    for dir in ctx.store.list_prefixes(RunLayout::runs_root()).await? {
        let Some(id) = RunLayout::run_id_from_dir(&dir) else {
            continue;
        };
        if id == ctx.layout.run_id() {
            continue;
        }
        let Some(date) = stamp(id) else {
            continue;
        };
        match published_layout(ctx, id).await {
            Ok(Some(layout)) => runs.push((date, layout)),
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(run = %id, error = %e, "segment retention could not inspect run");
                failures += 1;
            }
        }
    }
    runs.push((current_stamp.clone(), ctx.layout.clone()));
    runs.sort_by(|a, b| b.0.cmp(&a.0));
    let boundary = runs.get(retain.saturating_sub(1)).map(|r| r.0.clone());
    for (date, layout) in runs {
        if layout.run_id() == ctx.layout.run_id()
            || date >= current_stamp
            || boundary.as_ref().is_none_or(|b| date >= *b)
        {
            continue;
        }
        match clean_run(ctx, &layout, skew).await {
            Ok(n) => failures += n,
            Err(e) => {
                tracing::warn!(run = %layout.run_id(), error = %e, "segment retention could not clean run");
                failures += 1;
            }
        }
    }
    Ok(failures)
}

#[async_trait]
impl Stage for PublishStage {
    fn name(&self) -> StageName {
        StageName::Publish
    }

    async fn tasks(&self, _ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        Ok(vec![TaskSpec::single(StageName::Publish)])
    }

    /// Idempotent: publishing an index the alias already points at changes nothing, an attempt
    /// that resumes after the alias moved still deletes the indexes `publish.json` recorded,
    /// and the summary is rewritten with the same content (and a later `finished_at`).
    #[allow(
        clippy::too_many_lines,
        reason = "publish builds the complete run summary"
    )]
    async fn run(
        &self,
        ctx: &RunContext,
        _task: &TaskSpec,
        _attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let started = Instant::now();
        let plan = Plan::load(ctx).await?;
        let mut stages = self.stage_summaries(ctx).await?;
        let documents = stages
            .iter()
            .find(|s| s.stage == StageName::Assemble)
            .map_or(0, Summary::documents_of);

        let es = &self.config.es;
        let counter = |stage, name: &str| {
            stages
                .iter()
                .find(|s| s.stage == stage)
                .and_then(|s| s.counters.get(name))
                .copied()
                .unwrap_or(0)
        };
        let resolved = counter(StageName::Assemble, "roles_resolved");
        let unresolved = counter(StageName::Assemble, "roles_unresolved");
        let organizations = counter(StageName::Assemble, "organizations");
        if ctx
            .store
            .get_json::<PublishRecord>(&ctx.layout.publish_record())
            .await?
            .is_none()
        {
            check_feed_bounds(resolved, unresolved, organizations, es)?;
        }
        let mut spec = PublishSpec::new(&plan.alias, &plan.index, documents);
        spec.serving = ServingSettings {
            replicas: es.replicas,
            ..ServingSettings::default()
        };
        spec.min_share = es.publish_min_share;
        spec.wait_for = es.wait_for;
        spec.health_timeout = es.health_timeout;
        let alias = &plan.alias;
        let index = &plan.index;
        let published = Publisher::new(IndexAdmin::new(self.client.clone()))
            .publish_recording(&spec, |on_alias, previous_docs| async move {
                record_previous(ctx, alias, index, on_alias, previous_docs)
                    .await
                    .map_err(|e| EsError::Record(format!("{e:#}")))
            })
            .await
            .map_err(|e| es_error(e, &format!("publishing {} as {}", plan.index, plan.alias)))?;
        tracing::info!(alias = %plan.alias, index = %plan.index, documents, "published");

        let (orphans_deleted, orphans_left) = delete_orphans(ctx).await?;

        let retention_failures =
            match retain_segments(ctx, es.retain_runs, es.lease_clock_skew).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(error = %e, "segment retention cleanup failed");
                    1
                }
            };
        let finished_at = OffsetDateTime::now_utc();
        let mut own = StageSummary::from_commits(StageName::Publish, &[]);
        own.tasks = 1;
        own.wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        own.task_ms = own.wall_ms;
        stages.push(own);
        let wall_ms = millis(finished_at - plan.created_at);
        let summary = Summary {
            run_id: plan.run_id.clone(),
            delivery: plan.delivery.clone(),
            alias: plan.alias.clone(),
            index: plan.index.clone(),
            partitions: plan.partitions,
            started_at: plan.created_at,
            finished_at,
            wall_ms,
            documents,
            persons_per_second: Summary::rate(documents, wall_ms),
            input_bytes: plan.total_bytes,
            stages,
            publish: PublishSummary {
                docs: published.docs,
                previous_docs: published.previous_docs,
                resolved_roles: resolved,
                unresolved_roles: unresolved,
                indexed_organizations: organizations,
                min_share: es.publish_min_share,
                max_unresolved: es.publish_max_unresolved,
                allow_empty_organizations: es.publish_allow_empty_organizations,
                previous: published.previous.clone(),
                deleted: published.deleted.clone(),
                not_deleted: published.not_deleted.clone(),
            },
            orphans_deleted,
            orphans_left,
            retention_failures,
        };
        let key = ctx.layout.summary();
        ctx.store.put_json(&key, &summary).await?;

        let mut report = TaskReport {
            outputs: vec![key],
            ..TaskReport::default()
        };
        report.add("documents", published.docs);
        report.add("previous_indexes", published.previous.len() as u64);
        report.add("deleted_indexes", published.deleted.len() as u64);
        report.add("orphans_deleted", orphans_deleted);
        report.add("orphans_left", orphans_left);
        report.add("retention_failures", retention_failures);
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use pi_store::Store;

    use super::*;

    async fn retention_scenario(store: Store) {
        let current = "20261028T120000Z-current";
        let ctx = RunContext::new(current, store.clone()).unwrap();
        let ids = [
            "20261024-120000",
            "20261025-120000",
            "20261026-120000",
            "20261027-120000",
            current,
            "20261023-120000",
            "manual",
        ];
        for id in ids {
            let layout = RunLayout::new(id).unwrap();
            for stage in DATA_STAGES {
                let key = format!("{}/segment.seg", layout.data_dir(stage.as_str()).unwrap());
                store.put(&key, "data").await.unwrap();
            }
            for key in [
                layout.plan(),
                layout.summary(),
                layout.publish_record(),
                layout.quarantine("task", "attempt").unwrap(),
            ] {
                store.put(&key, "history").await.unwrap();
            }
            let mut state = RunState::new(id);
            state.status = if id == "20261023-120000" {
                RunStatus::Running
            } else {
                RunStatus::Published
            };
            state.save(&store, &layout).await.unwrap();
        }
        retain_segments(&ctx, 0, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let old = RunLayout::new(ids[0]).unwrap();
        assert_eq!(
            store
                .list_keys(&old.data_dir("join").unwrap())
                .await
                .unwrap()
                .len(),
            1
        );
        retain_segments(&ctx, 3, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        for (i, id) in ids.iter().enumerate() {
            let layout = RunLayout::new(*id).unwrap();
            for stage in DATA_STAGES {
                assert_eq!(
                    store
                        .list_keys(&layout.data_dir(stage.as_str()).unwrap())
                        .await
                        .unwrap()
                        .is_empty(),
                    i < 2,
                    "{id}"
                );
            }
            for key in [
                layout.plan(),
                layout.summary(),
                layout.publish_record(),
                layout.quarantine("task", "attempt").unwrap(),
                layout.state(),
            ] {
                assert!(store.get_opt(&key).await.unwrap().is_some(), "{key}");
            }
        }
        retain_segments(&ctx, 3, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            store
                .list_keys(&old.data_dir("join").unwrap())
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn retention_leaves_unpublished_and_live_leases() {
        let store = Store::memory();
        let ctx = RunContext::new("20261028-120000", store.clone()).unwrap();
        for id in ["20261020-120000", "20261021-120000"] {
            let layout = RunLayout::new(id).unwrap();
            store
                .put(
                    &format!("{}/segment", layout.data_dir("join").unwrap()),
                    "data",
                )
                .await
                .unwrap();
            let mut state = RunState::new(id);
            state.status = RunStatus::Published;
            state.save(&store, &layout).await.unwrap();
            if id.contains("21") {
                store.put(&layout.publish_record(), "record").await.unwrap();
                let now = OffsetDateTime::now_utc();
                let lease = Lease {
                    worker: "w".into(),
                    holder: "h".into(),
                    acquired_at: now,
                    expires_at: now + time::Duration::minutes(10),
                    renewals: 0,
                    released: false,
                };
                store
                    .put_json(&layout.lease("join", "task").unwrap(), &lease)
                    .await
                    .unwrap();
            }
        }
        retain_segments(&ctx, 1, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        for id in ["20261020-120000", "20261021-120000"] {
            let layout = RunLayout::new(id).unwrap();
            assert_eq!(
                store
                    .list_keys(&layout.data_dir("join").unwrap())
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn orphan_cleanup_respects_quarantine_commits() {
        let ctx = RunContext::new("test", Store::memory()).unwrap();
        let kept = ctx.layout.quarantine("task", "kept").unwrap();
        let orphan = ctx.layout.quarantine("task", "orphan").unwrap();
        ctx.store.put(&kept, "kept").await.unwrap();
        ctx.store.put(&orphan, "orphan").await.unwrap();
        let record = pi_coord::CommitRecord {
            stage: StageName::Assemble,
            task_id: "task".into(),
            attempt: AttemptId::new(),
            worker: "test".into(),
            committed_at: OffsetDateTime::now_utc(),
            report: TaskReport {
                outputs: vec![kept.clone()],
                ..TaskReport::default()
            },
        };
        CommitLog::new(ctx.store.clone(), ctx.layout.clone())
            .commit(&record)
            .await
            .unwrap();
        assert_eq!(delete_orphans(&ctx).await.unwrap(), (1, 0));
        assert!(ctx.store.get_opt(&kept).await.unwrap().is_some());
        assert!(ctx.store.get_opt(&orphan).await.unwrap().is_none());
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one scenario checks boundaries, leases and retained history"
    )]
    async fn retention_boundaries_leases_and_history() {
        let store = Store::memory();
        let ctx = RunContext::new("20261028-120000", store.clone()).unwrap();
        let ids = [
            "20261029-120000",
            "20261026-120000",
            "20261026T120000Z-x",
            "20261025-120000",
            "20261024-120000",
            "20261023-120000",
        ];
        for id in ids {
            let layout = RunLayout::new(id).unwrap();
            store
                .put(
                    &format!("{}/segment", layout.data_dir("join").unwrap()),
                    "data",
                )
                .await
                .unwrap();
            store.put(&layout.publish_record(), "record").await.unwrap();
            let mut state = RunState::new(id);
            state.status = if id.contains("23-") {
                RunStatus::Failed
            } else {
                RunStatus::Published
            };
            state.save(&store, &layout).await.unwrap();
            for key in [
                layout.plan(),
                layout.summary(),
                layout.quarantine("task", "attempt").unwrap(),
                layout.commit("join", "task").unwrap(),
                layout.stage_spec("join").unwrap(),
            ] {
                store.put(&key, "history").await.unwrap();
            }
            if id.contains("24-") || id.contains("25-") {
                let now = OffsetDateTime::now_utc();
                let lease = Lease {
                    worker: "w".into(),
                    holder: "h".into(),
                    acquired_at: now,
                    expires_at: if id.contains("24-") {
                        now - time::Duration::minutes(1)
                    } else {
                        now + time::Duration::minutes(1)
                    },
                    renewals: 0,
                    released: id.contains("25-"),
                };
                store
                    .put_json(&layout.lease("join", "task").unwrap(), &lease)
                    .await
                    .unwrap();
            }
        }
        assert_eq!(
            retain_segments(&ctx, 3, std::time::Duration::from_secs(5))
                .await
                .unwrap(),
            0
        );
        for (i, id) in ids.iter().enumerate() {
            let layout = RunLayout::new(*id).unwrap();
            assert_eq!(
                store
                    .list_keys(&layout.data_dir("join").unwrap())
                    .await
                    .unwrap()
                    .is_empty(),
                i == 3 || i == 4,
                "{id}"
            );
            for key in [
                layout.plan(),
                layout.summary(),
                layout.publish_record(),
                layout.state(),
                layout.quarantine("task", "attempt").unwrap(),
                layout.commit("join", "task").unwrap(),
                layout.stage_spec("join").unwrap(),
            ] {
                assert!(store.get_opt(&key).await.unwrap().is_some(), "{key}");
            }
            if i == 3 || i == 4 {
                assert!(
                    store
                        .get_opt(&layout.lease("join", "task").unwrap())
                        .await
                        .unwrap()
                        .is_some()
                );
            }
        }
        let manual = RunContext::new("manual", store.clone()).unwrap();
        let before = store.list_keys(RunLayout::runs_root()).await.unwrap();
        assert_eq!(
            retain_segments(&manual, 1, std::time::Duration::ZERO)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            before,
            store.list_keys(RunLayout::runs_root()).await.unwrap()
        );
    }

    #[tokio::test]
    async fn retention_keeps_data_with_unreadable_lease() {
        let store = Store::memory();
        let ctx = RunContext::new("20261028T120000Z-current", store.clone()).unwrap();
        let old = RunLayout::new("20261024T120000Z-old").unwrap();
        let data = format!("{}/segment", old.data_dir("join").unwrap());
        store.put(&data, "data").await.unwrap();
        store.put(&old.publish_record(), "record").await.unwrap();
        let mut state = RunState::new(old.run_id());
        state.status = RunStatus::Published;
        state.save(&store, &old).await.unwrap();
        store
            .put(&old.lease("join", "task").unwrap(), "invalid json")
            .await
            .unwrap();

        assert_eq!(
            retain_segments(&ctx, 1, std::time::Duration::ZERO)
                .await
                .unwrap(),
            1
        );
        assert!(store.get_opt(&data).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn retention_on_memory_and_local_stores() {
        retention_scenario(Store::memory()).await;
        let dir = tempfile::tempdir().unwrap();
        retention_scenario(Store::local(dir.path()).unwrap()).await;
    }

    #[test]
    fn feed_bounds_apply_even_on_first_publish() {
        let mut config = crate::config::EsStageConfig::default();
        assert!(check_feed_bounds(3, 1, 1, &config).is_ok());
        assert!(pi_coord::is_permanent(
            &check_feed_bounds(2, 1, 1, &config).unwrap_err()
        ));
        assert!(pi_coord::is_permanent(
            &check_feed_bounds(0, 0, 0, &config).unwrap_err()
        ));
        config.publish_allow_empty_organizations = true;
        assert!(check_feed_bounds(0, 1, 0, &config).is_err());
        config.publish_max_unresolved = 1.0;
        assert!(check_feed_bounds(0, 1, 0, &config).is_ok());
    }

    async fn stage_context(resolved: u64, unresolved: u64, organizations: u64) -> RunContext {
        let ctx = RunContext::new("run-test", Store::memory()).unwrap();
        let plan = Plan {
            run_id: "run-test".into(),
            partitions: 1,
            alias: "persons".into(),
            index: "persons-run-test".into(),
            organizations: vec![],
            persons: vec![],
            total_bytes: 0,
            created_at: OffsetDateTime::now_utc(),
            delivery: None,
        };
        ctx.store.put_json(&ctx.layout.plan(), &plan).await.unwrap();
        let mut report = TaskReport::default();
        report.add("documents", 50);
        report.add("roles_resolved", resolved);
        report.add("roles_unresolved", unresolved);
        report.add("organizations", organizations);
        let record = pi_coord::CommitRecord {
            stage: StageName::Assemble,
            task_id: "part-00000".into(),
            attempt: AttemptId::new(),
            worker: "test".into(),
            committed_at: OffsetDateTime::now_utc(),
            report,
        };
        CommitLog::new(ctx.store.clone(), ctx.layout.clone())
            .commit(&record)
            .await
            .unwrap();
        ctx
    }

    async fn run_publish(ctx: &RunContext, url: &str) -> anyhow::Result<TaskReport> {
        let client = EsClient::new(&pi_es::EsConfig::new(url)).unwrap();
        PublishStage::new(
            Arc::new(StagesConfig::default()),
            Arc::new(CommitCache::new()),
            client,
        )
        .run(
            ctx,
            &TaskSpec::single(StageName::Publish),
            &AttemptId::new(),
        )
        .await
    }

    #[tokio::test]
    async fn stage_refuses_feed_bounds_before_contacting_es() {
        let ctx = stage_context(2, 1, 1).await;
        let error = run_publish(&ctx, "http://localhost:1").await.unwrap_err();
        assert!(pi_coord::is_permanent(&error));
        assert!(error.to_string().contains("unresolved roles"));
        let ctx = stage_context(0, 0, 0).await;
        let error = run_publish(&ctx, "http://localhost:1").await.unwrap_err();
        assert!(pi_coord::is_permanent(&error));
        assert!(error.to_string().contains("indexed organizations"));
    }

    #[tokio::test]
    async fn stage_compares_previous_count_and_accepts_first_publish() {
        use serde_json::json;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        let shards = json!({"total": 1, "successful": 1, "failed": 0});
        for op in ["_refresh", "_flush"] {
            Mock::given(method("POST"))
                .and(path(format!("/persons-run-test/{op}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"_shards": shards})))
                .mount(&server)
                .await;
        }
        for (index, count) in [("persons-run-test", 50), ("persons-old", 100)] {
            Mock::given(method("POST"))
                .and(path(format!("/{index}/_count")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(json!({"count": count, "_shards": shards})),
                )
                .mount(&server)
                .await;
        }
        Mock::given(method("PUT"))
            .and(path("/persons-run-test/_settings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/_cluster/health/persons-run-test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "green"})))
            .mount(&server)
            .await;
        let alias = Mock::given(method("GET"))
            .and(path("/_alias/persons"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"persons-old": {}})))
            .mount_as_scoped(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_aliases"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"acknowledged": true})))
            .expect(1)
            .mount(&server)
            .await;
        let ctx = stage_context(3, 1, 1).await;
        let error = run_publish(&ctx, &server.uri()).await.unwrap_err();
        assert!(pi_coord::is_permanent(&error));
        assert!(format!("{error:#}").contains("minimum document share"));
        drop(alias);
        Mock::given(method("GET"))
            .and(path("/_alias/persons"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/persons"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        run_publish(&ctx, &server.uri()).await.unwrap();
        let summary: Summary = ctx
            .store
            .get_json(&ctx.layout.summary())
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(summary.publish.previous_docs, None);
    }

    #[tokio::test]
    async fn recorded_publish_skips_feed_bounds_on_resume() {
        let ctx = stage_context(0, 1, 0).await;
        record_previous(&ctx, "persons", "persons-run-test", vec![], None)
            .await
            .unwrap();
        // Reaching Elasticsearch proves the stricter bounds did not reject the recorded run.
        let error = run_publish(&ctx, "http://localhost:1").await.unwrap_err();
        assert!(!format!("{error:#}").contains("unresolved roles"));
        assert!(format!("{error:#}").contains("publishing"));
    }

    #[tokio::test]
    async fn the_first_record_wins() {
        let ctx = RunContext::new("run-r", Store::memory()).unwrap();
        let first = record_previous(
            &ctx,
            "persons",
            "persons-run-r",
            vec!["persons-old".into()],
            Some(10),
        )
        .await
        .unwrap();
        assert_eq!(first, (vec!["persons-old".to_owned()], Some(10)));
        // A resumed attempt sees the alias on its own index already: the record still names
        // what it served before.
        let again = record_previous(&ctx, "persons", "persons-run-r", Vec::new(), None)
            .await
            .unwrap();
        assert_eq!(again, (vec!["persons-old".to_owned()], Some(10)));
        let (stored, _) = ctx
            .store
            .get_json::<PublishRecord>(&ctx.layout.publish_record())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored,
            PublishRecord {
                alias: "persons".into(),
                index: "persons-run-r".into(),
                previous: vec!["persons-old".into()],
                previous_docs: Some(10),
            }
        );
    }
}
