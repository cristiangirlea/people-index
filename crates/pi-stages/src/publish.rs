//! `publish`: restores serving settings, checks the count, moves the alias, writes
//! `summary.json` and removes the files of attempts that never committed.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use pi_coord::{AttemptId, CommitLog, RunContext, Stage, StageName, TaskReport, TaskSpec};
use pi_es::{EsClient, IndexAdmin, PublishSpec, Publisher, ServingSettings};
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

#[async_trait]
impl Stage for PublishStage {
    fn name(&self) -> StageName {
        StageName::Publish
    }

    async fn tasks(&self, _ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        Ok(vec![TaskSpec::single(StageName::Publish)])
    }

    /// Idempotent: publishing an index the alias already points at changes nothing, and the
    /// summary is rewritten with the same content (and a later `finished_at`).
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
        let mut spec = PublishSpec::new(&plan.alias, &plan.index, documents);
        spec.serving = ServingSettings {
            replicas: es.replicas,
            ..ServingSettings::default()
        };
        spec.wait_for = es.wait_for;
        spec.health_timeout = es.health_timeout;
        let published = Publisher::new(IndexAdmin::new(self.client.clone()))
            .publish(&spec)
            .await
            .map_err(|e| es_error(e, &format!("publishing {} as {}", plan.index, plan.alias)))?;
        tracing::info!(alias = %plan.alias, index = %plan.index, documents, "published");

        let (orphans_deleted, orphans_left) = delete_orphans(ctx).await?;

        let finished_at = OffsetDateTime::now_utc();
        let mut own = StageSummary::from_commits(StageName::Publish, &[]);
        own.tasks = 1;
        own.wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        own.task_ms = own.wall_ms;
        stages.push(own);
        let wall_ms = millis(finished_at - plan.created_at);
        let summary = Summary {
            run_id: plan.run_id.clone(),
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
                previous: published.previous.clone(),
                deleted: published.deleted.clone(),
                not_deleted: published.not_deleted.clone(),
            },
            orphans_deleted,
            orphans_left,
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
        Ok(report)
    }
}
