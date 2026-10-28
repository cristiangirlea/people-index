//! `publish`: restores serving settings, checks the count, records the indexes the alias
//! serves in `publish.json`, moves the alias, deletes those indexes, writes `summary.json` and
//! removes the files of attempts that never committed.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use pi_coord::{AttemptId, CommitLog, RunContext, Stage, StageName, TaskReport, TaskSpec};
use pi_es::{EsClient, EsError, IndexAdmin, PublishSpec, Publisher, ServingSettings};
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

#[cfg(test)]
mod tests {
    use pi_store::Store;

    use super::*;

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
