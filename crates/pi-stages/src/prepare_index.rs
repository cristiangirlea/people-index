//! `prepare-index`: creates the run's index with load-time settings.

use std::sync::Arc;

use async_trait::async_trait;
use pi_coord::{AttemptId, RunContext, Stage, StageName, TaskReport, TaskSpec};
use pi_es::{CreateOutcome, EsClient, IndexAdmin, IndexSpec};

use crate::config::StagesConfig;
use crate::es_error::es_error;
use crate::plan_file::Plan;

/// The `prepare-index` stage: one task per run.
#[derive(Debug, Clone)]
pub struct PrepareIndexStage {
    config: Arc<StagesConfig>,
    client: EsClient,
}

impl PrepareIndexStage {
    /// The stage with the given settings, talking to the cluster through `client`.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>, client: EsClient) -> Self {
        Self { config, client }
    }
}

#[async_trait]
impl Stage for PrepareIndexStage {
    fn name(&self) -> StageName {
        StageName::PrepareIndex
    }

    async fn tasks(&self, _ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        Ok(vec![TaskSpec::single(StageName::PrepareIndex)])
    }

    /// Idempotent: an index left by an earlier attempt of this run is taken as it is (it was
    /// created with the same settings, and `load` resends every document by `_id`).
    async fn run(
        &self,
        ctx: &RunContext,
        _task: &TaskSpec,
        _attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let plan = Plan::load(ctx).await?;
        let admin = IndexAdmin::new(self.client.clone());
        let data_nodes = admin
            .data_node_count()
            .await
            .map_err(|e| es_error(e, "counting data nodes"))?;
        let shards = self
            .config
            .es
            .shards
            .unwrap_or_else(|| IndexSpec::default_shards(data_nodes));
        let spec = IndexSpec::new(shards, self.config.es.codec);
        let outcome = admin
            .create_index(&plan.index, &spec)
            .await
            .map_err(|e| es_error(e, &format!("creating index {}", plan.index)))?;
        let created = matches!(outcome, CreateOutcome::Created);
        tracing::info!(index = %plan.index, shards, data_nodes, created, "index ready for loading");
        let mut report = TaskReport::default();
        report.add("data_nodes", u64::from(data_nodes));
        report.add("shards", u64::from(shards));
        report.add("created", u64::from(created));
        Ok(report)
    }
}
