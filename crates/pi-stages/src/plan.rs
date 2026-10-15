//! `plan`: lists the inputs, fixes the partition count and the index name in `plan.json`.

use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use pi_coord::{AttemptId, RunContext, Stage, StageName, TaskReport, TaskSpec, permanent};
use pi_model::partition_count;
use pi_store::{CreateOutcome, Store};
use time::OffsetDateTime;

use crate::config::StagesConfig;
use crate::plan_file::{InputFile, Plan, index_name, is_input_key};

/// The `plan` stage: one task per run.
#[derive(Debug, Clone)]
pub struct PlanStage {
    config: Arc<StagesConfig>,
}

impl PlanStage {
    /// The stage with the given settings.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>) -> Self {
        Self { config }
    }

    async fn make(&self, ctx: &RunContext) -> anyhow::Result<Plan> {
        let inputs = self.config.input_store(&ctx.store);
        let organizations = list_inputs(inputs, &ctx.layout.raw_organizations()).await?;
        let persons = list_inputs(inputs, &ctx.layout.raw_persons()).await?;
        if persons.is_empty() {
            return Err(permanent(anyhow::anyhow!(
                "no person files (*.ndjson.gz) under {}/{}",
                inputs.url().trim_end_matches('/'),
                ctx.layout.raw_persons()
            )));
        }
        if organizations.is_empty() {
            tracing::warn!(
                prefix = %ctx.layout.raw_organizations(),
                "no organization files: every organization id will be unresolved"
            );
        }
        // Seq::file is a u32 position in the feed.
        for (feed, files) in [("organization", &organizations), ("person", &persons)] {
            if u32::try_from(files.len()).is_err() {
                return Err(permanent(anyhow::anyhow!(
                    "{} {feed} files: more than a feed can hold",
                    files.len()
                )));
            }
        }
        let total_bytes = organizations
            .iter()
            .chain(&persons)
            .map(|f| f.size)
            .sum::<u64>();
        let partitions = partition_count(
            total_bytes,
            self.config.partition_target_bytes,
            self.config.min_partitions,
        )
        .map_err(|e| permanent(anyhow::anyhow!("choosing the partition count: {e}")))?;
        let index = index_name(&self.config.alias, &ctx.run_id);
        pi_es::validate_index_name(&index)
            .map_err(|e| permanent(anyhow::anyhow!("index name {index:?}: {e}")))?;
        Ok(Plan {
            run_id: ctx.run_id.clone(),
            partitions,
            alias: self.config.alias.clone(),
            index,
            organizations,
            persons,
            total_bytes,
            created_at: OffsetDateTime::now_utc(),
        })
    }
}

/// The input files under `prefix`, sorted by key (feed order).
async fn list_inputs(store: &Store, prefix: &str) -> anyhow::Result<Vec<InputFile>> {
    Ok(store
        .list(prefix)
        .await
        .with_context(|| format!("listing {prefix}"))?
        .into_iter()
        .filter(|o| is_input_key(&o.key))
        .map(|o| InputFile {
            key: o.key,
            size: o.size,
        })
        .collect())
}

#[async_trait]
impl Stage for PlanStage {
    fn name(&self) -> StageName {
        StageName::Plan
    }

    async fn tasks(&self, _ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        Ok(vec![TaskSpec::single(StageName::Plan)])
    }

    async fn run(
        &self,
        ctx: &RunContext,
        _task: &TaskSpec,
        _attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let key = ctx.layout.plan();
        // The first plan written is the plan: a retry, or a second coordinator, reads it back
        // instead of listing inputs that may have changed since.
        let plan = if let Some((plan, _)) = ctx.store.get_json::<Plan>(&key).await? {
            plan
        } else {
            let plan = self.make(ctx).await?;
            match ctx.store.put_json_create_if_absent(&key, &plan).await? {
                CreateOutcome::Created(_) => plan,
                CreateOutcome::AlreadyExists => Plan::load(ctx).await?,
            }
        };
        tracing::info!(
            partitions = plan.partitions,
            organization_files = plan.organizations.len(),
            person_files = plan.persons.len(),
            input_bytes = plan.total_bytes,
            index = %plan.index,
            "plan ready"
        );
        let mut report = TaskReport {
            outputs: vec![key],
            bytes_in: plan.total_bytes,
            ..TaskReport::default()
        };
        report.add("partitions", u64::from(plan.partitions));
        report.add("organization_files", plan.organizations.len() as u64);
        report.add("person_files", plan.persons.len() as u64);
        Ok(report)
    }
}
