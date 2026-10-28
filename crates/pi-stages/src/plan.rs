//! `plan`: lists the inputs, fixes the partition count and the index name in `plan.json`.

use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use pi_coord::{AttemptId, RunContext, Stage, StageName, TaskReport, TaskSpec, permanent};
use pi_model::partition_count;
use pi_store::{CreateOutcome, ObjectInfo, Store};
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
        if self.config.require_complete_marker {
            let marker = ctx.layout.input_key("_COMPLETE");
            if inputs.head(&marker).await?.is_none() {
                return Err(permanent(anyhow::anyhow!(
                    "missing completion marker {marker}"
                )));
            }
        }
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
            delivery: self.config.delivery.clone(),
            partitions,
            alias: self.config.alias.clone(),
            index,
            organizations,
            persons,
            total_bytes,
            schema: Some(self.config.schema_pin()),
            created_at: OffsetDateTime::now_utc(),
        })
    }
}

/// The input files under `prefix`, sorted by key (feed order), with the versions extract
/// will insist on.
async fn list_inputs(store: &Store, prefix: &str) -> anyhow::Result<Vec<InputFile>> {
    let listed = store
        .list(prefix)
        .await
        .with_context(|| format!("listing {prefix}"))?;
    input_files(listed)
}

/// The input files among `listed`, each with the version the store listed. One without a
/// version fails the plan for good.
fn input_files(listed: Vec<ObjectInfo>) -> anyhow::Result<Vec<InputFile>> {
    listed
        .into_iter()
        .filter(|o| is_input_key(&o.key))
        .map(|o| {
            if o.version.is_empty() {
                // Without one, extract could not tell a replaced file from the planned one.
                return Err(permanent(anyhow::anyhow!(
                    "the store lists {} without an etag or version: the run cannot pin its inputs",
                    o.key
                )));
            }
            Ok(InputFile {
                key: o.key,
                size: o.size,
                version: o.version,
            })
        })
        .collect()
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
        plan.check_schema(&self.config.schema_pin())?;
        let pin = plan.schema_pin();
        tracing::info!(
            partitions = plan.partitions,
            organization_files = plan.organizations.len(),
            person_files = plan.persons.len(),
            input_bytes = plan.total_bytes,
            index = %plan.index,
            builtin_schema = self.config.schema.is_builtin(),
            feed_schema_sha256 = %pin.feed_schema_sha256,
            mapping_sha256 = %pin.mapping_sha256,
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use pi_store::ObjectVersion;

    use super::*;

    fn listed(key: &str, version: ObjectVersion) -> ObjectInfo {
        ObjectInfo {
            key: key.to_owned(),
            size: 7,
            version,
            last_modified: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn an_input_listed_without_a_version_fails_the_plan_for_good() {
        let etag = ObjectVersion {
            e_tag: Some("\"abc\"".into()),
            version: None,
        };
        let files = input_files(vec![
            listed("raw/persons/a.ndjson.gz", etag.clone()),
            listed("raw/persons/_SUCCESS", ObjectVersion::default()),
        ])
        .unwrap();
        assert_eq!(files.len(), 1, "only input keys are kept");
        assert_eq!(files[0].version, etag);

        let err = input_files(vec![
            listed("raw/persons/a.ndjson.gz", etag),
            listed("raw/persons/b.ndjson.gz", ObjectVersion::default()),
        ])
        .unwrap_err();
        let text = format!("{err:#}");
        assert!(pi_coord::is_permanent(&err), "{text}");
        assert!(text.contains("raw/persons/b.ndjson.gz"), "{text}");
    }
    #[tokio::test]
    async fn delivery_marker_and_input_selection() {
        let store = Store::memory();
        store
            .put("raw/other/persons/a.ndjson.gz", b"other".to_vec())
            .await
            .unwrap();
        store
            .put("raw/batch/persons/a.ndjson.gz", b"person".to_vec())
            .await
            .unwrap();
        store
            .put("raw/batch/organizations/a.ndjson.gz", b"org".to_vec())
            .await
            .unwrap();
        let layout = pi_store::RunLayout::new("r1")
            .unwrap()
            .with_input_root("raw/batch");
        let ctx = RunContext::with_layout(store.clone(), layout);
        let config = StagesConfig {
            delivery: Some("batch".into()),
            require_complete_marker: true,
            ..StagesConfig::default()
        };
        let stage = PlanStage::new(Arc::new(config.clone()));
        let err = stage.make(&ctx).await.unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        assert!(format!("{err:#}").contains("raw/batch/_COMPLETE"));
        store.put("raw/batch/_COMPLETE", Vec::new()).await.unwrap();
        let plan = stage.make(&ctx).await.unwrap();
        assert_eq!(plan.delivery.as_deref(), Some("batch"));
        assert_eq!(plan.persons.len(), 1);
        assert_eq!(plan.persons[0].key, "raw/batch/persons/a.ndjson.gz");
        assert_eq!(
            plan.organizations[0].key,
            "raw/batch/organizations/a.ndjson.gz"
        );
        let default_ctx = RunContext::new("r2", store.clone()).unwrap();
        store
            .put("raw/persons/a.ndjson.gz", b"default".to_vec())
            .await
            .unwrap();
        let default_plan = PlanStage::new(Arc::new(StagesConfig::default()))
            .make(&default_ctx)
            .await
            .unwrap();
        assert_eq!(default_plan.delivery, None);
        assert_eq!(default_plan.persons.len(), 1);
        assert_eq!(default_plan.persons[0].key, "raw/persons/a.ndjson.gz");
    }
    #[tokio::test]
    async fn resume_keeps_first_plan_when_delivery_changes() {
        let store = Store::memory();
        store
            .put("raw/a/persons/a.ndjson.gz", vec![1])
            .await
            .unwrap();
        store.put("raw/a/_COMPLETE", vec![]).await.unwrap();
        let ctx_a = RunContext::with_layout(
            store.clone(),
            pi_store::RunLayout::new("resume")
                .unwrap()
                .with_input_root("raw/a"),
        );
        let stage_a = PlanStage::new(Arc::new(StagesConfig {
            delivery: Some("a".into()),
            require_complete_marker: true,
            ..StagesConfig::default()
        }));
        let task = TaskSpec::single(StageName::Plan);
        stage_a.run(&ctx_a, &task, &AttemptId::new()).await.unwrap();
        let ctx_b = RunContext::with_layout(
            store.clone(),
            pi_store::RunLayout::new("resume")
                .unwrap()
                .with_input_root("raw/b"),
        );
        let stage_b = PlanStage::new(Arc::new(StagesConfig {
            delivery: Some("b".into()),
            require_complete_marker: true,
            ..StagesConfig::default()
        }));
        stage_b.run(&ctx_b, &task, &AttemptId::new()).await.unwrap();
        let plan = Plan::load(&ctx_b).await.unwrap();
        assert_eq!(plan.delivery.as_deref(), Some("a"));
        assert_eq!(plan.persons[0].key, "raw/a/persons/a.ndjson.gz");
    }

    #[tokio::test]
    async fn the_plan_pins_the_schema_and_a_resume_with_another_one_fails_for_good() {
        let store = Store::memory();
        store.put("raw/persons/a.ndjson.gz", vec![1]).await.unwrap();
        let ctx = RunContext::new("pinned", store.clone()).unwrap();
        let task = TaskSpec::single(StageName::Plan);
        let custom = StagesConfig {
            schema: Arc::new(
                pi_model::FeedSchema::from_json(r#"{"envelope": {"id": "key"}}"#).unwrap(),
            ),
            ..StagesConfig::default()
        };
        PlanStage::new(Arc::new(custom.clone()))
            .run(&ctx, &task, &AttemptId::new())
            .await
            .unwrap();
        let plan = Plan::load(&ctx).await.unwrap();
        assert_eq!(plan.schema, Some(custom.schema_pin()));
        assert_ne!(plan.schema_pin(), crate::plan_file::SchemaPin::builtin());
        // The same settings resume; the built-in ones do not, at the plan or in any task.
        PlanStage::new(Arc::new(custom.clone()))
            .run(&ctx, &task, &AttemptId::new())
            .await
            .unwrap();
        Plan::load_for(&ctx, &custom).await.unwrap();
        let builtin = StagesConfig::default();
        let err = PlanStage::new(Arc::new(builtin.clone()))
            .run(&ctx, &task, &AttemptId::new())
            .await
            .unwrap_err();
        assert!(pi_coord::is_permanent(&err), "{err:#}");
        assert!(format!("{err:#}").contains("PI_SCHEMA_FILE"), "{err:#}");
        assert!(pi_coord::is_permanent(
            &Plan::load_for(&ctx, &builtin).await.unwrap_err()
        ));
    }

    #[tokio::test]
    async fn local_input_root_marker_and_feed_marker() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::local(dir.path()).unwrap();
        store.put("persons/a.ndjson.gz", vec![1]).await.unwrap();
        store.put("persons/_COMPLETE", vec![]).await.unwrap();
        let ctx = RunContext::with_layout(
            store.clone(),
            pi_store::RunLayout::new("local")
                .unwrap()
                .with_input_root(""),
        );
        let stage = PlanStage::new(Arc::new(StagesConfig {
            input_store: Some(store.clone()),
            require_complete_marker: true,
            ..StagesConfig::default()
        }));
        assert!(stage.make(&ctx).await.is_err());
        store.put("_COMPLETE", vec![]).await.unwrap();
        let plan = stage.make(&ctx).await.unwrap();
        assert_eq!(plan.persons.len(), 1);
        assert_eq!(plan.persons[0].key, "persons/a.ndjson.gz");
    }
}
