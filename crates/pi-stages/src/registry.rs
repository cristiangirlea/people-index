//! [`build_registry`]: every stage, built from one [`StagesConfig`].

use std::sync::Arc;

use pi_coord::StageRegistry;
use pi_es::EsClient;

use crate::assemble::AssembleStage;
use crate::commits::CommitCache;
use crate::config::StagesConfig;
use crate::extract_orgs::ExtractOrgsStage;
use crate::extract_persons::ExtractPersonsStage;
use crate::join::JoinStage;
use crate::load::LoadStage;
use crate::metered::Metered;
use crate::plan::PlanStage;
use crate::prepare_index::PrepareIndexStage;
use crate::publish::PublishStage;

/// The registry of all eight stages, each wrapped in [`Metered`]. The Elasticsearch client is
/// built here (no request is sent), so a bad CA file or URL fails at startup.
pub fn build_registry(config: StagesConfig) -> anyhow::Result<StageRegistry> {
    config.validate()?;
    let client = EsClient::new(&config.es.client)
        .map_err(|e| anyhow::anyhow!("Elasticsearch client: {e}"))?;
    let config = Arc::new(config);
    let cache = Arc::new(CommitCache::new());
    Ok(StageRegistry::new()
        .with(Arc::new(Metered(PlanStage::new(Arc::clone(&config)))))
        .with(Arc::new(Metered(ExtractOrgsStage::new(Arc::clone(
            &config,
        )))))
        .with(Arc::new(Metered(ExtractPersonsStage::new(Arc::clone(
            &config,
        )))))
        .with(Arc::new(Metered(JoinStage::new(
            Arc::clone(&config),
            Arc::clone(&cache),
        ))))
        .with(Arc::new(Metered(AssembleStage::new(
            Arc::clone(&config),
            Arc::clone(&cache),
        ))))
        .with(Arc::new(Metered(PrepareIndexStage::new(
            Arc::clone(&config),
            client.clone(),
        ))))
        .with(Arc::new(Metered(LoadStage::new(
            Arc::clone(&config),
            Arc::clone(&cache),
            client.clone(),
        ))))
        .with(Arc::new(Metered(PublishStage::new(config, cache, client)))))
}

#[cfg(test)]
mod tests {
    use pi_coord::{AttemptId, RunContext, StageName, TaskSpec, is_permanent};
    use pi_model::FeedSchema;
    use pi_store::Store;
    use time::OffsetDateTime;

    use super::*;
    use crate::plan_file::{InputFile, Plan, SchemaPin};

    /// Every stage's task, the plan's included, refuses a run planned with another feed schema
    /// or index mapping before it reads or writes anything, and the error is permanent.
    #[tokio::test]
    async fn every_stage_refuses_a_run_planned_with_another_schema_or_mapping() {
        let renamed = FeedSchema::from_json(r#"{"envelope": {"id": "key"}}"#).unwrap();
        let other_schema = SchemaPin::of(&renamed, &pi_es::mapping());
        let other_mapping = SchemaPin {
            mapping_sha256: "0".repeat(64),
            ..SchemaPin::builtin()
        };
        let registry = build_registry(StagesConfig::default()).unwrap();
        let input = "raw/persons/a.ndjson.gz";
        for (pin, setting) in [
            (other_schema, "the feed schema (PI_SCHEMA_FILE)"),
            (other_mapping, "the index mapping (PI_MAPPING_FILE)"),
        ] {
            let store = Store::memory();
            let ctx = RunContext::new("pinned", store.clone()).unwrap();
            let file = InputFile {
                key: input.to_owned(),
                size: 1,
                version: pi_store::ObjectVersion::default(),
            };
            let plan = Plan {
                run_id: "pinned".into(),
                delivery: None,
                partitions: 1,
                alias: "persons".into(),
                index: "persons-pinned".into(),
                organizations: vec![file.clone()],
                persons: vec![file],
                total_bytes: 2,
                schema: Some(pin),
                created_at: OffsetDateTime::UNIX_EPOCH,
            };
            store.put_json(&ctx.layout.plan(), &plan).await.unwrap();
            for (stage, task) in [
                (StageName::Plan, TaskSpec::single(StageName::Plan)),
                (
                    StageName::ExtractOrgs,
                    TaskSpec::for_input(StageName::ExtractOrgs, 0, input),
                ),
                (
                    StageName::ExtractPersons,
                    TaskSpec::for_input(StageName::ExtractPersons, 0, input),
                ),
                (StageName::Join, TaskSpec::for_partition(StageName::Join, 0)),
                (
                    StageName::Assemble,
                    TaskSpec::for_partition(StageName::Assemble, 0),
                ),
                (
                    StageName::PrepareIndex,
                    TaskSpec::single(StageName::PrepareIndex),
                ),
                (StageName::Load, TaskSpec::for_partition(StageName::Load, 0)),
                (StageName::Publish, TaskSpec::single(StageName::Publish)),
            ] {
                let err = registry
                    .get(stage)
                    .unwrap()
                    .run(&ctx, &task, &AttemptId::new())
                    .await
                    .unwrap_err();
                let text = format!("{err:#}");
                assert!(is_permanent(&err), "{stage:?}: {text}");
                assert!(
                    text.contains(
                        "run pinned was planned with another feed schema or index mapping"
                    ),
                    "{stage:?}: {text}"
                );
                assert!(text.contains(setting), "{stage:?}: {text}");
            }
            // Nothing but the plan was written.
            let written: Vec<String> = store
                .list("")
                .await
                .unwrap()
                .into_iter()
                .map(|o| o.key)
                .collect();
            assert_eq!(written, [ctx.layout.plan()]);
        }
    }

    #[test]
    fn every_stage_is_registered_in_order() {
        let registry = build_registry(StagesConfig::default()).unwrap();
        assert_eq!(registry.names(), StageName::ALL);
        let bad = StagesConfig {
            upload_concurrency: 0,
            ..StagesConfig::default()
        };
        assert!(build_registry(bad).is_err());
    }
}
