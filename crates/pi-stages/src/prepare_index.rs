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

    /// Idempotent: an index left by an earlier attempt is checked again before `load`
    /// resends every document by `_id`.
    async fn run(
        &self,
        ctx: &RunContext,
        _task: &TaskSpec,
        _attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let plan = Plan::load_for(ctx, &self.config).await?;
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
        let mut spec = IndexSpec::new(shards, self.config.es.codec)
            .with_mapping((*self.config.mapping).clone());
        spec.mapping_source.clone_from(&self.config.mapping_source);
        let outcome = admin
            .create_index(&plan.index, &spec, &ctx.run_id)
            .await
            .map_err(|e| es_error(e, &format!("creating index {}", plan.index)))?;
        admin
            .check_index(&plan.index, &spec, &ctx.run_id)
            .await
            .map_err(|e| es_error(e, &format!("checking index {}", plan.index)))?;
        let created = matches!(outcome, CreateOutcome::Created);
        tracing::info!(index = %plan.index, shards, data_nodes, created, "index ready for loading");
        let mut report = TaskReport::default();
        report.add("data_nodes", u64::from(data_nodes));
        report.add("shards", u64::from(shards));
        report.add("created", u64::from(created));
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_coord::{LeaseConfig, LocalExecutor, RunDriver, StageRegistry, StoreCoordination};
    use pi_es::EsConfig;
    use pi_store::Store;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::OffsetDateTime;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    struct LoadProbe(Arc<AtomicUsize>);

    #[async_trait]
    impl Stage for LoadProbe {
        fn name(&self) -> StageName {
            StageName::Load
        }
        async fn tasks(&self, _: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }
        async fn run(
            &self,
            _: &RunContext,
            _: &TaskSpec,
            _: &AttemptId,
        ) -> anyhow::Result<TaskReport> {
            panic!("load must not run")
        }
    }

    #[tokio::test]
    async fn case_collision_stops_before_load() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_nodes/settings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"nodes": {"n": {"roles": ["data"]}}})),
            )
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/persons-r1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"acknowledged": true, "shards_acknowledged": true})),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/persons-r1"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({"error": {"type": "resource_already_exists_exception"}})),
            )
            .with_priority(2)
            .expect(2)
            .mount(&server)
            .await;
        let mut mapping = pi_es::mapping();
        mapping["_meta"] = json!({"people_index": {"run_id": "R1"}});
        Mock::given(method("GET"))
            .and(path("/persons-r1/_settings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1": {"settings": {"index": {}}}})),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/persons-r1/_mapping"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1": {"mappings": mapping}})),
            )
            .expect(4)
            .mount(&server)
            .await;
        let stage = PrepareIndexStage::new(
            Arc::new(StagesConfig::default()),
            EsClient::new(&EsConfig::new(server.uri())).unwrap(),
        );
        let task = TaskSpec::single(StageName::PrepareIndex);
        for run in ["R1", "r1"] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::local(dir.path()).unwrap();
            let ctx = RunContext::new(run, store.clone()).unwrap();
            let plan = Plan {
                run_id: run.into(),
                partitions: 1,
                alias: "persons".into(),
                index: crate::plan_file::index_name("persons", run),
                organizations: vec![],
                persons: vec![],
                total_bytes: 0,
                schema: None,
                created_at: OffsetDateTime::UNIX_EPOCH,
                delivery: None,
            };
            store
                .put_json_create_if_absent(&ctx.layout.plan(), &plan)
                .await
                .unwrap();
            if run == "R1" {
                stage.run(&ctx, &task, &AttemptId::new()).await.unwrap();
                stage.run(&ctx, &task, &AttemptId::new()).await.unwrap();
            } else {
                let loads = Arc::new(AtomicUsize::new(0));
                let coordination = Arc::new(StoreCoordination::new("test", LeaseConfig::default()));
                let executor = Arc::new(LocalExecutor::new(coordination.clone()));
                let registry = StageRegistry::new()
                    .with(Arc::new(stage.clone()))
                    .with(Arc::new(LoadProbe(loads.clone())));
                let err = RunDriver::new(executor, Arc::new(registry), coordination)
                    .run(&ctx)
                    .await
                    .unwrap_err();
                assert!(pi_coord::is_permanent(&err));
                assert_eq!(loads.load(Ordering::SeqCst), 0);
                let message = format!("{err:#}");
                assert!(
                    message.contains("persons-r1")
                        && message.contains("R1")
                        && message.contains("r1"),
                    "{message}"
                );
            }
        }
    }
    #[tokio::test]
    async fn reused_index_is_checked_again_and_bad_analysis_is_permanent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_nodes/settings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"nodes": {"n": {"roles": ["data"]}}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/persons-r1"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({"error": {"type": "resource_already_exists_exception"}})),
            )
            .mount(&server)
            .await;
        let mut mapping = pi_es::mapping();
        mapping["_meta"] = json!({"people_index": {"run_id": "r1"}});
        Mock::given(method("GET"))
            .and(path("/persons-r1/_mapping"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"persons-r1": {"mappings": mapping}})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET")).and(path("/persons-r1/_settings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"persons-r1": {"settings": {"index": {"analysis": {"analyzer": {"default": {"tokenizer": "keyword"}}}}}}}))).expect(1).mount(&server).await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::local(dir.path()).unwrap();
        let ctx = RunContext::new("r1", store.clone()).unwrap();
        let plan = Plan {
            run_id: "r1".into(),
            partitions: 1,
            alias: "persons".into(),
            index: "persons-r1".into(),
            organizations: vec![],
            persons: vec![],
            total_bytes: 0,
            schema: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            delivery: None,
        };
        store
            .put_json_create_if_absent(&ctx.layout.plan(), &plan)
            .await
            .unwrap();
        let stage = PrepareIndexStage::new(
            Arc::new(StagesConfig::default()),
            EsClient::new(&EsConfig::new(server.uri())).unwrap(),
        );
        let err = stage
            .run(
                &ctx,
                &TaskSpec::single(StageName::PrepareIndex),
                &AttemptId::new(),
            )
            .await
            .unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        assert!(format!("{err:#}").contains("index.analysis.analyzer"));
    }
}
