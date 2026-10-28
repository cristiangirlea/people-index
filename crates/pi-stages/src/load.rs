//! `load`: one document partition into the run's index through the bulk indexer.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use pi_coord::{
    AttemptId, CancellationToken, Cancelled, RunContext, Stage, StageName, TaskReport, TaskSpec,
    permanent,
};
use pi_es::{BulkIndexer, BulkStats, EsClient, IndexAdmin};
use pi_model::DocRow;
use pi_store::Dataset;

use crate::channel::{Inbox, input_channel};
use crate::commits::{CommitCache, partition_outputs};
use crate::config::StagesConfig;
use crate::counters;
use crate::doc_channel::{DocSender, doc_channel};
use crate::es_error::es_error;
use crate::feeder::feed_objects;
use crate::join::{PREFETCH, next_object, partition_tasks, task_partition};
use crate::pipeline::run_blocking;
use crate::plan_file::Plan;
use crate::segments_in::for_each_row;

/// The `load` stage: one task per document partition.
#[derive(Debug, Clone)]
pub struct LoadStage {
    config: Arc<StagesConfig>,
    cache: Arc<CommitCache>,
    client: EsClient,
}

impl LoadStage {
    /// The stage with the given settings, reading earlier commits through `cache` and
    /// sending to the cluster through `client`.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>, cache: Arc<CommitCache>, client: EsClient) -> Self {
        Self {
            config,
            cache,
            client,
        }
    }
}

/// Decodes document segments on the blocking side and hands the rows to the async side.
fn decode_documents(
    mut inbox: Inbox<(String, Bytes)>,
    docs: &DocSender,
    cancel: &CancellationToken,
) -> anyhow::Result<u64> {
    let mut n = 0;
    while let Some((key, bytes)) = next_object(&mut inbox)? {
        if cancel.is_cancelled() {
            return Err(anyhow::Error::new(Cancelled));
        }
        n += for_each_row::<DocRow, _>(&key, bytes, |doc| docs.send_blocking(doc))?;
    }
    Ok(n)
}

#[async_trait]
impl Stage for LoadStage {
    fn name(&self) -> StageName {
        StageName::Load
    }

    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        partition_tasks(ctx, StageName::Load).await
    }

    /// Idempotent: every document is indexed by its `_id`, so a retried or duplicated task
    /// overwrites the same documents with the same content.
    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        _attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let plan = Plan::load_for(ctx, &self.config).await?;
        let q = task_partition(task, &plan)?;
        let exists = IndexAdmin::new(self.client.clone())
            .exists(&plan.index)
            .await
            .map_err(|e| es_error(e, &format!("checking index {}", plan.index)))?;
        if !exists {
            return Err(permanent(anyhow::anyhow!(
                "run index {} does not exist; a resume cannot recreate it",
                plan.index
            )));
        }
        let commits = self.cache.finished(ctx, StageName::Assemble).await?;
        let keys = partition_outputs(ctx, &commits, StageName::Assemble, Dataset::Docs, q)?;
        // The assemble task of partition q has the same id as this one.
        let committed_docs: u64 = commits
            .iter()
            .filter(|c| c.task_id == task.task_id)
            .map(|c| c.report.counter(counters::DOCUMENTS))
            .sum();

        let mut indexer = BulkIndexer::new(self.client.clone(), &plan.index, self.config.es.bulk)
            .map_err(|e| es_error(e, "starting the bulk indexer"))?;
        let read = Arc::new(AtomicU64::new(0));
        let (feed_tx, inbox) = input_channel(1);
        // Decoded documents wait for the indexer within a byte budget, not a count.
        let (doc_tx, mut doc_rx) = doc_channel(self.config.effective_load_buffer_bytes());
        let cancel = ctx.cancel.clone();
        let decoding = run_blocking(move || decode_documents(inbox, &doc_tx, &cancel));
        let sending = async move {
            let mut sent = 0u64;
            while let Some(item) = doc_rx.recv().await {
                indexer
                    .add(item.doc.person_id, &item.doc.body)
                    .await
                    .map_err(|e| es_error(e, &format!("indexing into {}", plan.index)))?;
                // The body is in the bulk buffer now: give its share of the budget back.
                drop(item);
                sent += 1;
            }
            let stats = indexer
                .finish()
                .await
                .map_err(|e| es_error(e, &format!("indexing into {}", plan.index)))?;
            Ok::<(u64, BulkStats), anyhow::Error>((sent, stats))
        };
        let (fed, decode_result, send_result) = tokio::join!(
            feed_objects(
                ctx.store.clone(),
                keys,
                PREFETCH,
                feed_tx,
                Arc::clone(&read)
            ),
            decoding,
            sending
        );
        fed.with_context(|| format!("loading partition {q}"))?;
        let (sent, stats) = send_result.with_context(|| format!("loading partition {q}"))?;
        let decoded = decode_result.with_context(|| format!("loading partition {q}"))?;

        if decoded != committed_docs || stats.docs_indexed != decoded || sent != decoded {
            return Err(permanent(anyhow::anyhow!(
                "partition {q}: assemble committed {committed_docs} documents, {decoded} were read, {sent} sent and {} acknowledged",
                stats.docs_indexed
            )));
        }
        metrics::counter!("pi_documents_indexed_total").increment(stats.docs_indexed);
        let mut report = TaskReport {
            bytes_in: read.load(Ordering::Relaxed),
            bytes_out: stats.wire_bytes,
            ..TaskReport::default()
        };
        report.add(counters::DOCUMENTS, decoded);
        report.add(counters::DOCS_INDEXED, stats.docs_indexed);
        report.add("bulk_requests", stats.requests);
        report.add("request_retries", stats.request_retries);
        report.add("items_retried", stats.items_retried);
        report.add("bytes_sent", stats.bytes_sent);
        report.add("wire_bytes", stats.wire_bytes);
        report.add("backpressure_ms", duration_ms(stats.backpressure));
        Ok(report)
    }
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_coord::{CommitLog, CommitRecord};
    use pi_es::EsConfig;
    use pi_segment::{MemorySink, SegmentConfig, SegmentWriter};
    use pi_store::Store;
    use serde_json::json;
    use time::OffsetDateTime;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn fixture(server: &MockServer) -> (RunContext, LoadStage, TaskSpec) {
        let ctx = RunContext::new("r1", Store::memory()).unwrap();
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
        ctx.store.put_json(&ctx.layout.plan(), &plan).await.unwrap();
        let key = format!(
            "{}/test.seg",
            ctx.layout
                .partition_dir("assemble", Dataset::Docs, 0)
                .unwrap()
        );
        let mut writer = SegmentWriter::new(MemorySink::new(), SegmentConfig::default());
        writer
            .push(&DocRow {
                person_id: 1,
                body: br#"{"id":1}"#.to_vec(),
            })
            .unwrap();
        let (segment, _, _) = writer.finish().unwrap();
        ctx.store.put(&key, segment.unwrap().bytes).await.unwrap();
        let mut report = TaskReport {
            outputs: vec![key],
            ..TaskReport::default()
        };
        report.add(counters::DOCUMENTS, 1);
        let task = TaskSpec::for_partition(StageName::Load, 0);
        CommitLog::new(ctx.store.clone(), ctx.layout.clone())
            .commit(&CommitRecord {
                stage: StageName::Assemble,
                task_id: task.task_id.clone(),
                attempt: AttemptId::new(),
                worker: "test".into(),
                committed_at: OffsetDateTime::now_utc(),
                report,
            })
            .await
            .unwrap();
        let client = EsClient::new(&EsConfig::new(server.uri())).unwrap();
        (
            ctx,
            LoadStage::new(
                Arc::new(StagesConfig::default()),
                Arc::new(CommitCache::new()),
                client,
            ),
            task,
        )
    }

    #[tokio::test]
    async fn missing_index_fails_permanently_without_bulk() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/persons-r1"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/persons-r1/_bulk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let (ctx, stage, task) = fixture(&server).await;
        let err = stage.run(&ctx, &task, &AttemptId::new()).await.unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        let message = format!("{err:#}");
        assert!(
            message.contains("persons-r1") && message.contains("resume cannot recreate it"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn index_head_server_error_is_retryable_without_bulk() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/persons-r1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/persons-r1/_bulk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let (ctx, stage, task) = fixture(&server).await;
        let err = stage.run(&ctx, &task, &AttemptId::new()).await.unwrap_err();
        assert!(!pi_coord::is_permanent(&err));
    }

    #[tokio::test]
    async fn existing_index_proceeds_to_bulk() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/persons-r1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/persons-r1/_bulk"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"errors": false, "items": [{"index": {"status": 201}}]})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (ctx, stage, task) = fixture(&server).await;
        let report = stage.run(&ctx, &task, &AttemptId::new()).await.unwrap();
        assert_eq!(report.counter(counters::DOCS_INDEXED), 1);
    }
}
