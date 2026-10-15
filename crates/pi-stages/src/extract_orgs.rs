//! `extract-orgs`: one organization file into `orgs/p=<p>/` segments of `OrgRow`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use pi_coord::{AttemptId, RunContext, Stage, StageName, TaskReport, TaskSpec};
use pi_model::{MalformedRecord, OrgRow, Partitioner, Seq, parse_org_line};
use pi_store::Dataset;

use crate::channel::Outbox;
use crate::config::StagesConfig;
use crate::extract::{LineSink, SinkContext, extract_tasks, run_extract};
use crate::plan_file::Feed;
use crate::segments_out::{PartitionedWriter, SegmentNamer, Written};

/// Per-row overhead counted against the buffer budget besides the body.
const ROW_OVERHEAD: usize = 48;

/// The `extract-orgs` stage: one task per organization file.
#[derive(Debug, Clone)]
pub struct ExtractOrgsStage {
    config: Arc<StagesConfig>,
}

impl ExtractOrgsStage {
    /// The stage with the given settings.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>) -> Self {
        Self { config }
    }
}

struct OrgSink {
    partitioner: Partitioner,
    orgs: PartitionedWriter<OrgRow>,
}

impl OrgSink {
    fn create(ctx: &SinkContext, outbox: Outbox) -> Box<dyn LineSink> {
        let namer = SegmentNamer::new(
            ctx.layout.clone(),
            ctx.stage,
            Dataset::Orgs,
            &ctx.task_id,
            &ctx.attempt,
        );
        Box::new(Self {
            partitioner: ctx.partitioner,
            orgs: PartitionedWriter::new(
                ctx.partitioner.count(),
                ctx.buffer_bytes,
                namer,
                ctx.segment,
                outbox,
            ),
        })
    }
}

impl LineSink for OrgSink {
    fn handle(&mut self, line: &[u8], seq: Seq) -> anyhow::Result<Result<(), MalformedRecord>> {
        let row = match parse_org_line(line, seq) {
            Ok(row) => row,
            Err(m) => return Ok(Err(m)),
        };
        let size = row.body.len() + ROW_OVERHEAD;
        self.orgs
            .push(self.partitioner.partition(row.org_id), row, size)?;
        Ok(Ok(()))
    }

    fn finish(self: Box<Self>) -> anyhow::Result<(Written, BTreeMap<&'static str, u64>)> {
        Ok((self.orgs.finish()?, BTreeMap::new()))
    }
}

#[async_trait]
impl Stage for ExtractOrgsStage {
    fn name(&self) -> StageName {
        StageName::ExtractOrgs
    }

    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        extract_tasks(ctx, StageName::ExtractOrgs, Feed::Organizations).await
    }

    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        run_extract(
            &self.config,
            ctx,
            StageName::ExtractOrgs,
            Feed::Organizations,
            task,
            attempt,
            &OrgSink::create,
        )
        .await
    }
}
