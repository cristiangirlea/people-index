//! `extract-persons`: one person file into `persons/q=<q>/` segments of `PersonRow` and
//! `edges/p=<p>/` segments of `EdgeRow` (one edge per distinct organization id of a record).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use pi_coord::{AttemptId, RunContext, Stage, StageName, TaskReport, TaskSpec};
use pi_model::{EdgeRow, MalformedRecord, Partitioner, PersonRow, Seq, parse_person_line};
use pi_store::Dataset;

use crate::channel::Outbox;
use crate::commits::check_malformed;
use crate::config::StagesConfig;
use crate::counters;
use crate::extract::{LineSink, SinkContext, extract_tasks, run_extract};
use crate::plan_file::Feed;
use crate::segments_out::{PartitionedWriter, SegmentNamer, Written};

/// Per-row overhead counted against the buffer budget besides the body and ids.
const ROW_OVERHEAD: usize = 64;
/// The budget share of the person rows; edges are small and get the rest.
const PERSON_SHARE_NUM: usize = 3;
const PERSON_SHARE_DEN: usize = 4;

/// The `extract-persons` stage: one task per person file.
#[derive(Debug, Clone)]
pub struct ExtractPersonsStage {
    config: Arc<StagesConfig>,
}

impl ExtractPersonsStage {
    /// The stage with the given settings.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>) -> Self {
        Self { config }
    }
}

struct PersonSink {
    partitioner: Partitioner,
    persons: PartitionedWriter<PersonRow>,
    edges: PartitionedWriter<EdgeRow>,
    edge_count: u64,
}

impl PersonSink {
    fn create(ctx: &SinkContext, outbox: Outbox) -> Box<dyn LineSink> {
        let namer = |dataset| {
            SegmentNamer::new(
                ctx.layout.clone(),
                ctx.stage,
                dataset,
                &ctx.task_id,
                &ctx.attempt,
            )
        };
        let person_budget = ctx.buffer_bytes / PERSON_SHARE_DEN * PERSON_SHARE_NUM;
        let p = ctx.partitioner.count();
        Box::new(Self {
            partitioner: ctx.partitioner,
            persons: PartitionedWriter::new(
                p,
                person_budget,
                namer(Dataset::Persons),
                ctx.segment,
                outbox.clone(),
            ),
            edges: PartitionedWriter::new(
                p,
                ctx.buffer_bytes - person_budget,
                namer(Dataset::Edges),
                ctx.segment,
                outbox,
            ),
            edge_count: 0,
        })
    }
}

impl LineSink for PersonSink {
    fn handle(&mut self, line: &[u8], seq: Seq) -> anyhow::Result<Result<(), MalformedRecord>> {
        let row = match parse_person_line(line, seq) {
            Ok(row) => row,
            Err(m) => return Ok(Err(m)),
        };
        for edge in row.edges() {
            self.edges.push(
                self.partitioner.partition(edge.org_id),
                edge,
                std::mem::size_of::<EdgeRow>(),
            )?;
            self.edge_count += 1;
        }
        let size = row.body.len() + row.org_ids.len() * 8 + ROW_OVERHEAD;
        let q = self.partitioner.partition(row.person_id);
        self.persons.push(q, row, size)?;
        Ok(Ok(()))
    }

    fn finish(self: Box<Self>) -> anyhow::Result<(Written, BTreeMap<&'static str, u64>)> {
        let Self {
            persons,
            edges,
            edge_count,
            ..
        } = *self;
        let mut written = persons.finish()?;
        written.add(edges.finish()?);
        Ok((written, BTreeMap::from([(counters::EDGES, edge_count)])))
    }
}

#[async_trait]
impl Stage for ExtractPersonsStage {
    fn name(&self) -> StageName {
        StageName::ExtractPersons
    }

    /// Also the point where the organization feed's malformed total is final: refuse to go
    /// on when it is past the limit.
    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        check_malformed(ctx, Feed::Organizations, self.config.max_malformed).await?;
        extract_tasks(ctx, StageName::ExtractPersons, Feed::Persons).await
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
            StageName::ExtractPersons,
            Feed::Persons,
            task,
            attempt,
            &PersonSink::create,
        )
        .await
    }
}
