//! `join`: the organization half of the partitioned hash join.
//!
//! One task per organization partition `p`: the partition's organizations go into a hash map
//! (deduplicated last-wins by `Seq`), then the partition's edges stream past it, each becoming
//! a `JoinedRow` (the organization body, or `None` when the id did not resolve) routed to the
//! person partition `q` of its person. Memory is one organization partition plus the output
//! buffer, never the whole organization table.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use pi_coord::{
    AttemptId, CancellationToken, Cancelled, RunContext, Stage, StageName, TaskReport, TaskSpec,
    permanent,
};
use pi_model::{EdgeRow, JoinedRow, LastWins, OrgRow, Partitioner};
use pi_segment::SegmentConfig;
use pi_store::Dataset;

use crate::channel::{Inbox, Outbox, input_channel, upload_channel};
use crate::commits::{CommitCache, check_malformed, partition_outputs};
use crate::config::StagesConfig;
use crate::counters;
use crate::feeder::feed_objects;
use crate::pipeline::run_pipeline;
use crate::plan_file::{Feed, Plan};
use crate::segments_in::for_each_row;
use crate::segments_out::{PartitionedWriter, SegmentNamer, Written};

/// Segments fetched ahead of the one being decoded.
pub(crate) const PREFETCH: usize = 2;
/// Per-row overhead counted against the buffer budget besides the body.
const ROW_OVERHEAD: usize = 48;

/// The `join` stage: one task per organization partition.
#[derive(Debug, Clone)]
pub struct JoinStage {
    config: Arc<StagesConfig>,
    cache: Arc<CommitCache>,
}

impl JoinStage {
    /// The stage with the given settings, reading earlier commits through `cache`.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>, cache: Arc<CommitCache>) -> Self {
        Self { config, cache }
    }
}

/// The partition a partitioned task owns, or a permanent error.
pub(crate) fn task_partition(task: &TaskSpec, plan: &Plan) -> anyhow::Result<u32> {
    match task.partition {
        Some(p) if p < plan.partitions => Ok(p),
        other => Err(permanent(anyhow::anyhow!(
            "task {} has partition {other:?}; the plan has {} partitions",
            task.task_id,
            plan.partitions
        ))),
    }
}

/// The partitioned stages' task list: one task per partition.
pub(crate) async fn partition_tasks(
    ctx: &RunContext,
    stage: StageName,
) -> anyhow::Result<Vec<TaskSpec>> {
    let plan = Plan::load(ctx).await?;
    Ok((0..plan.partitions)
        .map(|p| TaskSpec::for_partition(stage, p))
        .collect())
}

/// Reads the next object, failing when the input ended early or the feeder failed.
pub(crate) fn next_object(
    inbox: &mut Inbox<(String, Bytes)>,
) -> anyhow::Result<Option<(String, Bytes)>> {
    inbox.next_blocking().map_err(anyhow::Error::new)
}

/// What the blocking side of a join task counted.
#[derive(Debug, Default)]
struct JoinCounts {
    org_rows: u64,
    organizations: u64,
    edges: u64,
    resolved: u64,
    unresolved: u64,
    written: Written,
}

/// The blocking side of one join task.
struct JoinJob {
    partition: u32,
    partitioner: Partitioner,
    org_segments: usize,
    namer: SegmentNamer,
    segment: SegmentConfig,
    buffer_bytes: usize,
    cancel: CancellationToken,
}

impl JoinJob {
    fn run(self, mut inbox: Inbox<(String, Bytes)>, outbox: Outbox) -> anyhow::Result<JoinCounts> {
        let mut counts = JoinCounts::default();
        let mut orgs: LastWins<u64, Vec<u8>> = LastWins::new();
        for _ in 0..self.org_segments {
            let (key, bytes) = next_object(&mut inbox)?
                .ok_or_else(|| anyhow::anyhow!("the input ended before every org segment"))?;
            self.check_cancel()?;
            counts.org_rows += for_each_row::<OrgRow, _>(&key, bytes, |row| {
                self.expect_partition(row.org_id, &key)?;
                orgs.offer(row.org_id, row.seq, row.body);
                Ok(())
            })?;
        }
        counts.organizations = orgs.len() as u64;

        let mut out = PartitionedWriter::<JoinedRow>::new(
            self.partitioner.count(),
            self.buffer_bytes,
            self.namer.clone(),
            self.segment,
            outbox,
        );
        while let Some((key, bytes)) = next_object(&mut inbox)? {
            self.check_cancel()?;
            let (resolved, unresolved) = (&mut counts.resolved, &mut counts.unresolved);
            let edges = for_each_row::<EdgeRow, _>(&key, bytes, |edge| {
                self.expect_partition(edge.org_id, &key)?;
                let body = orgs.get(&edge.org_id).map(|(_, body)| body.clone());
                if body.is_some() {
                    *resolved += 1;
                } else {
                    *unresolved += 1;
                }
                let size = body.as_ref().map_or(0, Vec::len) + ROW_OVERHEAD;
                out.push(
                    self.partitioner.partition(edge.person_id),
                    JoinedRow {
                        person_id: edge.person_id,
                        org_id: edge.org_id,
                        seq: edge.seq,
                        body,
                    },
                    size,
                )
            })?;
            counts.edges += edges;
        }
        counts.written = out.finish()?;
        Ok(counts)
    }

    /// A row in the wrong partition means the extract and join disagree on the partitioner:
    /// the join would silently lose rows, so stop.
    fn expect_partition(&self, org_id: u64, key: &str) -> anyhow::Result<()> {
        let p = self.partitioner.partition(org_id);
        if p == self.partition {
            Ok(())
        } else {
            Err(permanent(anyhow::anyhow!(
                "{key}: organization {org_id} belongs to partition {p}, not {}",
                self.partition
            )))
        }
    }

    fn check_cancel(&self) -> anyhow::Result<()> {
        if self.cancel.is_cancelled() {
            Err(anyhow::Error::new(Cancelled))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Stage for JoinStage {
    fn name(&self) -> StageName {
        StageName::Join
    }

    /// Also the point where the person feed's malformed total is final.
    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        check_malformed(ctx, Feed::Persons, self.config.max_malformed).await?;
        partition_tasks(ctx, StageName::Join).await
    }

    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let plan = Plan::load(ctx).await?;
        let p = task_partition(task, &plan)?;
        let org_commits = self.cache.finished(ctx, StageName::ExtractOrgs).await?;
        let person_commits = self.cache.finished(ctx, StageName::ExtractPersons).await?;
        let org_keys =
            partition_outputs(ctx, &org_commits, StageName::ExtractOrgs, Dataset::Orgs, p)?;
        let edge_keys = partition_outputs(
            ctx,
            &person_commits,
            StageName::ExtractPersons,
            Dataset::Edges,
            p,
        )?;
        let job = JoinJob {
            partition: p,
            partitioner: plan.partitioner()?,
            org_segments: org_keys.len(),
            namer: SegmentNamer::new(
                ctx.layout.clone(),
                StageName::Join,
                Dataset::Joined,
                &task.task_id,
                &attempt.to_string(),
            ),
            segment: self.config.segment,
            buffer_bytes: self.config.buffer_bytes,
            cancel: ctx.cancel.clone(),
        };
        let keys: Vec<String> = org_keys.into_iter().chain(edge_keys).collect();
        let read = Arc::new(AtomicU64::new(0));
        let (tx, inbox) = input_channel(1);
        let (outbox, uploads) = upload_channel(1);
        let (counts, uploaded) = run_pipeline(
            feed_objects(ctx.store.clone(), keys, PREFETCH, tx, Arc::clone(&read)),
            ctx.store.clone(),
            uploads,
            self.config.upload_concurrency,
            move || job.run(inbox, outbox),
        )
        .await
        .with_context(|| format!("joining partition {p}"))?;

        let mut report = TaskReport {
            outputs: uploaded.keys,
            bytes_in: read.load(Ordering::Relaxed),
            bytes_out: uploaded.bytes,
            ..TaskReport::default()
        };
        report.add(counters::ORG_ROWS, counts.org_rows);
        report.add(counters::ORGANIZATIONS, counts.organizations);
        report.add(counters::EDGES, counts.edges);
        report.add(counters::EDGES_RESOLVED, counts.resolved);
        report.add(counters::EDGES_UNRESOLVED, counts.unresolved);
        report.add(counters::JOINED_ROWS, counts.written.records);
        report.add(counters::SEGMENTS, counts.written.segments);
        Ok(report)
    }
}
