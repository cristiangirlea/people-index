//! `assemble`: the person half of the partitioned hash join.
//!
//! One task per person partition `q`: the partition's joined rows are grouped by person
//! (keeping only each person's newest record's rows), then the partition's person segments
//! are read twice: once to find each person's winning record (greatest `Seq`), once to build
//! the winners' documents. Holding the ids rather than the records keeps memory at the joined
//! rows plus one map entry per person.
//!
//! When that is estimated larger than `task_memory_bytes`, the task runs `k` passes keyed by a
//! second hash of the person id ([`pi_model::Passes`]): pass `i` holds the joined rows of the
//! persons of pass `i` and finds and builds only their documents, re-reading the segments each
//! time. Every person belongs to exactly one pass, so the set of documents is the same for any
//! `k`; their order inside the output segments is not.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use pi_coord::{
    AttemptId, CancellationToken, Cancelled, RunContext, Stage, StageName, TaskReport, TaskSpec,
    permanent,
};
use pi_model::{
    DocRow, DocumentBuilder, DocumentStats, JoinedGroups, JoinedRow, LastWins, Partitioner, Passes,
    PersonRow,
};
use pi_segment::SegmentConfig;
use pi_store::Dataset;

use crate::channel::{Inbox, Outbox, input_channel, upload_channel};
use crate::commits::{CommitCache, partition_outputs, partition_size};
use crate::config::StagesConfig;
use crate::counters;
use crate::feeder::feed_objects;
use crate::join::{
    PREFETCH, expect_end, next_object, partition_tasks, repeat_for_passes, task_partition,
};
use crate::passes::{assemble_estimate, plan_passes};
use crate::pipeline::run_pipeline;
use crate::plan_file::{Feed, Plan};
use crate::quarantine::{Quarantine, QuarantineEntry};
use crate::segments_in::for_each_row;
use crate::segments_out::{RollingUpload, SegmentNamer, Written};

/// The `assemble` stage: one task per person partition.
#[derive(Debug, Clone)]
pub struct AssembleStage {
    config: Arc<StagesConfig>,
    cache: Arc<CommitCache>,
}

impl AssembleStage {
    /// The stage with the given settings, reading earlier commits through `cache`.
    #[must_use]
    pub fn new(config: Arc<StagesConfig>, cache: Arc<CommitCache>) -> Self {
        Self { config, cache }
    }
}

/// What the blocking side of an assemble task counted.
#[derive(Debug, Default)]
struct AssembleCounts {
    joined_rows: u64,
    person_rows: u64,
    persons: u64,
    too_large: u64,
    stats: DocumentStats,
    written: Written,
}

/// The blocking side of one assemble task.
struct AssembleJob {
    partition: u32,
    partitioner: Partitioner,
    joined_segments: usize,
    person_segments: usize,
    passes: Passes,
    builder: DocumentBuilder,
    person_files: Vec<String>,
    namer: SegmentNamer,
    segment: SegmentConfig,
    quarantine_key: String,
    cancel: CancellationToken,
}

impl AssembleJob {
    fn run(
        self,
        mut inbox: Inbox<(String, Bytes)>,
        outbox: &Outbox,
    ) -> anyhow::Result<AssembleCounts> {
        let mut counts = AssembleCounts::default();
        let mut docs = RollingUpload::<DocRow>::new(
            Arc::new(self.namer.clone()),
            self.partition,
            0,
            self.segment,
            outbox.clone(),
        );
        let mut quarantine = Quarantine::default();
        for pass in 0..self.passes.count() {
            self.run_pass(pass, &mut inbox, &mut docs, &mut quarantine, &mut counts)?;
        }
        expect_end(&mut inbox)?;
        counts.too_large = quarantine.len();
        counts.written = docs.finish()?;
        if let Some(bytes) = quarantine.into_bytes() {
            outbox.send_blocking(self.quarantine_key.clone(), bytes)?;
        }
        Ok(counts)
    }

    /// One pass: the joined rows of the persons of `pass`, then their winners, then their
    /// documents.
    fn run_pass(
        &self,
        pass: u32,
        inbox: &mut Inbox<(String, Bytes)>,
        docs: &mut RollingUpload<DocRow>,
        quarantine: &mut Quarantine,
        counts: &mut AssembleCounts,
    ) -> anyhow::Result<()> {
        let mut groups = JoinedGroups::new();
        for _ in 0..self.joined_segments {
            let (key, bytes) = self.next(inbox)?;
            for_each_row::<JoinedRow, _>(&key, bytes, |row| {
                self.expect_partition(row.person_id, &key)?;
                if self.passes.pass_of(row.person_id) == pass {
                    counts.joined_rows += 1;
                    groups.push(row);
                }
                Ok(())
            })?;
        }

        let mut winners: LastWins<u64, ()> = LastWins::new();
        for _ in 0..self.person_segments {
            let (key, bytes) = self.next(inbox)?;
            for_each_row::<PersonRow, _>(&key, bytes, |row| {
                self.expect_partition(row.person_id, &key)?;
                if self.passes.pass_of(row.person_id) == pass {
                    counts.person_rows += 1;
                    winners.offer(row.person_id, row.seq, ());
                }
                Ok(())
            })?;
        }
        counts.persons += winners.len() as u64;

        let mut built: HashSet<u64> = HashSet::with_capacity(winners.len());
        for _ in 0..self.person_segments {
            let (key, bytes) = self.next(inbox)?;
            for_each_row::<PersonRow, _>(&key, bytes, |row| {
                // Persons of other passes are not winners here. The same record twice (it
                // cannot happen with one commit per file, but a duplicate document would
                // double the count) is built once.
                if !winners.is_winner(&row.person_id, row.seq) || !built.insert(row.person_id) {
                    return Ok(());
                }
                let joins = groups.for_person(row.person_id, row.seq);
                match self.builder.build_row(&row, &joins) {
                    Ok(doc) => {
                        counts.stats += doc.stats;
                        docs.push(&doc.doc)
                    }
                    Err(e) if e.is_data_error() => {
                        quarantine.push(&QuarantineEntry::new(
                            Feed::Persons.as_str(),
                            self.person_files
                                .get(row.seq.file as usize)
                                .map_or("?", String::as_str),
                            row.seq.line,
                            "document_too_large",
                            e.to_string(),
                            &row.body,
                        ))?;
                        Ok(())
                    }
                    Err(e) => Err(permanent(anyhow::Error::new(e).context(format!(
                        "building the document of person {} ({key})",
                        row.person_id
                    )))),
                }
            })?;
        }
        Ok(())
    }

    fn next(&self, inbox: &mut Inbox<(String, Bytes)>) -> anyhow::Result<(String, Bytes)> {
        if self.cancel.is_cancelled() {
            return Err(anyhow::Error::new(Cancelled));
        }
        next_object(inbox)?.ok_or_else(|| anyhow::anyhow!("the input ended early"))
    }

    fn expect_partition(&self, person_id: u64, key: &str) -> anyhow::Result<()> {
        let q = self.partitioner.partition(person_id);
        if q == self.partition {
            Ok(())
        } else {
            Err(permanent(anyhow::anyhow!(
                "{key}: person {person_id} belongs to partition {q}, not {}",
                self.partition
            )))
        }
    }
}

#[async_trait]
impl Stage for AssembleStage {
    fn name(&self) -> StageName {
        StageName::Assemble
    }

    async fn tasks(&self, ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        partition_tasks(ctx, StageName::Assemble).await
    }

    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let plan = Plan::load(ctx).await?;
        let q = task_partition(task, &plan)?;
        let join_commits = self.cache.finished(ctx, StageName::Join).await?;
        let person_commits = self.cache.finished(ctx, StageName::ExtractPersons).await?;
        let joined_keys =
            partition_outputs(ctx, &join_commits, StageName::Join, Dataset::Joined, q)?;
        let person_keys = partition_outputs(
            ctx,
            &person_commits,
            StageName::ExtractPersons,
            Dataset::Persons,
            q,
        )?;
        let passes = plan_passes(
            StageName::Assemble,
            q,
            assemble_estimate(
                partition_size(&join_commits, Dataset::Joined, q),
                partition_size(&person_commits, Dataset::Persons, q),
            ),
            self.config.task_memory_bytes,
        );
        let attempt = attempt.to_string();
        let job = AssembleJob {
            partition: q,
            partitioner: plan.partitioner()?,
            joined_segments: joined_keys.len(),
            person_segments: person_keys.len(),
            passes,
            builder: DocumentBuilder::new(self.config.effective_max_document_bytes()),
            person_files: plan.persons.iter().map(|f| f.key.clone()).collect(),
            namer: SegmentNamer::new(
                ctx.layout.clone(),
                StageName::Assemble,
                Dataset::Docs,
                &task.task_id,
                &attempt,
            ),
            segment: self.config.segment,
            quarantine_key: ctx.layout.quarantine(&task.task_id, &attempt)?,
            cancel: ctx.cancel.clone(),
        };
        // Per pass, persons twice: the first read finds the winners, the second builds them.
        let per_pass: Vec<String> = joined_keys
            .into_iter()
            .chain(person_keys.clone())
            .chain(person_keys)
            .collect();
        let keys = repeat_for_passes(&per_pass, passes);
        let read = Arc::new(AtomicU64::new(0));
        let (tx, inbox) = input_channel(1);
        let (outbox, uploads) = upload_channel(1);
        let (counts, uploaded) = run_pipeline(
            feed_objects(ctx.store.clone(), keys, PREFETCH, tx, Arc::clone(&read)),
            ctx.store.clone(),
            uploads,
            self.config.upload_concurrency,
            move || job.run(inbox, &outbox),
        )
        .await
        .with_context(|| format!("assembling partition {q}"))?;

        let mut report = TaskReport {
            outputs: uploaded.keys,
            bytes_in: read.load(Ordering::Relaxed),
            bytes_out: uploaded.bytes,
            ..TaskReport::default()
        };
        let s = &counts.stats;
        report.add(counters::JOINED_ROWS, counts.joined_rows);
        report.add(counters::PERSON_ROWS, counts.person_rows);
        report.add(counters::DUPLICATES, counts.person_rows - counts.persons);
        report.add(counters::DOCUMENTS, counts.written.records);
        report.add(counters::DOCUMENTS_TOO_LARGE, counts.too_large);
        report.add(
            counters::DOCUMENTS_WITH_UNRESOLVED,
            s.documents_with_unresolved,
        );
        report.add("roles", s.roles);
        report.add("roles_null_organization", s.roles_null_organization);
        report.add("roles_resolved", s.roles_resolved);
        report.add("roles_unresolved", s.roles_unresolved);
        report.add("organizations", s.organizations);
        report.add("unresolved_ids", s.unresolved_ids);
        report.add(counters::SEGMENTS, counts.written.segments);
        report.add(counters::PASSES, u64::from(passes.count()));
        report.partitions = counts.written.partitions;
        if counts.too_large > 0 {
            tracing::warn!(
                partition = q,
                documents = counts.too_large,
                "documents over the size limit quarantined"
            );
        }
        Ok(report)
    }
}
