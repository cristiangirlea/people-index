//! The shared half of `extract-orgs` and `extract-persons`: one gzipped NDJSON file in,
//! partitioned segments and a quarantine out.
//!
//! The file is streamed from the store into a blocking thread that gunzips it (every gzip
//! member, each one's CRC and length checked by reading to the very end), splits it into
//! capped lines, parses each line with pi-model and hands rows to a [`LineSink`]. Lines that
//! do not parse are quarantined with their origin and counted; past the feed's
//! `max_malformed` the task fails for good.

use std::collections::BTreeMap;
use std::io::{self, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use flate2::bufread::MultiGzDecoder;
use pi_coord::{
    AttemptId, CancellationToken, Cancelled, RunContext, StageName, TaskReport, TaskSpec, permanent,
};
use pi_model::{MalformedRecord, Partitioner, Seq};
use pi_segment::SegmentConfig;
use pi_store::RunLayout;

use crate::channel::{ChunkInbox, Outbox, input_channel, upload_channel};
use crate::channel_reader::{ChannelReader, is_input_error};
use crate::config::StagesConfig;
use crate::counters;
use crate::feeder::feed_object_stream;
use crate::lines::LineReader;
use crate::pipeline::run_pipeline;
use crate::plan_file::{Feed, Plan};
use crate::quarantine::{Quarantine, QuarantineEntry};
use crate::segments_out::Written;

/// Chunks of the compressed input buffered between the feeder and the decoder.
const INPUT_CHUNKS: usize = 16;
/// Read buffer of the decoder and of the line splitter.
const READ_BUFFER: usize = 256 << 10;
/// Lines between two checks of the cancellation token.
const CANCEL_CHECK_LINES: u64 = 4096;

/// Where the rows of one extract task go: the feed-specific half.
pub(crate) trait LineSink: Send + 'static {
    /// Parses one line and routes its rows. `Ok(Err(..))` is a malformed line; `Err(..)` is a
    /// failure of the task (writing a segment failed).
    fn handle(&mut self, line: &[u8], seq: Seq) -> anyhow::Result<Result<(), MalformedRecord>>;

    /// Writes out everything buffered; returns the totals and feed-specific counters.
    fn finish(self: Box<Self>) -> anyhow::Result<(Written, BTreeMap<&'static str, u64>)>;
}

/// Builds the sink of one task attempt.
pub(crate) type SinkFactory = dyn Fn(&SinkContext, Outbox) -> Box<dyn LineSink> + Send + Sync;

/// What a sink needs to name and route its segments.
#[derive(Debug, Clone)]
pub(crate) struct SinkContext {
    pub(crate) layout: RunLayout,
    pub(crate) stage: StageName,
    pub(crate) task_id: String,
    pub(crate) attempt: String,
    pub(crate) partitioner: Partitioner,
    pub(crate) segment: SegmentConfig,
    pub(crate) buffer_bytes: usize,
}

/// The tasks of an extract stage: one per input file of the feed, in feed order.
pub(crate) async fn extract_tasks(
    ctx: &RunContext,
    stage: StageName,
    feed: Feed,
) -> anyhow::Result<Vec<TaskSpec>> {
    let plan = Plan::load(ctx).await?;
    Ok(plan
        .files(feed)
        .iter()
        .enumerate()
        .map(|(i, f)| TaskSpec::for_input(stage, i, f.key.clone()))
        .collect())
}

/// Runs one extract task.
pub(crate) async fn run_extract(
    config: &StagesConfig,
    ctx: &RunContext,
    stage: StageName,
    feed: Feed,
    task: &TaskSpec,
    attempt: &AttemptId,
    make_sink: &SinkFactory,
) -> anyhow::Result<TaskReport> {
    let plan = Plan::load(ctx).await?;
    let key = task
        .input
        .clone()
        .ok_or_else(|| permanent(anyhow::anyhow!("task {} names no input file", task.task_id)))?;
    let file_index = plan
        .files(feed)
        .iter()
        .position(|f| f.key == key)
        .ok_or_else(|| {
            permanent(anyhow::anyhow!(
                "{key} is not a {} input of the plan",
                feed.as_str()
            ))
        })?;
    let file_index = u32::try_from(file_index)?;

    // One file past the feed's limit fails at once. The feed's total over every file is
    // checked from the commits when the next stage lists its tasks: reading every other
    // commit here would cost each task one read per file of the feed.
    let malformed_allowed = config.max_malformed;

    let sink_ctx = SinkContext {
        layout: ctx.layout.clone(),
        stage,
        task_id: task.task_id.clone(),
        attempt: attempt.to_string(),
        partitioner: plan.partitioner()?,
        segment: config.segment,
        buffer_bytes: config.buffer_bytes,
    };
    let (feed_tx, inbox) = input_channel(INPUT_CHUNKS);
    let (outbox, uploads) = upload_channel(1);
    let sink = make_sink(&sink_ctx, outbox.clone());
    let read = Arc::new(AtomicU64::new(0));
    let job = Job {
        feed,
        file: key.clone(),
        file_index,
        max_line_bytes: config.max_line_bytes,
        malformed_allowed,
        quarantine_key: ctx.layout.quarantine(&task.task_id, &sink_ctx.attempt)?,
        read: Arc::clone(&read),
        cancel: ctx.cancel.clone(),
    };
    let input_store = config.input_store(&ctx.store).clone();
    let (counts, uploaded) = run_pipeline(
        feed_object_stream(input_store, key.clone(), feed_tx, Arc::clone(&read)),
        ctx.store.clone(),
        uploads,
        config.upload_concurrency,
        move || job.run(inbox, sink, &outbox),
    )
    .await
    .with_context(|| format!("extracting {key}"))?;

    let mut report = TaskReport {
        outputs: uploaded.keys,
        bytes_in: read.load(Ordering::Relaxed),
        bytes_out: uploaded.bytes,
        ..TaskReport::default()
    };
    report.add(counters::LINES, counts.lines);
    report.add(counters::RECORDS, counts.records);
    report.add(counters::MALFORMED, counts.malformed);
    report.add(counters::DECOMPRESSED_BYTES, counts.decompressed);
    report.add(counters::SEGMENTS, counts.written.segments);
    for (name, n) in counts.extra {
        report.add(name, n);
    }
    for (kind, n) in counts.malformed_by_kind {
        report.add(&format!("malformed_{kind}"), n);
    }
    metrics::counter!("pi_malformed_lines_total", "feed" => feed.as_str())
        .increment(counts.malformed);
    if counts.malformed > 0 {
        tracing::warn!(file = %key, malformed = counts.malformed, "malformed lines quarantined");
    }
    Ok(report)
}

/// What the blocking side of one extract task counted.
#[derive(Debug, Default)]
struct Counts {
    lines: u64,
    records: u64,
    malformed: u64,
    malformed_by_kind: BTreeMap<&'static str, u64>,
    decompressed: u64,
    written: Written,
    extra: BTreeMap<&'static str, u64>,
}

/// The blocking side of one extract task.
struct Job {
    feed: Feed,
    file: String,
    file_index: u32,
    max_line_bytes: usize,
    malformed_allowed: u64,
    quarantine_key: String,
    read: Arc<AtomicU64>,
    cancel: CancellationToken,
}

impl Job {
    fn run(
        self,
        inbox: ChunkInbox,
        mut sink: Box<dyn LineSink>,
        outbox: &Outbox,
    ) -> anyhow::Result<Counts> {
        let decoder = MultiGzDecoder::new(BufReader::with_capacity(
            READ_BUFFER,
            ChannelReader::new(inbox),
        ));
        let mut lines = LineReader::new(
            BufReader::with_capacity(READ_BUFFER, decoder),
            self.max_line_bytes,
        );
        let mut quarantine = Quarantine::default();
        let mut counts = Counts::default();
        let mut buf = Vec::new();
        loop {
            let info = match lines.read_line(&mut buf) {
                Ok(Some(info)) => info,
                Ok(None) => break,
                Err(e) => return Err(self.read_error(e)),
            };
            counts.lines += 1;
            if counts.lines % CANCEL_CHECK_LINES == 0 && self.cancel.is_cancelled() {
                return Err(anyhow::Error::new(Cancelled));
            }
            let outcome = if info.too_long {
                Err((
                    "too_long",
                    format!(
                        "line of {} bytes is longer than {}",
                        info.len, self.max_line_bytes
                    ),
                ))
            } else {
                match sink.handle(&buf, Seq::new(self.file_index, info.number))? {
                    Ok(()) => Ok(()),
                    Err(m) => Err((m.kind().as_str(), m.reason().to_owned())),
                }
            };
            match outcome {
                Ok(()) => counts.records += 1,
                Err((kind, reason)) => {
                    counts.malformed += 1;
                    *counts.malformed_by_kind.entry(kind).or_default() += 1;
                    quarantine.push(&QuarantineEntry::new(
                        self.feed.as_str(),
                        &self.file,
                        info.number,
                        kind,
                        reason,
                        &buf,
                    ))?;
                    if counts.malformed > self.malformed_allowed {
                        return Err(permanent(anyhow::anyhow!(
                            "{} has more than the {} malformed {} lines allowed (last at line {}: {kind})",
                            self.file,
                            self.malformed_allowed,
                            self.feed.as_str(),
                            info.number,
                        )));
                    }
                }
            }
        }
        // An empty object decodes to nothing without complaint; it is not a gzip file.
        if self.read.load(Ordering::Relaxed) == 0 {
            return Err(permanent(anyhow::anyhow!(
                "{} is empty, not a gzip file",
                self.file
            )));
        }
        counts.decompressed = lines.bytes();
        let (written, extra) = sink.finish()?;
        counts.written = written;
        counts.extra = extra;
        if let Some(bytes) = quarantine.into_bytes() {
            outbox.send_blocking(self.quarantine_key.clone(), bytes)?;
        }
        Ok(counts)
    }

    /// A failed read is the store's fault (worth a retry) when the feeder failed, and the
    /// file's fault (bad gzip, bad CRC, truncated) otherwise: retrying cannot fix the bytes.
    fn read_error(&self, err: io::Error) -> anyhow::Error {
        if is_input_error(&err) {
            anyhow::Error::new(err).context(format!("reading {}", self.file))
        } else if self.read.load(Ordering::Relaxed) == 0 {
            permanent(anyhow::anyhow!("{} is empty, not a gzip file", self.file))
        } else {
            permanent(
                anyhow::Error::new(err).context(format!("{} is not a valid gzip file", self.file)),
            )
        }
    }
}
