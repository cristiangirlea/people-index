//! Writing rows as segments: [`RollingUpload`] for one partition, [`PartitionedWriter`] for
//! many. Both run on the blocking side and hand finished segments to the uploader.

use std::sync::Arc;

use bytes::Bytes;
use pi_coord::StageName;
use pi_segment::{MemorySink, SegmentConfig, SegmentWriter};
use pi_store::{Dataset, RunLayout};
use serde::Serialize;

use crate::channel::Outbox;

/// Builds the keys of one task attempt's segments of one dataset.
#[derive(Debug, Clone)]
pub(crate) struct SegmentNamer {
    layout: RunLayout,
    stage: StageName,
    dataset: Dataset,
    task: String,
    attempt: String,
}

impl SegmentNamer {
    pub(crate) fn new(
        layout: RunLayout,
        stage: StageName,
        dataset: Dataset,
        task: &str,
        attempt: &str,
    ) -> Self {
        Self {
            layout,
            stage,
            dataset,
            task: task.to_owned(),
            attempt: attempt.to_owned(),
        }
    }

    /// The key of segment `n` of `partition`.
    pub(crate) fn key(&self, partition: u32, n: u32) -> anyhow::Result<String> {
        Ok(self.layout.segment(
            self.stage.as_str(),
            self.dataset,
            Some(partition),
            &self.task,
            &self.attempt,
            n,
        )?)
    }
}

/// What a writer wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Written {
    pub(crate) segments: u64,
    pub(crate) records: u64,
    /// Encoded row bytes before compression.
    pub(crate) raw_bytes: u64,
    /// Segment bytes.
    pub(crate) bytes: u64,
}

impl Written {
    pub(crate) fn add(&mut self, other: Written) {
        self.segments += other.segments;
        self.records += other.records;
        self.raw_bytes += other.raw_bytes;
        self.bytes += other.bytes;
    }
}

/// Rows of one partition into rolling segments, each uploaded as soon as it is finished.
pub(crate) struct RollingUpload<T> {
    writer: SegmentWriter<T, MemorySink>,
    namer: Arc<SegmentNamer>,
    partition: u32,
    first_segment: u32,
    outbox: Outbox,
}

impl<T: Serialize> RollingUpload<T> {
    /// A writer whose first segment is number `first_segment` of the partition.
    pub(crate) fn new(
        namer: Arc<SegmentNamer>,
        partition: u32,
        first_segment: u32,
        config: SegmentConfig,
        outbox: Outbox,
    ) -> Self {
        Self {
            writer: SegmentWriter::new(MemorySink::new(), config),
            namer,
            partition,
            first_segment,
            outbox,
        }
    }

    /// Appends a row, uploading the segment it completes.
    pub(crate) fn push(&mut self, row: &T) -> anyhow::Result<()> {
        if let Some(done) = self.writer.push(row)? {
            self.send(done.index, done.bytes)?;
        }
        Ok(())
    }

    /// Finishes and uploads the last segment.
    pub(crate) fn finish(self) -> anyhow::Result<Written> {
        let Self {
            writer,
            namer,
            partition,
            first_segment,
            outbox,
        } = self;
        let (last, totals, _) = writer.finish()?;
        if let Some(done) = last {
            let key = namer.key(partition, first_segment + done.index)?;
            outbox.send_blocking(key, done.bytes)?;
        }
        Ok(Written {
            segments: u64::from(totals.segments),
            records: totals.records,
            raw_bytes: totals.raw_bytes,
            bytes: totals.bytes,
        })
    }

    fn send(&self, index: u32, bytes: Bytes) -> anyhow::Result<()> {
        let key = self.namer.key(self.partition, self.first_segment + index)?;
        self.outbox.send_blocking(key, bytes)
    }
}

/// Rows routed to many partitions, buffered under one byte budget.
///
/// Keeping one open segment per partition would cost a zstd stream and a segment buffer per
/// partition, so memory would grow with the partition count. Instead rows wait in plain
/// buffers; when the buffered total passes the budget, the largest partition is encoded into
/// segments and uploaded. Memory stays near the budget whatever the partition count, and the
/// largest partitions get the largest (fewest) segments.
pub(crate) struct PartitionedWriter<T> {
    buffers: Vec<Vec<T>>,
    sizes: Vec<usize>,
    next_segment: Vec<u32>,
    total: usize,
    budget: usize,
    namer: Arc<SegmentNamer>,
    config: SegmentConfig,
    outbox: Outbox,
    written: Written,
}

impl<T: Serialize> PartitionedWriter<T> {
    pub(crate) fn new(
        partitions: u32,
        budget: usize,
        namer: SegmentNamer,
        config: SegmentConfig,
        outbox: Outbox,
    ) -> Self {
        let n = partitions as usize;
        Self {
            buffers: (0..n).map(|_| Vec::new()).collect(),
            sizes: vec![0; n],
            next_segment: vec![0; n],
            total: 0,
            budget,
            namer: Arc::new(namer),
            config,
            outbox,
            written: Written::default(),
        }
    }

    /// Buffers a row of about `size` bytes for `partition`, writing out the largest
    /// partitions while the budget is exceeded.
    pub(crate) fn push(&mut self, partition: u32, row: T, size: usize) -> anyhow::Result<()> {
        let p = partition as usize;
        anyhow::ensure!(
            p < self.buffers.len(),
            "partition {partition} out of range ({} partitions)",
            self.buffers.len()
        );
        self.buffers[p].push(row);
        self.sizes[p] += size;
        self.total += size;
        while self.total > self.budget {
            let largest = self
                .sizes
                .iter()
                .enumerate()
                .max_by_key(|(_, s)| **s)
                .map_or(p, |(i, _)| i);
            self.flush(largest)?;
        }
        Ok(())
    }

    /// Writes out every partition still buffered and returns the totals.
    pub(crate) fn finish(mut self) -> anyhow::Result<Written> {
        for p in 0..self.buffers.len() {
            self.flush(p)?;
        }
        Ok(self.written)
    }

    fn flush(&mut self, p: usize) -> anyhow::Result<()> {
        let rows = std::mem::take(&mut self.buffers[p]);
        self.total -= std::mem::take(&mut self.sizes[p]);
        if rows.is_empty() {
            return Ok(());
        }
        // `p` indexes a vector built from a u32 count, so it fits in u32.
        let partition = u32::try_from(p)?;
        let mut writer = RollingUpload::new(
            Arc::clone(&self.namer),
            partition,
            self.next_segment[p],
            self.config,
            self.outbox.clone(),
        );
        for row in &rows {
            writer.push(row)?;
        }
        let written = writer.finish()?;
        self.next_segment[p] += u32::try_from(written.segments)?;
        self.written.add(written);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pi_segment::decode_segment;

    use super::*;
    use crate::channel::upload_channel;

    fn namer() -> SegmentNamer {
        SegmentNamer::new(
            RunLayout::new("r1").unwrap(),
            StageName::Join,
            Dataset::Joined,
            "part-00001",
            "a1",
        )
    }

    #[tokio::test]
    async fn partitions_are_written_under_the_budget() {
        let (outbox, mut rx) = upload_channel(1024);
        let written = tokio::task::spawn_blocking(move || {
            let mut w =
                PartitionedWriter::<u64>::new(4, 100, namer(), SegmentConfig::default(), outbox);
            for i in 0..100u64 {
                w.push(u32::try_from(i % 4).unwrap(), i, 10).unwrap();
            }
            w.finish().unwrap()
        })
        .await
        .unwrap();
        assert_eq!(written.records, 100);
        let mut per_partition = std::collections::BTreeMap::<String, Vec<u64>>::new();
        let mut keys = Vec::new();
        while let Some(up) = rx.recv().await {
            let dir = up.key.rsplit_once('/').unwrap().0.to_owned();
            keys.push(up.key.clone());
            per_partition
                .entry(dir)
                .or_default()
                .extend(decode_segment::<u64>(&up.bytes).unwrap());
        }
        assert_eq!(per_partition.len(), 4);
        for (dir, rows) in &per_partition {
            let p: u64 = dir.rsplit_once("q=").unwrap().1.parse().unwrap();
            assert!(rows.iter().all(|r| r % 4 == p));
            assert_eq!(rows.len(), 25);
        }
        // Several flushes per partition, numbered without gaps or repeats.
        assert!(written.segments > 4);
        let unique: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len());
        assert!(keys.iter().any(|k| k.ends_with("part-00001-a1-1.seg")));
    }

    #[tokio::test]
    async fn rolling_upload_numbers_from_the_first_segment() {
        let (outbox, mut rx) = upload_channel(16);
        tokio::task::spawn_blocking(move || {
            let config = SegmentConfig {
                target_bytes: 1,
                ..SegmentConfig::default()
            };
            let mut w = RollingUpload::<u64>::new(Arc::new(namer()), 2, 5, config, outbox);
            for i in 0..3 {
                w.push(&i).unwrap();
            }
            w.finish().unwrap()
        })
        .await
        .unwrap();
        let mut keys = Vec::new();
        while let Some(up) = rx.recv().await {
            keys.push(up.key);
        }
        assert_eq!(keys.len(), 3);
        assert!(keys[0].ends_with("q=2/part-00001-a1-5.seg"), "{keys:?}");
        assert!(keys[2].ends_with("-7.seg"));
    }
}
