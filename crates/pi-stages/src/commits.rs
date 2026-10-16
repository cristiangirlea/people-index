//! What stages read from earlier stages: committed outputs only, never data prefixes.
//!
//! [`CommitCache`] keeps the commit list of a finished stage in memory. A stage only starts
//! once the previous one committed every task, and commits are never rewritten, so the list
//! of an earlier stage cannot change while later stages run; caching it saves every task of a
//! partitioned stage from re-reading one commit object per task of the stage before.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use pi_coord::{CommitLog, CommitRecord, PartitionSize, RunContext, StageName, permanent};
use pi_store::Dataset;

use crate::plan_file::Feed;
use crate::segments_out::partition_name;

/// Cached commit lists by (run id, stage).
type Lists = HashMap<(String, StageName), Arc<Vec<CommitRecord>>>;

/// Commit lists of finished stages, per run.
#[derive(Debug, Default)]
pub struct CommitCache {
    lists: Mutex<Lists>,
}

impl CommitCache {
    /// An empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The commits of `stage`, which must be a finished stage of `ctx`'s run.
    pub async fn finished(
        &self,
        ctx: &RunContext,
        stage: StageName,
    ) -> anyhow::Result<Arc<Vec<CommitRecord>>> {
        let key = (ctx.run_id.clone(), stage);
        if let Some(list) = self.lock().get(&key) {
            return Ok(Arc::clone(list));
        }
        let list = Arc::new(
            CommitLog::new(ctx.store.clone(), ctx.layout.clone())
                .list(stage)
                .await?,
        );
        self.lock().insert(key, Arc::clone(&list));
        Ok(list)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Lists> {
        // The map is only inserted into; a panic elsewhere cannot leave it inconsistent.
        self.lists.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The committed outputs of `stage` in partition `partition` of `dataset`, sorted.
pub(crate) fn partition_outputs(
    ctx: &RunContext,
    commits: &[CommitRecord],
    stage: StageName,
    dataset: Dataset,
    partition: u32,
) -> anyhow::Result<Vec<String>> {
    let dir = format!(
        "{}/",
        ctx.layout
            .partition_dir(stage.as_str(), dataset, partition)?
    );
    let mut keys: Vec<String> = commits
        .iter()
        .flat_map(|c| c.report.outputs.iter())
        .filter(|k| k.starts_with(&dir))
        .cloned()
        .collect();
    keys.sort();
    Ok(keys)
}

/// What the commits wrote to partition `partition` of `dataset`, summed over the tasks. Zero
/// for commits that recorded no sizes.
pub(crate) fn partition_size(
    commits: &[CommitRecord],
    dataset: Dataset,
    partition: u32,
) -> PartitionSize {
    let name = partition_name(dataset, partition);
    let mut total = PartitionSize::default();
    for size in commits
        .iter()
        .filter_map(|c| c.report.partitions.get(&name))
    {
        total.add(*size);
    }
    total
}

/// Fails the run when the extract stage of `feed` committed more than `max` malformed lines
/// in total. Counted from commits, so every participant sees the same number.
pub(crate) async fn check_malformed(ctx: &RunContext, feed: Feed, max: u64) -> anyhow::Result<u64> {
    let stage = extract_stage(feed);
    let total: u64 = CommitLog::new(ctx.store.clone(), ctx.layout.clone())
        .list(stage)
        .await?
        .iter()
        .map(|c| c.report.counter(crate::counters::MALFORMED))
        .sum();
    if total > max {
        return Err(permanent(anyhow::anyhow!(
            "{total} malformed {} lines, more than the {max} allowed (see {})",
            feed.as_str(),
            ctx.layout.quarantine_dir()
        )));
    }
    Ok(total)
}

/// The extract stage of a feed.
#[must_use]
pub(crate) fn extract_stage(feed: Feed) -> StageName {
    match feed {
        Feed::Organizations => StageName::ExtractOrgs,
        Feed::Persons => StageName::ExtractPersons,
    }
}

#[cfg(test)]
mod tests {
    use pi_coord::{AttemptId, TaskReport};
    use pi_store::Store;
    use time::OffsetDateTime;

    use super::*;

    fn commit(stage: StageName, task: &str, outputs: &[&str], malformed: u64) -> CommitRecord {
        let mut report = TaskReport {
            outputs: outputs.iter().map(|s| (*s).to_owned()).collect(),
            ..TaskReport::default()
        };
        report.add(crate::counters::MALFORMED, malformed);
        CommitRecord {
            stage,
            task_id: task.into(),
            attempt: AttemptId::new(),
            worker: "w".into(),
            committed_at: OffsetDateTime::now_utc(),
            report,
        }
    }

    #[tokio::test]
    async fn outputs_are_filtered_by_partition() {
        let ctx = RunContext::new("r1", Store::memory()).unwrap();
        let p1 = "runs/r1/extract-orgs/data/orgs/p=1/file-00000-a-0.seg";
        let p10 = "runs/r1/extract-orgs/data/orgs/p=10/file-00000-a-0.seg";
        let c = commit(StageName::ExtractOrgs, "file-00000", &[p10, p1], 0);
        let got = partition_outputs(&ctx, &[c], StageName::ExtractOrgs, Dataset::Orgs, 1).unwrap();
        assert_eq!(got, [p1]);
    }

    #[test]
    fn partition_sizes_are_summed_over_commits() {
        let mut a = commit(StageName::ExtractOrgs, "file-00000", &[], 0);
        let mut b = commit(StageName::ExtractOrgs, "file-00001", &[], 0);
        let size = |rows, raw_bytes| PartitionSize { rows, raw_bytes };
        a.report.partitions.insert("orgs/p=1".into(), size(2, 100));
        a.report.partitions.insert("orgs/p=10".into(), size(7, 700));
        b.report.partitions.insert("orgs/p=1".into(), size(3, 50));
        b.report.partitions.insert("edges/p=1".into(), size(9, 900));
        let commits = [a, b];
        assert_eq!(partition_size(&commits, Dataset::Orgs, 1), size(5, 150));
        assert_eq!(partition_size(&commits, Dataset::Orgs, 10), size(7, 700));
        assert_eq!(partition_size(&commits, Dataset::Edges, 1), size(9, 900));
        assert_eq!(partition_size(&commits, Dataset::Orgs, 2), size(0, 0));
    }

    #[tokio::test]
    async fn malformed_lines_are_summed_from_commits() {
        let ctx = RunContext::new("r1", Store::memory()).unwrap();
        let log = CommitLog::new(ctx.store.clone(), ctx.layout.clone());
        for (task, n) in [("file-00000", 3), ("file-00001", 4)] {
            log.commit(&commit(StageName::ExtractPersons, task, &[], n))
                .await
                .unwrap();
        }
        assert_eq!(check_malformed(&ctx, Feed::Persons, 7).await.unwrap(), 7);
        let err = check_malformed(&ctx, Feed::Persons, 6).await.unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        assert_eq!(
            check_malformed(&ctx, Feed::Organizations, 0).await.unwrap(),
            0
        );

        let cache = CommitCache::new();
        assert_eq!(
            cache
                .finished(&ctx, StageName::ExtractPersons)
                .await
                .unwrap()
                .len(),
            2
        );
        // A later commit is not seen: the cache is for finished stages only.
        log.commit(&commit(StageName::ExtractPersons, "file-00002", &[], 0))
            .await
            .unwrap();
        assert_eq!(
            cache
                .finished(&ctx, StageName::ExtractPersons)
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
