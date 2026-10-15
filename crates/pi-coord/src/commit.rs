//! [`CommitLog`]: the record that a task finished, written once with create-if-absent.
//!
//! Readers enumerate commits, never data prefixes, so a crashed or duplicate attempt's files
//! stay invisible and the first commit wins.

use std::collections::{BTreeMap, BTreeSet};

use futures::{StreamExt, TryStreamExt};
use pi_store::{CreateOutcome, RunLayout, Store, StoreError, StoreResult};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::stage_name::StageName;
use crate::task::{AttemptId, TaskReport};

/// How many commit objects are fetched at once when a stage's commits are listed.
const READ_CONCURRENCY: usize = 32;

/// The content of `_commit/<task>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    /// The stage.
    pub stage: StageName,
    /// The task.
    pub task_id: String,
    /// The attempt whose outputs are the committed ones.
    pub attempt: AttemptId,
    /// The worker that ran the attempt.
    pub worker: String,
    /// When the commit was written, by the worker's clock.
    #[serde(with = "time::serde::rfc3339")]
    pub committed_at: OffsetDateTime,
    /// The outputs and counters.
    pub report: TaskReport,
}

/// The result of trying to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// This attempt's commit is the one that counts.
    Committed,
    /// Another attempt committed first; this is its record. This attempt's outputs are
    /// orphans.
    AlreadyCommitted(Box<CommitRecord>),
}

/// Reads and writes the commits of one run.
#[derive(Debug, Clone)]
pub struct CommitLog {
    store: Store,
    layout: RunLayout,
}

impl CommitLog {
    /// The commit log of the run `layout` describes.
    #[must_use]
    pub fn new(store: Store, layout: RunLayout) -> Self {
        Self { store, layout }
    }

    /// Commits a task: creates its commit object unless one exists.
    pub async fn commit(&self, record: &CommitRecord) -> StoreResult<CommitOutcome> {
        let key = self.layout.commit(record.stage.as_str(), &record.task_id)?;
        match self.store.put_json_create_if_absent(&key, record).await? {
            CreateOutcome::Created(_) => Ok(CommitOutcome::Committed),
            CreateOutcome::AlreadyExists => match self.get(record.stage, &record.task_id).await? {
                Some(existing) => Ok(CommitOutcome::AlreadyCommitted(Box::new(existing))),
                // Commits are never deleted while a run is live; a vanished one means someone
                // removed the run under us, which must not pass for success.
                None => Err(StoreError::NotFound { key }),
            },
        }
    }

    /// The commit of a task, if it exists.
    pub async fn get(&self, stage: StageName, task_id: &str) -> StoreResult<Option<CommitRecord>> {
        let key = self.layout.commit(stage.as_str(), task_id)?;
        Ok(self.store.get_json(&key).await?.map(|(record, _)| record))
    }

    /// True when the task has a commit. A head request: cheaper than reading the record.
    pub async fn is_committed(&self, stage: StageName, task_id: &str) -> StoreResult<bool> {
        let key = self.layout.commit(stage.as_str(), task_id)?;
        Ok(self.store.head(&key).await?.is_some())
    }

    /// The ids of every committed task of a stage, from one listing.
    pub async fn committed_task_ids(&self, stage: StageName) -> StoreResult<BTreeSet<String>> {
        let dir = self.layout.commits_dir(stage.as_str())?;
        Ok(self
            .store
            .list_keys(&dir)
            .await?
            .iter()
            .filter_map(|key| RunLayout::task_id_from_key(key).map(str::to_owned))
            .collect())
    }

    /// Every commit record of a stage, sorted by task id.
    pub async fn list(&self, stage: StageName) -> StoreResult<Vec<CommitRecord>> {
        let ids = self.committed_task_ids(stage).await?;
        let mut records: Vec<CommitRecord> = futures::stream::iter(ids)
            .map(|id| async move {
                let key = self.layout.commit(stage.as_str(), &id)?;
                self.get(stage, &id)
                    .await?
                    .ok_or(StoreError::NotFound { key })
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        records.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        Ok(records)
    }

    /// Objects under the stage's data directory that no commit lists: the files of crashed,
    /// failed or losing attempts, which `publish` deletes.
    pub async fn orphans(&self, stage: StageName) -> StoreResult<Vec<String>> {
        let committed: BTreeSet<String> = self
            .list(stage)
            .await?
            .into_iter()
            .flat_map(|r| r.report.outputs)
            .collect();
        let dir = self.layout.data_dir(stage.as_str())?;
        Ok(self
            .store
            .list_keys(&dir)
            .await?
            .into_iter()
            .filter(|k| !committed.contains(k))
            .collect())
    }
}

/// Sums the counters of several commits.
#[must_use]
pub fn sum_counters<'a>(
    records: impl IntoIterator<Item = &'a CommitRecord>,
) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for r in records {
        for (k, v) in &r.report.counters {
            *out.entry(k.clone()).or_default() += v;
        }
    }
    out
}
