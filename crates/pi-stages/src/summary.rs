//! `summary.json`: what a published run did, per stage, from its commits.

use std::collections::BTreeMap;

use pi_coord::{CommitRecord, StageName, sum_counters};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::counters;

/// One stage of the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageSummary {
    /// The stage.
    pub stage: StageName,
    /// Its tasks.
    pub tasks: usize,
    /// From the first task's start to the last task's commit.
    pub wall_ms: u64,
    /// The tasks' durations, summed (CPU-ish time; `wall_ms` is what the run waited).
    pub task_ms: u64,
    /// Bytes read.
    pub bytes_in: u64,
    /// Bytes written.
    pub bytes_out: u64,
    /// The tasks' counters, summed.
    pub counters: BTreeMap<String, u64>,
}

impl StageSummary {
    /// The summary of a stage from its commits.
    #[must_use]
    pub fn from_commits(stage: StageName, commits: &[CommitRecord]) -> Self {
        let start = commits
            .iter()
            .map(|c| c.committed_at - c.report.duration)
            .min();
        let end = commits.iter().map(|c| c.committed_at).max();
        let wall_ms = match (start, end) {
            (Some(s), Some(e)) => millis(e - s),
            _ => 0,
        };
        Self {
            stage,
            tasks: commits.len(),
            wall_ms,
            task_ms: commits
                .iter()
                .map(|c| u64::try_from(c.report.duration.as_millis()).unwrap_or(u64::MAX))
                .sum(),
            bytes_in: commits.iter().map(|c| c.report.bytes_in).sum(),
            bytes_out: commits.iter().map(|c| c.report.bytes_out).sum(),
            counters: sum_counters(commits),
        }
    }
}

/// What `publish` did to the cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishSummary {
    /// The documents counted in the index.
    pub docs: u64,
    #[serde(default)]
    pub previous_docs: Option<u64>,
    #[serde(default)]
    pub resolved_roles: u64,
    #[serde(default)]
    pub unresolved_roles: u64,
    #[serde(default)]
    pub indexed_organizations: u64,
    #[serde(default)]
    pub min_share: f64,
    #[serde(default)]
    pub max_unresolved: f64,
    #[serde(default)]
    pub allow_empty_organizations: bool,
    /// The indexes the alias pointed at before.
    pub previous: Vec<String>,
    /// The ones deleted.
    pub deleted: Vec<String>,
    /// The ones that could not be deleted, with why.
    pub not_deleted: Vec<(String, String)>,
}

/// The content of `runs/<id>/summary.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    /// The run.
    pub run_id: String,
    /// The selected input delivery, absent in older summaries.
    #[serde(default)]
    pub delivery: Option<String>,
    /// The published alias.
    pub alias: String,
    /// The index it now points at.
    pub index: String,
    /// The partition count.
    pub partitions: u32,
    /// When the plan was made.
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    /// When the summary was written.
    #[serde(with = "time::serde::rfc3339")]
    pub finished_at: OffsetDateTime,
    /// From the plan to the summary.
    pub wall_ms: u64,
    /// Documents published.
    pub documents: u64,
    /// Documents per second of wall time over the whole run.
    pub persons_per_second: f64,
    /// Input bytes.
    pub input_bytes: u64,
    /// Every stage in run order, `publish` last.
    pub stages: Vec<StageSummary>,
    /// The cluster side of `publish`.
    pub publish: PublishSummary,
    /// Files of losing or failed attempts removed.
    pub orphans_deleted: u64,
    /// Orphans that could not be removed (they are harmless; a later cleanup may retry).
    pub orphans_left: u64,
}

impl Summary {
    /// Documents per second over `wall_ms`, 0 for an instant run.
    #[must_use]
    pub fn rate(documents: u64, wall_ms: u64) -> f64 {
        if wall_ms == 0 {
            return 0.0;
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "a rate for humans; 2^52 documents is far away"
        )]
        let rate = documents as f64 * 1000.0 / wall_ms as f64;
        rate
    }

    /// The published document count: what `assemble` committed.
    #[must_use]
    pub fn documents_of(assemble: &StageSummary) -> u64 {
        assemble
            .counters
            .get(counters::DOCUMENTS)
            .copied()
            .unwrap_or(0)
    }
}

/// Milliseconds of a non-negative duration, 0 for a negative one (clock skew).
#[must_use]
pub(crate) fn millis(d: time::Duration) -> u64 {
    u64::try_from(d.whole_milliseconds()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pi_coord::{AttemptId, TaskReport};

    use super::*;

    fn commit(task: &str, at_ms: i64, took_ms: u64, docs: u64) -> CommitRecord {
        let mut report = TaskReport {
            duration: Duration::from_millis(took_ms),
            bytes_in: 10,
            ..TaskReport::default()
        };
        report.add(counters::DOCUMENTS, docs);
        CommitRecord {
            stage: StageName::Assemble,
            task_id: task.into(),
            attempt: AttemptId::new(),
            worker: "w".into(),
            committed_at: OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(at_ms),
            report,
        }
    }

    #[test]
    fn stage_wall_time_spans_first_start_to_last_commit() {
        let s = StageSummary::from_commits(
            StageName::Assemble,
            &[commit("a", 1000, 500, 3), commit("b", 1800, 1000, 4)],
        );
        // Starts at 500 (1000 - 500), ends at 1800.
        assert_eq!(s.wall_ms, 1300);
        assert_eq!(s.task_ms, 1500);
        assert_eq!(s.bytes_in, 20);
        assert_eq!(Summary::documents_of(&s), 7);
        let empty = StageSummary::from_commits(StageName::Assemble, &[]);
        assert_eq!((empty.wall_ms, empty.tasks), (0, 0));
    }

    #[test]
    fn rates() {
        assert!((Summary::rate(500, 250) - 2000.0).abs() < 1e-9);
        assert!(Summary::rate(5, 0).abs() < f64::EPSILON);
    }
}
