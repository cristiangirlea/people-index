//! The [`Executor`] trait and the [`StageReport`] it returns.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::commit::{CommitRecord, sum_counters};
use crate::context::RunContext;
use crate::stage::Stage;
use crate::stage_name::StageName;

/// Runs every task of a stage to a commit, or fails the stage.
#[async_trait]
pub trait Executor: Send + Sync {
    /// Runs the stage. Tasks already committed are skipped, so calling it again on a finished
    /// stage returns the same report without running anything.
    async fn run_stage(
        &self,
        stage: Arc<dyn Stage>,
        ctx: &RunContext,
    ) -> anyhow::Result<StageReport>;
}

/// What a stage committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageReport {
    /// The stage.
    pub stage: StageName,
    /// One commit per task, sorted by task id, including tasks committed before this call.
    pub commits: Vec<CommitRecord>,
    /// Tasks committed during this call (by this process or, distributed, by the workers).
    pub ran: usize,
    /// Tasks that already had a commit when the call started.
    pub skipped: usize,
    /// Wall time of the call.
    pub duration: Duration,
}

impl StageReport {
    /// Number of tasks in the stage.
    #[must_use]
    pub fn tasks(&self) -> usize {
        self.commits.len()
    }

    /// The counters of every task, summed.
    #[must_use]
    pub fn counters(&self) -> BTreeMap<String, u64> {
        sum_counters(&self.commits)
    }

    /// One summed counter, zero when absent.
    #[must_use]
    pub fn counter(&self, name: &str) -> u64 {
        self.commits.iter().map(|c| c.report.counter(name)).sum()
    }

    /// Bytes read by every task.
    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        self.commits.iter().map(|c| c.report.bytes_in).sum()
    }

    /// Bytes written by every task.
    #[must_use]
    pub fn bytes_out(&self) -> u64 {
        self.commits.iter().map(|c| c.report.bytes_out).sum()
    }

    /// Every committed output key, in task order.
    pub fn outputs(&self) -> impl Iterator<Item = &str> {
        self.commits
            .iter()
            .flat_map(|c| c.report.outputs.iter().map(String::as_str))
    }
}
