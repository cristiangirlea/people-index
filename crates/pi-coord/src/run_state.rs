//! [`RunState`]: the coordinator's record of a run in `runs/<id>/state.json`.
//!
//! Only the coordinator writes it (plain overwrites: one writer). Workers read it to find the
//! current stage and to know when to exit.

use pi_store::{RunLayout, Store, StoreResult};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::stage_name::StageName;

/// Where a run is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// Stages are running.
    Running,
    /// Every stage committed; the alias points at the run's index.
    Published,
    /// A stage failed; the alias was not touched. Running the same run id again resumes.
    Failed,
}

/// The content of `state.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunState {
    /// The run.
    pub run_id: String,
    /// Where it is.
    pub status: RunStatus,
    /// The stage running now (or the last one that ran).
    pub stage: Option<StageName>,
    /// Stages that committed every task, in run order. A resumed run skips them.
    #[serde(default)]
    pub completed: Vec<StageName>,
    /// The error that failed the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When the run was first started.
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    /// When this state was written.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl RunState {
    /// A new running run with nothing done.
    #[must_use]
    pub fn new(run_id: impl Into<String>) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            run_id: run_id.into(),
            status: RunStatus::Running,
            stage: None,
            completed: Vec::new(),
            error: None,
            started_at: now,
            updated_at: now,
        }
    }

    /// Reads the state of a run, if it was ever started.
    pub async fn load(store: &Store, layout: &RunLayout) -> StoreResult<Option<Self>> {
        Ok(store.get_json(&layout.state()).await?.map(|(s, _)| s))
    }

    /// Writes the state, stamping `updated_at`.
    pub async fn save(&mut self, store: &Store, layout: &RunLayout) -> StoreResult<()> {
        self.updated_at = OffsetDateTime::now_utc();
        store.put_json(&layout.state(), self).await.map(|_| ())
    }

    /// True once the run is published or failed: workers exit.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(self.status, RunStatus::Published | RunStatus::Failed)
    }

    /// True when the stage committed every task in an earlier call.
    #[must_use]
    pub fn is_completed(&self, stage: StageName) -> bool {
        self.completed.contains(&stage)
    }
}

/// The newest run in the store: run ids start with their UTC start time, so the greatest id
/// is the newest. `None` when there are no runs.
pub async fn latest_run_id(store: &Store) -> StoreResult<Option<String>> {
    Ok(store
        .list_prefixes(RunLayout::runs_root())
        .await?
        .iter()
        .filter_map(|dir| RunLayout::run_id_from_dir(dir).map(str::to_owned))
        .max())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_and_latest() {
        let store = Store::memory();
        assert_eq!(latest_run_id(&store).await.unwrap(), None);
        for id in [
            "20261015T100000Z-aa",
            "20261015T120000Z-bb",
            "20261014T230000Z-cc",
        ] {
            let layout = RunLayout::new(id).unwrap();
            let mut s = RunState::new(id);
            s.stage = Some(StageName::Join);
            s.completed.push(StageName::Plan);
            s.save(&store, &layout).await.unwrap();
            let back = RunState::load(&store, &layout).await.unwrap().unwrap();
            assert_eq!(back, s);
            assert!(back.is_completed(StageName::Plan));
            assert!(!back.is_finished());
        }
        assert_eq!(
            latest_run_id(&store).await.unwrap().as_deref(),
            Some("20261015T120000Z-bb")
        );
        let v = serde_json::to_value(RunState::new("r")).unwrap();
        assert_eq!(v["status"], "running");
    }
}
