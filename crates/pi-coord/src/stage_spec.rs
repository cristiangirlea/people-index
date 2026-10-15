//! [`StageSpec`]: a stage's task list in `runs/<id>/stages/<stage>.json`.
//!
//! The coordinator computes the task list once and writes it; workers read it instead of
//! recomputing it, so they need no access to the plan logic and always agree on task ids.

use pi_store::{RunLayout, Store, StoreResult};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::stage_name::StageName;
use crate::task::TaskSpec;

/// Who runs a stage's tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunBy {
    /// The coordinator runs them itself (single-task stages, or the local executor).
    Coordinator,
    /// Workers claim them with leases.
    Workers,
}

/// The content of `stages/<stage>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageSpec {
    /// The stage.
    pub stage: StageName,
    /// Who runs the tasks.
    pub run_by: RunBy,
    /// The tasks, in the order the stage listed them.
    pub tasks: Vec<TaskSpec>,
    /// When the coordinator wrote the list.
    #[serde(with = "time::serde::rfc3339")]
    pub written_at: OffsetDateTime,
}

impl StageSpec {
    /// A spec stamped now.
    #[must_use]
    pub fn new(stage: StageName, run_by: RunBy, tasks: Vec<TaskSpec>) -> Self {
        Self {
            stage,
            run_by,
            tasks,
            written_at: OffsetDateTime::now_utc(),
        }
    }

    /// Reads a stage's task list, if the coordinator wrote one.
    pub async fn load(
        store: &Store,
        layout: &RunLayout,
        stage: StageName,
    ) -> StoreResult<Option<Self>> {
        let key = layout.stage_spec(stage.as_str())?;
        Ok(store.get_json(&key).await?.map(|(s, _)| s))
    }

    /// Writes the task list (an overwrite: the coordinator is the only writer and the list is
    /// deterministic, so a restarted coordinator writes the same content).
    pub async fn save(&self, store: &Store, layout: &RunLayout) -> StoreResult<()> {
        let key = layout.stage_spec(self.stage.as_str())?;
        store.put_json(&key, self).await.map(|_| ())
    }
}
