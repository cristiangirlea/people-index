//! [`RunFailure`]: the content of the run's failure marker (`runs/<id>/failed.json`).
//!
//! Whoever fails the run first (a worker whose task failed for good, or the coordinator)
//! creates it with create-if-absent; everyone else sees it and stops. It is the one channel a
//! worker has to tell the coordinator something went wrong.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::stage_name::StageName;

/// Why and where a run failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailure {
    /// The stage that failed, when known.
    pub stage: Option<StageName>,
    /// The task that failed, when it was one task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// The worker (or coordinator) that recorded the failure.
    pub worker: String,
    /// The error chain, formatted.
    pub error: String,
    /// When it was recorded.
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: OffsetDateTime,
}

impl RunFailure {
    /// A failure recorded now.
    #[must_use]
    pub fn new(
        stage: Option<StageName>,
        task_id: Option<String>,
        worker: impl Into<String>,
        error: &anyhow::Error,
    ) -> Self {
        Self {
            stage,
            task_id,
            worker: worker.into(),
            error: format!("{error:#}"),
            failed_at: OffsetDateTime::now_utc(),
        }
    }
}
