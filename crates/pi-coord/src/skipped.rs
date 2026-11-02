//! [`RunSkipped`]: the content of `runs/<id>/skipped.json`.
//!
//! A coordinator following the newest delivery (`PI_DELIVERY=latest`) that finds the alias
//! already serving that delivery, or a newer one, writes it, under the alias run lease and
//! instead of any run state, and exits. It is final for the run id: a coordinator started
//! again with the same id reads it back, and workers of the run (the scheduled Job's, which
//! learn the id from the Job name) exit as done when they see it.

use pi_store::{CreateOutcome, RunLayout, Store, StoreResult};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Why a run did nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSkipped {
    /// The run.
    pub run_id: String,
    /// Why, for people reading the store.
    pub reason: String,
    /// The newest complete delivery, which the run would have planned: the one the alias
    /// already serves, or an older one than it (`reason` names the served one then).
    pub delivery: String,
    /// The run whose index the alias points at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serving_run: Option<String>,
    /// When the decision was made.
    #[serde(with = "time::serde::rfc3339")]
    pub skipped_at: OffsetDateTime,
}

impl RunSkipped {
    /// A skip decided now.
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        reason: impl Into<String>,
        delivery: impl Into<String>,
        serving_run: Option<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            reason: reason.into(),
            delivery: delivery.into(),
            serving_run,
            skipped_at: OffsetDateTime::now_utc(),
        }
    }

    /// Reads the run's skip record, if it was skipped.
    pub async fn load(store: &Store, layout: &RunLayout) -> StoreResult<Option<Self>> {
        Ok(store.get_json(&layout.skipped()).await?.map(|(s, _)| s))
    }

    /// Writes the record create-if-absent and returns the one in the store: this one, or the
    /// one an earlier attempt wrote first.
    pub async fn record(self, store: &Store, layout: &RunLayout) -> StoreResult<Self> {
        match store
            .put_json_create_if_absent(&layout.skipped(), &self)
            .await?
        {
            CreateOutcome::Created(_) => Ok(self),
            CreateOutcome::AlreadyExists => Ok(Self::load(store, layout).await?.unwrap_or(self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_first_record_wins_and_reads_back() {
        let store = Store::memory();
        let layout = RunLayout::new("r1").unwrap();
        assert_eq!(RunSkipped::load(&store, &layout).await.unwrap(), None);
        let first = RunSkipped::new("r1", "no new delivery", "b2", Some("r0".into()));
        let written = first.clone().record(&store, &layout).await.unwrap();
        assert_eq!(written, first);
        let second = RunSkipped::new("r1", "other", "b3", None);
        assert_eq!(second.record(&store, &layout).await.unwrap(), first);
        let json: serde_json::Value = store.get_json(&layout.skipped()).await.unwrap().unwrap().0;
        assert_eq!(json["delivery"], "b2");
        assert_eq!(json["serving_run"], "r0");
        assert!(json["skipped_at"].is_string());
    }
}
