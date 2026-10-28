//! The `publish` step: turn the loaded index into the serving one, or leave everything as it
//! was. The alias moves only after every check has passed, in one atomic request; nothing
//! before that point changes what readers see.

use std::future::Future;
use std::time::Duration;

use serde::Serialize;

use crate::admin::IndexAdmin;
use crate::error::{EsError, Result};
use crate::health::HealthStatus;
use crate::names::validate_index_name;
use crate::runs::{is_older_run, run_stamp};
use crate::settings::ServingSettings;

/// What to publish and what it must contain.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishSpec {
    /// `PI_ALIAS`.
    pub alias: String,
    /// The run's index, `<alias>-<run_id>`.
    pub index: String,
    /// Documents `assemble` committed: the index must hold exactly this many.
    pub expected_docs: u64,
    pub serving: ServingSettings,
    /// The health the index must reach once replicas are restored, before the alias moves.
    pub wait_for: HealthStatus,
    pub health_timeout: Duration,
    /// Minimum fraction of the previous count; zero disables the check.
    pub min_share: f64,
}

impl PublishSpec {
    /// Defaults: one replica, `1s` refresh, wait up to 30 minutes for green (replica recovery
    /// of a large index copies every segment once), and a minimum previous-count share of 0.9.
    #[must_use]
    pub fn new(alias: impl Into<String>, index: impl Into<String>, expected_docs: u64) -> Self {
        Self {
            alias: alias.into(),
            index: index.into(),
            expected_docs,
            serving: ServingSettings::default(),
            wait_for: HealthStatus::Green,
            health_timeout: Duration::from_secs(30 * 60),
            min_share: 0.9,
        }
    }
}

/// What `publish` did, for `summary.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublishReport {
    pub alias: String,
    pub index: String,
    pub docs: u64,
    /// Count of documents in the indexes replaced, if any.
    pub previous_docs: Option<u64>,
    /// Indices the alias pointed at before (the new index excluded): the ones the caller
    /// recorded before the first swap, and any the alias points at now besides the new index.
    pub previous: Vec<String>,
    /// Indices deleted: the previous ones, then those of older runs that never published.
    pub deleted: Vec<String>,
    /// Indices that could not be deleted, with the reason (`<alias>-*` when listing the older
    /// runs' indices failed). The alias already moved, so the run succeeded; these are left for
    /// the operator.
    pub not_deleted: Vec<(String, String)>,
}

/// Runs the publish step against one cluster.
#[derive(Debug, Clone)]
pub struct Publisher {
    admin: IndexAdmin,
}

impl Publisher {
    #[must_use]
    pub fn new(admin: IndexAdmin) -> Self {
        Self { admin }
    }

    /// The order is chosen so the checks are exact and cheap to fail:
    ///
    /// 1. refresh and flush while the index still has no replicas, so the complete-shard check
    ///    covers every copy there is and replicas later recover from committed segments;
    /// 2. count, and stop on any difference from what `assemble` committed;
    /// 3. restore replicas, refresh interval and request durability, and wait for `wait_for`;
    /// 4. reject a newer serving run, then check the new count against the sum of the
    ///    serving indices' counts when the minimum share is enabled;
    /// 5. move the alias in one `_aliases` request;
    /// 6. delete the indices it served before;
    /// 7. delete the `<alias>-*` indices of runs older than this one that no alias points at
    ///    (runs that failed after `prepare-index`; `runs.rs` has the ordering rule). A
    ///    newer run's index is never touched: it may still be loading.
    ///
    /// Any failure in 1 to 5 returns an error with the alias untouched. Safe to run again: the
    /// checks repeat and moving an alias to where it already points changes nothing. An attempt
    /// interrupted between 5 and 6 forgets the old indices, though; [`Self::publish_recording`]
    /// is the variant that survives that.
    pub async fn publish(&self, spec: &PublishSpec) -> Result<PublishReport> {
        self.publish_recording(spec, |on_alias, previous_docs| async move {
            Ok((on_alias, previous_docs))
        })
        .await
    }

    /// [`Self::publish`], with `record` called just before the alias moves. It gets the indices
    /// the alias points at now (this index excluded) and their count, and returns the recorded
    /// count and the indices to delete once the
    /// alias has moved. A caller that stores the list on the first attempt and hands the stored
    /// list back on every later one makes the cleanup survive an interruption: an attempt that
    /// resumes after the swap finds the alias on this index already, and only the record still
    /// names what it served before. Whatever the alias points at now is deleted too.
    pub async fn publish_recording<F, Fut>(
        &self,
        spec: &PublishSpec,
        record: F,
    ) -> Result<PublishReport>
    where
        F: FnOnce(Vec<String>, Option<u64>) -> Fut,
        Fut: Future<Output = Result<(Vec<String>, Option<u64>)>>,
    {
        validate_index_name(&spec.alias)?;
        validate_index_name(&spec.index)?;
        if spec.alias == spec.index {
            return Err(EsError::Config(
                "the index cannot have the alias's name".into(),
            ));
        }
        let admin = &self.admin;

        admin.refresh(&spec.index).await?;
        admin.flush(&spec.index).await?;
        let docs = admin.count(&spec.index, None).await?;
        if docs != spec.expected_docs {
            return Err(EsError::CountMismatch {
                index: spec.index.clone(),
                expected: spec.expected_docs,
                actual: docs,
            });
        }

        admin
            .update_settings(&spec.index, &spec.serving.body())
            .await?;
        admin
            .wait_for_health(Some(&spec.index), spec.wait_for, spec.health_timeout)
            .await?;

        let targets = admin.alias_targets(&spec.alias).await?;
        if targets.is_empty() && admin.exists(&spec.alias).await? {
            return Err(EsError::ConcreteIndexAtAlias(spec.alias.clone()));
        }
        let on_alias: Vec<String> = targets
            .into_iter()
            .filter(|name| name != &spec.index)
            .collect();
        // An older run resumed after a newer one published (its index deleted by that publish
        // and re-created by `load`) must not take the alias back.
        if let Some(newer) = on_alias
            .iter()
            .find(|name| is_older_run(&spec.alias, &spec.index, name))
        {
            return Err(EsError::NewerRunServing {
                alias: spec.alias.clone(),
                index: spec.index.clone(),
                serving: newer.clone(),
            });
        }
        let previous_docs = if on_alias.is_empty() || spec.min_share == 0.0 {
            None
        } else {
            let mut count = 0_u64;
            for index in &on_alias {
                count = count
                    .checked_add(admin.count(index, None).await?)
                    .ok_or_else(|| EsError::Config("previous count overflow".into()))?;
            }
            Some(count)
        };
        #[allow(clippy::cast_precision_loss, reason = "comparison of document counts")]
        if previous_docs.is_some_and(|old| (docs as f64) < spec.min_share * (old as f64)) {
            return Err(EsError::PublishRefused(format!(
                "minimum document share: new {docs}, previous {}, minimum {}",
                previous_docs.unwrap_or(0),
                spec.min_share
            )));
        }
        let (mut previous, previous_docs) = record(on_alias.clone(), previous_docs).await?;
        previous.extend(on_alias.iter().cloned());
        previous.retain(|name| name != &spec.index);
        previous.sort();
        previous.dedup();
        self.move_alias(spec, &on_alias).await?;
        tracing::info!(alias = %spec.alias, index = %spec.index, docs, ?previous, "alias moved");

        let mut deleted = Vec::new();
        let mut not_deleted = Vec::new();
        for old in &previous {
            match admin.delete_index(old).await {
                Ok(()) => deleted.push(old.clone()),
                Err(error) => {
                    tracing::warn!(index = %old, %error, "could not delete a previous index");
                    not_deleted.push((old.clone(), error.to_string()));
                }
            }
        }
        self.delete_older_runs(spec, &mut deleted, &mut not_deleted)
            .await;
        Ok(PublishReport {
            alias: spec.alias.clone(),
            index: spec.index.clone(),
            docs,
            previous_docs,
            previous,
            deleted,
            not_deleted,
        })
    }

    /// Deletes the indices of older runs that never published (step 7). Nothing here is fatal:
    /// the alias has moved, so failures are reported in `not_deleted` and logged.
    async fn delete_older_runs(
        &self,
        spec: &PublishSpec,
        deleted: &mut Vec<String>,
        not_deleted: &mut Vec<(String, String)>,
    ) {
        if run_stamp(&spec.alias, &spec.index).is_none() {
            // A run id without a start stamp orders nothing: no index is older than it.
            return;
        }
        let pattern = format!("{}-*", spec.alias);
        let unaliased = match self
            .admin
            .unaliased_indices(&format!("{}-", spec.alias))
            .await
        {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(%pattern, %error, "could not list the indices of older runs");
                not_deleted.push((pattern, error.to_string()));
                return;
            }
        };
        let handled = |name: &String| {
            deleted.contains(name) || not_deleted.iter().any(|(other, _)| other == name)
        };
        let stale: Vec<String> = unaliased
            .into_iter()
            .filter(|name| is_older_run(&spec.alias, name, &spec.index) && !handled(name))
            .collect();
        for old in stale {
            match self.admin.delete_index(&old).await {
                Ok(()) => {
                    tracing::info!(index = %old, "deleted the index of an older run");
                    deleted.push(old);
                }
                Err(error) => {
                    tracing::warn!(index = %old, %error, "could not delete an older run's index");
                    not_deleted.push((old, error.to_string()));
                }
            }
        }
    }

    /// Moves the alias from `on_alias` to the index. A failed request whose effect is already
    /// visible (the alias points at the index and nothing else) counts as done: that is a
    /// resent request whose first copy was applied and whose answer was lost.
    async fn move_alias(&self, spec: &PublishSpec, on_alias: &[String]) -> Result<()> {
        let admin = &self.admin;
        let Err(error) = admin.swap_alias(&spec.alias, &spec.index, on_alias).await else {
            return Ok(());
        };
        match admin.alias_targets(&spec.alias).await {
            Ok(targets) if targets == [spec.index.as_str()] => {
                tracing::warn!(alias = %spec.alias, index = %spec.index, %error,
                    "the alias request failed, but the alias already points at the index");
                Ok(())
            }
            _ => Err(error),
        }
    }
}
