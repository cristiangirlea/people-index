//! The `publish` step: turn the loaded index into the serving one, or leave everything as it
//! was. The alias moves only after every check has passed, in one atomic request; nothing
//! before that point changes what readers see.

use std::time::Duration;

use serde::Serialize;

use crate::admin::IndexAdmin;
use crate::error::{EsError, Result};
use crate::health::HealthStatus;
use crate::names::validate_index_name;
use crate::settings::ServingSettings;

/// What to publish and what it must contain.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl PublishSpec {
    /// Defaults: one replica, `1s` refresh, wait up to 30 minutes for green (replica recovery
    /// of a large index copies every segment once).
    #[must_use]
    pub fn new(alias: impl Into<String>, index: impl Into<String>, expected_docs: u64) -> Self {
        Self {
            alias: alias.into(),
            index: index.into(),
            expected_docs,
            serving: ServingSettings::default(),
            wait_for: HealthStatus::Green,
            health_timeout: Duration::from_secs(30 * 60),
        }
    }
}

/// What `publish` did, for `summary.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublishReport {
    pub alias: String,
    pub index: String,
    pub docs: u64,
    /// Indices the alias pointed at before (the new index excluded).
    pub previous: Vec<String>,
    /// Previous indices deleted.
    pub deleted: Vec<String>,
    /// Previous indices that could not be deleted, with the reason. The alias already moved,
    /// so the run succeeded; these are left for the operator.
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
    /// 4. move the alias from the indices it served to this one in one `_aliases` request;
    /// 5. delete the indices it served before.
    ///
    /// Any failure in 1 to 4 returns an error with the alias untouched. Safe to run again: the
    /// checks repeat and moving an alias to where it already points changes nothing.
    pub async fn publish(&self, spec: &PublishSpec) -> Result<PublishReport> {
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
        let previous: Vec<String> = targets
            .into_iter()
            .filter(|name| name != &spec.index)
            .collect();
        admin
            .swap_alias(&spec.alias, &spec.index, &previous)
            .await?;
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
        Ok(PublishReport {
            alias: spec.alias.clone(),
            index: spec.index.clone(),
            docs,
            previous,
            deleted,
            not_deleted,
        })
    }
}
