//! [`LeaseManager`]: who is working on a task right now, on the object store.
//!
//! A worker claims a task by creating `_lease/<task>.json` with create-if-absent, renews it
//! every `ttl / 3` with update-if-match on the version it holds, and takes over a lease that
//! expired with update-if-match on the version it observed. Every write is conditional, so
//! two workers can never both believe they hold the same lease.
//!
//! Leases keep work from being done twice; they are not what makes the result correct. The
//! commit is: outputs carry the attempt id and only the first commit counts, so a worker that
//! keeps running past a lost lease wastes time but cannot corrupt the run.

use std::time::Duration;

use pi_store::{CreateOutcome, ObjectVersion, RunLayout, Store, StoreError, StoreResult};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::stage_name::StageName;

/// Lease timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseConfig {
    /// How long a lease lives without renewal (`PI_LEASE_TTL`). A dead worker's task is taken
    /// over after about this long.
    pub ttl: Duration,
    /// Extra time before another worker treats a lease as expired, to absorb clock differences
    /// between nodes (expiry is wall-clock time written by the holder).
    pub clock_skew: Duration,
}

impl LeaseConfig {
    /// A config with the given ttl and a skew allowance of a quarter of it, at most 5 s.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            clock_skew: (ttl / 4).min(Duration::from_secs(5)),
        }
    }

    /// How often the holder renews: three chances before the lease would expire.
    #[must_use]
    pub fn renew_interval(&self) -> Duration {
        (self.ttl / 3).max(Duration::from_millis(1))
    }
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

/// The content of `_lease/<task>.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// The worker holding the lease (`PI_WORKER_ID`), for operators.
    pub worker: String,
    /// A token unique to this claim, so a worker can recognise its own lease even when two
    /// processes share a worker id.
    pub holder: String,
    /// When the lease lapses unless renewed, by the holder's clock.
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    /// When this claim started.
    #[serde(with = "time::serde::rfc3339")]
    pub acquired_at: OffsetDateTime,
    /// Successful renewals so far.
    #[serde(default)]
    pub renewals: u64,
    /// Set when the holder let go, so another worker can claim at once.
    #[serde(default)]
    pub released: bool,
}

impl Lease {
    /// True when another worker may take the lease over.
    #[must_use]
    pub fn is_expired(&self, now: OffsetDateTime, clock_skew: Duration) -> bool {
        self.released || now >= add(self.expires_at, clock_skew)
    }
}

fn add(t: OffsetDateTime, d: Duration) -> OffsetDateTime {
    t.saturating_add(time::Duration::try_from(d).unwrap_or(time::Duration::MAX))
}

/// Claims and inspects the leases of one run on behalf of one worker.
#[derive(Debug, Clone)]
pub struct LeaseManager {
    store: Store,
    layout: RunLayout,
    worker: String,
    config: LeaseConfig,
}

impl LeaseManager {
    /// A lease manager for `worker` on the run `layout` describes.
    #[must_use]
    pub fn new(
        store: Store,
        layout: RunLayout,
        worker: impl Into<String>,
        config: LeaseConfig,
    ) -> Self {
        Self {
            store,
            layout,
            worker: worker.into(),
            config,
        }
    }

    /// The timing in use.
    #[must_use]
    pub fn config(&self) -> LeaseConfig {
        self.config
    }

    /// Reads a task's lease and its version.
    pub async fn read(
        &self,
        stage: StageName,
        task_id: &str,
    ) -> StoreResult<Option<(Lease, ObjectVersion)>> {
        let key = self.layout.lease(stage.as_str(), task_id)?;
        self.store.get_json(&key).await
    }

    /// Tries to claim a task. `Some` holds the lease and keeps renewing it until released or
    /// dropped; `None` means another worker holds a live lease.
    pub async fn try_claim(
        &self,
        stage: StageName,
        task_id: &str,
    ) -> StoreResult<Option<LeaseGuard>> {
        let key = self.layout.lease(stage.as_str(), task_id)?;
        let holder = Uuid::now_v7().simple().to_string();
        // Two rounds cover a lease deleted between our create and our read.
        for _ in 0..2 {
            let now = OffsetDateTime::now_utc();
            let fresh = Lease {
                worker: self.worker.clone(),
                holder: holder.clone(),
                expires_at: add(now, self.config.ttl),
                acquired_at: now,
                renewals: 0,
                released: false,
            };
            if let CreateOutcome::Created(version) =
                self.store.put_json_create_if_absent(&key, &fresh).await?
            {
                return Ok(Some(self.guard(key, fresh, version, false)));
            }
            let Some(existing) = self.store.get_opt(&key).await? else {
                continue;
            };
            let takeable = match serde_json::from_slice::<Lease>(&existing.bytes) {
                Ok(lease) if lease.holder == holder => {
                    // Our own create, whose response was lost and retried by the client.
                    return Ok(Some(self.guard(key, lease, existing.version, false)));
                }
                Ok(lease) => lease.is_expired(now, self.config.clock_skew),
                // An unreadable lease would block the task forever; overwrite it, still
                // conditionally, so only one worker wins.
                Err(_) => true,
            };
            if !takeable {
                return Ok(None);
            }
            return match self
                .store
                .put_json_update_if_match(&key, &fresh, &existing.version)
                .await
            {
                Ok(version) => {
                    metrics::counter!("pi_lease_takeovers_total", "stage" => stage.as_str())
                        .increment(1);
                    tracing::info!(stage = %stage, task = task_id, worker = %self.worker, "took over an expired lease");
                    Ok(Some(self.guard(key, fresh, version, true)))
                }
                Err(StoreError::PreconditionFailed { .. }) => Ok(None),
                Err(e) => Err(e),
            };
        }
        Ok(None)
    }

    fn guard(
        &self,
        key: String,
        lease: Lease,
        version: ObjectVersion,
        took_over: bool,
    ) -> LeaseGuard {
        let stop = CancellationToken::new();
        let lost = CancellationToken::new();
        let renewer = tokio::spawn(renew_loop(Renewal {
            store: self.store.clone(),
            key: key.clone(),
            config: self.config,
            lease,
            version,
            stop: stop.clone(),
            lost: lost.clone(),
        }));
        LeaseGuard {
            store: self.store.clone(),
            key,
            stop,
            lost,
            renewer: Some(renewer),
            took_over,
        }
    }
}

/// A held lease. It renews itself in the background until [`LeaseGuard::release`] (or drop,
/// which stops renewing and lets the lease expire).
#[derive(Debug)]
pub struct LeaseGuard {
    store: Store,
    key: String,
    stop: CancellationToken,
    lost: CancellationToken,
    renewer: Option<JoinHandle<Option<(Lease, ObjectVersion)>>>,
    took_over: bool,
}

impl LeaseGuard {
    /// Cancelled when the lease is lost: another worker took it over, or renewals failed long
    /// enough that others may consider it expired. The task must stop.
    #[must_use]
    pub fn lost_token(&self) -> CancellationToken {
        self.lost.clone()
    }

    /// True once the lease is lost.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        self.lost.is_cancelled()
    }

    /// True when this claim took over an expired lease (its previous holder likely died).
    #[must_use]
    pub fn took_over(&self) -> bool {
        self.took_over
    }

    /// The lease key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Stops renewing and marks the lease released, conditionally on the version held, so a
    /// lease another worker took over in the meantime is left alone.
    pub async fn release(mut self) -> StoreResult<()> {
        self.stop.cancel();
        let Some(handle) = self.renewer.take() else {
            return Ok(());
        };
        let Ok(Some((mut lease, version))) = handle.await else {
            return Ok(());
        };
        lease.released = true;
        lease.expires_at = OffsetDateTime::now_utc();
        match self
            .store
            .put_json_update_if_match(&self.key, &lease, &version)
            .await
        {
            Ok(_) | Err(StoreError::PreconditionFailed { .. } | StoreError::Unsupported { .. }) => {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(handle) = self.renewer.take() {
            handle.abort();
        }
    }
}

struct Renewal {
    store: Store,
    key: String,
    config: LeaseConfig,
    lease: Lease,
    version: ObjectVersion,
    stop: CancellationToken,
    lost: CancellationToken,
}

/// Renews until stopped (returns the last lease and version, for release) or lost (`None`).
async fn renew_loop(mut r: Renewal) -> Option<(Lease, ObjectVersion)> {
    let interval = r.config.renew_interval();
    let mut last_ok = Instant::now();
    loop {
        tokio::select! {
            () = r.stop.cancelled() => return Some((r.lease, r.version)),
            () = tokio::time::sleep(interval) => {}
        }
        let mut next = r.lease.clone();
        next.expires_at = add(OffsetDateTime::now_utc(), r.config.ttl);
        next.renewals += 1;
        match r
            .store
            .put_json_update_if_match(&r.key, &next, &r.version)
            .await
        {
            Ok(version) => {
                r.lease = next;
                r.version = version;
                last_ok = Instant::now();
            }
            Err(StoreError::PreconditionFailed { .. }) => {
                tracing::warn!(key = %r.key, "lease lost: another worker took it over");
                return lose(&r.lost);
            }
            Err(e) => {
                // Give up before the lease could lapse: past this point another worker may
                // already consider it expired, and two holders is what leases exist to prevent.
                if last_ok.elapsed() + interval >= r.config.ttl {
                    tracing::warn!(key = %r.key, error = %e, "lease lost: renewals kept failing");
                    return lose(&r.lost);
                }
                tracing::warn!(key = %r.key, error = %e, "lease renewal failed; retrying");
            }
        }
    }
}

fn lose(lost: &CancellationToken) -> Option<(Lease, ObjectVersion)> {
    metrics::counter!("pi_lease_lost_total").increment(1);
    lost.cancel();
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_respects_skew_and_release() {
        let now = OffsetDateTime::now_utc();
        let mut lease = Lease {
            worker: "w".into(),
            holder: "h".into(),
            expires_at: now,
            acquired_at: now,
            renewals: 0,
            released: false,
        };
        assert!(lease.is_expired(now, Duration::ZERO));
        assert!(!lease.is_expired(now, Duration::from_secs(1)));
        lease.expires_at = add(now, Duration::from_secs(60));
        assert!(!lease.is_expired(now, Duration::ZERO));
        lease.released = true;
        assert!(lease.is_expired(now, Duration::ZERO));
    }

    #[test]
    fn config_defaults() {
        let c = LeaseConfig::default();
        assert_eq!(c.ttl, Duration::from_secs(60));
        assert_eq!(c.renew_interval(), Duration::from_secs(20));
        assert_eq!(c.clock_skew, Duration::from_secs(5));
        assert_eq!(
            LeaseConfig::new(Duration::from_millis(400)).clock_skew,
            Duration::from_millis(100)
        );
    }
}
