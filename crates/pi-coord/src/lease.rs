//! [`LeaseManager`]: who is working on a task right now, on the object store.
//!
//! A worker claims a task by creating `_lease/<task>.json` with create-if-absent, renews it
//! every `ttl / 3` with update-if-match on the version it holds, and takes over a lease that
//! expired with update-if-match on the version it observed. Every write is conditional, so
//! two workers can never both believe they hold the same lease.
//!
//! A renewal can land on the store while its response is lost; the next renewal then presents
//! a stale version and is refused. So a refused renewal re-reads the lease, and the holder keeps
//! going on the version it reads when the lease still carries its own `holder` token. Only a
//! lease another claim owns, or one that is gone, counts as lost.
//!
//! Leases keep work from being done twice; they are not what makes the result correct. The
//! commit is: outputs carry the attempt id and only the first commit counts, so a worker that
//! keeps running past a lost lease wastes time but cannot corrupt the run.
//!
//! A polling worker asks [`LeaseManager::live_task_ids`] which tasks someone else holds before
//! it tries to claim any: one listing of `_lease/`, plus a read of each lease whose version
//! changed since the last poll (a [`LeaseCache`] remembers the rest), so a live lease costs no
//! request on most polls instead of a create and a read on every one.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use pi_store::{
    CreateOutcome, ObjectInfo, ObjectVersion, RunLayout, Store, StoreError, StoreResult,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::stage_name::StageName;

/// Lease bodies read at once when a poll finds many changed leases.
const READ_CONCURRENCY: usize = 32;

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

/// The leases a poller has read, by key, with the version each body came with: a lease whose
/// listed version is unchanged is not read again. Clones share the cache.
///
/// Only the leases of the last listing are kept, so it holds at most one stage's tasks.
#[derive(Clone, Default)]
pub struct LeaseCache {
    seen: Arc<Mutex<HashMap<String, SeenLease>>>,
}

impl std::fmt::Debug for LeaseCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseCache")
            .field("leases", &self.lock().len())
            .finish()
    }
}

impl LeaseCache {
    // A poisoned lock only means another poller panicked mid-update; every entry is still a
    // consistent (version, body) pair, and a stale one is only a missed skip or an extra read.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, SeenLease>> {
        self.seen.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One lease as last read.
#[derive(Debug, Clone)]
struct SeenLease {
    version: ObjectVersion,
    /// `None` when the body was unreadable (`try_claim` overwrites such a lease).
    lease: Option<Lease>,
}

/// True when a listed version names the same object as the one a body was read with. S3
/// listings carry the etag but not the version id, so the etag decides when there is one; a
/// listing with neither is treated as changed.
fn unchanged(listed: &ObjectVersion, read: &ObjectVersion) -> bool {
    match (&listed.e_tag, &listed.version) {
        (Some(tag), _) => read.e_tag.as_ref() == Some(tag),
        (None, Some(version)) => read.version.as_ref() == Some(version),
        (None, None) => false,
    }
}

/// Claims and inspects the leases of one run on behalf of one worker.
#[derive(Debug, Clone)]
pub struct LeaseManager {
    store: Store,
    layout: RunLayout,
    worker: String,
    config: LeaseConfig,
    cache: LeaseCache,
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
            cache: LeaseCache::default(),
        }
    }

    /// Uses `cache` for [`LeaseManager::live_task_ids`], so the leases read on one poll are not
    /// read again on the next while they are unchanged.
    #[must_use]
    pub fn with_cache(mut self, cache: LeaseCache) -> Self {
        self.cache = cache;
        self
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

    /// The tasks among `task_ids` whose lease is live: held, not released, and not past
    /// `expires_at + clock_skew`. Absent, expired, released and unreadable leases are left out,
    /// so those are the tasks worth a [`LeaseManager::try_claim`].
    ///
    /// Costs one listing of the stage's `_lease/` plus one read per lease of `task_ids` that is
    /// new or whose version changed since the cache saw it; expiry of an unchanged lease is
    /// judged from the cached `expires_at`. The answer is a snapshot: a lease taken or released
    /// right after is caught by the claim itself or by the next poll.
    pub async fn live_task_ids(
        &self,
        stage: StageName,
        task_ids: &BTreeSet<String>,
    ) -> StoreResult<BTreeSet<String>> {
        let dir = self.layout.leases_dir(stage.as_str())?;
        let listed: Vec<(String, ObjectInfo)> = self
            .store
            .list(&dir)
            .await?
            .into_iter()
            .filter_map(|info| {
                let task = RunLayout::task_id_from_key(&info.key)?;
                task_ids
                    .contains(task)
                    .then(|| (task.to_owned(), info.clone()))
            })
            .collect();
        let changed: Vec<String> = {
            let seen = self.cache.lock();
            listed
                .iter()
                .filter(|(_, info)| {
                    seen.get(&info.key)
                        .is_none_or(|s| !unchanged(&info.version, &s.version))
                })
                .map(|(_, info)| info.key.clone())
                .collect()
        };
        let read: Vec<(String, Option<SeenLease>)> = futures::stream::iter(changed)
            .map(|key| async move {
                let seen = self.store.get_opt(&key).await?.map(|obj| SeenLease {
                    lease: serde_json::from_slice(&obj.bytes).ok(),
                    version: obj.version,
                });
                Ok::<_, StoreError>((key, seen))
            })
            .buffer_unordered(READ_CONCURRENCY)
            .try_collect()
            .await?;

        let now = OffsetDateTime::now_utc();
        let mut seen = self.cache.lock();
        let keys: HashSet<&str> = listed.iter().map(|(_, info)| info.key.as_str()).collect();
        seen.retain(|key, _| keys.contains(key.as_str()));
        for (key, lease) in read {
            match lease {
                Some(lease) => {
                    seen.insert(key, lease);
                }
                // Deleted between the listing and the read: nobody holds it.
                None => {
                    seen.remove(&key);
                }
            }
        }
        Ok(listed
            .into_iter()
            .filter(|(_, info)| {
                seen.get(&info.key)
                    .and_then(|s| s.lease.as_ref())
                    .is_some_and(|l| !l.is_expired(now, self.config.clock_skew))
            })
            .map(|(task, _)| task)
            .collect())
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
    // Set after adopting a version re-read from the store, to renew on it at once rather than
    // an interval later; never twice in a row, so a store that keeps refusing cannot spin.
    let mut renew_at_once = false;
    loop {
        let immediate = std::mem::take(&mut renew_at_once);
        if immediate {
            if r.stop.is_cancelled() {
                return Some((r.lease, r.version));
            }
        } else {
            tokio::select! {
                () = r.stop.cancelled() => return Some((r.lease, r.version)),
                () = tokio::time::sleep(interval) => {}
            }
        }
        let mut next = r.lease.clone();
        next.expires_at = add(OffsetDateTime::now_utc(), r.config.ttl);
        next.renewals += 1;
        let failure = match r
            .store
            .put_json_update_if_match(&r.key, &next, &r.version)
            .await
        {
            Ok(version) => {
                r.lease = next;
                r.version = version;
                last_ok = Instant::now();
                continue;
            }
            Err(StoreError::PreconditionFailed { .. }) => match still_ours(&r).await {
                Ok(Some((lease, version))) => {
                    // The stored expiry bounds the holder: a store that keeps refusing renewals
                    // of a lease that is still ours must not keep us past it. The next renewal
                    // comes at once, or an interval from now after an immediate one.
                    let next_try = if immediate { interval } else { Duration::ZERO };
                    if add(OffsetDateTime::now_utc(), next_try) >= lease.expires_at {
                        tracing::warn!(key = %r.key, "lease lost: renewals kept being refused");
                        return lose(&r.lost);
                    }
                    // An earlier renewal of ours landed but its response was lost.
                    tracing::info!(key = %r.key, "lease renewal refused but the lease is still ours; continuing on the version read");
                    r.lease = lease;
                    r.version = version;
                    renew_at_once = !immediate;
                    continue;
                }
                Ok(None) => return lose(&r.lost),
                Err(e) => e,
            },
            Err(e) => e,
        };
        // Give up before the lease could lapse: past this point another worker may already
        // consider it expired, and two holders is what leases exist to prevent.
        if last_ok.elapsed() + interval >= r.config.ttl {
            tracing::warn!(key = %r.key, error = %failure, "lease lost: renewals kept failing");
            return lose(&r.lost);
        }
        tracing::warn!(key = %r.key, error = %failure, "lease renewal failed; retrying");
    }
}

/// After a refused renewal: the lease as it is now when it still belongs to this claim,
/// `None` when another claim owns it or it is gone (logged), an error when it cannot be read.
async fn still_ours(r: &Renewal) -> StoreResult<Option<(Lease, ObjectVersion)>> {
    let Some(current) = r.store.get_opt(&r.key).await? else {
        tracing::warn!(key = %r.key, "lease lost: the lease object is gone");
        return Ok(None);
    };
    match serde_json::from_slice::<Lease>(&current.bytes) {
        Ok(lease) if lease.holder == r.lease.holder && !lease.released => {
            Ok(Some((lease, current.version)))
        }
        // Unreadable counts as taken: a claimer overwrites an unreadable lease.
        _ => {
            tracing::warn!(key = %r.key, "lease lost: another worker took it over");
            Ok(None)
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
