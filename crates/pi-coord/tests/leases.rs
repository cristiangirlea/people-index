//! Leases and commits on the in-memory store: races, expiry, renewal and loss.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{CountingStore, FlakyStore, Op, ctx, eventually, short_leases};
use pi_coord::{
    AttemptId, CommitLog, CommitOutcome, CommitRecord, Lease, LeaseManager, StageName, TaskReport,
};
use pi_store::Store;
use time::OffsetDateTime;

fn manager(store: &Store, worker: &str, ttl_ms: u64) -> LeaseManager {
    LeaseManager::new(
        store.clone(),
        ctx(store, "r1").layout,
        worker,
        short_leases(ttl_ms),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_claims_have_exactly_one_winner() {
    let store = Store::memory();
    let mut handles = Vec::new();
    for i in 0..12 {
        let m = manager(&store, &format!("w{i}"), 60_000);
        handles.push(tokio::spawn(async move {
            m.try_claim(StageName::Join, "part-00001").await.unwrap()
        }));
    }
    let mut guards = Vec::new();
    for h in handles {
        if let Some(g) = h.await.unwrap() {
            guards.push(g);
        }
    }
    assert_eq!(guards.len(), 1, "exactly one claim wins");
    assert!(!guards[0].took_over());
}

#[tokio::test]
async fn a_live_lease_is_kept_alive_by_renewals() {
    let store = Store::memory();
    let a = manager(&store, "a", 150);
    let b = manager(&store, "b", 150);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            b.try_claim(StageName::Join, "t").await.unwrap().is_none(),
            "a renewed lease cannot be taken"
        );
    }
    let (lease, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    assert!(lease.renewals >= 3, "renewed {} times", lease.renewals);
    assert_eq!(lease.worker, "a");
    assert!(!guard.is_lost());
}

#[tokio::test]
async fn an_expired_lease_is_taken_over() {
    let store = Store::memory();
    let a = manager(&store, "a", 200);
    let b = manager(&store, "b", 200);
    let crashed = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    // A crash: the guard (and its renewals) disappear without a release.
    drop(crashed);
    assert!(
        b.try_claim(StageName::Join, "t").await.unwrap().is_none(),
        "not expired yet"
    );
    tokio::time::sleep(Duration::from_millis(260)).await;
    let taken = b
        .try_claim(StageName::Join, "t")
        .await
        .unwrap()
        .expect("expired lease taken");
    assert!(taken.took_over());
    let (lease, _) = b.read(StageName::Join, "t").await.unwrap().unwrap();
    assert_eq!(lease.worker, "b");
}

#[tokio::test]
async fn a_holder_whose_lease_was_taken_is_cancelled() {
    let store = Store::memory();
    let a = manager(&store, "a", 150);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    let lost = guard.lost_token();
    // Someone else rewrites the lease (a takeover after a long pause, say): the holder's next
    // conditional renewal fails and it must stop.
    let key = guard.key().to_owned();
    let now = OffsetDateTime::now_utc();
    store
        .put_json(
            &key,
            &Lease {
                worker: "intruder".into(),
                holder: "x".into(),
                expires_at: now + time::Duration::minutes(5),
                acquired_at: now,
                renewals: 0,
                released: false,
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), lost.cancelled())
        .await
        .expect("the holder learns it lost the lease");
    assert!(guard.is_lost());
    // Releasing a lost lease leaves the new holder's lease alone.
    guard.release().await.unwrap();
    let (lease, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    assert_eq!(lease.worker, "intruder");
}

#[tokio::test]
async fn renewals_failing_past_the_ttl_cancel_the_holder() {
    let (store, failing) = FlakyStore::store();
    let a = manager(&store, "a", 150);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    failing.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), guard.lost_token().cancelled())
        .await
        .expect("an unreachable store cancels the holder before its lease lapses");
    failing.store(false, Ordering::SeqCst);
}

/// Waits until `counter` is back to zero: the fault it armed has fired.
async fn fired(counter: &Arc<AtomicUsize>) -> bool {
    eventually(Duration::from_secs(2), || {
        let counter = Arc::clone(counter);
        async move { counter.load(Ordering::SeqCst) == 0 }
    })
    .await
}

#[tokio::test]
async fn a_lost_renewal_response_does_not_cancel_the_holder() {
    let (store, faults) = FlakyStore::with_faults();
    let a = manager(&store, "a", 300);
    let b = manager(&store, "b", 300);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    let (claimed, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    // The next renewal lands on the store, but the holder only sees an error, so it still
    // holds the old version and its following renewal is refused.
    faults.lost_responses.store(1, Ordering::SeqCst);
    assert!(fired(&faults.lost_responses).await, "a renewal ran");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!guard.is_lost(), "a lost response is not a takeover");
    assert!(
        b.try_claim(StageName::Join, "t").await.unwrap().is_none(),
        "the lease is still held"
    );
    let (lease, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    assert_eq!(lease.holder, claimed.holder);
    assert!(lease.renewals >= 3, "renewed {} times", lease.renewals);
    // The holder renews on the version it re-read, so its release still applies.
    guard.release().await.unwrap();
    let (lease, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    assert!(lease.released);
}

#[tokio::test]
async fn a_poller_follows_a_lease_whose_holder_adopted_a_reread_version() {
    // The two halves together: the holder's renewal response is lost and it adopts the version
    // it re-reads, while a polling worker caches the lease by version. The poller must keep
    // seeing the lease as live (the adopted renewals change its version, so it reads it again)
    // and must take it over only once it has really expired.
    let (store, faults) = FlakyStore::with_faults();
    let a = manager(&store, "a", 300);
    let b = manager(&store, "b", 300);
    let asked = BTreeSet::from(["t".to_owned()]);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    assert_eq!(
        b.live_task_ids(StageName::Join, &asked).await.unwrap(),
        asked
    );

    faults.lost_responses.store(1, Ordering::SeqCst);
    assert!(fired(&faults.lost_responses).await, "a renewal ran");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!guard.is_lost(), "a lost response is not a takeover");
    assert_eq!(
        b.live_task_ids(StageName::Join, &asked).await.unwrap(),
        asked,
        "the adopted lease is still live for the poller"
    );
    assert!(
        b.try_claim(StageName::Join, "t").await.unwrap().is_none(),
        "the lease is still held"
    );

    // The holder stops renewing without releasing (a crash): past the lifetime the poller sees
    // it expired and takes it over.
    drop(guard);
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert!(
        b.live_task_ids(StageName::Join, &asked)
            .await
            .unwrap()
            .is_empty(),
        "an expired lease is not live"
    );
    assert!(
        b.try_claim(StageName::Join, "t").await.unwrap().is_some(),
        "the expired lease is taken over"
    );
}

#[tokio::test]
async fn a_refused_renewal_whose_reread_fails_is_retried() {
    let (store, faults) = FlakyStore::with_faults();
    let a = manager(&store, "a", 300);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    // The holder's own lease under a new version, as a renewal whose response was lost leaves
    // it; the re-read after the refused renewal fails once.
    let (lease, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    faults.failed_gets.store(1, Ordering::SeqCst);
    store.put_json(guard.key(), &lease).await.unwrap();
    assert!(fired(&faults.failed_gets).await, "the re-read ran");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!guard.is_lost(), "one failed re-read is one failed renewal");
    let (now, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    assert_eq!(now.holder, lease.holder);
    assert!(now.renewals > lease.renewals, "renewals went on");
}

#[tokio::test]
async fn refusals_of_a_lease_still_ours_neither_spin_nor_outlive_it() {
    let (store, faults) = FlakyStore::with_faults();
    let a = manager(&store, "a", 600);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    // Every renewal is refused while the lease stays ours: the re-read keeps adopting it.
    faults.refused_updates.store(true, Ordering::SeqCst);
    let before = faults.puts.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), guard.lost_token().cancelled())
        .await
        .expect("the holder lets go once its lease could lapse");
    let (lease, _) = a.read(StageName::Join, "t").await.unwrap().unwrap();
    assert!(
        OffsetDateTime::now_utc() < lease.expires_at,
        "cancelled before the stored lease lapsed"
    );
    // About two renewal intervals passed: each refused renewal is retried at once, then waits.
    let puts = faults.puts.load(Ordering::SeqCst) - before;
    assert!(puts <= 6, "{puts} renewals: refusals must not spin");
    faults.refused_updates.store(false, Ordering::SeqCst);
}

#[tokio::test]
async fn a_holder_whose_lease_is_gone_is_cancelled() {
    let store = Store::memory();
    let a = manager(&store, "a", 150);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    store.delete(guard.key()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), guard.lost_token().cancelled())
        .await
        .expect("a deleted lease is lost");
}

#[tokio::test]
async fn a_released_lease_can_be_claimed_at_once() {
    let store = Store::memory();
    let a = manager(&store, "a", 60_000);
    let b = manager(&store, "b", 60_000);
    let guard = a.try_claim(StageName::Join, "t").await.unwrap().unwrap();
    assert!(b.try_claim(StageName::Join, "t").await.unwrap().is_none());
    guard.release().await.unwrap();
    let g = b
        .try_claim(StageName::Join, "t")
        .await
        .unwrap()
        .expect("released lease claimed");
    assert!(g.took_over());
}

#[tokio::test]
async fn an_unreadable_lease_does_not_block_the_task_forever() {
    let store = Store::memory();
    let layout = ctx(&store, "r1").layout;
    store
        .put(&layout.lease("join", "t").unwrap(), "{not json")
        .await
        .unwrap();
    let g = manager(&store, "a", 60_000)
        .try_claim(StageName::Join, "t")
        .await
        .unwrap();
    assert!(g.is_some());
}

fn is_lease(path: &str) -> bool {
    path.contains("/_lease/")
}

#[tokio::test]
async fn polling_reads_only_the_leases_that_changed() {
    let (store, ops) = CountingStore::store();
    let holder = manager(&store, "a", 60_000);
    let held: BTreeSet<String> = (0..20).map(|p| format!("part-{p:05}")).collect();
    let mut guards = Vec::new();
    for id in &held {
        guards.push(
            holder
                .try_claim(StageName::Join, id)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    // A lease outside the tasks asked about (a committed task, say) is never read.
    let _other = holder
        .try_claim(StageName::Join, "part-00099")
        .await
        .unwrap()
        .unwrap();
    let mut asked = held.clone();
    asked.insert("part-00050".to_owned()); // no lease at all
    let poller = manager(&store, "b", 60_000);

    ops.clear();
    let live = poller.live_task_ids(StageName::Join, &asked).await.unwrap();
    assert_eq!(
        live, held,
        "every held lease is live; the absent one is not"
    );
    assert_eq!(ops.count(Op::List, |p| p.ends_with("/join/_lease")), 1);
    assert_eq!(
        ops.count(Op::Get, is_lease),
        20,
        "first poll reads each held lease"
    );
    assert_eq!(ops.count(Op::Get, |p| p.contains("part-00099")), 0);
    assert_eq!(
        ops.count(Op::Create, is_lease) + ops.count(Op::Update, is_lease),
        0
    );

    ops.clear();
    let live = poller.live_task_ids(StageName::Join, &asked).await.unwrap();
    assert_eq!(live, held);
    assert_eq!(ops.count(Op::List, |p| p.ends_with("/join/_lease")), 1);
    assert_eq!(
        ops.count(Op::Get, is_lease),
        0,
        "unchanged leases are not read again"
    );

    // Releasing one changes its version: only that lease is read, and it is no longer live.
    let released = guards.pop().unwrap();
    released.release().await.unwrap();
    ops.clear();
    let live = poller.live_task_ids(StageName::Join, &asked).await.unwrap();
    assert_eq!(ops.count(Op::Get, is_lease), 1);
    assert_eq!(live.len(), 19);
    assert!(!live.contains("part-00019"));
}

#[tokio::test]
async fn an_unchanged_lease_expires_without_being_read_again() {
    let (store, ops) = CountingStore::store();
    let crashed = manager(&store, "a", 200)
        .try_claim(StageName::Join, "t")
        .await
        .unwrap()
        .unwrap();
    drop(crashed);
    let poller = manager(&store, "b", 200);
    let asked = BTreeSet::from(["t".to_owned()]);
    assert_eq!(
        poller.live_task_ids(StageName::Join, &asked).await.unwrap(),
        asked
    );
    tokio::time::sleep(Duration::from_millis(260)).await;
    ops.clear();
    assert!(
        poller
            .live_task_ids(StageName::Join, &asked)
            .await
            .unwrap()
            .is_empty(),
        "the cached expiry has passed"
    );
    assert_eq!(ops.count(Op::Get, is_lease), 0);
    let taken = poller
        .try_claim(StageName::Join, "t")
        .await
        .unwrap()
        .expect("the expired lease is taken over");
    assert!(taken.took_over());
}

#[tokio::test]
async fn an_unreadable_lease_is_not_live() {
    let store = Store::memory();
    let layout = ctx(&store, "r1").layout;
    store
        .put(&layout.lease("join", "t").unwrap(), "{not json")
        .await
        .unwrap();
    let asked = BTreeSet::from(["t".to_owned()]);
    let live = manager(&store, "a", 60_000)
        .live_task_ids(StageName::Join, &asked)
        .await
        .unwrap();
    assert!(live.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_commits_have_exactly_one_winner() {
    let store = Store::memory();
    let log = Arc::new(CommitLog::new(store.clone(), ctx(&store, "r1").layout));
    let mut handles = Vec::new();
    for i in 0..10 {
        let log = Arc::clone(&log);
        handles.push(tokio::spawn(async move {
            let mut report = TaskReport::default();
            report.add("writer", i);
            let record = CommitRecord {
                stage: StageName::Join,
                task_id: "part-00003".into(),
                attempt: AttemptId::new(),
                worker: format!("w{i}"),
                committed_at: OffsetDateTime::now_utc(),
                report,
            };
            (record.clone(), log.commit(&record).await.unwrap())
        }));
    }
    let mut winners = Vec::new();
    let mut seen_by_losers = Vec::new();
    for h in handles {
        match h.await.unwrap() {
            (record, CommitOutcome::Committed) => winners.push(record),
            (_, CommitOutcome::AlreadyCommitted(existing)) => seen_by_losers.push(*existing),
        }
    }
    assert_eq!(winners.len(), 1);
    assert!(
        seen_by_losers.iter().all(|r| *r == winners[0]),
        "losers see the winner"
    );
    let stored = log
        .get(StageName::Join, "part-00003")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, winners[0]);
    assert_eq!(
        log.committed_task_ids(StageName::Join).await.unwrap().len(),
        1
    );
}

/// The lease protocol against a real S3-compatible endpoint, when `PI_TEST_S3_ENDPOINT` is set
/// (see pi-store's conformance tests for the variables). Skipped otherwise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leases_hold_on_a_real_s3_endpoint() {
    let Ok(endpoint) = std::env::var("PI_TEST_S3_ENDPOINT") else {
        eprintln!("PI_TEST_S3_ENDPOINT is not set: S3 lease case skipped");
        return;
    };
    let bucket =
        std::env::var("PI_TEST_S3_BUCKET").unwrap_or_else(|_| "people-index-test".to_owned());
    let prefix = format!("lease-test-{}", uuid::Uuid::now_v7().simple());
    let store = Store::s3(
        &bucket,
        &prefix,
        &pi_store::StoreOptions::with_endpoint(endpoint),
    )
    .unwrap();

    let mut handles = Vec::new();
    for i in 0..8 {
        let m = manager(&store, &format!("w{i}"), 600);
        handles.push(tokio::spawn(async move {
            m.try_claim(StageName::Join, "t").await.unwrap()
        }));
    }
    let mut guards = Vec::new();
    for h in handles {
        guards.extend(h.await.unwrap());
    }
    assert_eq!(guards.len(), 1, "exactly one claim wins on S3");
    drop(guards);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let taken = manager(&store, "late", 600)
        .try_claim(StageName::Join, "t")
        .await
        .unwrap()
        .expect("the expired lease is taken over on S3");
    assert!(taken.took_over());
    taken.release().await.unwrap();
}
