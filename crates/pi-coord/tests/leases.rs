//! Leases and commits on the in-memory store: races, expiry, renewal and loss.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{FlakyStore, ctx, short_leases};
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
