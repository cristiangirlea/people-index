//! Conformance of every backend to the semantics the commit and lease protocol relies on.
//!
//! The same checks run against the in-memory store and a local directory always, and against
//! a real S3-compatible endpoint when `PI_TEST_S3_ENDPOINT` is set (`MinIO` in CI or locally:
//! `PI_TEST_S3_ENDPOINT=http://localhost:9000 PI_TEST_S3_BUCKET=... AWS_ACCESS_KEY_ID=...
//! AWS_SECRET_ACCESS_KEY=... cargo test -p pi-store`). Without it the S3 cases print a note
//! and pass, so the suite needs no network by default.

use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use pi_store::{Backend, CreateOutcome, ObjectVersion, Store, StoreError, StoreOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A fresh store plus the key prefix this test owns (S3 buckets are shared between runs).
struct Fixture {
    store: Store,
    root: String,
    _dir: Option<tempfile::TempDir>,
}

impl Fixture {
    fn key(&self, rel: &str) -> String {
        format!("{}/{rel}", self.root)
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "same shape as s3(), which can skip"
)]
fn memory() -> Option<Fixture> {
    Some(Fixture {
        store: Store::memory(),
        root: "t".into(),
        _dir: None,
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "same shape as s3(), which can skip"
)]
fn local() -> Option<Fixture> {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::local(dir.path()).expect("local store");
    Some(Fixture {
        store,
        root: "t".into(),
        _dir: Some(dir),
    })
}

fn s3() -> Option<Fixture> {
    let Ok(endpoint) = std::env::var("PI_TEST_S3_ENDPOINT") else {
        eprintln!("PI_TEST_S3_ENDPOINT is not set: S3 conformance case skipped");
        return None;
    };
    let bucket =
        std::env::var("PI_TEST_S3_BUCKET").unwrap_or_else(|_| "people-index-test".to_owned());
    let store = Store::s3(&bucket, "", &StoreOptions::with_endpoint(endpoint)).expect("s3 store");
    Some(Fixture {
        store,
        root: format!("conformance-{}", uuid::Uuid::now_v7().simple()),
        _dir: None,
    })
}

async fn put_get_roundtrip(f: Fixture) {
    let key = f.key("a/object.json");
    let v = f
        .store
        .put(&key, Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let got = f.store.get(&key).await.unwrap();
    assert_eq!(&got.bytes[..], b"hello");
    assert!(!got.version.is_empty(), "a get must return a version");
    assert!(!v.is_empty(), "a put must return a version");
    let head = f
        .store
        .head(&key)
        .await
        .unwrap()
        .expect("head finds the object");
    assert_eq!(head.size, 5);
    assert_eq!(head.key, key);
}

async fn missing_objects(f: Fixture) {
    let key = f.key("nope.json");
    assert!(matches!(
        f.store.get(&key).await,
        Err(StoreError::NotFound { .. })
    ));
    assert!(f.store.get_opt(&key).await.unwrap().is_none());
    assert!(f.store.head(&key).await.unwrap().is_none());
    assert!(
        f.store
            .get_json::<serde_json::Value>(&key)
            .await
            .unwrap()
            .is_none()
    );
    // Deleting what is not there is not an error: cleanup must be retry-safe.
    f.store.delete(&key).await.unwrap();
}

async fn create_if_absent_once(f: Fixture) {
    let key = f.key("_commit/t1.json");
    let first = f.store.put_create_if_absent(&key, "first").await.unwrap();
    assert!(first.is_created());
    let second = f.store.put_create_if_absent(&key, "second").await.unwrap();
    assert_eq!(second, CreateOutcome::AlreadyExists);
    assert_eq!(&f.store.get(&key).await.unwrap().bytes[..], b"first");
}

async fn create_if_absent_race(f: Fixture) {
    let key = f.key("_commit/race.json");
    let store = Arc::new(f.store.clone());
    let mut handles = Vec::new();
    for i in 0..16 {
        let store = Arc::clone(&store);
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            store
                .put_create_if_absent(&key, format!("writer-{i}"))
                .await
                .unwrap()
        }));
    }
    let mut created = 0;
    for h in handles {
        if h.await.unwrap().is_created() {
            created += 1;
        }
    }
    assert_eq!(created, 1, "exactly one concurrent create must win");
}

async fn update_if_match(f: Fixture) {
    let key = f.key("_lease/t1.json");
    let CreateOutcome::Created(v1) = f.store.put_create_if_absent(&key, "v1").await.unwrap() else {
        panic!("fresh key must be created");
    };
    if !f.store.supports_conditional_update() {
        assert!(matches!(
            f.store.put_update_if_match(&key, "v2", &v1).await,
            Err(StoreError::Unsupported { .. })
        ));
        return;
    }
    let v2 = f.store.put_update_if_match(&key, "v2", &v1).await.unwrap();
    assert_ne!(v1, v2, "an update must change the version");
    // The stale version loses.
    assert!(matches!(
        f.store.put_update_if_match(&key, "v3", &v1).await,
        Err(StoreError::PreconditionFailed { .. })
    ));
    assert_eq!(&f.store.get(&key).await.unwrap().bytes[..], b"v2");
    // The version a get returns works as well as the one a put returned.
    let read = f.store.get(&key).await.unwrap();
    f.store
        .put_update_if_match(&key, "v4", &read.version)
        .await
        .unwrap();
    // An update of an absent object is a precondition failure, not a create.
    let absent = f.key("_lease/absent.json");
    assert!(matches!(
        f.store
            .put_update_if_match(&absent, "x", &read.version)
            .await,
        Err(StoreError::PreconditionFailed { .. })
    ));
    assert!(f.store.get_opt(&absent).await.unwrap().is_none());
    // An empty version never turns into an unconditional write.
    assert!(matches!(
        f.store
            .put_update_if_match(&key, "x", &ObjectVersion::default())
            .await,
        Err(StoreError::PreconditionFailed { .. })
    ));
}

async fn update_if_match_race(f: Fixture) {
    if !f.store.supports_conditional_update() {
        return;
    }
    let key = f.key("_lease/race.json");
    let v = f.store.put(&key, "base").await.unwrap();
    let store = Arc::new(f.store.clone());
    let mut handles = Vec::new();
    for i in 0..8 {
        let store = Arc::clone(&store);
        let key = key.clone();
        let v = v.clone();
        handles.push(tokio::spawn(async move {
            store
                .put_update_if_match(&key, format!("w{i}"), &v)
                .await
                .is_ok()
        }));
    }
    let mut won = 0;
    for h in handles {
        if h.await.unwrap() {
            won += 1;
        }
    }
    assert_eq!(won, 1, "exactly one update on the same version must win");
}

async fn listing(f: Fixture) {
    for rel in ["a/b/1", "a/b/2", "a/bc/3", "a/b/sub/4", "z/5"] {
        f.store.put(&f.key(rel), "x").await.unwrap();
    }
    let keys = f.store.list_keys(&f.key("a/b")).await.unwrap();
    assert_eq!(
        keys,
        vec![f.key("a/b/1"), f.key("a/b/2"), f.key("a/b/sub/4")],
        "listing is recursive, sorted and segment-aligned"
    );
    assert_eq!(
        f.store
            .list_keys(&format!("{}/", f.key("a/b")))
            .await
            .unwrap(),
        keys,
        "a trailing slash changes nothing"
    );
    let dirs = f.store.list_prefixes(&f.key("a")).await.unwrap();
    assert_eq!(dirs, vec![f.key("a/b"), f.key("a/bc")]);
    assert_eq!(
        f.store.list(&f.key("empty")).await.unwrap(),
        [] as [pi_store::ObjectInfo; 0]
    );
    f.store.delete(&f.key("a/b/1")).await.unwrap();
    assert_eq!(f.store.list(&f.key("a/b")).await.unwrap().len(), 2);
}

async fn streaming(f: Fixture) {
    let key = f.key("data/big.seg");
    let body: Vec<u8> = (0..200_000u32).flat_map(u32::to_le_bytes).collect();
    let mut w = f.store.writer(&key).unwrap();
    for chunk in body.chunks(7919) {
        w.write_all(chunk).await.unwrap();
    }
    w.shutdown().await.unwrap();

    let mut streamed = Vec::new();
    let mut s = f.store.get_stream(&key).await.unwrap();
    while let Some(chunk) = s.next().await {
        streamed.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(streamed, body);

    let mut via_reader = Vec::new();
    f.store
        .get_reader(&key)
        .await
        .unwrap()
        .read_to_end(&mut via_reader)
        .await
        .unwrap();
    assert_eq!(via_reader, body);
}

async fn collect(stream: pi_store::ByteStream) -> Result<Vec<u8>, StoreError> {
    let chunks: Vec<Bytes> = stream.try_collect().await?;
    Ok(chunks.concat())
}

async fn get_if_match(f: Fixture) {
    let key = f.key("raw/persons/part-00000.ndjson.gz");
    f.store.put(&key, "first").await.unwrap();
    // The version a listing reports is what plan.json records.
    let listed = f.store.list(&f.key("raw")).await.unwrap();
    let planned = listed[0].version.clone();
    assert!(!planned.is_empty(), "a listing must return a version");
    let body = collect(f.store.get_stream_if_match(&key, &planned).await.unwrap()).await;
    assert_eq!(
        body.unwrap(),
        b"first",
        "an unchanged object reads as usual"
    );

    // Replaced (a different body, so even a coarse clock changes the local etag): refused.
    f.store.put(&key, "second, longer").await.unwrap();
    let now = f.store.head(&key).await.unwrap().unwrap().version;
    assert_ne!(now, planned, "a replacement must change the version");
    assert!(matches!(
        f.store.get_stream_if_match(&key, &planned).await,
        Err(StoreError::PreconditionFailed { .. })
    ));
    assert!(
        collect(f.store.get_stream_if_match(&key, &now).await.unwrap())
            .await
            .is_ok()
    );

    // An empty version never turns into an unconditional read.
    assert!(matches!(
        f.store
            .get_stream_if_match(&key, &ObjectVersion::default())
            .await,
        Err(StoreError::PreconditionFailed { .. })
    ));
    // Gone is not found, not a precondition failure.
    f.store.delete(&key).await.unwrap();
    assert!(matches!(
        f.store.get_stream_if_match(&key, &now).await,
        Err(StoreError::NotFound { .. })
    ));
}

async fn json_helpers(f: Fixture) {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Doc {
        n: u32,
    }
    let key = f.key("doc.json");
    assert!(
        f.store
            .put_json_create_if_absent(&key, &Doc { n: 1 })
            .await
            .unwrap()
            .is_created()
    );
    let (doc, version) = f.store.get_json::<Doc>(&key).await.unwrap().unwrap();
    assert_eq!(doc, Doc { n: 1 });
    if f.store.supports_conditional_update() {
        f.store
            .put_json_update_if_match(&key, &Doc { n: 2 }, &version)
            .await
            .unwrap();
    } else {
        f.store.put_json(&key, &Doc { n: 2 }).await.unwrap();
    }
    assert_eq!(
        f.store.get_json::<Doc>(&key).await.unwrap().unwrap().0,
        Doc { n: 2 }
    );
    f.store.put(&key, "not json").await.unwrap();
    assert!(matches!(
        f.store.get_json::<Doc>(&key).await,
        Err(StoreError::Json { .. })
    ));
}

macro_rules! conformance {
    ($backend:ident: $($case:ident),+ $(,)?) => {
        mod $backend {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                async fn $case() {
                    if let Some(f) = super::$backend() {
                        super::$case(f).await;
                    }
                }
            )+
        }
    };
}

conformance!(memory: put_get_roundtrip, missing_objects, create_if_absent_once,
    create_if_absent_race, update_if_match, update_if_match_race, listing, streaming,
    get_if_match, json_helpers);
conformance!(local: put_get_roundtrip, missing_objects, create_if_absent_once,
    create_if_absent_race, update_if_match, update_if_match_race, listing, streaming,
    get_if_match, json_helpers);
conformance!(s3: put_get_roundtrip, missing_objects, create_if_absent_once,
    create_if_absent_race, update_if_match, update_if_match_race, listing, streaming,
    get_if_match, json_helpers);

#[test]
fn open_picks_the_backend_from_the_url() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file://{}", dir.path().join("store").display());
    assert_eq!(
        Store::open(&url, &StoreOptions::default())
            .unwrap()
            .backend(),
        Backend::Local
    );
    assert!(dir.path().join("store").is_dir(), "the root is created");
    assert_eq!(
        Store::open("memory://", &StoreOptions::default())
            .unwrap()
            .backend(),
        Backend::Memory
    );
    let s3 = Store::open(
        "s3://bucket/prefix/",
        &StoreOptions::with_endpoint("http://127.0.0.1:9"),
    )
    .unwrap();
    assert_eq!(s3.backend(), Backend::S3);
    assert_eq!(s3.url(), "s3://bucket/prefix/");
    assert!(Store::open("ftp://x", &StoreOptions::default()).is_err());
}
