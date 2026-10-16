//! Feeders: the async side that reads from the store and fills an input channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use pi_coord::permanent;
use pi_store::{ObjectVersion, Store, StoreError};

use crate::channel::{FeedTx, Msg};

/// Streams one input object's bytes into `tx` chunk by chunk, counting them in `read`. The
/// object must still be the `planned` version: one that changed or vanished since the plan
/// fails for good (a retry cannot bring the planned bytes back, and reading the new ones would
/// mix inputs within the run).
///
/// Returns `Ok` also when the consumer stopped reading (its result explains why). On a store
/// error the consumer gets [`Msg::Failed`] and this returns the error.
pub(crate) async fn feed_object_stream(
    store: Store,
    key: String,
    planned: ObjectVersion,
    tx: FeedTx<Bytes>,
    read: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let result = async {
        let mut stream = match store.get_stream_if_match(&key, &planned).await {
            Ok(stream) => stream,
            Err(e) => return Err(input_error(&store, &key, &planned, e, "opening").await),
        };
        loop {
            let chunk = match stream.try_next().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(e) => return Err(input_error(&store, &key, &planned, e, "reading").await),
            };
            if chunk.is_empty() {
                // The local filesystem yields empty chunks without end once the file it reads
                // is cut short under it.
                still_planned(&store, &key, &planned).await?;
                continue;
            }
            read.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if tx.send(Msg::Item(chunk)).await.is_err() {
                return Ok(false);
            }
        }
        // The local filesystem checks the version when it opens the file, then reads whatever
        // the file holds: an in-place overwrite during the read shows only here.
        still_planned(&store, &key, &planned).await?;
        Ok::<bool, anyhow::Error>(true)
    }
    .await;
    finish(&tx, result).await
}

/// Fails, as [`input_error`] does, unless `key` still carries the planned etag. A read of a
/// version id got that version's bytes whatever came after, so it passes unchecked.
async fn still_planned(store: &Store, key: &str, planned: &ObjectVersion) -> anyhow::Result<()> {
    if planned.version.is_some() {
        return Ok(());
    }
    let now = store
        .head(key)
        .await
        .with_context(|| format!("checking {key} after reading it"))?;
    if now.is_some_and(|info| info.version.e_tag == planned.e_tag) {
        return Ok(());
    }
    let changed = StoreError::PreconditionFailed {
        key: key.to_owned(),
    };
    Err(input_error(store, key, planned, changed, "reading").await)
}

/// A failed read of a planned input: permanent when the input is no longer the planned
/// version, retryable (a store error) otherwise.
async fn input_error(
    store: &Store,
    key: &str,
    planned: &ObjectVersion,
    err: StoreError,
    doing: &str,
) -> anyhow::Error {
    match err {
        StoreError::PreconditionFailed { .. } if planned.is_empty() => permanent(anyhow::anyhow!(
            "plan.json records no version for {key} (a plan from an older release): start a new run"
        )),
        StoreError::PreconditionFailed { .. } => {
            let now = match store.head(key).await {
                Ok(Some(info)) => info.version.to_string(),
                Ok(None) => "absent".to_owned(),
                Err(e) => format!("unknown ({e})"),
            };
            permanent(anyhow::anyhow!(
                "{key} changed since the run was planned (planned {planned}, now {now}): \
                 inputs must not change under a run; start a new run"
            ))
        }
        StoreError::NotFound { .. } => permanent(anyhow::anyhow!(
            "{key} was removed since the run was planned (planned {planned}): \
             inputs must not change under a run; start a new run"
        )),
        other => anyhow::Error::new(other).context(format!("{doing} {key}")),
    }
}

/// Reads whole objects (segments) in order, `prefetch` at a time, into `tx`.
pub(crate) async fn feed_objects(
    store: Store,
    keys: Vec<String>,
    prefetch: usize,
    tx: FeedTx<(String, Bytes)>,
    read: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let result = async {
        let mut objects = futures::stream::iter(keys)
            .map(|key| {
                let store = store.clone();
                async move {
                    let object = store
                        .get(&key)
                        .await
                        .with_context(|| format!("reading {key}"))?;
                    Ok::<_, anyhow::Error>((key, object.bytes))
                }
            })
            .buffered(prefetch.max(1));
        while let Some(next) = objects.next().await {
            let (key, bytes) = next?;
            read.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if tx.send(Msg::Item((key, bytes))).await.is_err() {
                return Ok(false);
            }
        }
        Ok::<bool, anyhow::Error>(true)
    }
    .await;
    finish(&tx, result).await
}

/// Sends the end marker matching `result`: `Done` after a complete feed, `Failed` after an
/// error. Nothing when the consumer already stopped.
async fn finish<T>(tx: &FeedTx<T>, result: anyhow::Result<bool>) -> anyhow::Result<()> {
    match result {
        Ok(true) => {
            // A consumer that is gone no longer needs the marker.
            let _ = tx.send(Msg::Done).await;
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(e) => {
            let _ = tx.send(Msg::Failed).await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::input_channel;

    #[tokio::test]
    async fn objects_arrive_in_order_then_done() {
        let store = Store::memory();
        for k in ["a/1", "a/2", "a/3"] {
            store.put(k, Bytes::from(k.to_owned())).await.unwrap();
        }
        let (tx, mut inbox) = input_channel(1);
        let read = Arc::new(AtomicU64::new(0));
        let feeder = tokio::spawn(feed_objects(
            store,
            vec!["a/1".into(), "a/2".into(), "a/3".into()],
            2,
            tx,
            Arc::clone(&read),
        ));
        let got = tokio::task::spawn_blocking(move || {
            let mut keys = Vec::new();
            while let Some((k, _)) = inbox.next_blocking().unwrap() {
                keys.push(k);
            }
            keys
        })
        .await
        .unwrap();
        feeder.await.unwrap().unwrap();
        assert_eq!(got, ["a/1", "a/2", "a/3"]);
        assert_eq!(read.load(Ordering::Relaxed), 9);
    }

    /// Feeds `key` as planned at `planned` and returns what arrived, or the feeder's error.
    async fn feed_input(
        store: &Store,
        key: &str,
        planned: ObjectVersion,
    ) -> anyhow::Result<Vec<u8>> {
        let (tx, mut inbox) = input_channel(4);
        let feeder = tokio::spawn(feed_object_stream(
            store.clone(),
            key.to_owned(),
            planned,
            tx,
            Arc::default(),
        ));
        let got = tokio::task::spawn_blocking(move || {
            let mut body = Vec::new();
            while let Ok(Some(chunk)) = inbox.next_blocking() {
                body.extend_from_slice(&chunk);
            }
            body
        })
        .await
        .unwrap();
        feeder.await.unwrap().map(|()| got)
    }

    async fn an_input_must_be_the_planned_version(store: Store) {
        let key = "raw/persons/part-00000.ndjson.gz";
        let planned = store
            .put(key, Bytes::from_static(b"planned"))
            .await
            .unwrap();
        assert_eq!(
            feed_input(&store, key, planned.clone()).await.unwrap(),
            b"planned",
            "an unchanged input reads as usual"
        );

        store
            .put(key, Bytes::from_static(b"replaced, longer"))
            .await
            .unwrap();
        let now = store.head(key).await.unwrap().unwrap().version;
        let err = feed_input(&store, key, planned.clone()).await.unwrap_err();
        let text = format!("{err:#}");
        assert!(pi_coord::is_permanent(&err), "{text}");
        assert!(text.contains(key), "{text}");
        assert!(text.contains(&planned.to_string()), "{text}");
        assert!(text.contains(&now.to_string()), "{text}");

        let err = feed_input(&store, key, ObjectVersion::default())
            .await
            .unwrap_err();
        assert!(pi_coord::is_permanent(&err), "{err:#}");
        assert!(format!("{err:#}").contains("records no version"), "{err:#}");

        store.delete(key).await.unwrap();
        let err = feed_input(&store, key, now).await.unwrap_err();
        assert!(pi_coord::is_permanent(&err), "{err:#}");
        assert!(format!("{err:#}").contains("was removed"), "{err:#}");
    }

    #[tokio::test]
    async fn a_changed_input_fails_for_good_in_memory() {
        an_input_must_be_the_planned_version(Store::memory()).await;
    }

    #[tokio::test]
    async fn a_changed_input_fails_for_good_on_a_local_store() {
        let dir = tempfile::tempdir().unwrap();
        an_input_must_be_the_planned_version(Store::local(dir.path()).unwrap()).await;
    }

    /// Plans a local input of several chunks, lets `overwrite` change the file in place (same
    /// inode) after the first chunk arrives, and checks that the feed fails for good.
    async fn overwrite_while_reading(overwrite: impl FnOnce(&mut std::fs::File) + Send + 'static) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::local(dir.path()).unwrap();
        let key = "raw/persons/part-00000.ndjson.gz";
        let planned = store
            .put(key, Bytes::from(vec![b'a'; 64 * 1024]))
            .await
            .unwrap();
        let path = dir.path().join(key);
        let (tx, mut inbox) = input_channel(1);
        let feeder = tokio::spawn(feed_object_stream(
            store,
            key.to_owned(),
            planned.clone(),
            tx,
            Arc::default(),
        ));
        let consumer = tokio::task::spawn_blocking(move || {
            let first = inbox.next_blocking().unwrap().unwrap();
            assert!(
                first.len() < 64 * 1024,
                "the input arrives in several chunks"
            );
            let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            overwrite(&mut file);
            drop(file);
            loop {
                match inbox.next_blocking() {
                    Ok(Some(_)) => {}
                    Ok(None) => return false,
                    Err(_) => return true,
                }
            }
        });
        let err = feeder.await.unwrap().unwrap_err();
        assert!(
            consumer.await.unwrap(),
            "the consumer is told the feed failed"
        );
        let text = format!("{err:#}");
        assert!(pi_coord::is_permanent(&err), "{text}");
        assert!(text.contains("changed since the run was planned"), "{text}");
        assert!(text.contains(&planned.to_string()), "{text}");
    }

    #[tokio::test]
    async fn a_local_input_overwritten_in_place_during_the_read_fails_for_good() {
        overwrite_while_reading(|file| {
            use std::io::Write as _;
            file.write_all(&vec![b'b'; 80 * 1024]).unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn a_local_input_cut_short_during_the_read_fails_for_good() {
        overwrite_while_reading(|file| file.set_len(1000).unwrap()).await;
    }

    #[tokio::test]
    async fn a_missing_object_fails_the_feed_and_the_consumer() {
        let store = Store::memory();
        let (tx, mut inbox) = input_channel(1);
        let feeder = tokio::spawn(feed_objects(
            store,
            vec!["missing".into()],
            1,
            tx,
            Arc::default(),
        ));
        let got = tokio::task::spawn_blocking(move || inbox.next_blocking())
            .await
            .unwrap();
        assert!(got.is_err());
        assert!(feeder.await.unwrap().is_err());
    }
}
