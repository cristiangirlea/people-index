//! Feeders: the async side that reads from the store and fills an input channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use pi_store::Store;

use crate::channel::{FeedTx, Msg};

/// Streams one object's bytes into `tx` chunk by chunk, counting them in `read`.
///
/// Returns `Ok` also when the consumer stopped reading (its result explains why). On a store
/// error the consumer gets [`Msg::Failed`] and this returns the error.
pub(crate) async fn feed_object_stream(
    store: Store,
    key: String,
    tx: FeedTx<Bytes>,
    read: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let result = async {
        let mut stream = store
            .get_stream(&key)
            .await
            .with_context(|| format!("opening {key}"))?;
        while let Some(chunk) = stream
            .try_next()
            .await
            .with_context(|| format!("reading {key}"))?
        {
            read.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if tx.send(Msg::Item(chunk)).await.is_err() {
                return Ok(false);
            }
        }
        Ok::<bool, anyhow::Error>(true)
    }
    .await;
    finish(&tx, result).await
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
