//! One task as three concurrent parts: a feeder reading the store, blocking work doing the
//! CPU-heavy part on the blocking pool, and an uploader writing what it finishes.

use std::future::Future;

use pi_coord::permanent;
use pi_store::Store;
use tokio::sync::mpsc;

use crate::channel::Upload;
use crate::uploader::{Uploaded, upload_all};

/// Runs `work` on the blocking pool while `feeder` fills its input and the uploader drains its
/// output, and returns the work's result with what was uploaded.
///
/// Errors are reported root cause first: a feeder (store read) error, then an uploader (store
/// write) error, then the work's own error; the work's "input stopped" or "uploader stopped"
/// errors are only symptoms of the first two. If this future is dropped (the task was
/// cancelled), the work notices its channels closing and stops without finishing its output.
pub(crate) async fn run_pipeline<R, F, W>(
    feeder: F,
    store: Store,
    uploads: mpsc::Receiver<Upload>,
    upload_concurrency: usize,
    work: W,
) -> anyhow::Result<(R, Uploaded)>
where
    R: Send + 'static,
    F: Future<Output = anyhow::Result<()>>,
    W: FnOnce() -> anyhow::Result<R> + Send + 'static,
{
    let blocking = tokio::task::spawn_blocking(work);
    let (fed, uploaded, worked) = tokio::join!(
        feeder,
        upload_all(store, uploads, upload_concurrency),
        blocking
    );
    fed?;
    let uploaded = uploaded?;
    let result = worked.map_err(|e| permanent(anyhow::anyhow!("blocking work failed: {e}")))??;
    Ok((result, uploaded))
}

/// Runs blocking work with no store input or output (hashing, decoding a small object).
pub(crate) async fn run_blocking<R, W>(work: W) -> anyhow::Result<R>
where
    R: Send + 'static,
    W: FnOnce() -> anyhow::Result<R> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| permanent(anyhow::anyhow!("blocking work failed: {e}")))?
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::channel::{input_channel, upload_channel};
    use crate::feeder::feed_objects;

    #[tokio::test]
    async fn the_root_cause_wins() {
        // The feeder fails (missing object); the work only sees its input stop.
        let store = Store::memory();
        let (tx, mut inbox) = input_channel::<(String, Bytes)>(1);
        let (outbox, rx) = upload_channel(1);
        let err = run_pipeline(
            feed_objects(store.clone(), vec!["nope".into()], 1, tx, Arc::default()),
            store,
            rx,
            1,
            move || {
                let _outbox = outbox;
                inbox.next_blocking()?;
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("reading nope"), "{err:#}");
    }

    #[tokio::test]
    async fn work_output_is_uploaded() {
        let store = Store::memory();
        store.put("in", Bytes::from_static(b"hello")).await.unwrap();
        let (tx, mut inbox) = input_channel::<(String, Bytes)>(1);
        let (outbox, rx) = upload_channel(1);
        let (n, up) = run_pipeline(
            feed_objects(store.clone(), vec!["in".into()], 1, tx, Arc::default()),
            store.clone(),
            rx,
            1,
            move || {
                let mut n = 0;
                while let Some((_, bytes)) = inbox.next_blocking()? {
                    n += bytes.len();
                    outbox.send_blocking("out".into(), bytes)?;
                }
                Ok(n)
            },
        )
        .await
        .unwrap();
        assert_eq!(n, 5);
        assert_eq!(up.keys, ["out"]);
        assert_eq!(store.get("out").await.unwrap().bytes, &b"hello"[..]);
    }
}
