//! The uploader: the async side that writes what the blocking side finished.

use anyhow::Context as _;
use futures::{StreamExt, TryStreamExt};
use pi_store::Store;
use tokio::sync::mpsc;

use crate::channel::Upload;

/// What a task uploaded: the keys its commit lists, and their size.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Uploaded {
    /// Every key written, sorted (the commit lists them in a stable order).
    pub(crate) keys: Vec<String>,
    /// Bytes written.
    pub(crate) bytes: u64,
}

/// Writes every object received on `rx`, `concurrency` at a time, until the sender is
/// dropped. The first failure stops it; dropping `rx` then fails the blocking side's next
/// send, so it stops too.
pub(crate) async fn upload_all(
    store: Store,
    rx: mpsc::Receiver<Upload>,
    concurrency: usize,
) -> anyhow::Result<Uploaded> {
    let incoming = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|upload| (upload, rx))
    });
    let done = incoming
        .map(|upload| {
            let store = store.clone();
            async move {
                let len = upload.bytes.len() as u64;
                store
                    .put(&upload.key, upload.bytes)
                    .await
                    .with_context(|| format!("writing {}", upload.key))?;
                Ok::<_, anyhow::Error>((upload.key, len))
            }
        })
        .buffer_unordered(concurrency.max(1));
    let mut done = std::pin::pin!(done);
    let mut uploaded = Uploaded::default();
    while let Some((key, len)) = done.try_next().await? {
        uploaded.keys.push(key);
        uploaded.bytes += len;
    }
    uploaded.keys.sort();
    Ok(uploaded)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::channel::upload_channel;

    #[tokio::test]
    async fn uploads_everything_and_sorts_the_keys() {
        let store = Store::memory();
        let (outbox, rx) = upload_channel(1);
        let up = tokio::spawn(upload_all(store.clone(), rx, 2));
        tokio::task::spawn_blocking(move || {
            for k in ["b", "a", "c"] {
                outbox
                    .send_blocking(format!("x/{k}"), Bytes::from_static(b"12"))
                    .unwrap();
            }
        })
        .await
        .unwrap();
        let uploaded = up.await.unwrap().unwrap();
        assert_eq!(uploaded.keys, ["x/a", "x/b", "x/c"]);
        assert_eq!(uploaded.bytes, 6);
        assert_eq!(store.list_keys("x").await.unwrap().len(), 3);
    }
}
