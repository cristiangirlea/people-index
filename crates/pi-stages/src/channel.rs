//! The channels between a task's async side (store reads and writes) and its blocking side
//! (gzip, JSON, zstd, hashing).
//!
//! Both are bounded, so a slow consumer holds the producer back and a task's memory stays at
//! a few chunks or segments. The input channel carries an explicit end marker: a producer
//! that vanishes (its future dropped on cancellation) must never look like a clean end of
//! input to the blocking side, or it would write and upload a truncated result.

use bytes::Bytes;
use tokio::sync::mpsc;

/// One message from a feeder.
#[derive(Debug)]
pub(crate) enum Msg<T> {
    /// The next item.
    Item(T),
    /// The feeder failed; its own future returns the error.
    Failed,
    /// Every item was sent.
    Done,
}

/// The input side of the blocking work: items until a clean end, or an error.
#[derive(Debug)]
pub(crate) struct Inbox<T> {
    rx: mpsc::Receiver<Msg<T>>,
    finished: bool,
}

/// Why the blocking side stopped reading its input early.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InputError {
    /// The feeder reported a failure (its future returns the cause).
    #[error("the input feeder failed")]
    FeederFailed,
    /// The feeder went away without finishing (the task was cancelled).
    #[error("the input feeder stopped before the end of its input")]
    FeederGone,
}

impl<T> Inbox<T> {
    /// The next item, `None` at the clean end. Blocks: call it on a blocking thread only.
    pub(crate) fn next_blocking(&mut self) -> Result<Option<T>, InputError> {
        if self.finished {
            return Ok(None);
        }
        match self.rx.blocking_recv() {
            Some(Msg::Item(item)) => Ok(Some(item)),
            Some(Msg::Done) => {
                self.finished = true;
                Ok(None)
            }
            Some(Msg::Failed) => Err(InputError::FeederFailed),
            None => Err(InputError::FeederGone),
        }
    }
}

/// The feeder's end of an input channel.
pub(crate) type FeedTx<T> = mpsc::Sender<Msg<T>>;

/// A bounded input channel holding `capacity` items.
pub(crate) fn input_channel<T>(capacity: usize) -> (FeedTx<T>, Inbox<T>) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (
        tx,
        Inbox {
            rx,
            finished: false,
        },
    )
}

/// One object to write.
#[derive(Debug)]
pub(crate) struct Upload {
    pub(crate) key: String,
    pub(crate) bytes: Bytes,
}

/// The blocking side's way to hand finished objects to the uploader.
#[derive(Debug, Clone)]
pub(crate) struct Outbox {
    tx: mpsc::Sender<Upload>,
}

impl Outbox {
    /// Queues an object for upload, waiting while the uploader is busy. Blocks: call it on a
    /// blocking thread only. Fails when the uploader stopped (an upload failed, or the task
    /// was cancelled); the uploader's own result says why.
    pub(crate) fn send_blocking(&self, key: String, bytes: Bytes) -> anyhow::Result<()> {
        self.tx
            .blocking_send(Upload { key, bytes })
            .map_err(|_| anyhow::anyhow!("the uploader stopped"))
    }
}

/// A bounded upload channel holding `capacity` objects.
pub(crate) fn upload_channel(capacity: usize) -> (Outbox, mpsc::Receiver<Upload>) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    (Outbox { tx }, rx)
}

/// Bytes chunks of one object.
pub(crate) type ChunkInbox = Inbox<Bytes>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_vanished_feeder_is_not_a_clean_end() {
        let (tx, mut inbox) = input_channel::<u32>(4);
        tx.send(Msg::Item(1)).await.unwrap();
        drop(tx);
        let got =
            tokio::task::spawn_blocking(move || (inbox.next_blocking(), inbox.next_blocking()))
                .await
                .unwrap();
        assert_eq!(got.0, Ok(Some(1)));
        assert_eq!(got.1, Err(InputError::FeederGone));
    }

    #[tokio::test]
    async fn done_and_failed_markers() {
        let (tx, mut inbox) = input_channel::<u32>(4);
        tx.send(Msg::Done).await.unwrap();
        drop(tx);
        let got =
            tokio::task::spawn_blocking(move || (inbox.next_blocking(), inbox.next_blocking()))
                .await
                .unwrap();
        assert_eq!(got, (Ok(None), Ok(None)));

        let (tx, mut inbox) = input_channel::<u32>(4);
        tx.send(Msg::Failed).await.unwrap();
        let got = tokio::task::spawn_blocking(move || inbox.next_blocking())
            .await
            .unwrap();
        assert_eq!(got, Err(InputError::FeederFailed));
    }
}
