//! [`ChannelReader`]: an object's byte stream, arriving through a channel, as a blocking
//! `io::Read` for the gzip decoder.

use std::io::{self, Read};

use bytes::{Buf, Bytes};

use crate::channel::{ChunkInbox, InputError};

/// Reads the chunks a feeder sends. A feeder failure surfaces as an `io::Error` wrapping an
/// [`InputError`], so the caller can tell "the store failed" (retry the task) from "the
/// bytes are bad" (the gzip decoder's own errors: fail the task for good).
#[derive(Debug)]
pub(crate) struct ChannelReader {
    inbox: ChunkInbox,
    current: Bytes,
}

impl ChannelReader {
    pub(crate) fn new(inbox: ChunkInbox) -> Self {
        Self {
            inbox,
            current: Bytes::new(),
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.current.is_empty() {
            match self.inbox.next_blocking() {
                Ok(Some(chunk)) => self.current = chunk,
                Ok(None) => return Ok(0),
                Err(e) => return Err(io::Error::other(e)),
            }
        }
        let n = buf.len().min(self.current.len());
        self.current.copy_to_slice(&mut buf[..n]);
        Ok(n)
    }
}

/// True when an I/O error came from the feeder (the store), not from the data.
pub(crate) fn is_input_error(err: &io::Error) -> bool {
    err.get_ref()
        .is_some_and(<dyn std::error::Error + Send + Sync>::is::<InputError>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{Msg, input_channel};

    #[tokio::test]
    async fn reads_across_chunks_and_flags_feeder_errors() {
        let (tx, inbox) = input_channel(4);
        tx.send(Msg::Item(Bytes::from_static(b"abc")))
            .await
            .unwrap();
        tx.send(Msg::Item(Bytes::new())).await.unwrap();
        tx.send(Msg::Item(Bytes::from_static(b"de"))).await.unwrap();
        tx.send(Msg::Failed).await.unwrap();
        let (data, err) = tokio::task::spawn_blocking(move || {
            let mut r = ChannelReader::new(inbox);
            let mut out = vec![0u8; 5];
            r.read_exact(&mut out).unwrap();
            let err = r.read(&mut [0u8; 4]).unwrap_err();
            (out, err)
        })
        .await
        .unwrap();
        assert_eq!(data, b"abcde");
        assert!(is_input_error(&err));
        assert!(!is_input_error(&io::Error::other("corrupt")));
    }
}
