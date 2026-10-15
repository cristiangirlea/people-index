//! The bulk indexer a `load` task feeds: documents in, byte-bounded bulk requests out, with
//! `senders` requests in flight and the producer held back when all of them are busy.

use std::sync::Arc;
use std::time::Instant;

use tokio::task::JoinSet;

use super::buffer::BulkBuffer;
use super::counters::{BulkCounters, BulkStats};
use super::sender::Sender;
use crate::client::EsClient;
use crate::error::{EsError, Result};
use crate::names::validate_index_name;

/// Load pressure knobs (`PI_BULK_MAX_BYTES`, `PI_SENDERS_PER_WORKER`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkConfig {
    /// The largest bulk body, in bytes. A single document larger than this is an error.
    pub max_bytes: usize,
    /// Bulk requests in flight at once.
    pub senders: usize,
    /// Permanent item failures quoted in the error.
    pub max_reasons: usize,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            max_bytes: 8 << 20,
            senders: 2,
            max_reasons: 5,
        }
    }
}

/// Feeds documents to Elasticsearch in bulk.
///
/// Memory is `(senders + 1) * max_bytes` for the bodies, allocated up front and reused. Call
/// [`BulkIndexer::finish`] at the end: dropping the indexer aborts requests still in flight,
/// which is right after an error and wrong after success.
#[derive(Debug)]
pub struct BulkIndexer {
    sender: Arc<Sender>,
    current: BulkBuffer,
    free: Vec<BulkBuffer>,
    in_flight: JoinSet<Result<BulkBuffer>>,
    counters: Arc<BulkCounters>,
    max_bytes: usize,
    failed: bool,
}

impl BulkIndexer {
    /// An indexer writing into `index` (a concrete index or an alias with one write index).
    pub fn new(client: EsClient, index: &str, config: BulkConfig) -> Result<Self> {
        validate_index_name(index)?;
        if config.senders == 0 {
            return Err(EsError::Config(
                "senders_per_worker must be at least 1".into(),
            ));
        }
        // The smallest useful body holds one small document; below that nothing could be sent.
        if config.max_bytes < 1024 {
            return Err(EsError::Config(
                "bulk_max_bytes must be at least 1 KiB".into(),
            ));
        }
        let counters = Arc::new(BulkCounters::default());
        Ok(Self {
            sender: Arc::new(Sender::new(
                client,
                index,
                config.max_reasons,
                Arc::clone(&counters),
            )),
            current: BulkBuffer::new(config.max_bytes),
            free: (0..config.senders)
                .map(|_| BulkBuffer::new(config.max_bytes))
                .collect(),
            in_flight: JoinSet::new(),
            counters,
            max_bytes: config.max_bytes,
            failed: false,
        })
    }

    /// The live counters, for metrics and progress logs while the load runs.
    #[must_use]
    pub fn counters(&self) -> Arc<BulkCounters> {
        Arc::clone(&self.counters)
    }

    /// Adds one document: `id` becomes `_id`, `body` is its JSON on one line. Waits when every
    /// sender is busy and the current buffer is full.
    pub async fn add(&mut self, id: u64, body: &[u8]) -> Result<()> {
        self.check_usable()?;
        if body.first() != Some(&b'{') {
            return Err(EsError::InvalidDocument {
                id,
                reason: "a document must be a JSON object".into(),
            });
        }
        if memchr::memchr(b'\n', body).is_some() {
            return Err(EsError::InvalidDocument {
                id,
                reason: "a raw line break would split the bulk line".into(),
            });
        }
        let size = BulkBuffer::item_len(id, body.len());
        if size > self.max_bytes {
            return Err(EsError::DocumentTooLarge {
                id,
                size,
                max: self.max_bytes,
            });
        }
        if !self.current.fits(size) {
            self.dispatch().await?;
        }
        self.current.push(id, body);
        self.counters.added();
        Ok(())
    }

    /// Sends what is buffered without waiting for it to be acknowledged.
    pub async fn flush(&mut self) -> Result<()> {
        self.check_usable()?;
        self.dispatch().await
    }

    /// Sends what is left, waits for every request, and returns the final counters. Only
    /// after this returns `Ok` is every added document acknowledged by Elasticsearch.
    pub async fn finish(mut self) -> Result<BulkStats> {
        self.check_usable()?;
        self.dispatch().await?;
        while let Some(joined) = self.in_flight.join_next().await {
            let buffer = self.settle(joined)?;
            self.free.push(buffer);
        }
        Ok(self.counters.snapshot())
    }

    fn check_usable(&self) -> Result<()> {
        if self.failed {
            return Err(EsError::Task(
                "the bulk indexer stopped after an earlier error".into(),
            ));
        }
        Ok(())
    }

    /// Hands the current buffer to a sender task and takes a free one, waiting for a sender
    /// to finish when none is free (the backpressure).
    async fn dispatch(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        while let Some(joined) = self.in_flight.try_join_next() {
            let buffer = self.settle(joined)?;
            self.free.push(buffer);
        }
        if self.free.is_empty() {
            let started = Instant::now();
            let joined = self.in_flight.join_next().await;
            self.counters.backpressure(started.elapsed());
            let Some(joined) = joined else {
                // Unreachable while the buffer count holds; fail rather than spin.
                self.failed = true;
                return Err(EsError::Task("no bulk buffer left".into()));
            };
            let buffer = self.settle(joined)?;
            self.free.push(buffer);
        }
        let Some(next) = self.free.pop() else {
            self.failed = true;
            return Err(EsError::Task("no bulk buffer left".into()));
        };
        let full = std::mem::replace(&mut self.current, next);
        let sender = Arc::clone(&self.sender);
        self.in_flight.spawn(async move { sender.send(full).await });
        Ok(())
    }

    /// The buffer back from a finished task, or its error (which stops the indexer: the other
    /// requests are aborted when it is dropped).
    fn settle(
        &mut self,
        joined: std::result::Result<Result<BulkBuffer>, tokio::task::JoinError>,
    ) -> Result<BulkBuffer> {
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(e) => Err(EsError::Task(format!("bulk sender: {e}"))),
        };
        if outcome.is_err() {
            self.failed = true;
            self.in_flight.abort_all();
        }
        outcome
    }
}
