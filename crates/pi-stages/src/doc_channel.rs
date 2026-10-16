//! The `load` stage's hand-off from the decoder (blocking side) to the bulk indexer (async
//! side), bounded by bytes rather than by a count of documents.
//!
//! Each document takes permits from a semaphore for its body length before it is queued and
//! gives them back once the indexer has copied it into its bulk buffer, so the decoded
//! documents waiting at any time hold at most the budget, or one document when a single
//! document is larger than the whole budget (it then takes the whole budget and passes
//! alone). The document the decoder is waiting to queue comes on top of that.

use std::sync::Arc;

use pi_model::DocRow;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Permits are counted in units of this many bytes, so any budget fits the semaphore's `u32`
/// acquire count. A document takes at least one unit, which also bounds how many documents
/// can be queued.
const UNIT: usize = 1 << 10;

/// A queued document with the share of the budget it holds; dropping it gives the share back.
#[derive(Debug)]
pub(crate) struct Budgeted {
    /// The document.
    pub(crate) doc: DocRow,
    _permit: OwnedSemaphorePermit,
}

/// The decoder's end.
#[derive(Debug)]
pub(crate) struct DocSender {
    tx: mpsc::UnboundedSender<Budgeted>,
    budget: Arc<Semaphore>,
    units: u32,
}

/// The indexer's end. Dropping it (the task was cancelled or the indexer failed) closes the
/// budget, so a decoder waiting for room stops instead of waiting forever, and the documents
/// still queued give their share back.
#[derive(Debug)]
pub(crate) struct DocReceiver {
    rx: mpsc::UnboundedReceiver<Budgeted>,
    budget: Arc<Semaphore>,
}

/// A channel holding at most `budget_bytes` of document bodies (see the module docs).
pub(crate) fn doc_channel(budget_bytes: usize) -> (DocSender, DocReceiver) {
    let units = (budget_bytes / UNIT).clamp(1, Semaphore::MAX_PERMITS);
    let units = u32::try_from(units).unwrap_or(u32::MAX);
    let budget = Arc::new(Semaphore::new(units as usize));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        DocSender {
            tx,
            budget: Arc::clone(&budget),
            units,
        },
        DocReceiver { rx, budget },
    )
}

impl DocSender {
    /// The units `len` bytes take: rounded up, at least one, at most the whole budget.
    fn units_for(&self, len: usize) -> u32 {
        let units = len.div_ceil(UNIT).max(1);
        u32::try_from(units).map_or(self.units, |u| u.min(self.units))
    }

    /// Queues `doc` once the budget has room for it. Blocks: call it on a blocking thread
    /// only. Fails when the indexer's end is gone.
    pub(crate) fn send_blocking(&self, doc: DocRow) -> anyhow::Result<()> {
        let units = self.units_for(doc.body.len());
        let permit =
            futures::executor::block_on(Arc::clone(&self.budget).acquire_many_owned(units))
                .map_err(|_| anyhow::anyhow!("the bulk indexer stopped"))?;
        self.tx
            .send(Budgeted {
                doc,
                _permit: permit,
            })
            .map_err(|_| anyhow::anyhow!("the bulk indexer stopped"))
    }
}

impl DocReceiver {
    /// The next document, `None` once the decoder's end is gone and the queue is empty.
    pub(crate) async fn recv(&mut self) -> Option<Budgeted> {
        self.rx.recv().await
    }
}

impl Drop for DocReceiver {
    fn drop(&mut self) {
        self.budget.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;

    fn doc(id: u64, len: usize) -> DocRow {
        DocRow {
            person_id: id,
            body: vec![b'x'; len],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_decoder_never_holds_more_than_the_budget_and_one_document() {
        const BUDGET: usize = 64 << 10;
        const MAX_DOC: usize = 9 << 10;
        let (tx, mut rx) = doc_channel(BUDGET);
        // Bytes the decoder holds: counted before a document is offered, uncounted when the
        // indexer gives its share back, both under the lock so the count is exact.
        let held = Arc::new(Mutex::new((0usize, 0usize)));
        let sizes: Vec<usize> = (0..400).map(|i| (i * 7919) % MAX_DOC + 1).collect();
        let total = sizes.len();

        let producer = {
            let held = Arc::clone(&held);
            let sizes = sizes.clone();
            tokio::task::spawn_blocking(move || {
                for (i, len) in sizes.into_iter().enumerate() {
                    {
                        let mut h = held.lock().unwrap();
                        h.0 += len;
                        h.1 = h.1.max(h.0);
                    }
                    tx.send_blocking(doc(i as u64, len)).unwrap();
                }
            })
        };
        let mut got = 0;
        while let Some(item) = rx.recv().await {
            // A slow indexer, so the decoder runs into the budget.
            if got % 16 == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let mut h = held.lock().unwrap();
            h.0 -= item.doc.body.len();
            drop(item);
            got += 1;
        }
        producer.await.unwrap();

        assert_eq!(got, total);
        let (now, peak) = *held.lock().unwrap();
        assert_eq!(now, 0);
        assert!(peak <= BUDGET + MAX_DOC, "peak {peak}");
        assert!(
            peak > BUDGET / 2,
            "the slow indexer filled the budget: {peak}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_document_larger_than_the_budget_passes_alone() {
        let (tx, mut rx) = doc_channel(4 << 10);
        let producer = tokio::task::spawn_blocking(move || {
            tx.send_blocking(doc(1, 100 << 10))?;
            tx.send_blocking(doc(2, 10))?;
            tx.send_blocking(doc(3, 1 << 20))
        });
        let mut ids = Vec::new();
        while let Some(item) = rx.recv().await {
            ids.push((item.doc.person_id, item.doc.body.len()));
        }
        producer.await.unwrap().unwrap();
        assert_eq!(ids, [(1, 100 << 10), (2, 10), (3, 1 << 20)]);

        // A budget below one unit still lets every document through, one at a time.
        let (tx, mut rx) = doc_channel(0);
        let producer = tokio::task::spawn_blocking(move || {
            (0..3).try_for_each(|i| tx.send_blocking(doc(i, 5000)))
        });
        let mut n = 0;
        while rx.recv().await.is_some() {
            n += 1;
        }
        producer.await.unwrap().unwrap();
        assert_eq!(n, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_releases_the_budget_and_the_waiting_decoder() {
        let (tx, rx) = doc_channel(8 << 10);
        let budget = Arc::clone(&tx.budget);
        let producer = tokio::task::spawn_blocking(move || {
            for i in 0..4 {
                tx.send_blocking(doc(i, 4 << 10))?;
            }
            Ok::<(), anyhow::Error>(())
        });
        // Two documents fill the budget; the third waits for room.
        tokio::time::timeout(Duration::from_secs(5), async {
            while budget.available_permits() > 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("two documents fill the budget");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!producer.is_finished(), "the decoder waits for room");

        // The indexer's end goes away (its future dropped on cancellation).
        drop(rx);
        let err = tokio::time::timeout(Duration::from_secs(5), producer)
            .await
            .expect("the waiting decoder is released")
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("stopped"), "{err}");
        assert_eq!(
            budget.available_permits(),
            8,
            "queued documents gave their share back"
        );
    }

    #[tokio::test]
    async fn a_document_dropped_by_the_indexer_gives_its_share_back() {
        let (tx, mut rx) = doc_channel(8 << 10);
        let budget = Arc::clone(&tx.budget);
        tokio::task::spawn_blocking(move || tx.send_blocking(doc(1, 3000)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.available_permits(), 5);
        let item = rx.recv().await.unwrap();
        assert_eq!(
            budget.available_permits(),
            5,
            "held until the indexer is done"
        );
        drop(item);
        assert_eq!(budget.available_permits(), 8);
        assert!(rx.recv().await.is_none());
    }
}
