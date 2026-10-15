//! [`StoreSink`]: lets `pi-datagen` write straight into an object store (`s3://`, `file://`).
//!
//! The generator runs on rayon threads and writes through `std::io::Write`; each file is a
//! streaming (multipart, for large files) upload driven by `Handle::block_on`, so a file never
//! has to fit in memory and becomes visible only when its upload completes.

use std::io::{self, Write};

use pi_datagen::{Sink, SinkWriter, path_segments};
use pi_store::{ObjectWriter, Store};
use tokio::io::AsyncWriteExt as _;
use tokio::runtime::Handle;

/// A datagen sink writing `<prefix>/<path>` objects into a store.
#[derive(Debug, Clone)]
pub struct StoreSink {
    store: Store,
    prefix: String,
    handle: Handle,
}

impl StoreSink {
    /// A sink under `prefix` (`""` for the store's root). `handle` drives the uploads: the
    /// generator must run off the runtime's worker threads (in `spawn_blocking`).
    #[must_use]
    pub fn new(store: Store, prefix: &str, handle: Handle) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
            handle,
        }
    }

    fn key(&self, path: &str) -> io::Result<String> {
        path_segments(path)?;
        Ok(if self.prefix.is_empty() {
            path.to_owned()
        } else {
            format!("{}/{path}", self.prefix)
        })
    }
}

impl Sink for StoreSink {
    fn create(&mut self, path: &str) -> io::Result<Box<dyn SinkWriter>> {
        let key = self.key(path)?;
        let writer = self.store.writer(&key).map_err(io::Error::other)?;
        Ok(Box::new(StoreFile {
            writer,
            handle: self.handle.clone(),
        }))
    }

    fn put(&mut self, path: &str, bytes: &[u8]) -> io::Result<()> {
        let key = self.key(path)?;
        let store = self.store.clone();
        let body = bytes.to_vec();
        self.handle
            .block_on(async move { store.put(&key, body).await })
            .map(|_| ())
            .map_err(io::Error::other)
    }
}

/// One file being uploaded. Dropping it unfinished aborts the upload.
struct StoreFile {
    writer: ObjectWriter,
    handle: Handle,
}

impl Write for StoreFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let writer = &mut self.writer;
        self.handle.block_on(writer.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        let writer = &mut self.writer;
        self.handle.block_on(writer.flush())
    }
}

impl SinkWriter for StoreFile {
    fn finish(self: Box<Self>) -> io::Result<()> {
        let StoreFile { mut writer, handle } = *self;
        handle.block_on(writer.shutdown())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn files_appear_only_when_finished() {
        let store = Store::memory();
        let handle = Handle::current();
        let s = store.clone();
        tokio::task::spawn_blocking(move || {
            let mut sink = StoreSink::new(s.clone(), "raw/", handle);
            let mut w = sink.create("persons/part-00000.ndjson.gz").unwrap();
            w.write_all(b"hello").unwrap();
            assert!(
                Handle::current()
                    .block_on(s.head("raw/persons/part-00000.ndjson.gz"))
                    .unwrap()
                    .is_none(),
                "not visible before finish"
            );
            w.finish().unwrap();
            sink.put("expected.json", b"{}").unwrap();
            assert!(sink.create("../escape").is_err());
            let abandoned = sink.create("persons/part-00001.ndjson.gz").unwrap();
            drop(abandoned);
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .get("raw/persons/part-00000.ndjson.gz")
                .await
                .unwrap()
                .bytes,
            &b"hello"[..]
        );
        assert!(store.head("raw/expected.json").await.unwrap().is_some());
        assert!(
            store
                .head("raw/persons/part-00001.ndjson.gz")
                .await
                .unwrap()
                .is_none()
        );
    }
}
