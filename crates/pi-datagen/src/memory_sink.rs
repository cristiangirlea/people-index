//! A [`Sink`] that keeps files in memory, for tests and small in-process runs.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, PoisonError};

use crate::sink::{Sink, SinkWriter, path_segments};

type Files = Arc<Mutex<BTreeMap<String, Vec<u8>>>>;

/// Holds finished files by path. Cloning shares the same storage.
#[derive(Debug, Clone, Default)]
pub struct MemorySink {
    files: Files,
}

impl MemorySink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of every finished file, by path.
    #[must_use]
    pub fn files(&self) -> BTreeMap<String, Vec<u8>> {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// One finished file.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<Vec<u8>> {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(path)
            .cloned()
    }
}

impl Sink for MemorySink {
    fn create(&mut self, path: &str) -> io::Result<Box<dyn SinkWriter>> {
        path_segments(path)?;
        Ok(Box::new(MemoryFile {
            path: path.to_owned(),
            buf: Vec::new(),
            files: Arc::clone(&self.files),
        }))
    }
}

struct MemoryFile {
    path: String,
    buf: Vec<u8>,
    files: Files,
}

impl Write for MemoryFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SinkWriter for MemoryFile {
    fn finish(self: Box<Self>) -> io::Result<()> {
        let MemoryFile { path, buf, files } = *self;
        files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path, buf);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_finished_files_are_kept() {
        let mut sink = MemorySink::new();
        let mut a = sink.create("a").unwrap();
        a.write_all(b"1").unwrap();
        let b = sink.create("b").unwrap();
        a.finish().unwrap();
        drop(b);
        assert_eq!(sink.get("a").unwrap(), b"1");
        assert!(sink.get("b").is_none());
        assert_eq!(sink.files().len(), 1);
    }
}
