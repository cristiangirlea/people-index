//! A [`Sink`] writing into a local directory.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::sink::{Sink, SinkWriter, path_segments};

/// Writes files under a root directory, each through a `.partial` file renamed into place
/// on `finish`, so readers never see a half-written file under its final name.
#[derive(Debug, Clone)]
pub struct LocalDirSink {
    root: PathBuf,
}

impl LocalDirSink {
    /// A sink rooted at `root`, created if missing.
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// The root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Sink for LocalDirSink {
    fn create(&mut self, path: &str) -> io::Result<Box<dyn SinkWriter>> {
        let mut dest = self.root.clone();
        for segment in path_segments(path)? {
            dest.push(segment);
        }
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut partial = dest.clone().into_os_string();
        partial.push(".partial");
        let partial = PathBuf::from(partial);
        let file = File::create(&partial)?;
        Ok(Box::new(LocalFile {
            out: Some(BufWriter::with_capacity(256 << 10, file)),
            partial,
            dest,
        }))
    }
}

struct LocalFile {
    out: Option<BufWriter<File>>,
    partial: PathBuf,
    dest: PathBuf,
}

impl LocalFile {
    fn out(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.out
            .as_mut()
            .ok_or_else(|| io::Error::other("write after finish"))
    }
}

impl Write for LocalFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.out()?.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.out()?.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out()?.flush()
    }
}

impl SinkWriter for LocalFile {
    fn finish(mut self: Box<Self>) -> io::Result<()> {
        let out = self
            .out
            .take()
            .ok_or_else(|| io::Error::other("finished twice"))?;
        let file = out.into_inner().map_err(io::IntoInnerError::into_error)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&self.partial, &self.dest)
    }
}

impl Drop for LocalFile {
    fn drop(&mut self) {
        if self.out.take().is_some() {
            // Abandoned: best effort, the run has already failed for another reason.
            let _ = fs::remove_file(&self.partial);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn files_appear_only_on_finish() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalDirSink::new(dir.path().join("out")).unwrap();
        let mut w = sink.create("a/b.txt").unwrap();
        w.write_all(b"hello").unwrap();
        let dest = sink.root().join("a").join("b.txt");
        assert!(!dest.exists());
        w.finish().unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"hello");
        assert!(!sink.root().join("a").join("b.txt.partial").exists());
    }

    #[test]
    fn abandoned_files_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalDirSink::new(dir.path()).unwrap();
        let mut w = sink.create("x.txt").unwrap();
        w.write_all(b"partial").unwrap();
        drop(w);
        assert!(!dir.path().join("x.txt").exists());
        assert!(!dir.path().join("x.txt.partial").exists());
    }

    #[test]
    fn put_and_path_checks() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalDirSink::new(dir.path()).unwrap();
        sink.put("expected.json", b"{}").unwrap();
        assert_eq!(fs::read(dir.path().join("expected.json")).unwrap(), b"{}");
        assert!(sink.create("../escape").is_err());
    }
}
