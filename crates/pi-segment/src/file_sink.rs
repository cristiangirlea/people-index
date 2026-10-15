//! Segments streamed to files in a directory.
//!
//! Files are named `<stem>-<index>.seg`; with the stem `<task>-<attempt>` that is the layout's
//! `<task>-<attempt>-<n>.seg`. A file is written in place, not renamed into place: readers find
//! segments through the task's commit, never by listing, so a half-written file of a crashed
//! attempt is never read. Each file is synced before it is reported finished, so a commit
//! written afterwards never points at data still in the page cache.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::format::SegmentStats;
use crate::sink::SegmentSink;

/// A finished segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishedFile {
    /// The segment's number within its writer.
    pub index: u32,
    /// Where it was written.
    pub path: PathBuf,
    /// What went into it.
    pub stats: SegmentStats,
}

/// Writes each segment to `<dir>/<stem>-<index>.seg`.
#[derive(Debug, Clone)]
pub struct FileSink {
    dir: PathBuf,
    stem: String,
}

impl FileSink {
    /// A sink into `dir` (which must exist) with file names starting with `stem`.
    pub fn new(dir: impl Into<PathBuf>, stem: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            stem: stem.into(),
        }
    }

    /// The path segment `index` is written to.
    #[must_use]
    pub fn path_for(&self, index: u32) -> PathBuf {
        self.dir.join(format!("{}-{index}.seg", self.stem))
    }

    /// The directory segments are written to.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl SegmentSink for FileSink {
    type Writer = BufWriter<File>;
    type Output = FinishedFile;

    fn open(&mut self, index: u32) -> io::Result<BufWriter<File>> {
        let file = File::create(self.path_for(index))?;
        Ok(BufWriter::with_capacity(256 << 10, file))
    }

    fn close(
        &mut self,
        index: u32,
        mut writer: BufWriter<File>,
        stats: SegmentStats,
    ) -> io::Result<FinishedFile> {
        writer.flush()?;
        let file = writer
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.sync_all()?;
        Ok(FinishedFile {
            index,
            path: self.path_for(index),
            stats,
        })
    }
}
