//! Where generated files go: a small trait so the CLI can add an object-store sink.

use std::io::{self, Write};

/// A destination for generated files, addressed by relative paths such as
/// `persons/part-00000.ndjson.gz` or `expected.json` (always `/`-separated).
///
/// `create` hands out an owned writer so the generator can fill several files from rayon
/// threads at once; only `create` itself is serialised. A file must become visible only when
/// its writer's [`SinkWriter::finish`] succeeds, so a crashed run never leaves a truncated
/// file that looks complete.
pub trait Sink: Send {
    /// Opens `path` for writing. Called once per path per run.
    fn create(&mut self, path: &str) -> io::Result<Box<dyn SinkWriter>>;

    /// Writes a whole file at once. Sinks that upload finished objects can override this.
    fn put(&mut self, path: &str, bytes: &[u8]) -> io::Result<()> {
        let mut writer = self.create(path)?;
        writer.write_all(bytes)?;
        writer.finish()
    }
}

/// One file being written. Dropping it without `finish` abandons the file.
pub trait SinkWriter: Write + Send {
    /// Flushes and publishes the file.
    fn finish(self: Box<Self>) -> io::Result<()>;
}

/// Checks a sink path: relative, `/`-separated, no empty, `.` or `..` segments, no drive or
/// backslash. Returns the segments. Sinks use it so a path can never escape their root.
pub fn path_segments(path: &str) -> io::Result<Vec<&str>> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("sink path must be relative and plain: {path:?}"),
        )
    };
    if path.is_empty() || path.contains(['\\', ':', '\0']) {
        return Err(invalid());
    }
    let segments: Vec<&str> = path.split('/').collect();
    if segments
        .iter()
        .any(|s| s.is_empty() || *s == "." || *s == "..")
    {
        return Err(invalid());
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_relative_paths_pass() {
        assert_eq!(
            path_segments("persons/part-00000.ndjson.gz").unwrap(),
            ["persons", "part-00000.ndjson.gz"]
        );
        assert!(path_segments("expected.json").is_ok());
    }

    #[test]
    fn escaping_paths_fail() {
        for p in [
            "", "/abs", "../up", "a/../b", "a//b", "./a", "a/", "C:x", "a\\b", "a\0b",
        ] {
            assert!(path_segments(p).is_err(), "{p:?}");
        }
    }
}
