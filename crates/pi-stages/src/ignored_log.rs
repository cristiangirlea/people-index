//! The values of kept records that Elasticsearch will skip, per extract task: all counted, the
//! first few lines logged.

use pi_model::LineStats;

/// Lines with values the index will skip that a task logs; the counter has the rest.
pub(crate) const LOGGED_IGNORED_LINES: u64 = 5;

/// One extract task's count of values the index will skip, and how many lines it logged.
#[derive(Debug, Default)]
pub(crate) struct IgnoredLog {
    total: u64,
    logged: u64,
}

impl IgnoredLog {
    /// Counts the values of the kept line `line` of `file` that Elasticsearch will skip, and
    /// logs the first of them when fewer than [`LOGGED_IGNORED_LINES`] lines were logged: its
    /// file, line, field path and kind, never the value. Returns whether it logged.
    pub(crate) fn note(&mut self, file: &str, line: u64, stats: LineStats) -> bool {
        self.total += stats.ignored_by_index;
        let Some(first) = stats.first_ignored else {
            return false;
        };
        if self.logged >= LOGGED_IGNORED_LINES {
            return false;
        }
        self.logged += 1;
        tracing::info!(
            file,
            line,
            field = %first.field,
            found = first.found,
            values = stats.ignored_by_index,
            "kept a record with values the index will skip (ignore_malformed)"
        );
        true
    }

    /// The values counted so far.
    pub(crate) fn total(&self) -> u64 {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use pi_model::{Seq, parse_person_line};

    use super::*;

    /// A log writer that keeps what it is given.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn logs_the_first_lines_only_and_never_a_value() {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let line = br#"{"id":1,"record":{"followers":"secret-value","connections":["x",3]}}"#;
        let mut log = IgnoredLog::default();
        let extra = 3;
        let logged: Vec<bool> = tracing::subscriber::with_default(subscriber, || {
            // A line with nothing skipped is neither counted nor logged.
            assert!(!log.note("raw/persons/f.ndjson.gz", 1, LineStats::default()));
            (2..2 + LOGGED_IGNORED_LINES + extra)
                .map(|n| {
                    let (_, stats) = parse_person_line(line, Seq::default()).unwrap();
                    log.note("raw/persons/f.ndjson.gz", n, stats)
                })
                .collect()
        });
        let cap = usize::try_from(LOGGED_IGNORED_LINES).unwrap();
        assert!(logged[..cap].iter().all(|l| *l), "{logged:?}");
        assert!(logged[cap..].iter().all(|l| !*l), "{logged:?}");
        // Every line's values are counted, logged or not.
        assert_eq!(log.total(), 2 * (LOGGED_IGNORED_LINES + extra));

        let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("secret-value"), "{output}");
        assert!(!output.contains("\"x\""), "{output}");
        let events: Vec<serde_json::Value> = output
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(events.len(), cap);
        for (event, number) in events.iter().zip(2u64..) {
            let fields = event["fields"].as_object().unwrap();
            let mut keys: Vec<&str> = fields.keys().map(String::as_str).collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                ["field", "file", "found", "line", "message", "values"]
            );
            assert_eq!(fields["file"], "raw/persons/f.ndjson.gz");
            assert_eq!(fields["line"], number);
            assert_eq!(fields["field"], "connections[0]");
            assert_eq!(fields["found"], "a string that is not a number");
            assert_eq!(fields["values"], 2);
        }
    }
}
