//! Repeated keys at any depth, reduced to each key's last occurrence.
//!
//! Elasticsearch rejects a document that repeats a key in any object ("Duplicate field"),
//! observed against a real Elasticsearch 9.5.5 at the top level, in objects inside arrays, in
//! objects several levels deep, in unmapped fields and inside `enabled: false` objects alike.
//! Extraction keeps such a record and drops every occurrence of a repeated key but the last, in
//! every object, which is the occurrence [`crate::raw::RawObject::get`] reads and the shape
//! checks judge.
//!
//! [`RepeatedKeys`] watches the record's single [`crate::scan::scan`] (the number-length check's
//! pass) and notes, per object, the members a later member repeats. Such a member is never the
//! last of its object, so it spans from its key's opening quote to the next key's opening quote,
//! its comma included. [`RepeatedKeys::into_body`] copies the record without those spans: every
//! other byte, keys, values and whitespace, stays as written.

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::{ControlFlow, Range};

use crate::scan::{JsonPath, ScanVisitor, Segment};

/// Objects with up to this many members are searched for a repeat; larger ones are hashed.
const SMALL: usize = 16;

/// The members seen so far of one open object.
#[derive(Debug, Default)]
struct Frame<'a> {
    /// Each member's key, decoded, and the offset of its opening quote.
    members: Vec<(Cow<'a, str>, usize)>,
    /// The latest occurrence of each key, filled once the object outgrows [`SMALL`].
    latest: HashMap<Cow<'a, str>, usize>,
    /// The members a later member repeats.
    repeated: Vec<usize>,
}

impl Frame<'_> {
    fn clear(&mut self) {
        self.members.clear();
        self.latest.clear();
        self.repeated.clear();
    }
}

/// Notes the spans of repeated keys' earlier occurrences during a scan; it never stops it.
#[derive(Debug, Default)]
pub(crate) struct RepeatedKeys<'a> {
    /// One frame per open object, innermost last; frames past `depth` are kept for reuse.
    frames: Vec<Frame<'a>>,
    depth: usize,
    /// The byte ranges to drop.
    drops: Vec<Range<usize>>,
}

impl<'a> ScanVisitor<'a> for RepeatedKeys<'a> {
    type Break = Infallible;

    fn object_start(&mut self, _: &JsonPath<'a>) -> ControlFlow<Infallible> {
        match self.frames.get_mut(self.depth) {
            Some(frame) => frame.clear(),
            None => self.frames.push(Frame::default()),
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn object_end(&mut self, _: &JsonPath<'a>) -> ControlFlow<Infallible> {
        let Some(depth) = self.depth.checked_sub(1) else {
            return ControlFlow::Continue(());
        };
        self.depth = depth;
        if let Some(frame) = self.frames.get(depth) {
            for &index in &frame.repeated {
                // A repeated member always has a later member: the one that repeats it.
                if let (Some((_, start)), Some((_, end))) =
                    (frame.members.get(index), frame.members.get(index + 1))
                {
                    self.drops.push(*start..*end);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn key(&mut self, path: &JsonPath<'a>, at: usize) -> ControlFlow<Infallible> {
        let (Some(depth), Some(Segment::Key(raw))) = (self.depth.checked_sub(1), path.last())
        else {
            return ControlFlow::Continue(());
        };
        let Some(frame) = self.frames.get_mut(depth) else {
            return ControlFlow::Continue(());
        };
        let key = decode(raw);
        let index = frame.members.len();
        let previous = if index < SMALL {
            frame.members.iter().rposition(|(k, _)| *k == key)
        } else {
            if frame.latest.is_empty() {
                for (i, (k, _)) in frame.members.iter().enumerate() {
                    frame.latest.insert(k.clone(), i);
                }
            }
            frame.latest.insert(key.clone(), index)
        };
        if let Some(previous) = previous {
            frame.repeated.push(previous);
        }
        frame.members.push((key, at));
        ControlFlow::Continue(())
    }
}

impl RepeatedKeys<'_> {
    /// Whether the scanned record repeats a key in some object.
    pub(crate) fn found(&self) -> bool {
        !self.drops.is_empty()
    }

    /// The body to keep for the scanned `text`: its bytes as they are when no key repeats,
    /// otherwise the bytes without each repeated key's earlier occurrences.
    pub(crate) fn into_body(mut self, text: &str) -> Vec<u8> {
        if self.drops.is_empty() {
            return text.as_bytes().to_vec();
        }
        // Spans of one object are disjoint; an inner object's span lies inside one member of its
        // parent, so it is either kept whole or inside a dropped span.
        self.drops.sort_unstable_by_key(|r| r.start);
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(text.len());
        let mut pos = 0;
        for drop in &self.drops {
            if drop.start < pos {
                continue;
            }
            out.extend_from_slice(bytes.get(pos..drop.start).unwrap_or_default());
            pos = drop.end;
        }
        out.extend_from_slice(bytes.get(pos..).unwrap_or_default());
        // The extract budget counts `body.len()`: drop the capacity the dropped spans left.
        out.into_boxed_slice().into_vec()
    }
}

/// A key as written between its quotes, decoded: `"\u006eame"` and `"name"` are one key.
fn decode(raw: &str) -> Cow<'_, str> {
    if !raw.contains('\\') {
        return Cow::Borrowed(raw);
    }
    serde_json::from_str::<String>(&format!("\"{raw}\"")).map_or(Cow::Borrowed(raw), Cow::Owned)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::raw::RawObject;
    use crate::scan::scan;

    /// Panics unless `text` is JSON in which no object, at any depth, repeats a key (keys
    /// compared decoded). Reads with serde, independently of the scan.
    pub(crate) fn assert_no_repeated_key(text: &[u8]) {
        fn check(text: &str) {
            if text.starts_with('{') {
                let object = RawObject::parse(text).unwrap();
                let mut keys = std::collections::HashSet::new();
                for (key, value) in &object.entries {
                    assert!(keys.insert(&key.0), "{:?} repeats in {text}", key.0);
                    check(value.get());
                }
            } else if text.starts_with('[') {
                let items: Vec<&serde_json::value::RawValue> = serde_json::from_str(text).unwrap();
                for item in items {
                    check(item.get());
                }
            }
        }
        let text = std::str::from_utf8(text).unwrap();
        serde_json::from_str::<serde::de::IgnoredAny>(text).unwrap();
        check(text);
    }

    fn reduce(text: &str) -> String {
        let mut repeats = RepeatedKeys::default();
        let _ = scan(text, &mut repeats);
        String::from_utf8(repeats.into_body(text)).unwrap()
    }

    #[test]
    fn keeps_the_last_occurrence_in_every_object() {
        for (text, kept) in [
            (r#"{"a":1,"a":2}"#, r#"{"a":2}"#),
            (r#"{ "a" : 1 , "b":2, "a":3 }"#, r#"{ "b":2, "a":3 }"#),
            (
                r#"{"m":[{"name":"A","name":"B"},{"x":1}]}"#,
                r#"{"m":[{"name":"B"},{"x":1}]}"#,
            ),
            (
                r#"{"a":{"b":{"c":[[{"d":1,"e":2,"d":"3"}]]}}}"#,
                r#"{"a":{"b":{"c":[[{"e":2,"d":"3"}]]}}}"#,
            ),
            // A dropped occurrence takes its own repeats with it; the kept one is reduced.
            (
                r#"{"a":{"x":1,"x":2},"a":{"y":1,"y":2}}"#,
                r#"{"a":{"y":2}}"#,
            ),
            (r#"{"k":1,"k":2,"k":3}"#, r#"{"k":3}"#),
            (r#"{"name":"A","\u006eame":"B"}"#, r#"{"\u006eame":"B"}"#),
            // Values keep their text: escapes, number spelling, other keys' order.
            (
                r#"{"s":"a\"b\u00e9","n":1.50e+3,"s":"\\x\n","z":-0}"#,
                r#"{"n":1.50e+3,"s":"\\x\n","z":-0}"#,
            ),
        ] {
            let body = reduce(text);
            assert_eq!(body, kept, "{text}");
            assert_no_repeated_key(body.as_bytes());
        }
    }

    #[test]
    fn objects_past_the_small_size_are_hashed() {
        let members: Vec<String> = (0..40).map(|i| format!("\"k{}\":{i}", i % 20)).collect();
        let text = format!("{{{}}}", members.join(","));
        let body = reduce(&text);
        assert_eq!(body, format!("{{{}}}", members[20..].join(",")));
        assert_no_repeated_key(body.as_bytes());
    }

    #[test]
    fn records_without_a_repeat_keep_their_bytes() {
        let text = r#"{ "a": {"a": [{"a": 1}, {"a": 2}]}, "b" : "\u0062", "c":[{"x":1},{"x":1}] }"#;
        let mut repeats = RepeatedKeys::default();
        let _ = scan(text, &mut repeats);
        assert!(!repeats.found());
        assert_eq!(repeats.into_body(text), text.as_bytes());
    }

    #[test]
    fn deep_records_are_reduced_without_recursion() {
        let depth = 100_000;
        let text = format!(
            "{}{{\"k\":1,\"k\":2}}{}",
            "[{\"a\":".repeat(depth),
            "}]".repeat(depth)
        );
        let body = reduce(&text);
        assert_eq!(body.len(), text.len() - "\"k\":1,".len());
        assert!(body.contains("{\"k\":2}"));
    }
}
