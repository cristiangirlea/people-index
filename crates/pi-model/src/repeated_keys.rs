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
//! its comma included. [`RepeatedKeys::body`] copies the record without those spans: every other
//! byte, keys, values and whitespace, stays as written.
//!
//! Cost on the common path, a record without a repeat:
//! - Keys are compared as written, borrowed from the record. Only when two spellings differ and
//!   one has a backslash escape are both decoded (`"\u006eame"` is `"name"`), which allocates.
//! - An object of up to [`SMALL`] members is searched from its end for each new key; a larger one
//!   keeps a map from each key's hash to its latest member, so the work stays linear.
//! - Members are stored as offsets, so the state borrows nothing from the record and is reused
//!   from line to line: each thread keeps one ([`with_repeated_keys`]), reset per record, so a
//!   record allocates nothing for the watch once the frames, member lists and maps have grown
//!   to the record shapes seen. The state is dropped after a record that made it hold more than
//!   [`RETAINED`] entries, so one huge record does not keep its memory on the thread.
//! - The body is a copy of the record's bytes; it is the row's own buffer either way.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::{BuildHasher, RandomState};
use std::ops::{ControlFlow, Range};

use crate::scan::{JsonPath, ScanVisitor, Segment};

/// Objects with up to this many members are searched for a repeat; larger ones are hashed.
const SMALL: usize = 16;

/// The most frames, members, map entries and spans a thread keeps between records.
const RETAINED: usize = 1 << 16;

/// One member of an object: where its key is in the record.
#[derive(Debug, Clone, Copy)]
struct Member {
    /// The offset of the key's opening quote.
    at: usize,
    /// The key's length as written between its quotes.
    len: usize,
    /// Whether the key has a backslash escape.
    escaped: bool,
}

impl Member {
    fn raw<'a>(&self, text: &'a str) -> &'a str {
        text.get(self.at + 1..self.at + 1 + self.len)
            .unwrap_or_default()
    }

    /// The key, decoded when it has an escape.
    fn key<'a>(&self, text: &'a str) -> Cow<'a, str> {
        let raw = self.raw(text);
        if !self.escaped {
            return Cow::Borrowed(raw);
        }
        serde_json::from_str::<String>(&format!("\"{raw}\"")).map_or(Cow::Borrowed(raw), Cow::Owned)
    }

    /// Whether `self` and `other` are one key: same spelling, or the same once decoded.
    fn same_key(&self, other: &Member, text: &str) -> bool {
        let (a, b) = (self.raw(text), other.raw(text));
        a == b || ((self.escaped || other.escaped) && self.key(text) == other.key(text))
    }
}

/// The members seen so far of one open object.
#[derive(Debug, Default)]
struct Frame {
    members: Vec<Member>,
    /// Each key's hash to its latest member, filled once the object outgrows [`SMALL`].
    latest: HashMap<u64, usize>,
    /// The members a later member repeats.
    repeated: Vec<usize>,
}

impl Frame {
    fn clear(&mut self) {
        self.members.clear();
        self.latest.clear();
        self.repeated.clear();
    }

    fn retained(&self) -> usize {
        self.members.capacity() + self.latest.capacity() + self.repeated.capacity()
    }

    /// The latest earlier member with the same key as `member`, the member at `index`.
    fn previous(
        &mut self,
        member: &Member,
        index: usize,
        text: &str,
        hasher: &RandomState,
    ) -> Option<usize> {
        let earlier = |members: &[Member]| members.iter().rposition(|m| m.same_key(member, text));
        if index < SMALL {
            return earlier(&self.members);
        }
        if self.latest.is_empty() {
            for (i, m) in self.members.iter().enumerate() {
                self.latest.insert(hasher.hash_one(m.key(text)), i);
            }
        }
        match self.latest.insert(hasher.hash_one(member.key(text)), index) {
            None => None,
            Some(i)
                if self
                    .members
                    .get(i)
                    .is_some_and(|m| m.same_key(member, text)) =>
            {
                Some(i)
            }
            // Another key with the same hash: search.
            Some(_) => earlier(&self.members),
        }
    }
}

/// Notes the spans of repeated keys' earlier occurrences during a scan ([`RepeatedKeys::watch`]);
/// it never stops it.
#[derive(Debug, Default)]
pub(crate) struct RepeatedKeys {
    /// One frame per open object, innermost last; frames past `depth` are kept for reuse.
    frames: Vec<Frame>,
    depth: usize,
    /// The byte ranges to drop; after [`RepeatedKeys::watch`]'s scan and
    /// [`RepeatedKeys::settle`], sorted and disjoint.
    drops: Vec<Range<usize>>,
    hasher: RandomState,
}

thread_local! {
    static STATE: RefCell<RepeatedKeys> = RefCell::default();
}

/// Runs `f` with this thread's [`RepeatedKeys`], taken out for the call (a nested call gets a
/// fresh one) and put back, trimmed, after it.
pub(crate) fn with_repeated_keys<R>(f: impl FnOnce(&mut RepeatedKeys) -> R) -> R {
    let taken = STATE.try_with(|slot| slot.try_borrow_mut().map(|mut s| std::mem::take(&mut *s)));
    let mut state = taken.ok().and_then(Result::ok).unwrap_or_default();
    let out = f(&mut state);
    state.trim();
    let _ = STATE.try_with(|slot| {
        if let Ok(mut slot) = slot.try_borrow_mut() {
            *slot = state;
        }
    });
    out
}

/// [`RepeatedKeys`] watching one scan of `text`.
pub(crate) struct Watch<'s, 'a> {
    state: &'s mut RepeatedKeys,
    text: &'a str,
}

impl<'a> ScanVisitor<'a> for Watch<'_, 'a> {
    type Break = Infallible;

    fn object_start(&mut self, _: &JsonPath<'a>) -> ControlFlow<Infallible> {
        let state = &mut *self.state;
        match state.frames.get_mut(state.depth) {
            Some(frame) => frame.clear(),
            None => state.frames.push(Frame::default()),
        }
        state.depth += 1;
        ControlFlow::Continue(())
    }

    fn object_end(&mut self, _: &JsonPath<'a>) -> ControlFlow<Infallible> {
        let state = &mut *self.state;
        let Some(depth) = state.depth.checked_sub(1) else {
            return ControlFlow::Continue(());
        };
        state.depth = depth;
        if let Some(frame) = state.frames.get(depth) {
            for &index in &frame.repeated {
                // A repeated member always has a later member: the one that repeats it.
                if let (Some(start), Some(end)) =
                    (frame.members.get(index), frame.members.get(index + 1))
                {
                    state.drops.push(start.at..end.at);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn key(&mut self, path: &JsonPath<'a>, at: usize) -> ControlFlow<Infallible> {
        let state = &mut *self.state;
        let (Some(depth), Some(Segment::Key(raw))) = (state.depth.checked_sub(1), path.last())
        else {
            return ControlFlow::Continue(());
        };
        let Some(frame) = state.frames.get_mut(depth) else {
            return ControlFlow::Continue(());
        };
        let member = Member {
            at,
            len: raw.len(),
            escaped: raw.contains('\\'),
        };
        let index = frame.members.len();
        if let Some(previous) = frame.previous(&member, index, self.text, &state.hasher) {
            frame.repeated.push(previous);
        }
        frame.members.push(member);
        ControlFlow::Continue(())
    }
}

impl RepeatedKeys {
    /// Resets the state and watches a scan of `text`; call [`RepeatedKeys::settle`] after it.
    pub(crate) fn watch<'s, 'a>(&'s mut self, text: &'a str) -> Watch<'s, 'a> {
        self.depth = 0;
        self.drops.clear();
        Watch { state: self, text }
    }

    /// Sorts the spans to drop and leaves out those inside another: spans of one object are
    /// disjoint, and an inner object's span lies inside one member of its parent, so it is
    /// either inside a dropped span or disjoint from every one.
    pub(crate) fn settle(&mut self) {
        self.drops.sort_unstable_by_key(|r| r.start);
        let mut end = 0;
        self.drops.retain(|r| {
            let outer = r.start >= end;
            if outer {
                end = r.end;
            }
            outer
        });
    }

    /// Whether the scanned record repeats a key in some object.
    pub(crate) fn found(&self) -> bool {
        !self.drops.is_empty()
    }

    /// Whether the byte at offset `at` of the scanned record is dropped.
    pub(crate) fn dropped(&self, at: usize) -> bool {
        let after = self.drops.partition_point(|r| r.start <= at);
        after > 0 && self.drops.get(after - 1).is_some_and(|r| at < r.end)
    }

    /// The body to keep for the scanned `text`: its bytes as they are when no key repeats,
    /// otherwise the bytes without each repeated key's earlier occurrences.
    pub(crate) fn body(&self, text: &str) -> Vec<u8> {
        if self.drops.is_empty() {
            return text.as_bytes().to_vec();
        }
        let bytes = text.as_bytes();
        let kept = text.len() - self.drops.iter().map(Range::len).sum::<usize>();
        // Exactly the kept length: the extract budget counts `body.len()`.
        let mut out = Vec::with_capacity(kept);
        let mut pos = 0;
        for drop in &self.drops {
            out.extend_from_slice(bytes.get(pos..drop.start).unwrap_or_default());
            pos = drop.end;
        }
        out.extend_from_slice(bytes.get(pos..).unwrap_or_default());
        out
    }

    /// Drops what this state holds if one record made it grow past [`RETAINED`].
    fn trim(&mut self) {
        let held = self.frames.capacity()
            + self.drops.capacity()
            + self.frames.iter().map(Frame::retained).sum::<usize>();
        if held > RETAINED {
            *self = Self::default();
        }
    }
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

    fn reduce_with(state: &mut RepeatedKeys, text: &str) -> String {
        let ControlFlow::Continue(()) = scan(text, &mut state.watch(text));
        state.settle();
        String::from_utf8(state.body(text)).unwrap()
    }

    fn reduce(text: &str) -> String {
        reduce_with(&mut RepeatedKeys::default(), text)
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
            (r#"{"\u006eame":"A","name":"B"}"#, r#"{"name":"B"}"#),
            // A surrogate pair is the character it encodes, and `\/` is `/`.
            (r#"{"\ud83d\ude00":1,"😀":2}"#, r#"{"😀":2}"#),
            (r#"{"😀":1,"\ud83d\ude00":2}"#, r#"{"\ud83d\ude00":2}"#),
            (r#"{"a\/b":1,"a/b":2}"#, r#"{"a/b":2}"#),
            (r#"{"a/b":1,"a\/b":2}"#, r#"{"a\/b":2}"#),
            // Different escapes of different keys stay apart.
            (r#"{"\u0061":1,"\u0062":2}"#, r#"{"\u0061":1,"\u0062":2}"#),
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
        // Escaped spellings meet in the map too.
        let mut members: Vec<String> = (0..20).map(|i| format!("\"k{i}\":{i}")).collect();
        members.push(r#""\u006b3":"x""#.to_owned());
        let text = format!("{{{}}}", members.join(","));
        let mut kept = members.clone();
        kept.remove(3);
        assert_eq!(reduce(&text), format!("{{{}}}", kept.join(",")));
    }

    #[test]
    fn a_hash_collision_still_finds_the_repeat() {
        // `a`, then `SMALL - 1` other keys, then `a` again.
        let keys: Vec<String> = std::iter::once("a".to_owned())
            .chain((1..SMALL).map(|i| format!("k{i}")))
            .chain(std::iter::once("a".to_owned()))
            .collect();
        let text = keys
            .iter()
            .map(|k| format!("\"{k}\""))
            .collect::<Vec<_>>()
            .join(" ");
        let mut at = 0;
        let members: Vec<Member> = keys
            .iter()
            .map(|k| {
                let member = Member {
                    at,
                    len: k.len(),
                    escaped: false,
                };
                at += k.len() + 3;
                member
            })
            .collect();
        let hasher = RandomState::new();
        let mut frame = Frame {
            members: members[..SMALL].to_vec(),
            ..Frame::default()
        };
        // `a`'s hash slot names `k1`, as if the two collided.
        frame.latest.insert(hasher.hash_one("a"), 1);
        assert_eq!(
            frame.previous(&members[SMALL], SMALL, &text, &hasher),
            Some(0)
        );
    }

    #[test]
    fn records_without_a_repeat_keep_their_bytes() {
        let text = r#"{ "a": {"a": [{"a": 1}, {"a": 2}]}, "b" : "\u0062", "c":[{"x":1},{"x":1}] }"#;
        let mut state = RepeatedKeys::default();
        assert_eq!(reduce_with(&mut state, text), text);
        assert!(!state.found());
    }

    #[test]
    fn the_state_is_reused_from_record_to_record() {
        let mut state = RepeatedKeys::default();
        assert_eq!(
            reduce_with(&mut state, r#"{"a":{"b":1,"b":2},"a":0}"#),
            r#"{"a":0}"#
        );
        // Nothing of the first record leaks into the next.
        let text = r#"{"b":{"a":1},"c":2}"#;
        assert_eq!(reduce_with(&mut state, text), text);
        assert!(!state.found());
        assert_eq!(
            reduce_with(&mut state, r#"{"x":[{"y":1,"y":2}]}"#),
            r#"{"x":[{"y":2}]}"#
        );
        // The thread's state is trimmed after a record that made it grow large.
        let wide = format!(
            "{{{}}}",
            (0..RETAINED)
                .map(|i| format!("\"k{i}\":0"))
                .collect::<Vec<_>>()
                .join(",")
        );
        with_repeated_keys(|state| {
            reduce_with(state, &wide);
        });
        with_repeated_keys(|state| assert!(state.frames.is_empty()));
        // Nested use gets a fresh state rather than panicking.
        with_repeated_keys(|outer| {
            let inner = with_repeated_keys(|inner| reduce_with(inner, r#"{"a":1,"a":2}"#));
            assert_eq!(inner, r#"{"a":2}"#);
            assert_eq!(reduce_with(outer, r#"{"a":1}"#), r#"{"a":1}"#);
        });
    }

    #[test]
    fn dropped_reports_the_bytes_of_dropped_spans() {
        let text = r#"{"a":{"b":1,"b":2},"a":0,"c":3}"#;
        let mut state = RepeatedKeys::default();
        reduce_with(&mut state, text);
        let inner = text.find("1,").unwrap();
        let kept = text.find(":0").unwrap() + 1;
        assert!(state.dropped(1));
        assert!(state.dropped(inner));
        assert!(!state.dropped(kept));
        assert!(!state.dropped(text.find('3').unwrap()));
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
