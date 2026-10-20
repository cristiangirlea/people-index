//! A single linear pass over raw JSON that reports where each token is, without parsing values.
//!
//! Some checks need to see every value of a record, unknown and `enabled: false` fields
//! included, but none needs the values themselves: [`scan`] walks the bytes once, keeps the path
//! of object keys and array indexes, and hands each event to a [`ScanVisitor`]. Nothing is
//! allocated but the path. The input is expected to be valid JSON (extraction scans records
//! `serde_json` has already read); on anything else the pass still ends without panicking, but
//! its events are unspecified.

use std::convert::Infallible;
use std::fmt;
use std::ops::ControlFlow;

/// One step of a [`JsonPath`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Segment<'a> {
    /// An object key as written between its quotes, escapes not decoded.
    Key(&'a str),
    /// An array index.
    Index(usize),
}

/// Where a token is: the keys and indexes from the scanned value down to it.
#[derive(Debug, Default)]
pub(crate) struct JsonPath<'a> {
    segments: Vec<Segment<'a>>,
}

impl<'a> JsonPath<'a> {
    /// The innermost step, if any.
    pub(crate) fn last(&self) -> Option<Segment<'a>> {
        self.segments.last().copied()
    }
}

/// Written as the quarantine names fields: `roles[2].title`, keys decoded.
impl fmt::Display for JsonPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.segments.iter().enumerate() {
            match segment {
                Segment::Key(raw) => {
                    if i > 0 {
                        f.write_str(".")?;
                    }
                    if raw.contains('\\') {
                        let decoded: Option<String> =
                            serde_json::from_str(&format!("\"{raw}\"")).ok();
                        f.write_str(decoded.as_deref().unwrap_or(raw))?;
                    } else {
                        f.write_str(raw)?;
                    }
                }
                Segment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// Receives the events of a [`scan`]; every method defaults to carrying on. Returning
/// `ControlFlow::Break` ends the scan with that value.
pub(crate) trait ScanVisitor<'a> {
    /// What a visitor stops with.
    type Break;

    /// An object begins; `path` is the object's own.
    fn object_start(&mut self, path: &JsonPath<'a>) -> ControlFlow<Self::Break> {
        let _ = path;
        ControlFlow::Continue(())
    }

    /// An object ends; `path` is the object's own.
    fn object_end(&mut self, path: &JsonPath<'a>) -> ControlFlow<Self::Break> {
        let _ = path;
        ControlFlow::Continue(())
    }

    /// A key of the innermost open object; `path` ends with it, `at` is the byte offset of its
    /// opening quote.
    fn key(&mut self, path: &JsonPath<'a>, at: usize) -> ControlFlow<Self::Break> {
        let _ = (path, at);
        ControlFlow::Continue(())
    }

    /// A number token as written (sign, digits, point, exponent) at `path`; `at` is the byte
    /// offset of its first character.
    fn number(
        &mut self,
        path: &JsonPath<'a>,
        at: usize,
        token: &'a str,
    ) -> ControlFlow<Self::Break> {
        let _ = (path, at, token);
        ControlFlow::Continue(())
    }
}

/// Watches nothing: the second of a pair when there is nothing else to see.
impl ScanVisitor<'_> for () {
    type Break = Infallible;
}

impl<'a, V: ScanVisitor<'a>> ScanVisitor<'a> for &mut V {
    type Break = V::Break;

    fn object_start(&mut self, path: &JsonPath<'a>) -> ControlFlow<V::Break> {
        (**self).object_start(path)
    }

    fn object_end(&mut self, path: &JsonPath<'a>) -> ControlFlow<V::Break> {
        (**self).object_end(path)
    }

    fn key(&mut self, path: &JsonPath<'a>, at: usize) -> ControlFlow<V::Break> {
        (**self).key(path, at)
    }

    fn number(&mut self, path: &JsonPath<'a>, at: usize, token: &'a str) -> ControlFlow<V::Break> {
        (**self).number(path, at, token)
    }
}

/// Two visitors on one pass: the first may stop the scan, the second only watches. Each event
/// reaches the second even when the first stops on it.
impl<'a, A, B> ScanVisitor<'a> for (A, B)
where
    A: ScanVisitor<'a>,
    B: ScanVisitor<'a, Break = Infallible>,
{
    type Break = A::Break;

    fn object_start(&mut self, path: &JsonPath<'a>) -> ControlFlow<A::Break> {
        let ControlFlow::Continue(()) = self.1.object_start(path);
        self.0.object_start(path)
    }

    fn object_end(&mut self, path: &JsonPath<'a>) -> ControlFlow<A::Break> {
        let ControlFlow::Continue(()) = self.1.object_end(path);
        self.0.object_end(path)
    }

    fn key(&mut self, path: &JsonPath<'a>, at: usize) -> ControlFlow<A::Break> {
        let ControlFlow::Continue(()) = self.1.key(path, at);
        self.0.key(path, at)
    }

    fn number(&mut self, path: &JsonPath<'a>, at: usize, token: &'a str) -> ControlFlow<A::Break> {
        let ControlFlow::Continue(()) = self.1.number(path, at, token);
        self.0.number(path, at, token)
    }
}

/// An open container, and for an object whether the path ends with one of its keys.
#[derive(Clone, Copy)]
enum Open {
    Object { has_key: bool },
    Array,
}

/// Walks `text` once, front to back, and reports its events to `visitor`.
pub(crate) fn scan<'a, V: ScanVisitor<'a>>(
    text: &'a str,
    visitor: &mut V,
) -> ControlFlow<V::Break> {
    let bytes = text.as_bytes();
    let mut path = JsonPath::default();
    let mut open: Vec<Open> = Vec::new();
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        match b {
            b'{' => {
                visitor.object_start(&path)?;
                open.push(Open::Object { has_key: false });
                i += 1;
            }
            b'[' => {
                open.push(Open::Array);
                path.segments.push(Segment::Index(0));
                i += 1;
            }
            b'}' => {
                if let Some(Open::Object { has_key }) = open.pop() {
                    if has_key {
                        path.segments.pop();
                    }
                    visitor.object_end(&path)?;
                }
                i += 1;
            }
            b']' => {
                if let Some(Open::Array) = open.pop() {
                    path.segments.pop();
                }
                i += 1;
            }
            b',' => {
                match open.last_mut() {
                    Some(Open::Object { has_key }) => {
                        if *has_key {
                            path.segments.pop();
                        }
                        *has_key = false;
                    }
                    Some(Open::Array) => {
                        if let Some(Segment::Index(index)) = path.segments.last_mut() {
                            *index += 1;
                        }
                    }
                    None => {}
                }
                i += 1;
            }
            b'"' => {
                let start = i + 1;
                let mut end = start;
                while let Some(&c) = bytes.get(end) {
                    match c {
                        b'\\' => end += 2,
                        b'"' => break,
                        _ => end += 1,
                    }
                }
                let end = end.min(bytes.len());
                i = end + 1;
                // A string in an object with no key yet is that key; any other is a value.
                if let Some(Open::Object { has_key }) = open.last_mut()
                    && !*has_key
                {
                    *has_key = true;
                    path.segments
                        .push(Segment::Key(text.get(start..end).unwrap_or_default()));
                    visitor.key(&path, start - 1)?;
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while bytes
                    .get(i)
                    .is_some_and(|c| matches!(c, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
                {
                    i += 1;
                }
                visitor.number(&path, start, text.get(start..i).unwrap_or_default())?;
            }
            // Whitespace, ':' and the letters of true, false and null.
            _ => i += 1,
        }
    }
    ControlFlow::Continue(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records every event as text.
    #[derive(Default)]
    struct Log(Vec<String>);

    impl<'a> ScanVisitor<'a> for Log {
        type Break = ();

        fn object_start(&mut self, path: &JsonPath<'a>) -> ControlFlow<()> {
            self.0.push(format!("{{ {path}"));
            ControlFlow::Continue(())
        }

        fn object_end(&mut self, path: &JsonPath<'a>) -> ControlFlow<()> {
            self.0.push(format!("}} {path}"));
            ControlFlow::Continue(())
        }

        fn key(&mut self, path: &JsonPath<'a>, _: usize) -> ControlFlow<()> {
            self.0.push(format!("key {path}"));
            ControlFlow::Continue(())
        }

        fn number(&mut self, path: &JsonPath<'a>, _: usize, token: &'a str) -> ControlFlow<()> {
            self.0.push(format!("{path} = {token}"));
            ControlFlow::Continue(())
        }
    }

    fn events(text: &str) -> Vec<String> {
        let mut log = Log::default();
        let _ = scan(text, &mut log);
        log.0
    }

    #[test]
    fn reports_keys_numbers_and_objects_with_their_paths() {
        assert_eq!(
            events(r#"{"a": -1.5e+3, "b": {"c": [1, {"d": 0}]}}"#),
            [
                "{ ",
                "key a",
                "a = -1.5e+3",
                "key b",
                "{ b",
                "key b.c",
                "b.c[0] = 1",
                "{ b.c[1]",
                "key b.c[1].d",
                "b.c[1].d = 0",
                "} b.c[1]",
                "} b",
                "} ",
            ]
        );
    }

    #[test]
    fn digits_inside_strings_are_not_numbers() {
        assert_eq!(
            events(r#"{"k1": "123", "q\"9": "a\\\"5\u0031", "2": "x"}"#),
            ["{ ", "key k1", "key q\"9", "key 2", "} "]
        );
    }

    #[test]
    fn nested_and_empty_containers() {
        assert_eq!(
            events(r#"{"a": [], "b": {}, "c": [[], [7, [8]], {}], "d": 9}"#),
            [
                "{ ",
                "key a",
                "key b",
                "{ b",
                "} b",
                "key c",
                "c[1][0] = 7",
                "c[1][1][0] = 8",
                "{ c[2]",
                "} c[2]",
                "key d",
                "d = 9",
                "} ",
            ]
        );
        assert_eq!(events("[1, 2]"), ["[0] = 1", "[1] = 2"]);
        assert_eq!(events("[]"), Vec::<String>::new());
    }

    #[test]
    fn literals_and_whitespace_are_skipped() {
        assert_eq!(
            events("{ \"t\" : true ,\n\"n\":null,\t\"f\":false,\"x\":1 }"),
            ["{ ", "key t", "key n", "key f", "key x", "x = 1", "} "]
        );
    }

    #[test]
    fn escaped_keys_are_decoded_in_the_path() {
        assert_eq!(
            events(r#"{"a\u0062": 1}"#),
            ["{ ", "key ab", "ab = 1", "} "]
        );
        assert_eq!(
            events(r#"{"q\"x": {"t\tk": 2}}"#),
            [
                "{ ",
                "key q\"x",
                "{ q\"x",
                "key q\"x.t\tk",
                "q\"x.t\tk = 2",
                "} q\"x",
                "} "
            ]
        );
    }

    #[test]
    fn offsets_point_at_the_key_quote_and_the_number() {
        struct Offsets(Vec<usize>, Vec<(usize, String)>);
        impl<'a> ScanVisitor<'a> for Offsets {
            type Break = ();
            fn key(&mut self, _: &JsonPath<'a>, at: usize) -> ControlFlow<()> {
                self.0.push(at);
                ControlFlow::Continue(())
            }
            fn number(&mut self, _: &JsonPath<'a>, at: usize, token: &'a str) -> ControlFlow<()> {
                self.1.push((at, token.to_owned()));
                ControlFlow::Continue(())
            }
        }
        let text = r#"{"a": "b", "c\"": [{ "d": 1}, -2.5]}"#;
        let mut offsets = Offsets(Vec::new(), Vec::new());
        let _ = scan(text, &mut offsets);
        assert_eq!(offsets.0, [1, 11, 21]);
        for at in offsets.0 {
            assert_eq!(text.as_bytes()[at], b'"');
        }
        assert_eq!(offsets.1, [(26, "1".to_owned()), (30, "-2.5".to_owned())]);
        for (at, token) in offsets.1 {
            assert_eq!(&text[at..at + token.len()], token);
        }
    }

    #[test]
    fn a_pair_of_visitors_sees_every_event_in_one_pass() {
        struct Numbers(usize);
        impl<'a> ScanVisitor<'a> for Numbers {
            type Break = Infallible;
            fn number(
                &mut self,
                _: &JsonPath<'a>,
                _: usize,
                _: &'a str,
            ) -> ControlFlow<Infallible> {
                self.0 += 1;
                ControlFlow::Continue(())
            }
        }
        let mut log = Log::default();
        let mut numbers = Numbers(0);
        let _ = scan(r#"{"a": [1, {"b": 2}]}"#, &mut (&mut log, &mut numbers));
        assert_eq!(numbers.0, 2);
        assert_eq!(log.0, events(r#"{"a": [1, {"b": 2}]}"#));
    }

    #[test]
    fn a_break_stops_the_scan() {
        struct First;
        impl<'a> ScanVisitor<'a> for First {
            type Break = &'a str;
            fn number(
                &mut self,
                _: &JsonPath<'a>,
                _: usize,
                token: &'a str,
            ) -> ControlFlow<&'a str> {
                ControlFlow::Break(token)
            }
        }
        assert_eq!(scan("[1, 2]", &mut First), ControlFlow::Break("1"));
    }

    #[test]
    fn invalid_input_ends_without_panicking() {
        for text in [
            "", "{", "}", "]]", "{\"a", "{\"a\\", "[1,", "\"", "{\"a\":}", "1e",
        ] {
            let _ = events(text);
        }
    }

    #[test]
    fn long_records_scan_in_one_pass() {
        struct Count<'c>(&'c mut usize);
        impl<'a> ScanVisitor<'a> for Count<'_> {
            type Break = ();
            fn number(&mut self, _: &JsonPath<'a>, _: usize, _: &'a str) -> ControlFlow<()> {
                *self.0 += 1;
                ControlFlow::Continue(())
            }
        }
        // A wide array, a deep nesting and a long string: each is linear and must not
        // overflow the stack or panic.
        let wide = format!("[{}0]", "0,".repeat(1 << 20));
        let mut count = 0usize;
        let _ = scan(&wide, &mut Count(&mut count));
        assert_eq!(count, (1 << 20) + 1);

        let deep = format!("{}1{}", "[{\"a\":".repeat(100_000), "}]".repeat(100_000));
        let _ = scan(&deep, &mut Count(&mut count));
        assert_eq!(count, (1 << 20) + 2);

        let long = format!("{{\"s\":\"{}\"}}", "9\\\"".repeat(1 << 20));
        assert_eq!(events(&long), ["{ ", "key s", "} "]);
    }
}
