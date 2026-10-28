//! The line envelope: `{"id": ..., "updated_at": ..., "record": {...}}`.
//!
//! Only `id` and `record` matter to the pipeline, under the names the feed schema gives them
//! ([`parse_envelope_with_names`]; [`parse_envelope`] reads the built-in ones). `id` follows
//! the rules in [`crate::id`]; `record` must be an object and is kept as raw JSON, borrowed
//! from the line. Other envelope fields (such as `updated_at`) are ignored. A duplicated `id` or `record` key is malformed
//! rather than last-wins: the envelope is the pipeline's own framing, and an ambiguous one is
//! more likely a broken writer than a deliberate update.

use serde_json::value::RawValue;

use crate::error::{MalformedKind, MalformedRecord};
use crate::id::RecordId;
use crate::raw::{RawObject, is_object};

/// A parsed envelope, borrowing the record from the line it came from.
#[derive(Debug, Clone, Copy)]
pub struct Envelope<'a> {
    /// The record's id: the person id or the organization id.
    pub id: u64,
    /// The record, as the raw JSON text of an object (no surrounding whitespace).
    pub record: &'a RawValue,
}

impl<'a> Envelope<'a> {
    /// The record's raw JSON text.
    #[must_use]
    pub fn record_text(&self) -> &'a str {
        self.record.get()
    }
}

/// The byte-order mark some tools put before the first line of a file.
const BOM: &str = "\u{feff}";

/// Parses one NDJSON line (a trailing `\n` or `\r\n` is fine) into an envelope.
pub fn parse_envelope(line: &[u8]) -> Result<Envelope<'_>, MalformedRecord> {
    parse_envelope_with_names(line, "id", "record")
}

/// Parses one NDJSON line into an envelope whose id and record are under `id_key` and
/// `record_key`.
pub fn parse_envelope_with_names<'a>(
    line: &'a [u8],
    id_key: &str,
    record_key: &str,
) -> Result<Envelope<'a>, MalformedRecord> {
    let text = std::str::from_utf8(line).map_err(|e| {
        MalformedRecord::new(MalformedKind::Syntax, format!("line is not UTF-8: {e}"))
    })?;
    let text = text.strip_prefix(BOM).unwrap_or(text);
    if text.trim_ascii().is_empty() {
        return Err(MalformedRecord::new(MalformedKind::Syntax, "empty line"));
    }
    let object = RawObject::parse(text)
        .map_err(|e| MalformedRecord::from_json(MalformedKind::Envelope, &e))?;

    let mut id = None;
    let mut record = None;
    for (key, value) in &object.entries {
        let slot = if key.0.as_ref() == id_key {
            &mut id
        } else if key.0.as_ref() == record_key {
            &mut record
        } else {
            continue;
        };
        if slot.replace(*value).is_some() {
            return Err(MalformedRecord::new(
                MalformedKind::Envelope,
                format!("duplicate envelope field `{}`", key.0),
            ));
        }
    }
    let id = id.ok_or_else(|| {
        MalformedRecord::new(MalformedKind::Envelope, format!("missing `{id_key}`"))
    })?;
    let record = record.ok_or_else(|| {
        MalformedRecord::new(MalformedKind::Envelope, format!("missing `{record_key}`"))
    })?;
    let id = serde_json::from_str::<RecordId>(id.get())
        .map_err(|e| MalformedRecord::new(MalformedKind::Envelope, format!("`{id_key}`: {e}")))?
        .0;
    if !is_object(record) {
        return Err(MalformedRecord::new(
            MalformedKind::Envelope,
            format!("`{record_key}` is not an object"),
        ));
    }
    Ok(Envelope { id, record })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind(line: &str) -> MalformedKind {
        parse_envelope(line.as_bytes()).unwrap_err().kind()
    }

    #[test]
    fn parses_numeric_and_string_ids() {
        let env = parse_envelope(br#"{"id": 12, "updated_at": "x", "record": {"a": 1}}"#).unwrap();
        assert_eq!(env.id, 12);
        assert_eq!(env.record_text(), r#"{"a": 1}"#);
        let env = parse_envelope(b"{\"record\":{},\"id\":\"99\"}\r\n").unwrap();
        assert_eq!(env.id, 99);
        assert_eq!(env.record_text(), "{}");
    }

    #[test]
    fn parses_named_envelope_fields() {
        let env = parse_envelope_with_names(
            br#"{"employee_id":"7","payload":{"name":"Ada"}}"#,
            "employee_id",
            "payload",
        )
        .unwrap();
        assert_eq!(env.id, 7);
        assert_eq!(env.record_text(), r#"{"name":"Ada"}"#);
        let error = parse_envelope_with_names(
            br#"{"employee_id":1,"employee_id":2,"payload":{}}"#,
            "employee_id",
            "payload",
        )
        .unwrap_err();
        assert!(
            error
                .reason()
                .contains("duplicate envelope field `employee_id`")
        );
    }

    #[test]
    fn a_leading_bom_is_ignored() {
        let env = parse_envelope("\u{feff}{\"id\":1,\"record\":{}}".as_bytes()).unwrap();
        assert_eq!(env.id, 1);
    }

    #[test]
    fn syntax_errors() {
        assert_eq!(kind(""), MalformedKind::Syntax);
        assert_eq!(kind("   \n"), MalformedKind::Syntax);
        assert_eq!(kind("{\"id\":1,"), MalformedKind::Syntax);
        assert_eq!(
            kind("{\"id\":1,\"record\":{}} trailing"),
            MalformedKind::Syntax
        );
        assert_eq!(kind("not json"), MalformedKind::Syntax);
        let err = parse_envelope(b"{\"id\":1,\"record\":{\"a\":\"\xff\"}}").unwrap_err();
        assert_eq!(err.kind(), MalformedKind::Syntax);
        assert!(err.reason().contains("UTF-8"), "{err}");
    }

    #[test]
    fn envelope_errors() {
        for line in [
            "[1, {}]",
            "42",
            "null",
            "{\"record\":{}}",
            "{\"id\":1}",
            "{\"id\":-1,\"record\":{}}",
            "{\"id\":1.5,\"record\":{}}",
            "{\"id\":\"1a\",\"record\":{}}",
            "{\"id\":null,\"record\":{}}",
            "{\"id\":1,\"record\":null}",
            "{\"id\":1,\"record\":[]}",
            "{\"id\":1,\"record\":\"{}\"}",
            "{\"id\":1,\"id\":2,\"record\":{}}",
            "{\"id\":1,\"record\":{},\"record\":{}}",
        ] {
            assert_eq!(kind(line), MalformedKind::Envelope, "{line}");
        }
    }

    #[test]
    fn reasons_name_the_problem() {
        let err = parse_envelope(b"{\"record\":{}}").unwrap_err();
        assert!(err.reason().contains("missing `id`"), "{err}");
        let err = parse_envelope(b"{\"id\":1,\"record\":[]}").unwrap_err();
        assert!(err.reason().contains("not an object"), "{err}");
    }
}
