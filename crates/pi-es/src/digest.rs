//! A content digest of a set of documents that does not depend on the order they are read in.
//!
//! Each document contributes `sha256(len(_id) || _id || canonical(_source))`, and the
//! contributions are added as 256-bit integers (mod 2^256). Addition commutes, so the same set
//! of documents gives the same digest whether it is read from Elasticsearch in shard order,
//! folded from `docs/` segments partition by partition, or merged from parallel workers. This
//! is an integrity check against loss and corruption, not a defence against an adversary who
//! picks documents to collide.

use std::io::Write as _;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::error::{EsError, Result};

/// Domain separation, so this digest is never mistaken for a plain sha256 of something.
const DOMAIN: &[u8] = b"people-index/document-set/v1";

/// The digest of a document set and its size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DigestReport {
    pub docs: u64,
    /// Lowercase hex sha256.
    pub sha256: String,
}

/// An order-independent accumulator of documents.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentDigest {
    docs: u64,
    /// Little-endian 256-bit sum of the per-document hashes.
    sum: [u64; 4],
}

impl DocumentDigest {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one document: its `_id` and its source as JSON.
    pub fn add(&mut self, id: &str, source: &Value) {
        let mut hasher = Sha256::new();
        hasher.update((id.len() as u64).to_le_bytes());
        hasher.update(id.as_bytes());
        let mut canonical = Vec::with_capacity(256);
        write_canonical(&mut canonical, source);
        hasher.update(&canonical);
        let hash = hasher.finalize();
        let mut carry = false;
        let (words, _) = hash.as_slice().as_chunks::<8>();
        for (limb, word) in self.sum.iter_mut().zip(words) {
            let (partial, overflow_a) = limb.overflowing_add(u64::from_le_bytes(*word));
            let (total, overflow_b) = partial.overflowing_add(u64::from(carry));
            *limb = total;
            carry = overflow_a || overflow_b;
        }
        self.docs += 1;
    }

    /// Adds one document from its raw JSON bytes (a `DocRow` body, for instance).
    pub fn add_json(&mut self, id: &str, source: &[u8]) -> Result<()> {
        let value: Value = serde_json::from_slice(source).map_err(|e| EsError::Decode {
            path: format!("document {id}"),
            message: e.to_string(),
        })?;
        self.add(id, &value);
        Ok(())
    }

    /// Folds another accumulator in, as if its documents had been added here.
    pub fn merge(&mut self, other: &Self) {
        let mut carry = false;
        for (limb, add) in self.sum.iter_mut().zip(other.sum) {
            let (partial, overflow_a) = limb.overflowing_add(add);
            let (total, overflow_b) = partial.overflowing_add(u64::from(carry));
            *limb = total;
            carry = overflow_a || overflow_b;
        }
        self.docs += other.docs;
    }

    /// The number of documents added so far.
    #[must_use]
    pub fn docs(&self) -> u64 {
        self.docs
    }

    /// The final digest.
    #[must_use]
    pub fn finish(&self) -> DigestReport {
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN);
        hasher.update(self.docs.to_le_bytes());
        for limb in self.sum {
            hasher.update(limb.to_le_bytes());
        }
        DigestReport {
            docs: self.docs,
            sha256: hex(hasher.finalize().as_slice()),
        }
    }
}

/// Lowercase hex SHA-256 of the canonical JSON of `value` ([`write_canonical`]): the same for
/// any key order or whitespace. A plan pins the feed schema and the index mapping by it.
#[must_use]
pub fn canonical_sha256(value: &Value) -> String {
    let mut canonical = Vec::with_capacity(4096);
    write_canonical(&mut canonical, value);
    hex(Sha256::digest(&canonical).as_slice())
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// Compact JSON with object keys sorted by their bytes, so two sources that differ only in key
/// order or whitespace give the same bytes. Strings and numbers use `serde_json`'s formatting,
/// which is deterministic. Written by hand rather than relying on `serde_json::Map` ordering,
/// which another crate's `preserve_order` feature could change for the whole build.
pub fn write_canonical(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => {
            // Writing into a Vec cannot fail.
            let _ = write!(out, "{number}");
        }
        Value::String(text) => write_string(out, text),
        Value::Array(items) => {
            out.push(b'[');
            for (n, item) in items.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                write_canonical(out, item);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push(b'{');
            for (n, (key, item)) in entries.into_iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                write_string(out, key);
                out.push(b':');
                write_canonical(out, item);
            }
            out.push(b'}');
        }
    }
}

fn write_string(out: &mut Vec<u8>, text: &str) {
    // Serialising a &str into a Vec cannot fail.
    let _ = serde_json::to_writer(&mut *out, text);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_form_sorts_keys_and_drops_whitespace() {
        let a: Value =
            serde_json::from_str(r#"{ "b": [1, {"y":2,"x":"\n"}], "a": null }"#).expect("json");
        let mut out = Vec::new();
        write_canonical(&mut out, &a);
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            r#"{"a":null,"b":[1,{"x":"\n","y":2}]}"#
        );
    }

    #[test]
    fn the_canonical_sha256_ignores_key_order_and_whitespace() {
        let a: Value = serde_json::from_str(r#"{ "b": [1, {"y":2}], "a": null }"#).expect("json");
        let b: Value = serde_json::from_str(r#"{"a":null,"b":[1,{"y":2}]}"#).expect("json");
        assert_eq!(canonical_sha256(&a), canonical_sha256(&b));
        assert_eq!(canonical_sha256(&a).len(), 64);
        // sha256 of `{}`.
        assert_eq!(
            canonical_sha256(&json!({})),
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
        assert_ne!(canonical_sha256(&a), canonical_sha256(&json!({"a": null})));
    }

    #[test]
    fn order_of_documents_does_not_matter() {
        let docs = [
            ("1", json!({"a": 1, "b": "x"})),
            ("2", json!({"b": "y", "a": 2})),
            ("3", json!({"nested": {"z": [1, 2], "a": true}})),
        ];
        let mut forward = DocumentDigest::new();
        for (id, doc) in &docs {
            forward.add(id, doc);
        }
        let mut backward = DocumentDigest::new();
        for (id, doc) in docs.iter().rev() {
            backward.add(id, doc);
        }
        assert_eq!(forward.finish(), backward.finish());
        assert_eq!(forward.finish().docs, 3);
        assert_eq!(forward.finish().sha256.len(), 64);

        let mut left = DocumentDigest::new();
        left.add(docs[0].0, &docs[0].1);
        let mut right = DocumentDigest::new();
        right.add(docs[1].0, &docs[1].1);
        right
            .add_json(docs[2].0, br#"{"nested":{"a":true,"z":[1,2]}}"#)
            .expect("json");
        left.merge(&right);
        assert_eq!(left.finish(), forward.finish());
    }

    #[test]
    fn content_and_ids_matter() {
        let mut one = DocumentDigest::new();
        one.add("1", &json!({"a": 1}));
        let mut other_value = DocumentDigest::new();
        other_value.add("1", &json!({"a": 2}));
        let mut other_id = DocumentDigest::new();
        other_id.add("2", &json!({"a": 1}));
        assert_ne!(one.finish(), other_value.finish());
        assert_ne!(one.finish(), other_id.finish());
        assert_ne!(one.finish(), DocumentDigest::new().finish());
    }
}
