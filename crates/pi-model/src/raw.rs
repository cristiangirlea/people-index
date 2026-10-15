//! A JSON object read one level deep: keys decoded, values kept as raw text.
//!
//! The join only needs a handful of fields, and everything else must reach the output byte for
//! byte (numbers keep their precision, unknown fields their shape). So objects are read as an
//! ordered list of `(key, raw value)` pairs that borrow from the input; a value is parsed only
//! when it is one of the fields the join needs. Only a JSON object is accepted: serde would
//! otherwise read a struct from an array by position, which would let `[1, {...}]` pass as an
//! envelope.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::value::RawValue;

/// An object key, borrowed from the input unless it contained escapes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key<'a>(pub Cow<'a, str>);

struct KeyVisitor;

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Key<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an object key")
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Key<'de>, E> {
        Ok(Key(Cow::Borrowed(v)))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Key<'de>, E> {
        Ok(Key(Cow::Owned(v.to_owned())))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Key<'de>, E> {
        Ok(Key(Cow::Owned(v)))
    }
}

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(KeyVisitor)
    }
}

/// The entries of one JSON object in input order, duplicates included.
#[derive(Debug, Default)]
pub(crate) struct RawObject<'a> {
    pub entries: Vec<(Key<'a>, &'a RawValue)>,
}

impl<'a> RawObject<'a> {
    /// Reads `text` as an object; arrays, scalars and invalid JSON are errors.
    pub fn parse(text: &'a str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }

    /// The value of `key`, taking the last occurrence like every common JSON parser does.
    pub fn get(&self, key: &str) -> Option<&'a RawValue> {
        self.entries
            .iter()
            .rev()
            .find(|(k, _)| k.0 == key)
            .map(|(_, v)| *v)
    }

    /// For each entry, whether it is the last occurrence of its key. Output built from the kept
    /// entries has no duplicate keys, which Elasticsearch would reject.
    pub fn last_occurrences(&self) -> Vec<bool> {
        let n = self.entries.len();
        let mut keep = vec![false; n];
        // Small objects (nearly all of them) are cheaper to scan than to hash.
        if n <= 16 {
            for (i, slot) in keep.iter_mut().enumerate() {
                let key = &self.entries[i].0;
                *slot = !self.entries[i + 1..].iter().any(|(k, _)| k == key);
            }
        } else {
            let mut seen: HashSet<&str> = HashSet::with_capacity(n);
            for i in (0..n).rev() {
                keep[i] = seen.insert(self.entries[i].0.0.as_ref());
            }
        }
        keep
    }
}

struct RawObjectVisitor;

impl<'de> Visitor<'de> for RawObjectVisitor {
    type Value = RawObject<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<RawObject<'de>, A::Error> {
        let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(16).min(64));
        while let Some(key) = map.next_key::<Key<'de>>()? {
            let value: &'de RawValue = map.next_value()?;
            entries.push((key, value));
        }
        Ok(RawObject { entries })
    }
}

impl<'de> Deserialize<'de> for RawObject<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(RawObjectVisitor)
    }
}

/// Whether a raw value is JSON `null` (raw values carry no surrounding whitespace).
pub(crate) fn is_null(value: &RawValue) -> bool {
    value.get() == "null"
}

/// Whether a raw value is an object, judged by its first byte (raw values are valid JSON).
pub(crate) fn is_object(value: &RawValue) -> bool {
    value.get().starts_with('{')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_order_raw_values_and_duplicates() {
        let obj = RawObject::parse(r#"{"b": 1.50, "a": {"x": [1, 2]}, "b": "two"}"#).unwrap();
        let keys: Vec<&str> = obj.entries.iter().map(|(k, _)| k.0.as_ref()).collect();
        assert_eq!(keys, ["b", "a", "b"]);
        assert_eq!(obj.entries[0].1.get(), "1.50", "numbers keep their text");
        assert_eq!(obj.get("b").unwrap().get(), "\"two\"");
        assert_eq!(obj.last_occurrences(), [false, true, true]);
    }

    #[test]
    fn last_occurrences_match_for_large_objects() {
        let entries: Vec<String> = (0..40).map(|i| format!("\"k{}\":{i}", i % 20)).collect();
        let text = format!("{{{}}}", entries.join(","));
        let obj = RawObject::parse(&text).unwrap();
        let keep = obj.last_occurrences();
        assert_eq!(keep.iter().filter(|k| **k).count(), 20);
        assert!(keep[20..].iter().all(|k| *k));
    }

    #[test]
    fn escaped_keys_are_decoded() {
        let obj = RawObject::parse(r#"{"roles": []}"#).unwrap();
        assert!(obj.get("roles").is_some());
    }

    #[test]
    fn only_objects_are_accepted() {
        for bad in ["[1, 2]", "1", "\"x\"", "null", "{", "{\"a\":}"] {
            assert!(RawObject::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn null_and_object_checks() {
        let obj = RawObject::parse(r#"{"a": null, "b": {}, "c": []}"#).unwrap();
        assert!(is_null(obj.get("a").unwrap()));
        assert!(is_object(obj.get("b").unwrap()));
        assert!(!is_object(obj.get("c").unwrap()));
    }
}
