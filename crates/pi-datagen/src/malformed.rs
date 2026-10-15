//! Lines the pipeline must count and quarantine instead of indexing.
//!
//! Every kind here is unambiguous under the contract (an `id` that is a non-negative 64-bit
//! integer or a decimal string of one, a `record` that is an object): a lenient parser that
//! accepted any of them would index a document the expected counts do not contain, and the
//! acceptance check would catch it. Malformed lines never reuse a real record's slot, so
//! they never change which record of an id is last.

use rand::{Rng, RngExt};
use serde_json::{Value, json};

/// The ways a line can be malformed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MalformedKind {
    /// A strict prefix of a valid envelope: invalid JSON.
    Truncated,
    /// A well-formed envelope whose `record` is not an object.
    NonObjectRecord,
    /// A well-formed envelope whose `id` is not a valid id.
    BadId,
}

/// Appends one malformed line (with its trailing `\n`) to `out` and returns its kind.
pub(crate) fn write_malformed<R: Rng + ?Sized>(
    rng: &mut R,
    updated_at: &str,
    out: &mut Vec<u8>,
) -> serde_json::Result<MalformedKind> {
    let id: u64 = rng.random_range(1..1_000_000_000);
    let small_record = json!({"id": id, "name": "partial", "note": "cut short"});
    let kind = match rng.random_range(0..3u8) {
        0 => MalformedKind::Truncated,
        1 => MalformedKind::NonObjectRecord,
        _ => MalformedKind::BadId,
    };
    match kind {
        MalformedKind::Truncated => {
            let full = serde_json::to_vec(
                &json!({"id": id, "updated_at": updated_at, "record": small_record}),
            )?;
            // Any strict prefix of one JSON object is invalid JSON: it lacks the closing brace.
            let cut = rng.random_range(1..full.len());
            out.extend_from_slice(&full[..cut]);
        }
        MalformedKind::NonObjectRecord => {
            let record = match rng.random_range(0..5u8) {
                0 => json!([1, 2, 3]),
                1 => json!("not an object"),
                2 => json!(42),
                3 => Value::Null,
                _ => json!(true),
            };
            serde_json::to_writer(
                &mut *out,
                &json!({"id": id, "updated_at": updated_at, "record": record}),
            )?;
        }
        MalformedKind::BadId => {
            // `id` < 1e9 is exact in f64.
            #[allow(clippy::cast_precision_loss)]
            let bad = match rng.random_range(0..7u8) {
                0 => json!(-i64::try_from(id).unwrap_or(1)),
                1 => json!(format!("{id}x")),
                2 => json!(id as f64 + 0.5),
                3 => Value::Null,
                4 => json!(""),
                5 => json!("18446744073709551616"),
                _ => json!({"value": id}),
            };
            let mut envelope = json!({"updated_at": updated_at, "record": small_record});
            if rng.random_bool(0.85) {
                envelope["id"] = bad;
            } // else: the id is missing altogether
            serde_json::to_writer(&mut *out, &envelope)?;
        }
    }
    out.push(b'\n');
    Ok(kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    /// The contract's validity rule, restated independently of the generator.
    fn is_valid(line: &[u8]) -> bool {
        let Ok(Value::Object(env)) = serde_json::from_slice::<Value>(line) else {
            return false;
        };
        let id_ok = match env.get("id") {
            Some(Value::Number(n)) => n.as_u64().is_some(),
            Some(Value::String(s)) => {
                !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && s.parse::<u64>().is_ok()
            }
            _ => false,
        };
        id_ok && matches!(env.get("record"), Some(Value::Object(_)))
    }

    #[test]
    fn every_kind_is_invalid() {
        let mut rng = ChaCha8Rng::seed_from_u64(11);
        let mut kinds = [0usize; 3];
        for _ in 0..3000 {
            let mut out = Vec::new();
            let kind = write_malformed(&mut rng, "2026-01-01T00:00:00Z", &mut out).unwrap();
            assert_eq!(out.last(), Some(&b'\n'));
            let line = &out[..out.len() - 1];
            assert!(!line.contains(&b'\n'));
            assert!(!is_valid(line), "{}", String::from_utf8_lossy(line));
            kinds[kind as usize] += 1;
        }
        assert!(kinds.iter().all(|&k| k > 800), "{kinds:?}");
    }
}
