//! The `_shards` header check. A count or refresh that some shard skipped is a partial answer
//! that looks like a whole one; every decision that rests on it must see all shards answer.

use serde_json::Value;

use crate::error::{EsError, Result};

/// The `_shards` header of a response that passed [`require_complete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardCounts {
    pub total: u64,
    pub successful: u64,
}

/// Accepts only `failed == 0`, `total > 0` and `successful == total`, and rejects any response
/// that says it timed out.
pub fn require_complete(path: &str, response: &Value) -> Result<ShardCounts> {
    if response.get("timed_out").and_then(Value::as_bool) == Some(true) {
        return Err(EsError::TimedOut {
            path: path.to_owned(),
        });
    }
    let shards = response.get("_shards");
    let field = |name: &str| shards.and_then(|s| s.get(name)).and_then(Value::as_u64);
    match (field("total"), field("successful"), field("failed")) {
        (Some(total), Some(successful), Some(0)) if total > 0 && successful == total => {
            Ok(ShardCounts { total, successful })
        }
        _ => Err(EsError::IncompleteShards {
            path: path.to_owned(),
            shards: shards.map_or_else(|| "missing".to_owned(), Value::to_string),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_complete_answers_pass() {
        let ok = json!({"_shards": {"total": 4, "successful": 4, "skipped": 0, "failed": 0}});
        assert_eq!(
            require_complete("/x", &ok).ok(),
            Some(ShardCounts {
                total: 4,
                successful: 4
            })
        );
        for bad in [
            json!({"_shards": {"total": 4, "successful": 3, "failed": 1}}),
            json!({"_shards": {"total": 4, "successful": 3, "failed": 0}}),
            json!({"_shards": {"total": 0, "successful": 0, "failed": 0}}),
            json!({"count": 3}),
            json!({"timed_out": true, "_shards": {"total": 1, "successful": 1, "failed": 0}}),
        ] {
            assert!(require_complete("/x", &bad).is_err(), "{bad}");
        }
    }
}
