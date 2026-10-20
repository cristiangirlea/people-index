//! Reading a bulk response item by item. `errors: false` is not trusted on its own: every item
//! must be there, in order, with a success status.

use serde::Deserialize;
use serde_json::Value;

use crate::error::{EsError, ItemFailure, Result, is_retryable_status};

/// `filter_path` that keeps a bulk response down to what the check needs: a few bytes per item
/// instead of the full item metadata.
pub(crate) const FILTER_PATH: &str =
    "filter_path=errors,items.*.status,items.*.error.type,items.*.error.reason";

/// What a 2xx bulk response says about the items that were sent.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Settled {
    /// Items Elasticsearch accepted.
    pub(crate) indexed: usize,
    /// Positions of items that failed with a retryable status, ascending.
    pub(crate) retry: Vec<usize>,
    /// The first retryable failure, for the error message if retries run out.
    pub(crate) retry_reason: Option<String>,
}

#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    items: Vec<serde_json::Map<String, Value>>,
}

/// Sorts every item into accepted, retryable or permanent. Any permanent failure fails the
/// whole call with up to `max_reasons` examples; `id_of` maps a position to its document id.
pub(crate) fn settle(
    path: &str,
    body: &[u8],
    sent: usize,
    max_reasons: usize,
    id_of: impl Fn(usize) -> Option<u64>,
) -> Result<Settled> {
    let decode = |message: String| EsError::Decode {
        path: path.to_owned(),
        message,
    };
    let response: Response =
        serde_json::from_slice(body).map_err(|e| decode(format!("bulk response: {e}")))?;
    if response.items.len() != sent {
        return Err(decode(format!(
            "bulk response has {} items for {sent} sent",
            response.items.len()
        )));
    }
    let mut settled = Settled::default();
    let mut permanent = 0usize;
    let mut reasons = Vec::new();
    for (n, item) in response.items.iter().enumerate() {
        // One key per item: the operation ("index").
        let outcome = item
            .values()
            .next()
            .ok_or_else(|| decode(format!("bulk item {n} is empty")))?;
        let status = outcome["status"]
            .as_u64()
            .and_then(|s| u16::try_from(s).ok())
            .ok_or_else(|| decode(format!("bulk item {n} has no status")))?;
        let error = &outcome["error"];
        if (200..300).contains(&status) && error.is_null() {
            settled.indexed += 1;
            continue;
        }
        let (kind, reason) = describe(error);
        if is_retryable_status(status) {
            settled.retry.push(n);
            if settled.retry_reason.is_none() {
                settled.retry_reason = Some(format!("item status {status} {kind}: {reason}"));
            }
        } else {
            permanent += 1;
            if reasons.len() < max_reasons {
                reasons.push(ItemFailure {
                    id: id_of(n).unwrap_or_default(),
                    status,
                    kind,
                    reason,
                });
            }
        }
    }
    if permanent > 0 {
        return Err(EsError::BulkItems {
            failed: permanent,
            reasons,
        });
    }
    Ok(settled)
}

/// The error type and reason of an item, whether Elasticsearch sent an object or a string.
fn describe(error: &Value) -> (String, String) {
    match error {
        Value::Object(map) => (
            map.get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            map.get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .chars()
                .take(500)
                .collect(),
        ),
        Value::String(text) => ("error".to_owned(), text.chars().take(500).collect()),
        _ => ("unknown".to_owned(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize) -> Option<u64> {
        u64::try_from(n).ok().map(|n| n + 100)
    }

    #[test]
    fn all_accepted() {
        let body =
            br#"{"errors":false,"items":[{"index":{"status":201}},{"index":{"status":200}}]}"#;
        let settled = settle("/b", body, 2, 3, ids).expect("settled");
        assert_eq!(settled.indexed, 2);
        assert_eq!(settled.retry, [] as [usize; 0]);
    }

    #[test]
    fn retryable_items_are_listed() {
        let body = br#"{"errors":true,"items":[
            {"index":{"status":201}},
            {"index":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"queue full"}}},
            {"index":{"status":201}},
            {"index":{"status":503,"error":{"type":"unavailable_shards_exception","reason":"x"}}}]}"#;
        let settled = settle("/b", body, 4, 3, ids).expect("settled");
        assert_eq!(settled.indexed, 2);
        assert_eq!(settled.retry, vec![1, 3]);
        assert!(
            settled
                .retry_reason
                .is_some_and(|r| r.contains("queue full"))
        );
    }

    #[test]
    fn permanent_items_fail_with_reasons() {
        let body = br#"{"errors":true,"items":[
            {"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad a"}}},
            {"index":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"q"}}},
            {"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad b"}}},
            {"index":{"status":400,"error":"plain string"}}]}"#;
        match settle("/b", body, 4, 2, ids) {
            Err(EsError::BulkItems { failed, reasons }) => {
                assert_eq!(failed, 3);
                assert_eq!(reasons.len(), 2);
                assert_eq!(reasons[0].id, 100);
                assert_eq!(reasons[0].kind, "mapper_parsing_exception");
                assert_eq!(reasons[1].id, 102);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn item_count_must_match() {
        let body = br#"{"errors":false,"items":[{"index":{"status":201}}]}"#;
        assert!(matches!(
            settle("/b", body, 2, 3, ids),
            Err(EsError::Decode { .. })
        ));
        assert!(settle("/b", b"not json", 1, 3, ids).is_err());
        let no_status = br#"{"errors":false,"items":[{"index":{}}]}"#;
        assert!(settle("/b", no_status, 1, 3, ids).is_err());
    }
}
