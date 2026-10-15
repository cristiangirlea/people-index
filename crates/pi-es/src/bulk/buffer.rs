//! A byte-bounded bulk request body that is filled, sent, compacted on partial failure and
//! reused. Each worker allocates `senders + 1` of these once, so the memory the load uses for
//! requests is fixed by configuration, whatever the input size.

use bytes::Bytes;

/// The action line before every document. The index goes in the request path, so the line
/// carries only the id: fewer bytes per item on the wire.
const ACTION_PREFIX: &[u8] = br#"{"index":{"_id":""#;
const ACTION_SUFFIX: &[u8] = b"\"}}\n";

/// Where one item (action line, document, newline) sits in the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Item {
    id: u64,
    start: usize,
    end: usize,
}

/// One bulk request body and the items in it, in order.
#[derive(Debug)]
pub(crate) struct BulkBuffer {
    data: Vec<u8>,
    items: Vec<Item>,
    capacity: usize,
}

impl BulkBuffer {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            items: Vec::new(),
            capacity,
        }
    }

    /// The bytes one document takes in a bulk body: action line, document, newline.
    pub(crate) fn item_len(id: u64, body_len: usize) -> usize {
        ACTION_PREFIX.len() + decimal_len(id) + ACTION_SUFFIX.len() + body_len + 1
    }

    /// Whether an item of `size` bytes still fits under the byte limit.
    pub(crate) fn fits(&self, size: usize) -> bool {
        self.data.len() + size <= self.capacity
    }

    /// Appends one document. The caller has checked [`Self::fits`] and that `body` is one line.
    pub(crate) fn push(&mut self, id: u64, body: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(ACTION_PREFIX);
        push_decimal(&mut self.data, id);
        self.data.extend_from_slice(ACTION_SUFFIX);
        self.data.extend_from_slice(body);
        self.data.push(b'\n');
        self.items.push(Item {
            id,
            start,
            end: self.data.len(),
        });
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn byte_len(&self) -> usize {
        self.data.len()
    }

    /// The id of item `n`, for error messages.
    pub(crate) fn id(&self, n: usize) -> Option<u64> {
        self.items.get(n).map(|item| item.id)
    }

    /// Ready for the next request; the allocation stays.
    pub(crate) fn clear(&mut self) {
        self.data.clear();
        self.items.clear();
    }

    /// Keeps only the items at `keep` (ascending positions), moved to the front in order, so a
    /// retry resends nothing that already succeeded.
    pub(crate) fn compact(&mut self, keep: &[usize]) {
        let mut write = 0;
        let mut kept = Vec::with_capacity(keep.len());
        for &n in keep {
            let Some(item) = self.items.get(n).copied() else {
                continue;
            };
            let size = item.end - item.start;
            self.data.copy_within(item.start..item.end, write);
            kept.push(Item {
                id: item.id,
                start: write,
                end: write + size,
            });
            write += size;
        }
        self.data.truncate(write);
        self.items = kept;
    }

    /// Hands the body out as `Bytes` for the request without copying it.
    pub(crate) fn freeze(&mut self) -> Bytes {
        Bytes::from(std::mem::take(&mut self.data))
    }

    /// Takes the body back after the request. When nothing else holds it (the usual case once
    /// the response is in) the allocation comes back as it was; otherwise it is copied.
    pub(crate) fn thaw(&mut self, bytes: Bytes) {
        self.data = match bytes.try_into_mut() {
            Ok(unique) => Vec::from(unique),
            Err(shared) => {
                let mut data = Vec::with_capacity(self.capacity.max(shared.len()));
                data.extend_from_slice(&shared);
                data
            }
        };
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

fn decimal_len(mut value: u64) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

/// Appends `value` in decimal without going through `fmt` (no allocation, nothing to unwrap).
fn push_decimal(out: &mut Vec<u8>, mut value: u64) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        // `value % 10` is below 10, so the cast cannot truncate.
        #[allow(clippy::cast_possible_truncation)]
        let digit = (value % 10) as u8;
        digits[at] = b'0' + digit;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[at..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_items_and_measures_them() {
        let mut buffer = BulkBuffer::new(1024);
        buffer.push(7, br#"{"a":1}"#);
        buffer.push(u64::MAX, br#"{"b":2}"#);
        let expected = format!(
            "{{\"index\":{{\"_id\":\"7\"}}}}\n{{\"a\":1}}\n{{\"index\":{{\"_id\":\"{}\"}}}}\n{{\"b\":2}}\n",
            u64::MAX
        );
        assert_eq!(buffer.as_bytes(), expected.as_bytes());
        assert_eq!(
            BulkBuffer::item_len(7, 7) + BulkBuffer::item_len(u64::MAX, 7),
            expected.len()
        );
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.id(1), Some(u64::MAX));
    }

    #[test]
    fn respects_the_byte_limit() {
        let size = BulkBuffer::item_len(1, 10);
        let mut buffer = BulkBuffer::new(size * 2);
        assert!(buffer.fits(size));
        buffer.push(1, b"{\"x\":\"12\"}");
        assert!(buffer.fits(size));
        buffer.push(1, b"{\"x\":\"12\"}");
        assert!(!buffer.fits(1));
    }

    #[test]
    fn compaction_keeps_only_failed_items_in_order() {
        let mut buffer = BulkBuffer::new(1024);
        for id in 1..=5 {
            buffer.push(id, format!("{{\"n\":{id}}}").as_bytes());
        }
        buffer.compact(&[1, 3]);
        let mut expected = BulkBuffer::new(1024);
        expected.push(2, b"{\"n\":2}");
        expected.push(4, b"{\"n\":4}");
        assert_eq!(buffer.as_bytes(), expected.as_bytes());
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.id(0), Some(2));
        assert_eq!(buffer.id(1), Some(4));
    }

    #[test]
    fn freeze_and_thaw_keep_the_allocation() {
        let mut buffer = BulkBuffer::new(4096);
        buffer.push(1, b"{}");
        let bytes = buffer.freeze();
        let on_the_wire = bytes.clone();
        drop(on_the_wire);
        buffer.thaw(bytes);
        assert_eq!(buffer.byte_len(), BulkBuffer::item_len(1, 2));
        assert!(buffer.capacity() >= 4096);
        let shared = buffer.freeze();
        let held = shared.clone();
        buffer.thaw(shared);
        assert_eq!(buffer.as_bytes(), &held[..]);
    }
}
