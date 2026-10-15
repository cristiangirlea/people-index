//! Reading rows back from committed segments, on the blocking side.

use std::io::Cursor;

use bytes::Bytes;
use pi_coord::permanent;
use pi_segment::{SegmentError, SegmentReader};
use serde::de::DeserializeOwned;

/// Calls `f` for every row of the segment `bytes` read from `key`.
///
/// A segment that does not decode is a committed output that is damaged: retrying the task
/// would read the same bytes, so the error is permanent. Only I/O errors (none for bytes in
/// memory) would be worth a retry.
pub(crate) fn for_each_row<T, F>(key: &str, bytes: Bytes, mut f: F) -> anyhow::Result<u64>
where
    T: DeserializeOwned,
    F: FnMut(T) -> anyhow::Result<()>,
{
    let reader = SegmentReader::<T, _>::new(Cursor::new(bytes)).map_err(|e| classify(key, e))?;
    let mut n = 0u64;
    for row in reader {
        f(row.map_err(|e| classify(key, e))?)?;
        n += 1;
    }
    Ok(n)
}

fn classify(key: &str, err: SegmentError) -> anyhow::Error {
    let transient = matches!(err, SegmentError::Io(_));
    let err = anyhow::Error::new(err).context(format!("decoding segment {key}"));
    if transient { err } else { permanent(err) }
}

#[cfg(test)]
mod tests {
    use pi_segment::encode_segment;

    use super::*;

    #[test]
    fn rows_come_back_and_damage_is_permanent() {
        let bytes = Bytes::from(encode_segment(&[1u64, 2, 3]).unwrap());
        let mut seen = Vec::new();
        let n = for_each_row::<u64, _>("k", bytes.clone(), |r| {
            seen.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!((n, seen), (3, vec![1, 2, 3]));
        let cut = bytes.slice(..bytes.len() - 3);
        let err = for_each_row::<u64, _>("k", cut, |_| Ok(())).unwrap_err();
        assert!(pi_coord::is_permanent(&err), "{err:#}");
        assert!(format!("{err:#}").contains("decoding segment k"));
    }
}
