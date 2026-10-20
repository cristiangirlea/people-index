//! Small sampling helpers over fixed lists.

use rand::{Rng, RngExt};

/// One element of a non-empty `list`, by value. Returning `T` rather than `&T` lets the
/// element type come from the slice alone: with `&T`, rustc 1.90 (the minimum supported
/// version) infers `T = str` from a `&str` destination and rejects `&[&str]` lists.
pub(crate) fn pick_one<T: Copy, R: Rng + ?Sized>(rng: &mut R, list: &[T]) -> T {
    list[rng.random_range(0..list.len())]
}

/// Between `min` and `max` distinct elements of `list`, in draw order.
pub(crate) fn pick_distinct<T: Copy + PartialEq, R: Rng + ?Sized>(
    rng: &mut R,
    list: &[T],
    min: usize,
    max: usize,
) -> Vec<T> {
    let max = max.min(list.len());
    let want = rng.random_range(min.min(max)..=max);
    let mut picked = Vec::with_capacity(want);
    // The lists are short and `want` is small, so retrying on a repeat is cheap; the bound
    // keeps a pathological draw from looping long.
    for _ in 0..want * 8 {
        if picked.len() == want {
            break;
        }
        let item = pick_one(rng, list);
        if !picked.contains(&item) {
            picked.push(item);
        }
    }
    picked
}

/// An index drawn from cumulative weights (`cumulative.last()` > 0).
pub(crate) fn weighted_index<R: Rng + ?Sized>(rng: &mut R, cumulative: &[u64]) -> usize {
    let total = cumulative.last().copied().unwrap_or(0);
    if total == 0 {
        return 0;
    }
    let x = rng.random_range(0..total);
    cumulative.partition_point(|&c| c <= x)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn distinct_picks_are_distinct_and_bounded() {
        let mut rng = ChaCha8Rng::seed_from_u64(2);
        for _ in 0..1000 {
            let v = pick_distinct(&mut rng, &[1, 2, 3, 4, 5], 1, 3);
            assert!((1..=3).contains(&v.len()));
            let mut s = v.clone();
            s.dedup();
            s.sort_unstable();
            s.dedup();
            assert_eq!(s.len(), v.len());
        }
        assert_eq!(pick_distinct(&mut rng, &[1], 0, 0), [] as [i32; 0]);
    }

    #[test]
    fn weights_are_respected() {
        let mut rng = ChaCha8Rng::seed_from_u64(3);
        let cumulative = [0, 10, 10, 30];
        let mut hits = [0; 4];
        for _ in 0..30_000 {
            hits[weighted_index(&mut rng, &cumulative)] += 1;
        }
        assert_eq!(hits[0], 0);
        assert_eq!(hits[2], 0);
        assert!((9_000..11_000).contains(&hits[1]), "{hits:?}");
    }
}
