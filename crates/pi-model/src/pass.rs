//! Splitting one partition's in-memory work into passes that each fit a memory budget.
//!
//! `join` and `assemble` hold one side of a partition in memory. When the estimate of that
//! side is larger than the task's budget, the task runs `k = ceil(estimate / budget)` passes
//! (a grace hash join): pass `i` holds only the ids with `xxh3_64(id LE, seed 1) % k == i`
//! and streams the other side filtered the same way. Every id falls in exactly one pass, so
//! the output is the same for any `k`. The seed differs from the partitioner's (seed 0):
//! inside one partition every id already shares its low bits, so the same hash would put
//! every id in the same pass whenever `k` divides `P`.

use xxhash_rust::xxh3::xxh3_64_with_seed;

/// The seed of the pass hash; part of no contract between processes (each task picks its own
/// `k` and filters both sides itself), but pinned by a test so the split stays even.
pub const PASS_SEED: u64 = 1;

/// More passes than this run as this many: a partition that needs more is far beyond what
/// the partition target was chosen for, and the cure is more partitions, not thousands of
/// re-reads of the same segments.
pub const MAX_PASSES: u32 = 1024;

/// The passes of one task: how many, and which ids each one holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Passes {
    count: u32,
}

impl Passes {
    /// One pass that holds every id.
    #[must_use]
    pub fn single() -> Self {
        Self { count: 1 }
    }

    /// `ceil(estimated_bytes / budget_bytes)` passes, at least 1 and at most [`MAX_PASSES`].
    /// A zero budget is read as one byte.
    #[must_use]
    pub fn for_estimate(estimated_bytes: u64, budget_bytes: u64) -> Self {
        let needed = estimated_bytes.div_ceil(budget_bytes.max(1)).max(1);
        let count = u32::try_from(needed).unwrap_or(MAX_PASSES).min(MAX_PASSES);
        Self { count }
    }

    /// Whether [`Passes::for_estimate`] had to stop at [`MAX_PASSES`].
    #[must_use]
    pub fn capped(estimated_bytes: u64, budget_bytes: u64) -> bool {
        estimated_bytes.div_ceil(budget_bytes.max(1)) > u64::from(MAX_PASSES)
    }

    /// The number of passes `k`.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.count
    }

    /// The pass that holds `id`, in `0..k`.
    #[must_use]
    pub fn pass_of(&self, id: u64) -> u32 {
        if self.count == 1 {
            return 0;
        }
        let pass = xxh3_64_with_seed(&id.to_le_bytes(), PASS_SEED) % u64::from(self.count);
        // Below `count`, a u32, so the value always fits.
        u32::try_from(pass).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Partitioner;

    #[test]
    fn pass_count_is_the_estimate_over_the_budget_rounded_up() {
        assert_eq!(Passes::for_estimate(0, 100).count(), 1);
        assert_eq!(Passes::for_estimate(1, 100).count(), 1);
        assert_eq!(Passes::for_estimate(100, 100).count(), 1);
        assert_eq!(Passes::for_estimate(101, 100).count(), 2);
        assert_eq!(Passes::for_estimate(300, 100).count(), 3);
        assert_eq!(Passes::for_estimate(301, 100).count(), 4);
        assert_eq!(Passes::for_estimate(5, 0).count(), 5);
        assert_eq!(Passes::for_estimate(u64::MAX, 1).count(), MAX_PASSES);
        assert!(Passes::capped(u64::MAX, 1));
        assert!(!Passes::capped(100 * u64::from(MAX_PASSES), 100));
        assert_eq!(Passes::single(), Passes::for_estimate(7, 7));
    }

    #[test]
    fn every_id_is_in_exactly_one_pass_and_one_pass_takes_everything() {
        let one = Passes::single();
        assert!((0..1000).all(|id| one.pass_of(id) == 0));
        let three = Passes::for_estimate(3, 1);
        assert!((0..1000).all(|id| three.pass_of(id) < 3));
        // Pinned: xxh3_64 of the eight bytes of 0u64 with seed 1, modulo 3.
        assert_eq!(
            three.pass_of(0),
            u32::try_from(xxh3_64_with_seed(&0u64.to_le_bytes(), 1) % 3).unwrap()
        );
    }

    #[test]
    fn passes_split_one_partition_evenly() {
        // The ids of one partition of 16 share the partitioner's low bits; the pass hash must
        // still spread them over k passes, including a k that divides P.
        let partitioner = Partitioner::new(16).unwrap();
        for k in [2u64, 3, 4, 8] {
            let passes = Passes::for_estimate(k, 1);
            let mut counts = vec![0u64; usize::try_from(k).unwrap()];
            let mut n = 0u64;
            for id in (0..400_000u64).filter(|id| partitioner.partition(*id) == 5) {
                counts[passes.pass_of(id) as usize] += 1;
                n += 1;
            }
            // Within 5% of an even split.
            let expected = n / k;
            assert!(
                counts.iter().all(|c| c.abs_diff(expected) * 20 < expected),
                "k={k}: {counts:?}"
            );
        }
    }
}
