//! Hash partitioning of ids, and the choice of the partition count.
//!
//! `p = xxh3_64(id as u64 little-endian, seed 0) % P` with `P` a power of two, so the modulo is
//! a mask. The function is fixed by the contract: any worker, any version, any language must
//! put an id in the same partition, or the partitioned hash join silently loses rows.

use xxhash_rust::xxh3::xxh3_64;

/// The partition count is not usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    /// `P` must be a power of two of at least 1.
    #[error("partition count {0} is not a power of two")]
    NotPowerOfTwo(u32),
    /// The input needs more partitions than a u32 power of two can hold.
    #[error("the input needs more than 2^31 partitions")]
    TooMany,
    /// A zero target would mean infinitely many partitions.
    #[error("partition_target_bytes must be greater than zero")]
    ZeroTarget,
}

/// Maps an id to its partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Partitioner {
    count: u32,
    mask: u64,
}

impl Partitioner {
    /// A partitioner over `count` partitions; `count` must be a power of two.
    pub fn new(count: u32) -> Result<Self, PartitionError> {
        if !count.is_power_of_two() {
            return Err(PartitionError::NotPowerOfTwo(count));
        }
        Ok(Self {
            count,
            mask: u64::from(count) - 1,
        })
    }

    /// The number of partitions `P`.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.count
    }

    /// The partition of `id`, in `0..P`.
    #[must_use]
    pub fn partition(&self, id: u64) -> u32 {
        let p = xxh3_64(&id.to_le_bytes()) & self.mask;
        // The mask is below `count`, a u32, so the value always fits.
        u32::try_from(p).unwrap_or(0)
    }
}

/// The partition count `plan` picks: enough partitions that each holds about `target_bytes` of
/// input, at least `min_partitions`, rounded up to a power of two.
pub fn partition_count(
    total_bytes: u64,
    target_bytes: u64,
    min_partitions: u32,
) -> Result<u32, PartitionError> {
    if target_bytes == 0 {
        return Err(PartitionError::ZeroTarget);
    }
    let needed = total_bytes
        .div_ceil(target_bytes)
        .max(u64::from(min_partitions))
        .max(1);
    let rounded = needed
        .checked_next_power_of_two()
        .ok_or(PartitionError::TooMany)?;
    u32::try_from(rounded)
        .ok()
        .filter(|p| *p <= 1 << 31)
        .ok_or(PartitionError::TooMany)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_counts_that_are_not_powers_of_two() {
        for bad in [0, 3, 6, 100] {
            assert_eq!(
                Partitioner::new(bad),
                Err(PartitionError::NotPowerOfTwo(bad))
            );
        }
        assert!(Partitioner::new(1).is_ok());
        assert!(Partitioner::new(1 << 31).is_ok());
    }

    #[test]
    fn hash_is_pinned() {
        // Pinned so a dependency upgrade or a refactor cannot move ids between partitions of
        // an existing run. The value is xxh3_64 of the eight bytes of 0u64 with seed 0.
        assert_eq!(xxh3_64(&0u64.to_le_bytes()), 0xC77B_3ABB_6F87_ACD9);
        let p = Partitioner::new(1 << 16).unwrap();
        assert_eq!(p.partition(0), 0xACD9);
    }

    #[test]
    fn one_partition_takes_everything() {
        let p = Partitioner::new(1).unwrap();
        assert!((0..1000).all(|id| p.partition(id) == 0));
    }

    #[test]
    fn sequential_ids_spread_evenly() {
        let p = Partitioner::new(16).unwrap();
        let mut counts = [0u32; 16];
        for id in 0..160_000u64 {
            counts[p.partition(id) as usize] += 1;
        }
        // 10_000 expected per partition; 5% is far outside random variation for a good hash.
        assert!(
            counts.iter().all(|c| (9_500..=10_500).contains(c)),
            "{counts:?}"
        );
    }

    #[test]
    fn partition_count_rules() {
        const MIB: u64 = 1 << 20;
        assert_eq!(partition_count(0, 256 * MIB, 16), Ok(16));
        assert_eq!(partition_count(256 * MIB * 16, 256 * MIB, 16), Ok(16));
        assert_eq!(partition_count(256 * MIB * 16 + 1, 256 * MIB, 16), Ok(32));
        assert_eq!(partition_count(10, 1, 3), Ok(16));
        assert_eq!(partition_count(0, 1, 0), Ok(1));
        assert_eq!(partition_count(5, 0, 1), Err(PartitionError::ZeroTarget));
        assert_eq!(
            partition_count(u64::MAX, 1, 1),
            Err(PartitionError::TooMany)
        );
    }
}
