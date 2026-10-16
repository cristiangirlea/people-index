//! How many passes a `join` or `assemble` task runs: the memory estimate of the side it holds,
//! from the per-partition sizes the earlier stages committed, against `task_memory_bytes`.
//!
//! The estimate is the rows' encoded size before compression (what the segments decode to)
//! plus a fixed overhead per row for the in-memory structures. Both are estimates, not
//! measurements: an encoded row is a little larger than its body in memory (varint ids and
//! `Seq`), and the overheads below assume a `HashMap` that may be up to half empty after
//! growing and a `Vec` that may be up to half empty, plus an allocator header per body.

use pi_coord::{PartitionSize, StageName};
use pi_model::Passes;

/// Bytes per organization held by `join` besides its body: the `LastWins` entry (`u64` key,
/// `Seq`, `Vec` header: 48 bytes), doubled for the map's spare capacity, plus the body's
/// allocation.
pub(crate) const ORG_ROW_OVERHEAD: u64 = 128;
/// Bytes per joined row held by `assemble` besides its body: its `(u64, Option<Vec<u8>>)`
/// slot in the person's group (32 bytes), doubled for the vector's spare capacity, plus the
/// body's allocation.
pub(crate) const JOINED_ROW_OVERHEAD: u64 = 96;
/// Bytes per person record `assemble` keeps track of: the group's map entry (48 bytes), the
/// winners' entry (24) and the built set's (8), each doubled for spare capacity, rounded up.
pub(crate) const PERSON_ROW_OVERHEAD: u64 = 192;

/// What `join` holds for an organization partition of this size.
pub(crate) fn join_estimate(orgs: PartitionSize) -> u64 {
    orgs.raw_bytes
        .saturating_add(orgs.rows.saturating_mul(ORG_ROW_OVERHEAD))
}

/// What `assemble` holds for a person partition with these joined rows and person rows.
pub(crate) fn assemble_estimate(joined: PartitionSize, persons: PartitionSize) -> u64 {
    joined
        .raw_bytes
        .saturating_add(joined.rows.saturating_mul(JOINED_ROW_OVERHEAD))
        .saturating_add(persons.rows.saturating_mul(PERSON_ROW_OVERHEAD))
}

/// The passes of one task, logged when there is more than one.
pub(crate) fn plan_passes(stage: StageName, partition: u32, estimate: u64, budget: u64) -> Passes {
    let passes = Passes::for_estimate(estimate, budget);
    if Passes::capped(estimate, budget) {
        tracing::warn!(
            stage = stage.as_str(),
            partition,
            estimate,
            budget,
            passes = passes.count(),
            "the partition needs more passes than allowed; raise PI_MIN_PARTITIONS or PI_TASK_MEMORY_BYTES"
        );
    } else if passes.count() > 1 {
        tracing::info!(
            stage = stage.as_str(),
            partition,
            estimate,
            budget,
            passes = passes.count(),
            "partition larger than the task memory budget; running it in passes"
        );
    }
    passes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn size(rows: u64, raw_bytes: u64) -> PartitionSize {
        PartitionSize { rows, raw_bytes }
    }

    #[test]
    fn estimates_are_raw_bytes_plus_overhead_per_row() {
        assert_eq!(join_estimate(size(0, 0)), 0);
        assert_eq!(join_estimate(size(10, 1000)), 1000 + 10 * ORG_ROW_OVERHEAD);
        assert_eq!(
            assemble_estimate(size(10, 1000), size(4, 400)),
            1000 + 10 * JOINED_ROW_OVERHEAD + 4 * PERSON_ROW_OVERHEAD
        );
        assert_eq!(join_estimate(size(u64::MAX, u64::MAX)), u64::MAX);
    }

    #[test]
    fn passes_follow_the_estimate_and_the_budget() {
        let estimate = join_estimate(size(1000, 1 << 20));
        assert_eq!(
            plan_passes(StageName::Join, 0, estimate, 512 << 20).count(),
            1
        );
        let k = plan_passes(StageName::Join, 0, estimate, estimate / 3).count();
        assert!((3..=4).contains(&k), "{k}");
        assert_eq!(
            plan_passes(StageName::Assemble, 0, u64::MAX, 1).count(),
            pi_model::MAX_PASSES
        );
    }
}
