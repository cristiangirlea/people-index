//! Record ids: scattered, unique by construction, and never colliding with unresolved ids.

use crate::hash::{Domain, keyed, mix64};

/// Ids live in a 40-bit space; inputs are bounded by [`MAX_RECORDS`] so the bijection holds.
const BITS: u32 = 40;
const MASK: u64 = (1 << BITS) - 1;

/// The largest number of records of one feed the id space can hold.
pub const MAX_RECORDS: u64 = 1 << BITS;

/// Unresolved organization ids start here: above every id [`IdSpace::org_id`] can return, so
/// "not in the organization feed" holds without keeping a set of the ids that are.
const UNRESOLVED_BASE: u64 = 1 << (BITS + 1);

/// Turns record indices into the ids written to the feeds.
///
/// Real feeds have sparse, unordered ids; sequential ids would hide bugs that depend on id
/// order or density (partitioning, sorting by id instead of by feed order). A keyed bijection
/// on 40 bits gives scattered ids that are still unique without a set.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IdSpace {
    org_key: u64,
    person_key: u64,
}

impl IdSpace {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            org_key: keyed(seed, Domain::OrgIdKey, 0) & MASK,
            person_key: keyed(seed, Domain::PersonIdKey, 0) & MASK,
        }
    }

    /// The id of organization `index` (`index < MAX_RECORDS`), in `[1, 2^40]`.
    pub(crate) fn org_id(self, index: u64) -> u64 {
        scatter(index, self.org_key) + 1
    }

    /// The id of person `index` (`index < MAX_RECORDS`), in `[1, 2^40]`.
    pub(crate) fn person_id(self, index: u64) -> u64 {
        scatter(index, self.person_key) + 1
    }

    /// An organization id guaranteed absent from the organization feed.
    pub(crate) fn unresolved_org_id(draw: u64) -> u64 {
        UNRESOLVED_BASE + (mix64(draw) & 0xffff_ffff)
    }
}

/// A bijection on `[0, 2^40)`: xor with a key, then multiply-xorshift rounds. Each step is
/// invertible modulo 2^40 (odd multipliers, right shifts within the masked width).
fn scatter(index: u64, key: u64) -> u64 {
    let mut x = (index ^ key) & MASK;
    x = x.wrapping_mul(0x9e37_79b9_7f4b) & MASK;
    x ^= x >> 19;
    x = x.wrapping_mul(0xd6e8_feb8_6659) & MASK;
    x ^= x >> 23;
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn ids_are_unique_and_in_range() {
        let ids = IdSpace::new(42);
        let mut seen = HashSet::new();
        for i in 0..200_000 {
            let id = ids.org_id(i);
            assert!((1..=MAX_RECORDS).contains(&id));
            assert!(seen.insert(id), "duplicate id at {i}");
        }
        assert!(ids.org_id(MAX_RECORDS - 1) <= MAX_RECORDS);
    }

    #[test]
    fn unresolved_ids_are_outside_the_org_space() {
        for d in 0..10_000 {
            assert!(IdSpace::unresolved_org_id(d) > MAX_RECORDS);
        }
    }

    #[test]
    fn seeds_change_ids() {
        assert_ne!(IdSpace::new(1).org_id(0), IdSpace::new(2).org_id(0));
        assert_ne!(IdSpace::new(1).org_id(5), IdSpace::new(1).person_id(5));
    }
}
