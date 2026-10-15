//! Stateless hashing for decisions that must be the same wherever they are made.
//!
//! Which records are duplicated, where a duplicate lands and what an organization is called
//! are pure functions of `(seed, domain, index)`. That is what lets the planner, the per-file
//! writers and the expected-count simulation agree without sharing a random stream, and what
//! keeps parallel generation byte-identical to a sequential run.

/// The `SplitMix64` finaliser: a cheap bijective mixer with good avalanche.
pub(crate) const fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Separates the hash streams of unrelated decisions so they never correlate.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Domain {
    OrgDuplicate = 1,
    PersonDuplicate,
    DuplicateTarget,
    DuplicateSlot,
    OrgName,
    OrgIdKey,
    PersonIdKey,
    Permutation,
    FileStream,
}

/// A 64-bit hash of `index` under `seed` and `domain`.
pub(crate) const fn keyed(seed: u64, domain: Domain, index: u64) -> u64 {
    let base = mix64(seed ^ (domain as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    mix64(base ^ index.wrapping_mul(0xd6e8_feb8_6659_fd93))
}

/// Maps a hash to `[0, 1)` with 53 bits of precision.
#[allow(clippy::cast_precision_loss)] // 53-bit integers are exact in f64
pub(crate) fn unit(hash: u64) -> f64 {
    (hash >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

/// True with probability `rate`, decided by `hash`.
pub(crate) fn chance(hash: u64, rate: f64) -> bool {
    unit(hash) < rate
}

/// `hash` reduced to `[0, n)` without modulo bias worth caring about (Lemire's reduction).
pub(crate) fn below(hash: u64, n: u64) -> u64 {
    // The high half of a 64x64 product is always < n, so the narrowing is lossless.
    #[allow(clippy::cast_possible_truncation)]
    let r = ((u128::from(hash) * u128::from(n)) >> 64) as u64;
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_give_independent_streams() {
        assert_ne!(
            keyed(7, Domain::OrgDuplicate, 1),
            keyed(7, Domain::PersonDuplicate, 1)
        );
        assert_ne!(keyed(7, Domain::OrgName, 1), keyed(8, Domain::OrgName, 1));
    }

    #[test]
    fn unit_and_below_stay_in_range() {
        for i in 0..10_000u64 {
            let h = mix64(i);
            let u = unit(h);
            assert!((0.0..1.0).contains(&u));
            assert!(below(h, 17) < 17);
        }
        assert_eq!(below(u64::MAX, 1), 0);
    }

    #[test]
    fn chance_tracks_the_rate() {
        let hits = (0..100_000u64)
            .filter(|&i| chance(keyed(1, Domain::OrgDuplicate, i), 0.1))
            .count();
        assert!((9_000..11_000).contains(&hits), "{hits}");
        assert!(!chance(0, 0.0));
    }
}
