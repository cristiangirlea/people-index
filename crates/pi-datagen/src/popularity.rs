//! Skewed popularity: Zipf-distributed ranks, mapped onto indices by a permutation.

use rand::Rng;
use rand_distr::{Distribution, Zipf};

use crate::error::{Error, Result};
use crate::hash::{Domain, below, keyed};

/// A bijection on `[0, n)`: `r -> (r * a + c) mod n` with `gcd(a, n) = 1`.
///
/// Popularity rank 0 must not mean "the first organization of the first file": that would
/// put every popular organization in one file and one partition. The permutation scatters
/// ranks across the feed, and its inverse is never needed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexPermutation {
    n: u64,
    a: u64,
    c: u64,
}

impl IndexPermutation {
    pub(crate) fn new(seed: u64, n: u64) -> Self {
        if n <= 1 {
            return Self { n, a: 1, c: 0 };
        }
        let mut a = (keyed(seed, Domain::Permutation, 0) % n).max(1);
        while gcd(a, n) != 1 {
            a = a % (n - 1) + 1;
        }
        let c = below(keyed(seed, Domain::Permutation, 1), n);
        Self { n, a, c }
    }

    /// The index for rank `r` (`r < n`).
    pub(crate) fn apply(self, r: u64) -> u64 {
        if self.n <= 1 {
            return 0;
        }
        // The result of `% n` is < n, which fits in u64.
        #[allow(clippy::cast_possible_truncation)]
        let i =
            ((u128::from(r) * u128::from(self.a) + u128::from(self.c)) % u128::from(self.n)) as u64;
        i
    }
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Draws ranks in `[0, n)` with probability proportional to `1 / (rank + 1)^s`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ZipfRanks {
    n: u64,
    zipf: Option<Zipf<f64>>,
}

impl ZipfRanks {
    /// `n == 0` is allowed (there is nothing to draw; callers check [`Self::is_empty`]).
    pub(crate) fn new(n: u64, exponent: f64) -> Result<Self> {
        if n == 0 {
            return Ok(Self { n, zipf: None });
        }
        // Precision loss above 2^53 elements only blurs the tail of the distribution.
        #[allow(clippy::cast_precision_loss)]
        let zipf = Zipf::new(n as f64, exponent)
            .map_err(|e| Error::Config(format!("zipf over {n} items, exponent {exponent}: {e}")))?;
        Ok(Self {
            n,
            zipf: Some(zipf),
        })
    }

    pub(crate) fn is_empty(self) -> bool {
        self.zipf.is_none()
    }

    /// A rank in `[0, n)`; 0 when the set is empty.
    pub(crate) fn sample<R: Rng + ?Sized>(self, rng: &mut R) -> u64 {
        let Some(zipf) = self.zipf else { return 0 };
        let x = zipf.sample(rng);
        // Zipf returns an integral float in [1, n]; the clamp guards the float edge cases.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let rank = (x as u64).saturating_sub(1);
        rank.min(self.n - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use std::collections::HashSet;

    #[test]
    fn permutation_is_a_bijection() {
        for n in [1u64, 2, 3, 10, 97, 1000, 1024] {
            let p = IndexPermutation::new(5, n);
            let seen: HashSet<u64> = (0..n).map(|r| p.apply(r)).collect();
            assert_eq!(seen.len() as u64, n, "n = {n}");
            assert!(seen.iter().all(|&i| i < n));
        }
    }

    #[test]
    fn zipf_is_skewed_and_in_range() {
        let ranks = ZipfRanks::new(1000, 1.0).unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let mut top = 0;
        for _ in 0..20_000 {
            let r = ranks.sample(&mut rng);
            assert!(r < 1000);
            if r < 10 {
                top += 1;
            }
        }
        // The first 1% of ranks carries roughly 40% of the mass at s = 1.
        assert!(top > 5_000, "{top}");
    }

    #[test]
    fn empty_sets_draw_nothing() {
        let ranks = ZipfRanks::new(0, 1.0).unwrap();
        assert!(ranks.is_empty());
        assert!(ZipfRanks::new(10, -1.0).is_err());
    }
}
