//! Which record goes into which file, and where duplicates land.
//!
//! Record `i` of a feed is written to file `file_of(i)` (contiguous blocks). A duplicated
//! record is written a second time *later in feed order*: into a later file at a hashed
//! position, or, from the last file, at the end of that same file. The second copy is the
//! one last-wins keeps, which is what the expected counts assume.

use std::ops::Range;

use crate::hash::{Domain, below, chance, keyed};

/// Splits `total` records over `files` files in contiguous, near-equal blocks.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FeedLayout {
    total: u64,
    files: u32,
}

impl FeedLayout {
    pub(crate) fn new(total: u64, files: u32) -> Self {
        Self { total, files }
    }

    fn start(self, file: u32) -> u64 {
        // file <= files, so the quotient is <= total and fits in u64.
        #[allow(clippy::cast_possible_truncation)]
        let s = (u128::from(file) * u128::from(self.total) / u128::from(self.files)) as u64;
        s
    }

    /// The record indices of `file`.
    pub(crate) fn block(self, file: u32) -> Range<u64> {
        self.start(file)..self.start(file + 1)
    }
}

/// A second copy of record `index`, to be written into a later position.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Duplicate {
    /// Written just before the target file's own record number `slot` (0-based within the
    /// file); `slot == own record count` or `u64::MAX` means after the last one.
    pub(crate) slot: u64,
    pub(crate) index: u64,
}

/// Every duplicate of one feed, grouped by the file it is written into.
///
/// Memory: one 16-byte entry per duplicated record (about `rate * total`), never per record.
#[derive(Debug)]
pub(crate) struct DuplicatePlan {
    seed: u64,
    domain: Domain,
    rate: f64,
    incoming: Vec<Vec<Duplicate>>,
}

impl DuplicatePlan {
    pub(crate) fn new(seed: u64, domain: Domain, rate: f64, layout: FeedLayout) -> Self {
        let mut plan = Self {
            seed,
            domain,
            rate,
            incoming: vec![Vec::new(); layout.files as usize],
        };
        if rate <= 0.0 {
            return plan;
        }
        let last = layout.files - 1;
        // Person and organization plans must not pick the same targets for the same index.
        let tag = (domain as u64) << 56;
        for file in 0..layout.files {
            for index in layout.block(file) {
                if !plan.is_duplicated(index) {
                    continue;
                }
                let (target, slot) = if file == last {
                    (file, u64::MAX)
                } else {
                    let h = keyed(seed, Domain::DuplicateTarget, index ^ tag);
                    // below(.., last - file) < last - file, which came from a u32.
                    #[allow(clippy::cast_possible_truncation)]
                    let target = file + 1 + below(h, u64::from(last - file)) as u32;
                    let own = layout.block(target);
                    let h = keyed(seed, Domain::DuplicateSlot, index ^ tag);
                    (target, below(h, own.end - own.start + 1))
                };
                plan.incoming[target as usize].push(Duplicate { slot, index });
            }
        }
        for list in &mut plan.incoming {
            list.sort_unstable();
        }
        plan
    }

    /// True when record `index` is written twice; its first copy is then stale.
    pub(crate) fn is_duplicated(&self, index: u64) -> bool {
        self.rate > 0.0 && chance(keyed(self.seed, self.domain, index), self.rate)
    }

    /// The second copies written into `file`, ordered by slot.
    pub(crate) fn incoming(&self, file: u32) -> &[Duplicate] {
        &self.incoming[file as usize]
    }

    /// The final version of record `index`: 1 when it is duplicated, else 0.
    pub(crate) fn final_version(&self, index: u64) -> u8 {
        u8::from(self.is_duplicated(index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_cover_every_record_once() {
        for (total, files) in [(0u64, 3u32), (1, 4), (10, 3), (1000, 7)] {
            let layout = FeedLayout::new(total, files);
            let mut next = 0;
            for f in 0..files {
                let b = layout.block(f);
                assert_eq!(b.start, next);
                next = b.end;
            }
            assert_eq!(next, total);
        }
    }

    #[test]
    fn duplicates_land_later_and_once() {
        let layout = FeedLayout::new(10_000, 5);
        let plan = DuplicatePlan::new(3, Domain::OrgDuplicate, 0.05, layout);
        let mut seen = 0;
        for f in 0..5 {
            let own = layout.block(f);
            for d in plan.incoming(f) {
                assert!(plan.is_duplicated(d.index));
                if f == 4 && d.slot == u64::MAX {
                    assert!(own.contains(&d.index));
                } else {
                    assert!(d.index < own.start, "copy must be in a later file");
                    assert!(d.slot <= own.end - own.start);
                }
                seen += 1;
            }
        }
        let expected = (0..10_000).filter(|&i| plan.is_duplicated(i)).count();
        assert_eq!(seen, expected);
        assert!((350..650).contains(&seen), "{seen}");
    }

    #[test]
    fn single_file_appends_at_the_end() {
        let layout = FeedLayout::new(1000, 1);
        let plan = DuplicatePlan::new(1, Domain::PersonDuplicate, 0.1, layout);
        assert!(plan.incoming(0).iter().all(|d| d.slot == u64::MAX));
        assert_ne!(plan.incoming(0), []);
        let none = DuplicatePlan::new(1, Domain::PersonDuplicate, 0.0, layout);
        assert_eq!(none.incoming(0), []);
        assert_eq!(none.final_version(5), 0);
    }
}
