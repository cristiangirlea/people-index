//! Everything derived from the config once and shared, read-only, by every file writer.

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::config::GenConfig;
use crate::error::Result;
use crate::hash::{Domain, keyed, mix64};
use crate::ids::IdSpace;
use crate::names::OrgNamer;
use crate::plan::{DuplicatePlan, FeedLayout};
use crate::popularity::{IndexPermutation, ZipfRanks};
use crate::targets::Targets;
use crate::words::TITLES;

/// The two feeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Feed {
    Organizations,
    Persons,
}

impl Feed {
    /// The directory of the feed's files, relative to the output root.
    pub(crate) fn dir(self) -> &'static str {
        match self {
            Self::Organizations => "organizations",
            Self::Persons => "persons",
        }
    }

    /// The path of file `file`: `persons/part-00042.ndjson.gz`.
    pub(crate) fn path(self, file: u32) -> String {
        format!("{}/part-{file:05}.ndjson.gz", self.dir())
    }
}

/// The shared, immutable state of one generation run.
#[derive(Debug)]
pub(crate) struct World<'a> {
    pub(crate) cfg: &'a GenConfig,
    pub(crate) ids: IdSpace,
    pub(crate) namer: OrgNamer,
    pub(crate) org_layout: FeedLayout,
    pub(crate) person_layout: FeedLayout,
    pub(crate) org_dups: DuplicatePlan,
    pub(crate) person_dups: DuplicatePlan,
    /// Organization popularity: a Zipf rank, then a permutation from rank to index.
    pub(crate) org_ranks: ZipfRanks,
    pub(crate) org_popularity: IndexPermutation,
    pub(crate) title_ranks: ZipfRanks,
    pub(crate) targets: Targets,
    pub(crate) roles_cumulative: [u64; 9],
}

impl<'a> World<'a> {
    pub(crate) fn new(cfg: &'a GenConfig) -> Result<Self> {
        cfg.validate()?;
        let org_layout = FeedLayout::new(cfg.organizations, cfg.files_per_feed);
        let person_layout = FeedLayout::new(cfg.persons, cfg.files_per_feed);
        let org_dups = DuplicatePlan::new(
            cfg.seed,
            Domain::OrgDuplicate,
            cfg.duplicate_org_rate,
            org_layout,
        );
        let person_dups = DuplicatePlan::new(
            cfg.seed,
            Domain::PersonDuplicate,
            cfg.duplicate_person_rate,
            person_layout,
        );
        let namer = OrgNamer::new(cfg.seed);
        let org_popularity = IndexPermutation::new(cfg.seed, cfg.organizations);
        let targets = Targets::build(cfg.organizations, namer, org_popularity, &org_dups)?;
        let mut roles_cumulative = [0u64; 9];
        let mut total = 0;
        for (c, &w) in roles_cumulative.iter_mut().zip(&cfg.roles_weights) {
            total += u64::from(w);
            *c = total;
        }
        Ok(Self {
            cfg,
            ids: IdSpace::new(cfg.seed),
            namer,
            org_layout,
            person_layout,
            org_dups,
            person_dups,
            org_ranks: ZipfRanks::new(cfg.organizations, cfg.org_popularity_exponent)?,
            org_popularity,
            title_ranks: ZipfRanks::new(TITLES.len() as u64, cfg.title_popularity_exponent)?,
            targets,
            roles_cumulative,
        })
    }

    /// The random stream of one file: independent of every other file, so files can be
    /// written in any order or in parallel and still come out byte-identical.
    pub(crate) fn file_rng(&self, feed: Feed, file: u32) -> ChaCha8Rng {
        let stream = (u64::from(feed == Feed::Persons) << 32) | u64::from(file);
        let mut h = keyed(self.cfg.seed, Domain::FileStream, stream);
        let mut key = [0u8; 32];
        for chunk in key.as_chunks_mut::<8>().0 {
            *chunk = h.to_le_bytes();
            h = mix64(h);
        }
        ChaCha8Rng::from_seed(key)
    }

    pub(crate) fn layout(&self, feed: Feed) -> FeedLayout {
        match feed {
            Feed::Organizations => self.org_layout,
            Feed::Persons => self.person_layout,
        }
    }

    pub(crate) fn duplicates(&self, feed: Feed) -> &DuplicatePlan {
        match feed {
            Feed::Organizations => &self.org_dups,
            Feed::Persons => &self.person_dups,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;

    #[test]
    fn paths_sort_in_feed_order() {
        assert_eq!(Feed::Persons.path(3), "persons/part-00003.ndjson.gz");
        assert!(Feed::Organizations.path(9) < Feed::Organizations.path(10));
    }

    #[test]
    fn file_streams_are_independent_and_stable() {
        let cfg = GenConfig::default();
        let world = World::new(&cfg).unwrap();
        let a: u64 = world.file_rng(Feed::Persons, 0).random();
        let b: u64 = world.file_rng(Feed::Persons, 0).random();
        let c: u64 = world.file_rng(Feed::Persons, 1).random();
        let d: u64 = world.file_rng(Feed::Organizations, 0).random();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
