//! The titles and organization names whose counts go into `expected.json`.
//!
//! Counting persons per value for every title and every organization name would need a map
//! as large as the organization feed per worker. The acceptance check only asks about a
//! handful, so the generator picks them up front (common, middling and rare ones, plus a
//! renamed organization's old and new names, which only last-wins gets right) and tags each
//! title and each organization with the slot of the target it matches.

use std::collections::HashMap;

use rayon::prelude::*;

use crate::error::{Error, Result};
use crate::names::OrgNamer;
use crate::plan::DuplicatePlan;
use crate::popularity::IndexPermutation;
use crate::words::TITLES;

/// "Not a target".
pub(crate) const NO_SLOT: u8 = u8::MAX;

/// At most this many titles and this many names (the bitmasks are `u32`).
pub(crate) const MAX_TARGETS: usize = 20;

/// Title popularity ranks to report: the head, the middle and the tail of the list.
const TITLE_RANKS: [usize; 19] = [
    0, 1, 2, 3, 5, 8, 12, 18, 25, 35, 50, 70, 90, 110, 130, 150, 170, 185, 195,
];

/// Organization popularity ranks to report; the rarest organization is added on top.
const ORG_RANKS: [u64; 16] = [
    0, 1, 2, 3, 4, 6, 9, 14, 20, 35, 60, 100, 200, 500, 1000, 5000,
];

/// Organizations per parallel slot-tagging chunk.
const CHUNK: usize = 1 << 16;

/// How far to look for a duplicated organization to use as the rename target.
const RENAME_SEARCH: u64 = 1 << 20;

/// The chosen targets and the lookup tables that map values onto them.
#[derive(Debug)]
pub(crate) struct Targets {
    pub(crate) titles: Vec<&'static str>,
    title_slot: Vec<u8>,
    pub(crate) org_names: Vec<String>,
    /// One byte per organization: memory is `organizations` bytes, the generator's only
    /// per-record state.
    org_slot: Vec<u8>,
}

impl Targets {
    pub(crate) fn build(
        organizations: u64,
        namer: OrgNamer,
        popularity: IndexPermutation,
        duplicates: &DuplicatePlan,
    ) -> Result<Self> {
        let mut titles: Vec<&'static str> = TITLE_RANKS
            .iter()
            .chain(std::iter::once(&(TITLES.len() - 1)))
            .filter_map(|&r| TITLES.get(r).copied())
            .collect();
        dedup_in_order(&mut titles);
        titles.truncate(MAX_TARGETS);
        let mut title_slot = vec![NO_SLOT; TITLES.len()];
        for (slot, title) in titles.iter().enumerate() {
            if let Some(i) = TITLES.iter().position(|t| t == title) {
                title_slot[i] = slot_byte(slot);
            }
        }

        let org_names = Self::pick_org_names(organizations, namer, popularity, duplicates);
        let lookup: HashMap<&str, u8> = org_names
            .iter()
            .enumerate()
            .map(|(slot, name)| (name.as_str(), slot_byte(slot)))
            .collect();
        let len = usize::try_from(organizations)
            .map_err(|_| Error::Config("too many organizations for this platform".into()))?;
        let mut org_slot = vec![NO_SLOT; len];
        org_slot
            .par_chunks_mut(CHUNK)
            .enumerate()
            .for_each(|(chunk, slots)| {
                let mut name = String::new();
                let first = (chunk * CHUNK) as u64;
                for (k, slot) in slots.iter_mut().enumerate() {
                    let index = first + k as u64;
                    namer.name(index, duplicates.final_version(index), &mut name);
                    *slot = lookup.get(name.as_str()).copied().unwrap_or(NO_SLOT);
                }
            });

        Ok(Self {
            titles,
            title_slot,
            org_names,
            org_slot,
        })
    }

    fn pick_org_names(
        organizations: u64,
        namer: OrgNamer,
        popularity: IndexPermutation,
        duplicates: &DuplicatePlan,
    ) -> Vec<String> {
        let mut picked = Vec::new();
        if organizations == 0 {
            return picked;
        }
        let name_of = |index: u64, version: u8| {
            let mut s = String::new();
            namer.name(index, version, &mut s);
            s
        };
        // A renamed organization: its new name must count its persons, its old name must
        // count only persons of other organizations that happen to carry that name.
        if let Some(renamed) = (0..organizations.min(RENAME_SEARCH))
            .map(|r| popularity.apply(r))
            .find(|&i| duplicates.is_duplicated(i))
        {
            picked.push(name_of(renamed, 1));
            picked.push(name_of(renamed, 0));
        }
        let ranks = ORG_RANKS
            .iter()
            .copied()
            .filter(|&r| r < organizations)
            .chain(std::iter::once(organizations - 1));
        for rank in ranks {
            let index = popularity.apply(rank);
            picked.push(name_of(index, duplicates.final_version(index)));
        }
        dedup_in_order(&mut picked);
        picked.truncate(MAX_TARGETS);
        picked
    }

    /// The target slot of title `TITLES[index]`, or [`NO_SLOT`].
    pub(crate) fn title_slot(&self, index: usize) -> u8 {
        self.title_slot.get(index).copied().unwrap_or(NO_SLOT)
    }

    /// The target slot of organization `index`'s final name, or [`NO_SLOT`].
    pub(crate) fn org_slot(&self, index: u64) -> u8 {
        usize::try_from(index)
            .ok()
            .and_then(|i| self.org_slot.get(i).copied())
            .unwrap_or(NO_SLOT)
    }
}

fn slot_byte(slot: usize) -> u8 {
    // Targets are truncated to MAX_TARGETS (20) before slots are assigned.
    u8::try_from(slot).unwrap_or(NO_SLOT)
}

fn dedup_in_order<T: PartialEq + Clone>(items: &mut Vec<T>) {
    let mut kept: Vec<T> = Vec::with_capacity(items.len());
    for item in items.drain(..) {
        if !kept.contains(&item) {
            kept.push(item);
        }
    }
    *items = kept;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Domain;
    use crate::plan::FeedLayout;

    fn build(orgs: u64, rate: f64) -> (Targets, DuplicatePlan) {
        let dups = DuplicatePlan::new(4, Domain::OrgDuplicate, rate, FeedLayout::new(orgs, 3));
        let t = Targets::build(
            orgs,
            OrgNamer::new(4),
            IndexPermutation::new(4, orgs),
            &dups,
        )
        .unwrap();
        (t, dups)
    }

    #[test]
    fn titles_span_head_and_tail() {
        let (t, _) = build(10, 0.0);
        assert_eq!(t.titles.len(), MAX_TARGETS);
        assert_eq!(t.titles[0], TITLES[0]);
        assert!(t.titles.contains(TITLES.last().unwrap()));
        assert_eq!(t.title_slot(0), 0);
    }

    #[test]
    fn org_slots_match_final_names() {
        let (t, dups) = build(5000, 0.05);
        assert!(t.org_names.len() <= MAX_TARGETS && t.org_names.len() >= 10);
        let namer = OrgNamer::new(4);
        let mut name = String::new();
        let mut tagged = 0;
        for i in 0..5000 {
            namer.name(i, dups.final_version(i), &mut name);
            let slot = t.org_slot(i);
            match t.org_names.iter().position(|n| *n == name) {
                Some(p) => {
                    assert_eq!(usize::from(slot), p);
                    tagged += 1;
                }
                None => assert_eq!(slot, NO_SLOT),
            }
        }
        assert!(
            tagged >= t.org_names.len() - 1,
            "the old name may match nothing"
        );
    }

    #[test]
    fn no_organizations_no_names() {
        let (t, _) = build(0, 0.01);
        assert_eq!(t.org_names, [] as [std::string::String; 0]);
        assert_eq!(t.org_slot(0), NO_SLOT);
    }
}
