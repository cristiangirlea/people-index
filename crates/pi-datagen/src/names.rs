//! Invented names: organizations from syllables, people from generic name parts.
//!
//! An organization's name is a pure function of `(seed, index, version)`, because three
//! places need it without sharing state: the organization writer, the person writer (a role's
//! own `organization_name` text) and the expected-count simulation, which must know every
//! organization's final name to count persons per name exactly.

use rand::{Rng, RngExt};

use crate::hash::{Domain, below, keyed, mix64};
use crate::words::{
    FIRST_NAMES, LAST_NAME_ENDINGS, LAST_NAME_STEMS, ORG_SUFFIXES, ORG_SYLLABLES, REBRAND_SUFFIXES,
};

/// How organization names are derived for one seed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OrgNamer {
    seed: u64,
}

impl OrgNamer {
    pub(crate) fn new(seed: u64) -> Self {
        Self { seed }
    }

    /// Writes the stem (the syllable word) of organization `index` into `out`, capitalised.
    ///
    /// The stem never changes between versions: a rebrand keeps the stem and swaps the
    /// suffix, the way real renames usually look.
    pub(crate) fn stem(self, index: u64, out: &mut String) {
        let h = keyed(self.seed, Domain::OrgName, index);
        // Two or three syllables; three-syllable stems make the name space large enough
        // that collisions stay realistic (some, not most) at millions of organizations.
        let count = if h.trailing_zeros() >= 2 { 2 } else { 3 };
        let mut bits = mix64(h);
        for k in 0..count {
            let syllable = pick(ORG_SYLLABLES, bits);
            bits = mix64(bits);
            if k == 0 {
                capitalise_into(syllable, out);
            } else {
                out.push_str(syllable);
            }
        }
    }

    /// Writes the full name of organization `index` as of record `version` into `out`
    /// (cleared first). Version 0 ends in an [`ORG_SUFFIXES`] word, later versions in a
    /// [`REBRAND_SUFFIXES`] word, so versions of one organization never share a name.
    pub(crate) fn name(self, index: u64, version: u8, out: &mut String) {
        out.clear();
        self.stem(index, out);
        out.push(' ');
        let h = mix64(keyed(self.seed, Domain::OrgName, index) ^ u64::from(version));
        if version == 0 {
            out.push_str(pick(ORG_SUFFIXES, h));
        } else {
            out.push_str(pick(REBRAND_SUFFIXES, h));
        }
    }

    /// The domain for a name: lower-case words joined by `-` under the reserved `.example`
    /// top-level domain, so generated data can never point at a real site.
    pub(crate) fn domain(name: &str) -> String {
        let mut domain: String = name
            .split(' ')
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>()
            .join("-");
        domain.push_str(".example");
        domain
    }
}

/// A plausible personal name drawn from `rng`: `(first, last)`.
pub(crate) fn person_name<R: Rng + ?Sized>(rng: &mut R) -> (&'static str, String) {
    let first = FIRST_NAMES[rng.random_range(0..FIRST_NAMES.len())];
    let stem = LAST_NAME_STEMS[rng.random_range(0..LAST_NAME_STEMS.len())];
    let ending = LAST_NAME_ENDINGS[rng.random_range(0..LAST_NAME_ENDINGS.len())];
    (first, format!("{stem}{ending}"))
}

/// An invented school name: an organization-style stem plus a school word.
pub(crate) fn school_name<R: Rng + ?Sized>(rng: &mut R) -> String {
    const KINDS: &[&str] = &[
        "University",
        "Institute of Technology",
        "Business School",
        "College",
        "Polytechnic",
    ];
    let mut name = String::new();
    capitalise_into(pick(ORG_SYLLABLES, rng.random()), &mut name);
    name.push_str(pick(ORG_SYLLABLES, rng.random()));
    name.push(' ');
    name.push_str(KINDS[rng.random_range(0..KINDS.len())]);
    name
}

/// The element of `list` selected by `hash`.
pub(crate) fn pick<'a>(list: &[&'a str], hash: u64) -> &'a str {
    // `below` returns a value < len, and len came from a usize, so the narrowing is lossless.
    #[allow(clippy::cast_possible_truncation)]
    let i = below(hash, list.len() as u64) as usize;
    list[i]
}

fn capitalise_into(word: &str, out: &mut String) {
    let mut chars = word.chars();
    if let Some(first) = chars.next() {
        out.extend(first.to_uppercase());
        out.push_str(chars.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn versions_differ_but_share_the_stem() {
        let namer = OrgNamer::new(9);
        let (mut a, mut b, mut stem) = (String::new(), String::new(), String::new());
        for i in 0..1000 {
            namer.name(i, 0, &mut a);
            namer.name(i, 1, &mut b);
            stem.clear();
            namer.stem(i, &mut stem);
            assert_ne!(a, b);
            assert!(a.starts_with(&stem) && b.starts_with(&stem));
            assert!(a.chars().next().is_some_and(char::is_uppercase));
        }
    }

    #[test]
    fn names_are_pure() {
        let (mut a, mut b) = (String::new(), String::new());
        OrgNamer::new(3).name(77, 0, &mut a);
        OrgNamer::new(3).name(77, 0, &mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn domains_use_the_reserved_tld() {
        assert_eq!(OrgNamer::domain("Velmor Labs"), "velmor-labs.example");
    }

    #[test]
    fn people_and_schools_have_names() {
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let (first, last) = person_name(&mut rng);
        assert!(!first.is_empty() && last.len() > 2);
        assert!(school_name(&mut rng).contains(' '));
    }
}
