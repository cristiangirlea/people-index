//! Person records, and the exact counts of the copies last-wins keeps.

use rand::{Rng, RngExt};
use rand_chacha::ChaCha8Rng;
use serde::Serialize;

use crate::envelope::{rfc3339, write_line};
use crate::error::Result;
use crate::feed_file::RecordSource;
use crate::ids::IdSpace;
use crate::names::{OrgNamer, person_name, school_name};
use crate::org_gen::{ExtraMeta, date};
use crate::sample::{pick_distinct, pick_one, weighted_index};
use crate::targets::{MAX_TARGETS, NO_SLOT};
use crate::words::{
    CERTIFICATIONS, DEGREES, EMPLOYMENT_TYPES, FREE_TEXT_EMPLOYERS, HEADLINE_HOOKS, INDUSTRIES,
    LANGUAGES, PLACES, PROFICIENCIES, SKILLS, TITLES,
};
use crate::world::World;

/// Person records are updated during 2025-2026.
const UPDATED_EPOCH: u64 = 1_735_689_600;
const UPDATED_SPAN: u64 = 2 * 31_536_000;

/// Role start dates fall between 1995 and early 2026 (days since 1970).
const ROLE_DAYS: std::ops::Range<u64> = 9_131..20_454;

#[derive(Serialize)]
struct PersonRecord<'a> {
    id: u64,
    first_name: &'static str,
    last_name: &'a str,
    headline: String,
    summary: String,
    country: &'static str,
    region: &'static str,
    city: &'static str,
    industry: &'static str,
    profile_url: String,
    skills: Vec<&'static str>,
    connections: u32,
    followers: u32,
    open_to_work: bool,
    roles: Vec<Role>,
    memberships: Vec<Membership>,
    educations: Vec<Education>,
    certifications: Vec<Certification>,
    languages: Vec<Language>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    extra: Option<PersonExtra>,
}

#[derive(Serialize)]
struct Role {
    id: u64,
    title: &'static str,
    /// Serialised as JSON `null` when absent: a null id is ordinary data.
    organization_id: Option<u64>,
    organization_name: String,
    organization_domain: Option<String>,
    employment_type: &'static str,
    start_date: String,
    end_date: Option<String>,
    location: String,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    x_team: Option<&'static str>,
}

#[derive(Serialize)]
struct Membership {
    name: String,
    role: &'static str,
    since: u16,
}

#[derive(Serialize)]
struct Education {
    school: String,
    degree: &'static str,
    start_year: u16,
    end_year: u16,
}

#[derive(Serialize)]
struct Certification {
    name: &'static str,
    issued_on: String,
}

#[derive(Serialize)]
struct Language {
    name: &'static str,
    proficiency: &'static str,
}

/// Fields the schema does not know; the pipeline must carry them through untouched.
/// The shared `x_` prefix is the point: it marks them as foreign.
#[allow(clippy::struct_field_names)]
#[derive(Serialize)]
struct PersonExtra {
    x_profile_score: u32,
    x_tags: Vec<&'static str>,
    x_meta: ExtraMeta,
}

/// Counts over the person copies last-wins keeps, for one file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PersonCounts {
    pub(crate) with_unresolved: u64,
    pub(crate) title_counts: [u64; MAX_TARGETS],
    pub(crate) org_name_counts: [u64; MAX_TARGETS],
}

impl PersonCounts {
    pub(crate) fn add(&mut self, other: &Self) {
        self.with_unresolved += other.with_unresolved;
        for (a, b) in self.title_counts.iter_mut().zip(&other.title_counts) {
            *a += b;
        }
        for (a, b) in self.org_name_counts.iter_mut().zip(&other.org_name_counts) {
            *a += b;
        }
    }
}

/// Which organization a role points at.
enum Employer {
    None,
    Resolved(u64),
    Unresolved(u64),
}

/// Writes person records and counts the final copies.
pub(crate) struct PersonSource<'w, 'c> {
    world: &'w World<'c>,
    pub(crate) counts: PersonCounts,
    name: String,
}

impl<'w, 'c> PersonSource<'w, 'c> {
    pub(crate) fn new(world: &'w World<'c>) -> Self {
        Self {
            world,
            counts: PersonCounts::default(),
            name: String::new(),
        }
    }

    fn employer(&self, rng: &mut ChaCha8Rng) -> Employer {
        let cfg = self.world.cfg;
        if rng.random_bool(cfg.null_org_rate) {
            Employer::None
        } else if self.world.org_ranks.is_empty() || rng.random_bool(cfg.unresolved_rate) {
            Employer::Unresolved(IdSpace::unresolved_org_id(rng.random()))
        } else {
            let rank = self.world.org_ranks.sample(rng);
            Employer::Resolved(self.world.org_popularity.apply(rank))
        }
    }

    fn role(
        &mut self,
        rng: &mut ChaCha8Rng,
        id: u64,
        employer: &Employer,
        title: &'static str,
        current: bool,
    ) -> Role {
        let world = self.world;
        let (organization_id, organization_name, organization_domain) = match *employer {
            Employer::None => {
                let name = (*pick_one(rng, FREE_TEXT_EMPLOYERS)).to_owned();
                (None, name, None)
            }
            Employer::Resolved(index) => {
                // The role carries the employer's name as the person typed it: the first
                // record's name, even when the organization was renamed later. Search uses
                // the spliced organization, not this text.
                world.namer.name(index, 0, &mut self.name);
                let domain = OrgNamer::domain(&self.name);
                (
                    Some(world.ids.org_id(index)),
                    self.name.clone(),
                    Some(domain),
                )
            }
            Employer::Unresolved(org_id) => {
                // An invented name from beyond the organization index range.
                world.namer.name(
                    world.cfg.organizations + (org_id & 0xffff_ffff),
                    0,
                    &mut self.name,
                );
                let domain = rng.random_bool(0.5).then(|| OrgNamer::domain(&self.name));
                (Some(org_id), self.name.clone(), domain)
            }
        };
        let start = rng.random_range(ROLE_DAYS);
        let end_date = (!current).then(|| date(start + rng.random_range(60..3_000u64)));
        let (city, _, country) = *pick_one(rng, PLACES);
        let description = format!(
            "{title} responsible for {} and {}.",
            pick_one(rng, SKILLS).to_lowercase(),
            pick_one(rng, SKILLS).to_lowercase()
        );
        Role {
            id,
            title,
            organization_id,
            organization_name,
            organization_domain,
            employment_type: pick_one(rng, EMPLOYMENT_TYPES),
            start_date: date(start),
            end_date,
            location: format!("{city}, {country}"),
            description,
            x_team: rng
                .random_bool(world.cfg.extra_field_rate)
                .then(|| *pick_one(rng, &["core", "platform", "growth", "field", "research"])),
        }
    }
}

impl RecordSource for PersonSource<'_, '_> {
    fn write_record(
        &mut self,
        rng: &mut ChaCha8Rng,
        index: u64,
        _version: u8,
        is_final: bool,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let world = self.world;
        let cfg = world.cfg;
        let id = world.ids.person_id(index);
        let (first_name, last_name) = person_name(rng);
        let (city, region, country) = *pick_one(rng, PLACES);
        let industry = *pick_one(rng, INDUSTRIES);

        let role_count = weighted_index(rng, &world.roles_cumulative);
        let mut roles = Vec::with_capacity(role_count);
        let (mut title_mask, mut org_mask, mut unresolved) = (0u32, 0u32, false);
        for k in 0..role_count {
            let title_index = usize::try_from(world.title_ranks.sample(rng)).unwrap_or(0);
            let title = TITLES[title_index.min(TITLES.len() - 1)];
            let employer = self.employer(rng);
            match employer {
                Employer::Resolved(org) => org_mask |= bit(world.targets.org_slot(org)),
                Employer::Unresolved(_) => unresolved = true,
                Employer::None => {}
            }
            title_mask |= bit(world.targets.title_slot(title_index));
            // Role ids only need to be unique per person; person ids are below 2^41.
            let role_id = id * 16 + k as u64;
            let current = k == 0 && rng.random_bool(0.7);
            roles.push(self.role(rng, role_id, &employer, title, current));
        }
        if is_final {
            self.counts.with_unresolved += u64::from(unresolved);
            for slot in 0..MAX_TARGETS {
                self.counts.title_counts[slot] += u64::from(title_mask >> slot & 1);
                self.counts.org_name_counts[slot] += u64::from(org_mask >> slot & 1);
            }
        }

        let headline = match roles.first() {
            Some(role) => format!(
                "{} at {} | {}",
                role.title,
                role.organization_name,
                pick_one(rng, HEADLINE_HOOKS)
            ),
            None => format!(
                "{} professional | {}",
                industry,
                pick_one(rng, HEADLINE_HOOKS)
            ),
        };
        let summary = format!(
            "Based in {city}. Experienced in {} and {}.",
            pick_one(rng, SKILLS).to_lowercase(),
            pick_one(rng, SKILLS).to_lowercase()
        );
        let record = PersonRecord {
            id,
            first_name,
            last_name: &last_name,
            headline,
            summary,
            country,
            region,
            city,
            industry,
            profile_url: format!(
                "https://profiles.example/p/{}-{}-{id}",
                first_name.to_ascii_lowercase(),
                last_name.to_ascii_lowercase()
            ),
            skills: pick_distinct(rng, SKILLS, 0, 12),
            connections: skewed(rng, 30_000),
            followers: skewed(rng, 100_000),
            open_to_work: rng.random_bool(0.12),
            roles,
            memberships: memberships(rng),
            educations: educations(rng),
            certifications: (0..rng.random_range(0..3u8))
                .map(|_| Certification {
                    name: pick_one(rng, CERTIFICATIONS),
                    issued_on: date(rng.random_range(14_000..20_400)),
                })
                .collect(),
            languages: pick_distinct(rng, LANGUAGES, 1, 3)
                .into_iter()
                .map(|name| Language {
                    name,
                    proficiency: pick_one(rng, PROFICIENCIES),
                })
                .collect(),
            extra: rng.random_bool(cfg.extra_field_rate).then(|| PersonExtra {
                x_profile_score: rng.random_range(0..1000),
                x_tags: pick_distinct(rng, &["imported", "enriched", "verified", "stale"], 1, 2),
                x_meta: ExtraMeta::draw(rng),
            }),
        };
        let updated_at = rfc3339(UPDATED_EPOCH + rng.random_range(0..UPDATED_SPAN));
        let as_string = rng.random_bool(cfg.string_id_rate);
        write_line(out, id, as_string, &updated_at, &record)?;
        Ok(())
    }
}

fn bit(slot: u8) -> u32 {
    if slot == NO_SLOT || usize::from(slot) >= MAX_TARGETS {
        0
    } else {
        1 << slot
    }
}

/// A heavy-tailed count in `[0, max)`: most people have few connections, some very many.
fn skewed<R: Rng + ?Sized>(rng: &mut R, max: u32) -> u32 {
    let a = rng.random_range(0..max);
    let b = rng.random_range(0..max);
    let c = rng.random_range(0..max);
    a.min(b).min(c)
}

fn memberships<R: Rng + ?Sized>(rng: &mut R) -> Vec<Membership> {
    (0..rng.random_range(0..3u8))
        .map(|_| Membership {
            name: format!("{} Professionals Guild", pick_one(rng, INDUSTRIES)),
            role: pick_one(rng, &["Member", "Fellow", "Volunteer", "Board Member"]),
            since: rng.random_range(1990..=2025),
        })
        .collect()
}

fn educations<R: Rng + ?Sized>(rng: &mut R) -> Vec<Education> {
    (0..rng.random_range(0..4u8))
        .map(|_| {
            let start_year = rng.random_range(1975..=2022u16);
            Education {
                school: school_name(rng),
                degree: pick_one(rng, DEGREES),
                start_year,
                end_year: start_year + rng.random_range(1..=5),
            }
        })
        .collect()
}
