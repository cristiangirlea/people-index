//! Organization records.

use rand::{Rng, RngExt};
use rand_chacha::ChaCha8Rng;
use serde::Serialize;

use crate::envelope::{civil_from_days, rfc3339, write_line};
use crate::error::Result;
use crate::feed_file::RecordSource;
use crate::names::OrgNamer;
use crate::sample::{pick_distinct, pick_one};
use crate::words::{FUNDING_STAGES, INDUSTRIES, KEYWORDS, PLACES, SIZE_RANGES, TECHNOLOGIES};
use crate::world::World;

/// Organization records are updated during 2024-2025.
const UPDATED_EPOCH: u64 = 1_704_067_200;
const UPDATED_SPAN: u64 = 2 * 31_536_000;

#[derive(Serialize)]
struct OrgRecord<'a> {
    id: u64,
    name: &'a str,
    domain: String,
    industry: &'static str,
    country: &'static str,
    city: &'static str,
    size_range: &'static str,
    employee_count: u32,
    founded_year: u16,
    keywords: Vec<&'static str>,
    technologies: Vec<&'static str>,
    description: String,
    addresses: Vec<Address>,
    locations: Vec<Location>,
    funding_rounds: Vec<FundingRound>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    extra: Option<OrgExtra>,
}

#[derive(Serialize)]
struct Address {
    street: String,
    city: &'static str,
    postal_code: String,
    country: &'static str,
}

#[derive(Serialize)]
struct Location {
    city: &'static str,
    country: &'static str,
    is_headquarters: bool,
}

#[derive(Serialize)]
struct FundingRound {
    stage: &'static str,
    amount_usd: u64,
    announced_on: String,
}

/// Fields the schema does not know; the pipeline must carry them through untouched.
/// The shared `x_` prefix is the point: it marks them as foreign.
#[allow(clippy::struct_field_names)]
#[derive(Serialize)]
struct OrgExtra {
    x_source_rank: u32,
    x_tags: Vec<&'static str>,
    x_meta: ExtraMeta,
}

#[derive(Serialize)]
pub(crate) struct ExtraMeta {
    pub(crate) batch: u32,
    pub(crate) confidence: f64,
    pub(crate) verified: bool,
}

impl ExtraMeta {
    pub(crate) fn draw<R: Rng + ?Sized>(rng: &mut R) -> Self {
        Self {
            batch: rng.random_range(1..5000),
            // Two decimals, so the JSON text is short and identical on every platform.
            confidence: f64::from(rng.random_range(0..=100u32)) / 100.0,
            verified: rng.random_bool(0.5),
        }
    }
}

/// Writes organization records; holds a reusable name buffer.
pub(crate) struct OrgSource<'w, 'c> {
    world: &'w World<'c>,
    name: String,
}

impl<'w, 'c> OrgSource<'w, 'c> {
    pub(crate) fn new(world: &'w World<'c>) -> Self {
        Self {
            world,
            name: String::new(),
        }
    }
}

impl RecordSource for OrgSource<'_, '_> {
    fn write_record(
        &mut self,
        rng: &mut ChaCha8Rng,
        index: u64,
        version: u8,
        _is_final: bool,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let world = self.world;
        let cfg = world.cfg;
        world.namer.name(index, version, &mut self.name);
        let id = world.ids.org_id(index);
        let (city, _, country) = pick_one(rng, PLACES);
        // Most organizations are small: weight the size ranges towards the first ones.
        let size = (rng.random_range(0..SIZE_RANGES.len()) * rng.random_range(0..=100usize)) / 100;
        let (size_range, low, high) = SIZE_RANGES[size];
        let industry = pick_one(rng, INDUSTRIES);
        let keywords = pick_distinct(rng, KEYWORDS, 1, 5);
        let technologies = pick_distinct(rng, TECHNOLOGIES, 0, 6);
        let founded_year = rng.random_range(1950..=2025u16);
        let description = format!(
            "{} is a {} company in {city}, {country}, working on {}.",
            self.name,
            industry.to_lowercase(),
            keywords.first().copied().unwrap_or("services"),
        );
        let mut addresses = vec![address(rng, city, country)];
        let mut locations = vec![Location {
            city,
            country,
            is_headquarters: true,
        }];
        for _ in 0..rng.random_range(0..3u8) {
            let (city, _, country) = pick_one(rng, PLACES);
            addresses.push(address(rng, city, country));
            locations.push(Location {
                city,
                country,
                is_headquarters: false,
            });
        }
        let funding_rounds = (0..rng.random_range(0..4u8))
            .map(|_| FundingRound {
                stage: pick_one(rng, FUNDING_STAGES),
                amount_usd: rng.random_range(50u64..2_000u64) * 25_000,
                announced_on: date(rng.random_range(14_000..20_400)),
            })
            .collect();
        let extra = rng.random_bool(cfg.extra_field_rate).then(|| OrgExtra {
            x_source_rank: rng.random_range(1..1_000_000),
            x_tags: pick_distinct(rng, &["imported", "merged", "manual", "crawled"], 1, 2),
            x_meta: ExtraMeta::draw(rng),
        });
        let record = OrgRecord {
            id,
            name: &self.name,
            domain: OrgNamer::domain(&self.name),
            industry,
            country,
            city,
            size_range,
            employee_count: rng.random_range(low..=high),
            founded_year,
            keywords,
            technologies,
            description,
            addresses,
            locations,
            funding_rounds,
            extra,
        };
        let updated_at = rfc3339(UPDATED_EPOCH + rng.random_range(0..UPDATED_SPAN));
        let as_string = rng.random_bool(cfg.string_id_rate);
        write_line(out, id, as_string, &updated_at, &record)?;
        Ok(())
    }
}

fn address<R: Rng + ?Sized>(rng: &mut R, city: &'static str, country: &'static str) -> Address {
    const STREETS: &[&str] = &[
        "Main", "Station", "Park", "Mill", "Harbour", "Market", "Hill",
    ];
    const KINDS: &[&str] = &["Street", "Road", "Avenue", "Square", "Lane"];
    Address {
        street: format!(
            "{} {} {}",
            rng.random_range(1..400u32),
            pick_one(rng, STREETS),
            pick_one(rng, KINDS)
        ),
        city,
        postal_code: format!("{:05}", rng.random_range(1000..99_999u32)),
        country,
    }
}

/// `YYYY-MM-DD` for days since 1970-01-01.
pub(crate) fn date(days: u64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}
