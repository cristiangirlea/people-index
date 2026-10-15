//! pi-datagen: seeded synthetic person and organization feeds, plus the exact counts the
//! acceptance check expects (`expected.json`). See docs/ARCHITECTURE.md, "The data".
//!
//! The output under a [`Sink`] is:
//!
//! ```text
//! organizations/part-00000.ndjson.gz ...   one envelope per line
//! persons/part-00000.ndjson.gz ...
//! expected.json                            see [`Expected`]
//! ```
//!
//! # Exactness
//!
//! The counts are simulated while generating, never measured afterwards. Every decision the
//! counts depend on (which ids are duplicated, where the second copy lands, each
//! organization's final name) is a pure hash of the seed and the record index, so the file
//! writers and the simulation agree without sharing state. Second copies always come later
//! in feed order (files sorted by name, then line), so they are what last-wins keeps.
//!
//! # Determinism
//!
//! Each file has its own `ChaCha8` stream keyed by `(seed, feed, file)`. Files are written in
//! parallel on the rayon pool and are byte-identical to a sequential run; the same config
//! gives the same bytes on the same platform (Zipf sampling uses `f64` maths from the
//! platform's libm, so a different platform may differ in rare draws, but its
//! `expected.json` always matches its own data).
//!
//! # Memory
//!
//! Records are streamed. Fixed state is one byte per organization (its target-name slot)
//! plus one 16-byte entry per duplicated record: about 10 MB for 10M organizations, and
//! nothing per person. Each rayon thread holds about 1 MiB of output buffer.
//!
//! [`generate`] is CPU-bound and blocking; async callers run it under `spawn_blocking`.

mod config;
mod envelope;
mod error;
mod expected;
mod feed_file;
mod hash;
mod ids;
mod local_sink;
mod malformed;
mod memory_sink;
mod names;
mod org_gen;
mod person_gen;
mod plan;
mod popularity;
mod sample;
mod sink;
mod targets;
mod words;
mod world;

use std::sync::{Mutex, PoisonError};

use flate2::Compression;
use flate2::write::GzEncoder;
use rayon::prelude::*;

pub use config::{GenConfig, MAX_FILES};
pub use error::{Error, Result};
pub use expected::{EXPECTED_FILE, Expected, MalformedCounts};
pub use ids::MAX_RECORDS;
pub use local_sink::LocalDirSink;
pub use memory_sink::MemorySink;
pub use sink::{Sink, SinkWriter, path_segments};

use feed_file::{FileCounts, write_feed_file};
use org_gen::OrgSource;
use person_gen::{PersonCounts, PersonSource};
use world::{Feed, World};

/// Generates both feeds into `sink`, writes `expected.json` beside them, and returns the
/// expected counts.
pub fn generate(cfg: &GenConfig, sink: &mut dyn Sink) -> Result<Expected> {
    let world = World::new(cfg)?;
    let sink = Mutex::new(sink);

    let orgs = write_feed(&world, Feed::Organizations, &sink)?;
    let persons = write_feed(&world, Feed::Persons, &sink)?;

    let org_totals = sum_counts(orgs.iter().map(|(c, _)| c));
    let person_totals = sum_counts(persons.iter().map(|(c, _)| c));
    let mut person_counts = PersonCounts::default();
    for (_, counts) in &persons {
        if let Some(c) = counts {
            person_counts.add(c);
        }
    }
    if org_totals.finals != cfg.organizations || person_totals.finals != cfg.persons {
        return Err(Error::Internal(format!(
            "kept {} of {} organizations and {} of {} persons",
            org_totals.finals, cfg.organizations, person_totals.finals, cfg.persons
        )));
    }

    let expected = Expected {
        persons: person_totals.finals,
        organizations: org_totals.finals,
        malformed: MalformedCounts {
            persons: person_totals.malformed,
            organizations: org_totals.malformed,
        },
        persons_with_unresolved: person_counts.with_unresolved,
        role_title_counts: world
            .targets
            .titles
            .iter()
            .zip(person_counts.title_counts)
            .map(|(t, n)| ((*t).to_owned(), n))
            .collect(),
        organization_name_counts: world
            .targets
            .org_names
            .iter()
            .zip(person_counts.org_name_counts)
            .map(|(name, n)| (name.clone(), n))
            .collect(),
    };
    let bytes = expected.to_json_bytes()?;
    let sink = sink.into_inner().unwrap_or_else(PoisonError::into_inner);
    sink.put(EXPECTED_FILE, &bytes)
        .map_err(|e| Error::io(EXPECTED_FILE, e))?;
    Ok(expected)
}

/// A file's line counts, plus the person counts when the feed is persons.
type FileResult = (FileCounts, Option<PersonCounts>);

/// Writes every file of `feed` in parallel, returning per-file results in file order.
fn write_feed(
    world: &World<'_>,
    feed: Feed,
    sink: &Mutex<&mut dyn Sink>,
) -> Result<Vec<FileResult>> {
    (0..world.cfg.files_per_feed)
        .into_par_iter()
        .map(|file| {
            let path = feed.path(file);
            let writer = sink
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .create(&path)
                .map_err(|e| Error::io(&path, e))?;
            let level = Compression::new(world.cfg.gzip_level);
            let mut gz = GzEncoder::new(writer, level);
            let (counts, person_counts) = match feed {
                Feed::Organizations => {
                    let mut source = OrgSource::new(world);
                    let c = write_feed_file(world, feed, file, &mut source, &mut gz, &path)?;
                    (c, None)
                }
                Feed::Persons => {
                    let mut source = PersonSource::new(world);
                    let c = write_feed_file(world, feed, file, &mut source, &mut gz, &path)?;
                    (c, Some(source.counts))
                }
            };
            let writer = gz.finish().map_err(|e| Error::io(&path, e))?;
            writer.finish().map_err(|e| Error::io(&path, e))?;
            Ok((counts, person_counts))
        })
        .collect()
}

fn sum_counts<'a>(counts: impl Iterator<Item = &'a FileCounts>) -> FileCounts {
    counts.fold(FileCounts::default(), |mut acc, c| {
        acc.records += c.records;
        acc.finals += c.finals;
        acc.malformed += c.malformed;
        acc
    })
}
