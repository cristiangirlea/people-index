//! Writes one feed file: own records in order, second copies of duplicates at their planned
//! slots, malformed lines sprinkled in. The record bodies come from a [`RecordSource`].

use std::io::Write;

use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use crate::envelope::rfc3339;
use crate::error::{Error, Result};
use crate::malformed::write_malformed;
use crate::world::{Feed, World};

/// Output is handed to the (gzip) writer in chunks of about this size.
const FLUSH_BYTES: usize = 1 << 20;

/// Timestamps of malformed lines fall in 2025.
const MALFORMED_EPOCH: u64 = 1_735_689_600;

/// Produces one feed's record lines.
pub(crate) trait RecordSource {
    /// Appends the envelope line of record `index` as of `version` to `out`. `is_final` is
    /// true when this copy is the one last-wins keeps, i.e. the one the counts describe.
    fn write_record(
        &mut self,
        rng: &mut ChaCha8Rng,
        index: u64,
        version: u8,
        is_final: bool,
        out: &mut Vec<u8>,
    ) -> Result<()>;
}

/// Line counts of one file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileCounts {
    /// Valid record lines, including stale first copies of duplicates.
    pub(crate) records: u64,
    /// Valid record lines that last-wins keeps (one per distinct id across the feed).
    pub(crate) finals: u64,
    pub(crate) malformed: u64,
}

/// Writes file `file` of `feed` into `out`; `path` is only used in error messages.
pub(crate) fn write_feed_file(
    world: &World<'_>,
    feed: Feed,
    file: u32,
    source: &mut dyn RecordSource,
    out: &mut dyn Write,
    path: &str,
) -> Result<FileCounts> {
    let mut rng = world.file_rng(feed, file);
    let duplicates = world.duplicates(feed);
    let incoming = duplicates.incoming(file);
    let mut next_incoming = 0;
    let mut counts = FileCounts::default();
    let mut buf: Vec<u8> = Vec::with_capacity(FLUSH_BYTES + (64 << 10));

    let mut emit = |rng: &mut ChaCha8Rng,
                    buf: &mut Vec<u8>,
                    index: u64,
                    version: u8,
                    is_final: bool,
                    counts: &mut FileCounts|
     -> Result<()> {
        if rng.random_bool(world.cfg.malformed_rate) {
            let at = rfc3339(MALFORMED_EPOCH + rng.random_range(0..31_536_000));
            write_malformed(rng, &at, buf)?;
            counts.malformed += 1;
        }
        source.write_record(rng, index, version, is_final, buf)?;
        counts.records += 1;
        counts.finals += u64::from(is_final);
        if buf.len() >= FLUSH_BYTES {
            out.write_all(buf).map_err(|e| Error::io(path, e))?;
            buf.clear();
        }
        Ok(())
    };

    for (slot, index) in world.layout(feed).block(file).enumerate() {
        let slot = slot as u64;
        while let Some(dup) = incoming.get(next_incoming).filter(|d| d.slot == slot) {
            emit(&mut rng, &mut buf, dup.index, 1, true, &mut counts)?;
            next_incoming += 1;
        }
        let stale = duplicates.is_duplicated(index);
        emit(&mut rng, &mut buf, index, 0, !stale, &mut counts)?;
    }
    // Copies slotted after the last own record, and same-file copies from the last file.
    for dup in &incoming[next_incoming..] {
        emit(&mut rng, &mut buf, dup.index, 1, true, &mut counts)?;
    }
    out.write_all(&buf).map_err(|e| Error::io(path, e))?;
    Ok(counts)
}
