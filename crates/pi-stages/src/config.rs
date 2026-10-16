//! [`StagesConfig`]: the settings the stages need beyond the run context.
//!
//! Settings every participant must agree on (the partition count, the index name, the alias)
//! are fixed once by `plan` and read back from `plan.json`; everything here may differ between
//! processes without changing the result, only its speed and memory.

use std::time::Duration;

use pi_es::{BulkConfig, Codec, EsConfig, HealthStatus};
use pi_segment::SegmentConfig;
use pi_store::Store;

/// Default `partition_target_bytes`: input bytes per partition (`PI_PARTITION_TARGET_BYTES`).
pub const DEFAULT_PARTITION_TARGET_BYTES: u64 = 256 << 20;
/// Default `min_partitions` (`PI_MIN_PARTITIONS`).
pub const DEFAULT_MIN_PARTITIONS: u32 = 16;
/// Default malformed lines tolerated per feed (`PI_MAX_MALFORMED`).
pub const DEFAULT_MAX_MALFORMED: u64 = 1000;
/// Default longest input line; longer lines are malformed (`PI_MAX_LINE_BYTES`).
pub const DEFAULT_MAX_LINE_BYTES: usize = 8 << 20;
/// Default rows held per task before the largest partition is written out
/// (`PI_BUFFER_BYTES`).
pub const DEFAULT_BUFFER_BYTES: usize = 256 << 20;
/// Default memory a `join` or `assemble` task plans to hold per pass
/// (`PI_TASK_MEMORY_BYTES`).
pub const DEFAULT_TASK_MEMORY_BYTES: u64 = 512 << 20;
/// Default uploads in flight per task (`PI_UPLOAD_CONCURRENCY`).
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;
/// The default load buffer, in bulk requests: `PI_LOAD_BUFFER_BYTES` defaults to this many
/// times `bulk_max_bytes`.
pub const DEFAULT_LOAD_BUFFER_BULKS: usize = 2;
/// Room left in a bulk request for the action line next to the largest document.
const BULK_ACTION_ROOM: usize = 4 << 10;

/// Everything the Elasticsearch stages (`prepare-index`, `load`, `publish`) need.
#[derive(Debug, Clone)]
pub struct EsStageConfig {
    /// How to reach the cluster.
    pub client: EsConfig,
    /// Bulk request size and senders per `load` task.
    pub bulk: BulkConfig,
    /// Primary shards; `None` means two per data node.
    pub shards: Option<u32>,
    /// Replicas restored by `publish`.
    pub replicas: u32,
    /// The index codec.
    pub codec: Codec,
    /// The health `publish` waits for before moving the alias.
    pub wait_for: HealthStatus,
    /// How long `publish` waits for that health.
    pub health_timeout: Duration,
}

impl Default for EsStageConfig {
    fn default() -> Self {
        Self {
            client: EsConfig::new("http://localhost:9200"),
            bulk: BulkConfig::default(),
            shards: None,
            replicas: 1,
            codec: Codec::BestCompression,
            wait_for: HealthStatus::Green,
            health_timeout: Duration::from_secs(30 * 60),
        }
    }
}

/// The stages' settings. Build one, adjust fields, then hand it to [`crate::build_registry`].
#[derive(Debug, Clone)]
pub struct StagesConfig {
    /// The alias `publish` moves; the index is `<alias>-<run_id>` (`PI_ALIAS`).
    pub alias: String,
    /// Input bytes per partition (`PI_PARTITION_TARGET_BYTES`).
    pub partition_target_bytes: u64,
    /// The fewest partitions (`PI_MIN_PARTITIONS`), rounded up to a power of two.
    pub min_partitions: u32,
    /// Malformed lines tolerated per feed before the run fails (`PI_MAX_MALFORMED`).
    pub max_malformed: u64,
    /// The longest input line, newline excluded; longer lines are malformed.
    pub max_line_bytes: usize,
    /// Segment size and compression (`PI_SEGMENT_TARGET_BYTES`).
    pub segment: SegmentConfig,
    /// Rows a partitioned writer holds before writing out its largest partition. Bounds a
    /// task's memory whatever the partition count, instead of one open zstd stream (and one
    /// segment buffer) per partition.
    pub buffer_bytes: usize,
    /// The memory a `join` or `assemble` task plans to hold per pass: the estimated size of
    /// the partition side it holds, divided by this and rounded up, is its pass count
    /// (`PI_TASK_MEMORY_BYTES`). Separate from `buffer_bytes`, which `join` also uses for its
    /// output.
    pub task_memory_bytes: u64,
    /// Segment uploads in flight per task.
    pub upload_concurrency: usize,
    /// The largest document `assemble` writes; larger ones are quarantined.
    pub max_document_bytes: usize,
    /// Decoded documents a `load` task holds waiting for the bulk indexer, in bytes
    /// (`PI_LOAD_BUFFER_BYTES`); `None` means [`DEFAULT_LOAD_BUFFER_BULKS`] bulk requests. A
    /// document larger than the buffer still passes, alone.
    pub load_buffer_bytes: Option<usize>,
    /// Where the inputs live when not under `raw/` in the run's store (`PI_INPUT_URL`). The
    /// run layout must then be built with `with_input_root("")`.
    pub input_store: Option<Store>,
    /// The Elasticsearch stages.
    pub es: EsStageConfig,
}

impl Default for StagesConfig {
    fn default() -> Self {
        Self {
            alias: "persons".to_owned(),
            partition_target_bytes: DEFAULT_PARTITION_TARGET_BYTES,
            min_partitions: DEFAULT_MIN_PARTITIONS,
            max_malformed: DEFAULT_MAX_MALFORMED,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            segment: SegmentConfig::default(),
            buffer_bytes: DEFAULT_BUFFER_BYTES,
            task_memory_bytes: DEFAULT_TASK_MEMORY_BYTES,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
            max_document_bytes: pi_model::DEFAULT_MAX_DOCUMENT_BYTES,
            load_buffer_bytes: None,
            input_store: None,
            es: EsStageConfig::default(),
        }
    }
}

impl StagesConfig {
    /// Checks the settings that would otherwise fail deep inside a task.
    pub fn validate(&self) -> anyhow::Result<()> {
        pi_es::validate_index_name(&self.alias)
            .map_err(|e| anyhow::anyhow!("alias {:?}: {e}", self.alias))?;
        anyhow::ensure!(
            self.partition_target_bytes > 0,
            "partition_target_bytes must be positive"
        );
        anyhow::ensure!(self.max_line_bytes > 0, "max_line_bytes must be positive");
        anyhow::ensure!(
            self.segment.target_bytes > 0,
            "segment_target_bytes must be positive"
        );
        anyhow::ensure!(
            self.task_memory_bytes > 0,
            "task_memory_bytes must be positive"
        );
        anyhow::ensure!(
            self.upload_concurrency > 0,
            "upload_concurrency must be positive"
        );
        anyhow::ensure!(
            self.es.bulk.senders > 0,
            "senders_per_worker must be positive"
        );
        anyhow::ensure!(
            self.es.bulk.max_bytes > BULK_ACTION_ROOM,
            "bulk_max_bytes must exceed {BULK_ACTION_ROOM} bytes"
        );
        anyhow::ensure!(
            self.load_buffer_bytes != Some(0),
            "load_buffer_bytes must be positive"
        );
        Ok(())
    }

    /// The document size limit `assemble` applies: the configured one, but never more than
    /// fits in one bulk request, so every document `assemble` keeps can be loaded.
    #[must_use]
    pub fn effective_max_document_bytes(&self) -> usize {
        self.max_document_bytes
            .min(self.es.bulk.max_bytes.saturating_sub(BULK_ACTION_ROOM))
    }

    /// The load buffer in bytes: the configured one, else [`DEFAULT_LOAD_BUFFER_BULKS`] times
    /// `bulk_max_bytes`.
    #[must_use]
    pub fn effective_load_buffer_bytes(&self) -> usize {
        self.load_buffer_bytes.unwrap_or_else(|| {
            self.es
                .bulk
                .max_bytes
                .saturating_mul(DEFAULT_LOAD_BUFFER_BULKS)
        })
    }

    /// The store the inputs are read from.
    #[must_use]
    pub fn input_store<'a>(&'a self, run_store: &'a Store) -> &'a Store {
        self.input_store.as_ref().unwrap_or(run_store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_documents_fit_a_bulk_request() {
        let cfg = StagesConfig::default();
        cfg.validate().unwrap();
        assert!(cfg.effective_max_document_bytes() < cfg.es.bulk.max_bytes);
        let mut small = cfg.clone();
        small.max_document_bytes = 1000;
        assert_eq!(small.effective_max_document_bytes(), 1000);
    }

    #[test]
    fn the_load_buffer_follows_the_bulk_size_unless_set() {
        let mut cfg = StagesConfig::default();
        assert_eq!(
            cfg.effective_load_buffer_bytes(),
            DEFAULT_LOAD_BUFFER_BULKS * cfg.es.bulk.max_bytes
        );
        cfg.es.bulk.max_bytes = 1 << 20;
        assert_eq!(cfg.effective_load_buffer_bytes(), 2 << 20);
        cfg.load_buffer_bytes = Some(5000);
        assert_eq!(cfg.effective_load_buffer_bytes(), 5000);
        cfg.validate().unwrap();
        cfg.load_buffer_bytes = Some(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn bad_settings_are_refused() {
        let cfg = StagesConfig {
            alias: "Bad Alias".into(),
            ..StagesConfig::default()
        };
        assert!(cfg.validate().is_err());
        let cfg = StagesConfig {
            upload_concurrency: 0,
            ..StagesConfig::default()
        };
        assert!(cfg.validate().is_err());
        let cfg = StagesConfig {
            task_memory_bytes: 0,
            ..StagesConfig::default()
        };
        assert!(cfg.validate().is_err());
        let mut cfg = StagesConfig::default();
        cfg.es.bulk.max_bytes = 10;
        assert!(cfg.validate().is_err());
    }
}
