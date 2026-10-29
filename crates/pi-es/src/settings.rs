//! Index settings for the two phases of an index's life: a write-optimised load and a
//! read-optimised serving state.

use std::fmt;
use std::str::FromStr;

use serde_json::{Value, json};

use crate::error::EsError;
use crate::mapping::mapping;

/// `index.codec` (`PI_INDEX_CODEC`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    /// LZ4 stored fields: faster, larger.
    Default,
    /// DEFLATE stored fields: fewer bytes on disk and in snapshots for some CPU at merge time.
    #[default]
    BestCompression,
}

impl Codec {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::BestCompression => "best_compression",
        }
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Codec {
    type Err = EsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "default" => Ok(Self::Default),
            "best_compression" => Ok(Self::BestCompression),
            other => Err(EsError::Config(format!(
                "index codec {other:?}: expected default or best_compression"
            ))),
        }
    }
}

/// What `prepare-index` creates.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSpec {
    /// Primary shards (`PI_INDEX_SHARDS`; default 2 per data node, see
    /// [`IndexSpec::default_shards`]).
    pub shards: u32,
    pub codec: Codec,
    /// The `mappings` body: the built-in [`crate::mapping::mapping`] unless
    /// [`IndexSpec::with_mapping`] gives another.
    pub mapping: Value,
    /// Description of the configured mapping source.
    pub mapping_source: String,
}

impl IndexSpec {
    #[must_use]
    pub fn new(shards: u32, codec: Codec) -> Self {
        Self {
            shards,
            codec,
            mapping: mapping(),
            mapping_source: "built-in mapping".into(),
        }
    }

    /// The spec with `mapping` as its `mappings` body.
    #[must_use]
    pub fn with_mapping(mut self, mapping: Value) -> Self {
        self.mapping = mapping;
        self
    }

    /// Two primaries per data node: every node takes part in the load, and a later node added
    /// to the cluster still gets shards to hold, without oversharding a small cluster.
    #[must_use]
    pub fn default_shards(data_nodes: u32) -> u32 {
        data_nodes.max(1).saturating_mul(2)
    }

    /// The create-index body with the load-time settings: no replicas (each document is
    /// written once; replicas are copied as whole segments at publish), no refresh (nobody
    /// reads the index before publish) and async translog durability (a lost acknowledgement
    /// is a resend, and every resend is idempotent by `_id`).
    #[must_use]
    pub fn create_body(&self) -> Value {
        json!({
            "settings": {
                "index": {
                    "number_of_shards": self.shards,
                    "number_of_replicas": 0,
                    "refresh_interval": "-1",
                    "translog": {"durability": "async"},
                    "codec": self.codec.as_str(),
                }
            },
            "mappings": self.mapping,
        })
    }
}

/// The settings `publish` restores before the index serves reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServingSettings {
    /// `PI_INDEX_REPLICAS`, default 1.
    pub replicas: u32,
    /// Default `1s`.
    pub refresh_interval: String,
}

impl Default for ServingSettings {
    fn default() -> Self {
        Self {
            replicas: 1,
            refresh_interval: "1s".to_owned(),
        }
    }
}

impl ServingSettings {
    /// The `_settings` body: replicas, refresh and request durability (fsync before
    /// acknowledging, the safe default for an index that now takes reads and maybe writes).
    #[must_use]
    pub fn body(&self) -> Value {
        json!({
            "index": {
                "number_of_replicas": self.replicas,
                "refresh_interval": self.refresh_interval,
                "translog": {"durability": "request"},
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_body_has_load_settings() {
        let body = IndexSpec::new(6, Codec::BestCompression).create_body();
        let index = &body["settings"]["index"];
        assert_eq!(index["number_of_shards"], 6);
        assert_eq!(index["number_of_replicas"], 0);
        assert_eq!(index["refresh_interval"], "-1");
        assert_eq!(index["translog"]["durability"], "async");
        assert_eq!(index["codec"], "best_compression");
        assert_eq!(body["mappings"]["dynamic"], false);
    }

    #[test]
    fn serving_body_restores_defaults() {
        let body = ServingSettings::default().body();
        assert_eq!(body["index"]["number_of_replicas"], 1);
        assert_eq!(body["index"]["refresh_interval"], "1s");
        assert_eq!(body["index"]["translog"]["durability"], "request");
    }

    #[test]
    fn codec_and_shards() {
        assert_eq!("default".parse::<Codec>().ok(), Some(Codec::Default));
        assert!("zstd".parse::<Codec>().is_err());
        assert_eq!(IndexSpec::default_shards(3), 6);
        assert_eq!(IndexSpec::default_shards(0), 2);
    }
}
