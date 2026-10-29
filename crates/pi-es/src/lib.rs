//! pi-es: everything people-index asks of Elasticsearch (docs/ARCHITECTURE.md, "Components").
//!
//! - [`EsClient`]: one pooled HTTP client (rustls, basic or API-key auth, an extra CA, timeouts,
//!   bounded response reads, retries with jittered backoff).
//! - [`IndexAdmin`]: data nodes, index creation with the mapping and load-time settings,
//!   settings, refresh, flush, exact counts, aliases, deletion, health waits.
//! - [`BulkIndexer`]: byte-bounded, reused bulk buffers with bounded concurrency, item-level
//!   retries and permanent-failure reporting (`load`).
//! - [`Publisher`]: the `publish` step; the alias moves only after every check passed.
//! - [`Verifier`]: the acceptance counts of `expected.json`, or the checks against what a run
//!   published, and an order-independent digest.
//! - [`mapping()`]: the explicit mapping (`mapping.json`, reasons in `mapping.md`), or one read
//!   from a file ([`parse_mapping`]), held against the feed schema by [`check_mapping`].

mod admin;
mod bulk;
mod client;
mod config;
mod digest;
mod error;
mod expected;
mod health;
mod mapping;
mod names;
mod publisher;
mod retry;
mod runs;
mod settings;
mod shards;
mod verifier;

pub use admin::{CreateOutcome, IndexAdmin};
pub use bulk::{BulkConfig, BulkCounters, BulkIndexer, BulkStats};
pub use client::{Body, EsClient, RawResponse};
pub use config::{Auth, EsConfig, Secret};
pub use digest::{DigestReport, DocumentDigest, canonical_sha256, write_canonical};
pub use error::{EsError, ItemFailure, Result, is_retryable_status};
pub use expected::{Expected, MalformedCounts};
pub use health::HealthStatus;
pub use mapping::{MAPPING_JSON, check_mapping, mapping, parse_mapping};
pub use names::validate_index_name;
pub use publisher::{PublishReport, PublishSpec, Publisher};
pub use reqwest::Method;
pub use retry::RetryPolicy;
pub use settings::{Codec, IndexSpec, ServingSettings};
pub use shards::{ShardCounts, require_complete};
pub use verifier::{AliasCheck, Check, Published, Verifier, VerifyReport};

/// Extracts the sortable UTC stamp from a run index.
pub use runs::run_stamp;
