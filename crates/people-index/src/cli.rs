//! The command line. Every option is also a `PI_*` environment variable (docs/ARCHITECTURE.md,
//! "Configuration"), which is how the deployment sets them.
//!
//! The doc comments here are the `--help` text, written for people at a terminal rather
//! than for rustdoc, hence no Markdown backticks.
#![allow(clippy::doc_markdown, reason = "doc comments are the --help text")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::byte_size::{parse_byte_size, parse_byte_size_usize};

fn parse_duration(text: &str) -> Result<Duration, String> {
    humantime::parse_duration(text).map_err(|e| format!("{text:?}: {e}"))
}

/// people-index: joins a person feed with an organization feed and publishes one searchable
/// Elasticsearch index, as staged, idempotent tasks over an object store.
#[derive(Debug, Parser)]
#[command(name = "people-index", version, about, long_about = None)]
pub struct Cli {
    /// Log format.
    #[arg(long, env = "PI_LOG_FORMAT", value_enum, default_value_t = LogFormat::Json, global = true)]
    pub log_format: LogFormat,

    #[command(subcommand)]
    pub command: Command,
}

/// Log output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// One JSON object per line (for log collectors).
    Json,
    /// Human-readable lines.
    Pretty,
}

/// The subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// The coordinator: runs (or resumes) a run until it is published.
    Run(RunArgs),
    /// Claims and runs tasks of a run until it is published or failed.
    Worker(WorkerArgs),
    /// Generates both feeds and their expected counts.
    Datagen(DatagenArgs),
    /// Checks the published alias against expected counts.
    Verify(VerifyArgs),
    /// Runs one task (or every task) of one stage, for debugging.
    Stage(StageArgs),
}

/// Where the data lives.
#[derive(Debug, Clone, Args)]
pub struct StoreArgs {
    /// Object store prefix holding runs (and, by default, the inputs under raw/).
    #[arg(long, env = "PI_STORE_URL", default_value = "file://./store")]
    pub store_url: String,
    /// Input prefix holding organizations/ and persons/, when not <store>/raw.
    #[arg(long, env = "PI_INPUT_URL")]
    pub input_url: Option<String>,
    /// S3-compatible endpoint (MinIO). Credentials come from AWS_* variables.
    #[arg(long, env = "PI_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
}

/// How to reach Elasticsearch.
#[derive(Debug, Clone, Args)]
pub struct EsArgs {
    /// Elasticsearch URL.
    #[arg(long, env = "PI_ES_URL", default_value = "http://localhost:9200")]
    pub es_url: String,
    /// Basic auth user.
    #[arg(long, env = "PI_ES_USERNAME")]
    pub es_username: Option<String>,
    /// Basic auth password.
    #[arg(long, env = "PI_ES_PASSWORD", hide_env_values = true)]
    pub es_password: Option<String>,
    /// API key (instead of basic auth).
    #[arg(long, env = "PI_ES_API_KEY", hide_env_values = true)]
    pub es_api_key: Option<String>,
    /// PEM file of a CA to trust.
    #[arg(long, env = "PI_ES_CA_CERT")]
    pub es_ca_cert: Option<PathBuf>,
    /// Timeout of one request.
    #[arg(long, env = "PI_ES_REQUEST_TIMEOUT", value_parser = parse_duration, default_value = "120s")]
    pub es_request_timeout: Duration,
    /// Gzip bulk request bodies.
    #[arg(long, env = "PI_ES_GZIP", default_value_t = false)]
    pub es_gzip: bool,
}

/// The pipeline's knobs.
#[derive(Debug, Clone, Args)]
pub struct PipelineArgs {
    /// The published alias; the run's index is <alias>-<run_id>.
    #[arg(long, env = "PI_ALIAS", default_value = "persons")]
    pub alias: String,
    /// Input bytes per partition.
    #[arg(long, env = "PI_PARTITION_TARGET_BYTES", value_parser = parse_byte_size, default_value = "256MiB")]
    pub partition_target_bytes: u64,
    /// The fewest partitions (rounded up to a power of two).
    #[arg(long, env = "PI_MIN_PARTITIONS", default_value_t = pi_stages::DEFAULT_MIN_PARTITIONS)]
    pub min_partitions: u32,
    /// Malformed lines tolerated per feed.
    #[arg(long, env = "PI_MAX_MALFORMED", default_value_t = pi_stages::DEFAULT_MAX_MALFORMED)]
    pub max_malformed: u64,
    /// The longest input line; longer lines are malformed.
    #[arg(long, env = "PI_MAX_LINE_BYTES", value_parser = parse_byte_size_usize, default_value = "8MiB")]
    pub max_line_bytes: usize,
    /// Compressed size at which a segment file is closed.
    #[arg(long, env = "PI_SEGMENT_TARGET_BYTES", value_parser = parse_byte_size, default_value = "64MiB")]
    pub segment_target_bytes: u64,
    /// Rows a partitioned writer buffers per task before writing out its largest partition.
    #[arg(long, env = "PI_BUFFER_BYTES", value_parser = parse_byte_size_usize, default_value = "256MiB")]
    pub buffer_bytes: usize,
    /// Segment uploads in flight per task.
    #[arg(long, env = "PI_UPLOAD_CONCURRENCY", default_value_t = pi_stages::DEFAULT_UPLOAD_CONCURRENCY)]
    pub upload_concurrency: usize,
    /// The largest document; larger ones are quarantined.
    #[arg(long, env = "PI_MAX_DOCUMENT_BYTES", value_parser = parse_byte_size_usize, default_value = "8MiB")]
    pub max_document_bytes: usize,
    /// The largest bulk request body.
    #[arg(long, env = "PI_BULK_MAX_BYTES", value_parser = parse_byte_size_usize, default_value = "8MiB")]
    pub bulk_max_bytes: usize,
    /// Bulk requests in flight per load task.
    #[arg(long, env = "PI_SENDERS_PER_WORKER", default_value_t = 2)]
    pub senders_per_worker: usize,
    /// Decoded documents a load task holds waiting for the bulk indexer (default: twice the
    /// bulk request size). A larger document still passes, alone.
    #[arg(long, env = "PI_LOAD_BUFFER_BYTES", value_parser = parse_byte_size_usize)]
    pub load_buffer_bytes: Option<usize>,
    /// Primary shards (default: 2 per data node).
    #[arg(long, env = "PI_INDEX_SHARDS")]
    pub index_shards: Option<u32>,
    /// Replicas restored by publish.
    #[arg(long, env = "PI_INDEX_REPLICAS", default_value_t = 1)]
    pub index_replicas: u32,
    /// Index codec: default or best_compression.
    #[arg(long, env = "PI_INDEX_CODEC", default_value = "best_compression")]
    pub index_codec: String,
    /// Cluster health publish waits for: green or yellow.
    #[arg(long, env = "PI_PUBLISH_HEALTH", default_value = "green")]
    pub publish_health: String,
    /// How long publish waits for that health.
    #[arg(long, env = "PI_HEALTH_TIMEOUT", value_parser = parse_duration, default_value = "30m")]
    pub health_timeout: Duration,
}

/// How tasks are executed in this process.
#[derive(Debug, Clone, Args)]
pub struct ExecArgs {
    /// Tasks at once in this process (default: number of CPUs).
    #[arg(long, env = "PI_CONCURRENCY")]
    pub concurrency: Option<usize>,
    /// Name in leases and commits (default: the host name).
    #[arg(long, env = "PI_WORKER_ID")]
    pub worker_id: Option<String>,
    /// Lease lifetime.
    #[arg(long, env = "PI_LEASE_TTL", value_parser = parse_duration, default_value = "60s")]
    pub lease_ttl: Duration,
    /// Attempts per task, the first one included.
    #[arg(long, env = "PI_TASK_ATTEMPTS", default_value_t = 3)]
    pub task_attempts: u32,
}

/// Prometheus exporter.
#[derive(Debug, Clone, Args)]
pub struct MetricsArgs {
    /// Address of the Prometheus endpoint; `off` disables it.
    #[arg(long, env = "PI_METRICS_ADDR", default_value = "0.0.0.0:9464")]
    pub metrics_addr: String,
}

impl MetricsArgs {
    /// The address to listen on, `None` when disabled.
    pub fn addr(&self) -> anyhow::Result<Option<SocketAddr>> {
        let t = self.metrics_addr.trim();
        if t.is_empty() || t.eq_ignore_ascii_case("off") {
            return Ok(None);
        }
        Ok(Some(t.parse().map_err(|e| {
            anyhow::anyhow!("PI_METRICS_ADDR {t:?}: {e}")
        })?))
    }
}

/// Executors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ExecutorKind {
    /// Every task in this process.
    Local,
    /// Tasks of multi-task stages claimed by workers.
    Distributed,
}

/// `people-index run`.
#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// The run id; an existing one is resumed (default: generated).
    #[arg(long, env = "PI_RUN_ID")]
    pub run_id: Option<String>,
    /// Who runs the tasks.
    #[arg(long, env = "PI_EXECUTOR", value_enum, default_value_t = ExecutorKind::Local)]
    pub executor: ExecutorKind,
    /// Give up on a distributed stage after this long (default: never).
    #[arg(long, env = "PI_STAGE_TIMEOUT", value_parser = parse_duration)]
    pub stage_timeout: Option<Duration>,
    #[command(flatten)]
    pub store: StoreArgs,
    #[command(flatten)]
    pub es: EsArgs,
    #[command(flatten)]
    pub pipeline: PipelineArgs,
    #[command(flatten)]
    pub exec: ExecArgs,
    #[command(flatten)]
    pub metrics: MetricsArgs,
}

/// `people-index worker`.
#[derive(Debug, Clone, Args)]
pub struct WorkerArgs {
    /// The run to work on (default: the newest run, waited for if there is none yet).
    #[arg(long, env = "PI_RUN_ID")]
    pub run_id: Option<String>,
    /// Exit after this long without work (default: never).
    #[arg(long, env = "PI_WORKER_IDLE_TIMEOUT", value_parser = parse_duration)]
    pub idle_timeout: Option<Duration>,
    /// How often to look for work.
    #[arg(long, env = "PI_POLL_INTERVAL", value_parser = parse_duration, default_value = "2s")]
    pub poll_interval: Duration,
    #[command(flatten)]
    pub store: StoreArgs,
    #[command(flatten)]
    pub es: EsArgs,
    #[command(flatten)]
    pub pipeline: PipelineArgs,
    #[command(flatten)]
    pub exec: ExecArgs,
    #[command(flatten)]
    pub metrics: MetricsArgs,
}

/// `people-index datagen`.
#[derive(Debug, Clone, Args)]
pub struct DatagenArgs {
    /// Distinct persons.
    #[arg(long, env = "PI_DATAGEN_PERSONS")]
    pub persons: Option<u64>,
    /// Distinct organizations.
    #[arg(long, env = "PI_DATAGEN_ORGANIZATIONS")]
    pub organizations: Option<u64>,
    /// Files per feed.
    #[arg(long, env = "PI_DATAGEN_FILES")]
    pub files: Option<u32>,
    /// Seed of every random choice.
    #[arg(long, env = "PI_DATAGEN_SEED")]
    pub seed: Option<u64>,
    /// A JSON file with the remaining generator settings (rates, skew); flags override it.
    #[arg(long, env = "PI_DATAGEN_CONFIG")]
    pub config: Option<PathBuf>,
    /// Where to write organizations/, persons/ and expected.json (default: <store>/raw, or
    /// the input prefix when one is set).
    #[arg(long, env = "PI_DATAGEN_OUT")]
    pub out: Option<String>,
    #[command(flatten)]
    pub store: StoreArgs,
}

/// `people-index verify`.
#[derive(Debug, Clone, Args)]
pub struct VerifyArgs {
    /// expected.json: a local path or a store URL (default: expected.json beside the inputs).
    #[arg(long, env = "PI_EXPECTED")]
    pub expected: Option<String>,
    /// Also print an order-independent digest of every `_id` and `_source`.
    #[arg(long, default_value_t = false)]
    pub digest: bool,
    /// Documents per page while computing the digest.
    #[arg(long, default_value_t = 1000)]
    pub page_size: u32,
    /// The alias (or index) to check.
    #[arg(long, env = "PI_ALIAS", default_value = "persons")]
    pub alias: String,
    #[command(flatten)]
    pub store: StoreArgs,
    #[command(flatten)]
    pub es: EsArgs,
}

/// `people-index stage`.
#[derive(Debug, Clone, Args)]
pub struct StageArgs {
    /// The stage (plan, extract-orgs, extract-persons, join, assemble, prepare-index, load,
    /// publish).
    pub name: String,
    /// The run.
    #[arg(long, env = "PI_RUN_ID")]
    pub run_id: String,
    /// The task id (e.g. part-00003, file-00000, run); every task of the stage when omitted.
    #[arg(long)]
    pub task: Option<String>,
    #[command(flatten)]
    pub store: StoreArgs,
    #[command(flatten)]
    pub es: EsArgs,
    #[command(flatten)]
    pub pipeline: PipelineArgs,
    #[command(flatten)]
    pub exec: ExecArgs,
    #[command(flatten)]
    pub metrics: MetricsArgs,
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn the_cli_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn flags_parse() {
        let cli = Cli::try_parse_from([
            "people-index",
            "run",
            "--run-id",
            "r1",
            "--executor",
            "distributed",
            "--partition-target-bytes",
            "1GiB",
            "--lease-ttl",
            "30s",
            "--load-buffer-bytes",
            "4MiB",
            "--metrics-addr",
            "off",
        ])
        .unwrap();
        let Command::Run(run) = cli.command else {
            panic!("not run")
        };
        assert_eq!(run.run_id.as_deref(), Some("r1"));
        assert_eq!(run.executor, ExecutorKind::Distributed);
        assert_eq!(run.pipeline.partition_target_bytes, 1 << 30);
        assert_eq!(run.exec.lease_ttl, Duration::from_secs(30));
        assert_eq!(run.metrics.addr().unwrap(), None);
        assert_eq!(run.pipeline.min_partitions, 16);
        assert_eq!(run.pipeline.load_buffer_bytes, Some(4 << 20));
    }
}
