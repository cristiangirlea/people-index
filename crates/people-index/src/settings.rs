//! From command-line arguments to the library types: stores, the run context, the stage
//! settings and the Elasticsearch client settings.

use std::sync::Arc;

use anyhow::Context as _;
use pi_coord::{
    CancellationToken, LeaseConfig, LocalExecutor, RetryPolicy, RunContext, StoreCoordination,
};
use pi_es::{Auth, EsConfig, HealthStatus, Secret};
use pi_segment::SegmentConfig;
use pi_stages::{EsStageConfig, StagesConfig};
use pi_store::{RunLayout, Store, StoreOptions};

use crate::cli::{EsArgs, ExecArgs, PipelineArgs, StoreArgs};

/// The stores a command works with.
#[derive(Debug, Clone)]
pub struct Stores {
    /// The run store (`PI_STORE_URL`).
    pub runs: Store,
    /// The input store when separate (`PI_INPUT_URL`).
    pub inputs: Option<Store>,
    /// Selected input delivery.
    pub delivery: Option<String>,
}

impl Stores {
    /// Opens the stores the arguments name.
    pub fn open(args: &StoreArgs) -> anyhow::Result<Self> {
        let options = store_options(args);
        let runs = Store::open(&args.store_url, &options)
            .with_context(|| format!("opening the store {}", args.store_url))?;
        let inputs = args
            .input_url
            .as_deref()
            .map(|url| {
                Store::open(url, &options).with_context(|| format!("opening the inputs {url}"))
            })
            .transpose()?;
        input_root(args)?;
        Ok(Self {
            runs,
            inputs,
            delivery: args.delivery.clone(),
        })
    }

    /// The layout of a run: inputs under `raw/` of the run store, or at the top of the input
    /// store when one is set.
    pub fn layout(&self, run_id: &str) -> anyhow::Result<RunLayout> {
        let layout = RunLayout::new(run_id)?;
        let root = input_root_parts(self.inputs.is_some(), self.delivery.as_deref())?;
        Ok(layout.with_input_root(&root))
    }

    /// The context of a run, tied to `cancel`.
    pub fn context(&self, run_id: &str, cancel: CancellationToken) -> anyhow::Result<RunContext> {
        Ok(RunContext::with_layout(self.runs.clone(), self.layout(run_id)?).with_cancel(cancel))
    }
}

/// Validates the delivery and returns its input prefix without opening a store.
pub fn input_root(args: &StoreArgs) -> anyhow::Result<String> {
    input_root_parts(args.input_url.is_some(), args.delivery.as_deref())
}

fn input_root_parts(separate: bool, delivery: Option<&str>) -> anyhow::Result<String> {
    if let Some(delivery) = delivery {
        for segment in delivery.split('/') {
            pi_store::validate_id("delivery segment", segment)?;
            anyhow::ensure!(
                !segment.eq_ignore_ascii_case("persons")
                    && !segment.eq_ignore_ascii_case("organizations"),
                "reserved delivery segment {segment:?}"
            );
        }
    }
    Ok(match (separate, delivery) {
        (true, None) => String::new(),
        (true, Some(d)) => d.to_owned(),
        (false, None) => "raw".to_owned(),
        (false, Some(d)) => format!("raw/{d}"),
    })
}

/// S3 options from the arguments; credentials and region come from the `AWS_*` variables.
#[must_use]
pub fn store_options(args: &StoreArgs) -> StoreOptions {
    StoreOptions {
        s3_endpoint: args.s3_endpoint.clone(),
        ..StoreOptions::default()
    }
}

/// The Elasticsearch client settings.
pub fn es_config(args: &EsArgs) -> anyhow::Result<EsConfig> {
    let auth = match (&args.es_username, &args.es_password, &args.es_api_key) {
        (None, None, None) => Auth::None,
        (Some(user), Some(password), None) => Auth::Basic {
            username: user.clone(),
            password: Secret::new(password.clone()),
        },
        (None, None, Some(key)) => Auth::ApiKey(Secret::new(key.clone())),
        (Some(_), None, _) | (None, Some(_), _) => {
            anyhow::bail!("PI_ES_USERNAME and PI_ES_PASSWORD go together")
        }
        (_, _, Some(_)) => anyhow::bail!("set PI_ES_API_KEY or a username and password, not both"),
    };
    let mut config = EsConfig::new(args.es_url.clone());
    config.auth = auth;
    config.ca_cert.clone_from(&args.es_ca_cert);
    config.request_timeout = args.es_request_timeout;
    config.gzip_requests = args.es_gzip;
    Ok(config)
}

/// The stage settings.
pub fn stages_config(
    pipeline: &PipelineArgs,
    es: &EsArgs,
    stores: &Stores,
) -> anyhow::Result<StagesConfig> {
    let codec = pipeline
        .index_codec
        .parse()
        .map_err(|e| anyhow::anyhow!("PI_INDEX_CODEC: {e}"))?;
    let wait_for: HealthStatus = pipeline
        .publish_health
        .parse()
        .map_err(|e| anyhow::anyhow!("PI_PUBLISH_HEALTH: {e}"))?;
    let config = StagesConfig {
        alias: pipeline.alias.clone(),
        delivery: stores.delivery.clone(),
        require_complete_marker: pipeline.require_complete_marker,
        partition_target_bytes: pipeline.partition_target_bytes,
        min_partitions: pipeline.min_partitions,
        max_malformed: pipeline.max_malformed,
        max_line_bytes: pipeline.max_line_bytes,
        segment: SegmentConfig {
            target_bytes: pipeline.segment_target_bytes,
            ..SegmentConfig::default()
        },
        buffer_bytes: pipeline.buffer_bytes,
        task_memory_bytes: pipeline.task_memory_bytes,
        upload_concurrency: pipeline.upload_concurrency,
        max_document_bytes: pipeline.max_document_bytes,
        load_buffer_bytes: pipeline.load_buffer_bytes,
        input_store: stores.inputs.clone(),
        es: EsStageConfig {
            client: es_config(es)?,
            bulk: pi_es::BulkConfig {
                max_bytes: pipeline.bulk_max_bytes,
                senders: pipeline.senders_per_worker,
                ..pi_es::BulkConfig::default()
            },
            shards: pipeline.index_shards,
            replicas: pipeline.index_replicas,
            codec,
            wait_for,
            health_timeout: pipeline.health_timeout,
            publish_min_share: pipeline.publish_min_share,
            publish_max_unresolved: pipeline.publish_max_unresolved,
            publish_allow_empty_organizations: pipeline.publish_allow_empty_organizations,
        },
    };
    config.validate()?;
    Ok(config)
}

/// The worker name: `PI_WORKER_ID`, else the host name (`HOSTNAME` is the pod name in
/// Kubernetes), else a random one.
#[must_use]
pub fn worker_id(exec: &ExecArgs) -> String {
    exec.worker_id
        .clone()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("worker-{:08x}", rand::random::<u32>()))
}

/// Coordination over the run store for this process.
#[must_use]
pub fn coordination(exec: &ExecArgs) -> Arc<StoreCoordination> {
    Arc::new(StoreCoordination::new(
        worker_id(exec),
        LeaseConfig::new(exec.lease_ttl),
    ))
}

/// The retry policy for tasks.
#[must_use]
pub fn retry_policy(exec: &ExecArgs) -> RetryPolicy {
    RetryPolicy {
        max_attempts: exec.task_attempts.max(1),
        ..RetryPolicy::default()
    }
}

/// Tasks at once: `PI_CONCURRENCY`, else the number of CPUs.
#[must_use]
pub fn concurrency(exec: &ExecArgs) -> usize {
    exec.concurrency
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, std::num::NonZero::get))
        .max(1)
}

/// The local executor these arguments describe.
#[must_use]
pub fn local_executor(exec: &ExecArgs, coord: Arc<StoreCoordination>) -> LocalExecutor {
    LocalExecutor::new(coord)
        .with_concurrency(concurrency(exec))
        .with_retry(retry_policy(exec))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::cli::{Cli, Command};

    fn run_args(extra: &[&str]) -> crate::cli::RunArgs {
        let mut argv = vec!["people-index", "run", "--metrics-addr", "off"];
        argv.extend_from_slice(extra);
        match Cli::try_parse_from(argv).unwrap().command {
            Command::Run(r) => r,
            _ => unreachable!(),
        }
    }

    #[test]
    fn stage_settings_come_from_the_flags() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let args = run_args(&[
            "--store-url",
            &url,
            "--alias",
            "people",
            "--index-codec",
            "default",
            "--senders-per-worker",
            "4",
            "--load-buffer-bytes",
            "1MiB",
            "--task-memory-bytes",
            "64MiB",
            "--publish-min-share",
            "0.7",
            "--publish-max-unresolved",
            "0.4",
        ]);
        let stores = Stores::open(&args.store).unwrap();
        let cfg = stages_config(&args.pipeline, &args.es, &stores).unwrap();
        assert!((cfg.es.publish_min_share - 0.7).abs() < f64::EPSILON);
        assert!((cfg.es.publish_max_unresolved - 0.4).abs() < f64::EPSILON);
        assert_eq!(cfg.alias, "people");
        assert_eq!(cfg.es.codec, pi_es::Codec::Default);
        assert_eq!(cfg.es.bulk.senders, 4);
        assert_eq!(cfg.effective_load_buffer_bytes(), 1 << 20);
        assert_eq!(cfg.task_memory_bytes, 64 << 20);
        assert!(cfg.input_store.is_none());
        assert_eq!(stores.layout("r1").unwrap().raw_persons(), "raw/persons");
    }

    #[test]
    fn a_separate_input_store_moves_the_input_root() {
        let args = run_args(&["--store-url", "memory://", "--input-url", "memory://"]);
        let stores = Stores::open(&args.store).unwrap();
        assert_eq!(stores.layout("r1").unwrap().raw_persons(), "persons");
    }

    #[test]
    fn delivery_validation_and_layout() {
        for bad in [
            "..",
            ".",
            "",
            "/a",
            "a/",
            "a//b",
            "a\\b",
            "persons",
            "organizations",
            "a/persons/b",
            "a/organizations",
            "Persons",
            "a/ORGANIZATIONS/b",
        ] {
            assert!(input_root_parts(false, Some(bad)).is_err(), "{bad:?}");
        }
        for (separate, expected) in [(false, "raw/a/b/persons"), (true, "a/b/persons")] {
            let args = if separate {
                run_args(&[
                    "--store-url",
                    "memory://",
                    "--input-url",
                    "memory://",
                    "--delivery",
                    "a/b",
                ])
            } else {
                run_args(&["--store-url", "memory://", "--delivery", "a/b"])
            };
            assert_eq!(
                Stores::open(&args.store)
                    .unwrap()
                    .layout("r1")
                    .unwrap()
                    .raw_persons(),
                expected
            );
        }
    }

    #[test]
    fn auth_combinations() {
        let mut args = run_args(&[]).es;
        assert!(matches!(es_config(&args).unwrap().auth, Auth::None));
        args.es_username = Some("u".into());
        assert!(es_config(&args).is_err());
        args.es_password = Some("p".into());
        assert!(matches!(es_config(&args).unwrap().auth, Auth::Basic { .. }));
        args.es_api_key = Some("k".into());
        assert!(es_config(&args).is_err());
        args.es_username = None;
        args.es_password = None;
        assert!(matches!(es_config(&args).unwrap().auth, Auth::ApiKey(_)));
    }

    #[test]
    fn bad_codec_is_refused() {
        let args = run_args(&["--store-url", "memory://", "--index-codec", "zip"]);
        let stores = Stores::open(&args.store).unwrap();
        assert!(stages_config(&args.pipeline, &args.es, &stores).is_err());
    }

    #[test]
    fn the_load_buffer_defaults_to_twice_the_bulk_size() {
        let args = run_args(&["--store-url", "memory://", "--bulk-max-bytes", "1MiB"]);
        let stores = Stores::open(&args.store).unwrap();
        let cfg = stages_config(&args.pipeline, &args.es, &stores).unwrap();
        assert_eq!(cfg.load_buffer_bytes, None);
        assert_eq!(cfg.effective_load_buffer_bytes(), 2 << 20);
        let args = run_args(&["--store-url", "memory://", "--load-buffer-bytes", "0"]);
        let stores = Stores::open(&args.store).unwrap();
        assert!(stages_config(&args.pipeline, &args.es, &stores).is_err());
    }
}
