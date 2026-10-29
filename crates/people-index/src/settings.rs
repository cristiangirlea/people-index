//! From command-line arguments to the library types: stores, the run context, the stage
//! settings (the feed schema and the index mapping included) and the Elasticsearch client
//! settings.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use pi_coord::{
    CancellationToken, LeaseConfig, LocalExecutor, RetryPolicy, RunContext, StoreCoordination,
};
use pi_es::{Auth, EsConfig, HealthStatus, Secret};
use pi_model::FeedSchema;
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

/// A settings path, `None` when unset or empty (compose passes an unset variable as empty).
fn file_setting(value: Option<&str>) -> Option<&Path> {
    value.filter(|v| !v.trim().is_empty()).map(Path::new)
}

/// Reads `PI_SCHEMA_FILE`, or the built-in schema when it is unset.
pub fn feed_schema(pipeline: &PipelineArgs) -> anyhow::Result<Option<FeedSchema>> {
    let Some(path) = file_setting(pipeline.schema_file.as_deref()) else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("PI_SCHEMA_FILE: reading {}", path.display()))?;
    FeedSchema::from_json(&text)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("PI_SCHEMA_FILE {}: {e}", path.display()))
}

/// Reads `PI_MAPPING_FILE`, or `None` for the built-in mapping when it is unset.
pub fn index_mapping(pipeline: &PipelineArgs) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(path) = file_setting(pipeline.mapping_file.as_deref()) else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("PI_MAPPING_FILE: reading {}", path.display()))?;
    pi_es::parse_mapping(&text)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("PI_MAPPING_FILE {}: {e}", path.display()))
}

/// The stage settings.
pub fn stages_config(
    pipeline: &PipelineArgs,
    es: &EsArgs,
    exec: &crate::cli::ExecArgs,
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
    let mut config = StagesConfig {
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
            retain_runs: pipeline.retain_runs,
            lease_clock_skew: pi_coord::LeaseConfig::new(exec.lease_ttl).clock_skew,
            publish_min_share: pipeline.publish_min_share,
            publish_max_unresolved: pipeline.publish_max_unresolved,
            publish_allow_empty_organizations: pipeline.publish_allow_empty_organizations,
        },
        ..StagesConfig::default()
    };
    if let Some(schema) = feed_schema(pipeline)? {
        config.schema = Arc::new(schema);
    }
    if let Some(mapping) = index_mapping(pipeline)? {
        config.mapping = Arc::new(mapping);
        config.mapping_source = format!(
            "PI_MAPPING_FILE {}",
            pipeline.mapping_file.as_deref().unwrap_or_default()
        );
    }
    pi_es::check_mapping(&config.mapping, &config.schema).map_err(|e| {
        let source = |value: Option<&str>| {
            file_setting(value).map_or("built-in".to_owned(), |p| p.display().to_string())
        };
        anyhow::anyhow!(
            "{e} (PI_SCHEMA_FILE: {}, PI_MAPPING_FILE: {})",
            source(pipeline.schema_file.as_deref()),
            source(pipeline.mapping_file.as_deref())
        )
    })?;
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
            "--retain-runs",
            "7",
            "--publish-min-share",
            "0.7",
            "--publish-max-unresolved",
            "0.4",
        ]);
        let stores = Stores::open(&args.store).unwrap();
        let cfg = stages_config(&args.pipeline, &args.es, &args.exec, &stores).unwrap();
        assert!((cfg.es.publish_min_share - 0.7).abs() < f64::EPSILON);
        assert!((cfg.es.publish_max_unresolved - 0.4).abs() < f64::EPSILON);
        assert_eq!(cfg.es.retain_runs, 7);
        assert_eq!(cfg.alias, "people");
        assert_eq!(cfg.es.codec, pi_es::Codec::Default);
        assert_eq!(cfg.es.bulk.senders, 4);
        assert_eq!(cfg.effective_load_buffer_bytes(), 1 << 20);
        assert_eq!(cfg.task_memory_bytes, 64 << 20);
        assert!(cfg.input_store.is_none());
        assert_eq!(stores.layout("r1").unwrap().raw_persons(), "raw/persons");
    }

    #[test]
    fn retain_runs_environment_reaches_stage_config() {
        if std::env::var_os("PI_TEST_RETAIN_CHILD").is_some() {
            let args = run_args(&["--store-url", "memory://"]);
            let stores = Stores::open(&args.store).unwrap();
            let cfg = stages_config(&args.pipeline, &args.es, &args.exec, &stores).unwrap();
            assert_eq!(cfg.es.retain_runs, 7);
        } else {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "settings::tests::retain_runs_environment_reaches_stage_config",
                ])
                .env("PI_TEST_RETAIN_CHILD", "1")
                .env("PI_RETAIN_RUNS", "7")
                .output()
                .unwrap();
            assert!(output.status.success());
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
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
        assert!(stages_config(&args.pipeline, &args.es, &args.exec, &stores).is_err());
    }

    #[test]
    fn schema_and_mapping_files_are_read_and_checked_against_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let path = |name: &str| dir.path().join(name).display().to_string();
        let schema = r#"{"envelope": {"id": "key"},
            "person": {"references": "jobs",
                "fields": [{"name": "name", "kind": "string"}, {"name": "jobs", "kind": "object_array"}],
                "reference_fields": [{"name": "organization_id", "kind": "number"}], "disabled": []},
            "organization": {"fields": [], "disabled": []}}"#;
        let mapping = r#"{"dynamic": false, "properties": {
            "name": {"type": "text"},
            "jobs": {"type": "object", "properties": {
                "organization_id": {"type": "long", "ignore_malformed": true},
                "organization_resolved": {"type": "boolean"}}},
            "organizations": {"type": "object", "properties": {}},
            "unresolved_organization_ids": {"type": "long", "ignore_malformed": true}}}"#;
        std::fs::write(path("schema.json"), schema).unwrap();
        std::fs::write(path("mapping.json"), mapping).unwrap();
        std::fs::write(path("bad.json"), r#"{"envelope": {"id": ""}}"#).unwrap();

        let config = |extra: &[&str]| {
            let mut command_line = vec!["--store-url", "memory://"];
            command_line.extend_from_slice(extra);
            let args = run_args(&command_line);
            let stores = Stores::open(&args.store).unwrap();
            stages_config(&args.pipeline, &args.es, &args.exec, &stores)
        };
        // Unset and empty both mean the built-in ones.
        for extra in [&[][..], &["--schema-file", "", "--mapping-file", " "][..]] {
            let cfg = config(extra).unwrap();
            assert!(cfg.schema.is_builtin());
            assert_eq!(*cfg.mapping, pi_es::mapping());
            assert_eq!(cfg.mapping_source, "built-in mapping");
        }

        let builtin_file = path("builtin.json");
        std::fs::write(&builtin_file, pi_es::mapping().to_string()).unwrap();
        let cfg = config(&["--mapping-file", &builtin_file]).unwrap();
        assert_eq!(*cfg.mapping, pi_es::mapping());
        assert_eq!(
            cfg.mapping_source,
            format!("PI_MAPPING_FILE {builtin_file}")
        );

        let (schema_file, mapping_file, bad) =
            (path("schema.json"), path("mapping.json"), path("bad.json"));
        let cfg = config(&[
            "--schema-file",
            &schema_file,
            "--mapping-file",
            &mapping_file,
        ])
        .unwrap();
        assert_eq!(cfg.schema.envelope_id(), "key");
        assert_eq!(cfg.schema.references(), "jobs");
        assert_eq!(cfg.mapping["properties"]["jobs"]["type"], "object");

        // Each without the other does not fit; the error names both sources.
        let err = format!(
            "{:#}",
            config(&["--schema-file", &schema_file]).unwrap_err()
        );
        assert!(err.contains("does not fit the feed schema"), "{err}");
        assert!(err.contains("PI_MAPPING_FILE: built-in"), "{err}");
        assert!(err.contains(&schema_file), "{err}");
        let err = format!(
            "{:#}",
            config(&["--mapping-file", &mapping_file]).unwrap_err()
        );
        assert!(err.contains("PI_SCHEMA_FILE: built-in"), "{err}");

        // A bad or missing file names the setting, the file and the field.
        let err = format!("{:#}", config(&["--schema-file", &bad]).unwrap_err());
        assert!(err.contains("PI_SCHEMA_FILE"), "{err}");
        assert!(err.contains(&bad), "{err}");
        assert!(err.contains("envelope.id: empty name"), "{err}");
        let missing = path("missing.json");
        let err = format!("{:#}", config(&["--mapping-file", &missing]).unwrap_err());
        assert!(err.contains("PI_MAPPING_FILE: reading"), "{err}");
        let err = format!("{:#}", config(&["--mapping-file", &bad]).unwrap_err());
        assert!(err.contains("does not fit the feed schema"), "{err}");
    }

    #[test]
    fn the_load_buffer_defaults_to_twice_the_bulk_size() {
        let args = run_args(&["--store-url", "memory://", "--bulk-max-bytes", "1MiB"]);
        let stores = Stores::open(&args.store).unwrap();
        let cfg = stages_config(&args.pipeline, &args.es, &args.exec, &stores).unwrap();
        assert_eq!(cfg.load_buffer_bytes, None);
        assert_eq!(cfg.effective_load_buffer_bytes(), 2 << 20);
        let args = run_args(&["--store-url", "memory://", "--load-buffer-bytes", "0"]);
        let stores = Stores::open(&args.store).unwrap();
        assert!(stages_config(&args.pipeline, &args.es, &args.exec, &stores).is_err());
    }
}
