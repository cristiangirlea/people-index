//! [`Plan`]: the content of `runs/<id>/plan.json`, fixed once per run.
//!
//! Everything every participant must agree on lives here: the input files and their order
//! (which is feed order, and so decides last-wins), the partition count, the index name, and
//! the digests of the feed schema and the index mapping ([`SchemaPin`]). It is written with
//! create-if-absent, so a retried or duplicated `plan` task can never change it under stages
//! that already read it.

use anyhow::Context as _;
use pi_coord::{RunContext, permanent};
use pi_model::{FeedSchema, Partitioner};
use pi_store::ObjectVersion;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::config::StagesConfig;

/// What a run reads and writes by name: the SHA-256 of the canonical JSON
/// ([`pi_es::canonical_sha256`]) of the feed schema ([`FeedSchema::to_json`], every name
/// spelled out) and of the index mapping. A resumed run, or a worker, with another schema or
/// mapping would mix rows read by different names in one index, so every task refuses it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPin {
    /// The feed schema's digest.
    pub feed_schema_sha256: String,
    /// The index mapping's digest.
    pub mapping_sha256: String,
}

impl SchemaPin {
    /// The digests of `schema` and `mapping`.
    #[must_use]
    pub fn of(schema: &FeedSchema, mapping: &Value) -> Self {
        Self {
            feed_schema_sha256: pi_es::canonical_sha256(&schema.to_json()),
            mapping_sha256: pi_es::canonical_sha256(mapping),
        }
    }

    /// The digests of the built-in schema and mapping: what a plan without a pin used.
    #[must_use]
    pub fn builtin() -> Self {
        Self::of(FeedSchema::builtin(), &pi_es::mapping())
    }
}

/// One input object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputFile {
    /// The key in the input store.
    pub key: String,
    /// Its size in bytes when the plan was made.
    pub size: u64,
    /// Its version (etag, and version id where the store keeps one) when the plan was made.
    /// Extract reads exactly this version and fails for good if the object changed. Absent in
    /// a plan written before versions were recorded; such a plan's extract fails for good.
    #[serde(default)]
    pub version: ObjectVersion,
}

/// What `plan` decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    /// The run.
    pub run_id: String,
    /// The selected input delivery, absent in older plans.
    #[serde(default)]
    pub delivery: Option<String>,
    /// The partition count `P`, a power of two.
    pub partitions: u32,
    /// The alias `publish` moves.
    pub alias: String,
    /// The run's index, `<alias>-<run_id>` in lowercase.
    pub index: String,
    /// The organization files in feed order (sorted by key). Position = `Seq::file`.
    pub organizations: Vec<InputFile>,
    /// The person files in feed order (sorted by key). Position = `Seq::file`.
    pub persons: Vec<InputFile>,
    /// The sum of every input file's size.
    pub total_bytes: u64,
    /// The feed schema and mapping the run uses. Absent in a plan written before they could
    /// be configured, which used the built-in ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaPin>,
    /// When the plan was made.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl Plan {
    /// The partitioner of the run.
    pub fn partitioner(&self) -> anyhow::Result<Partitioner> {
        Partitioner::new(self.partitions).map_err(|e| permanent(anyhow::anyhow!("plan.json: {e}")))
    }

    /// The input files of a feed.
    #[must_use]
    pub fn files(&self, feed: Feed) -> &[InputFile] {
        match feed {
            Feed::Organizations => &self.organizations,
            Feed::Persons => &self.persons,
        }
    }

    /// The schema and mapping the run uses (the built-in ones for a plan without a pin).
    #[must_use]
    pub fn schema_pin(&self) -> SchemaPin {
        self.schema.clone().unwrap_or_else(SchemaPin::builtin)
    }

    /// Fails for good unless this process reads and writes with the feed schema and the
    /// mapping the plan pins (`current`, from [`StagesConfig::schema_pin`]).
    pub fn check_schema(&self, current: &SchemaPin) -> anyhow::Result<()> {
        let planned = self.schema_pin();
        let mut differ = Vec::new();
        for (what, setting, planned, current) in [
            (
                "feed schema",
                "PI_SCHEMA_FILE",
                &planned.feed_schema_sha256,
                &current.feed_schema_sha256,
            ),
            (
                "index mapping",
                "PI_MAPPING_FILE",
                &planned.mapping_sha256,
                &current.mapping_sha256,
            ),
        ] {
            if planned != current {
                differ.push(format!(
                    "the {what} ({setting}) has sha256 {current}, the plan {planned}"
                ));
            }
        }
        if differ.is_empty() {
            return Ok(());
        }
        Err(permanent(anyhow::anyhow!(
            "run {} was planned with another feed schema or index mapping: {}; run it with the \
             PI_SCHEMA_FILE and PI_MAPPING_FILE it started with (unset for the built-in ones)",
            self.run_id,
            differ.join("; ")
        )))
    }

    /// Reads the run's plan and checks that `config` has the schema and mapping it pins
    /// ([`Plan::check_schema`]). Every task starts here.
    pub async fn load_for(ctx: &RunContext, config: &StagesConfig) -> anyhow::Result<Self> {
        let plan = Self::load(ctx).await?;
        plan.check_schema(&config.schema_pin())?;
        Ok(plan)
    }

    /// Reads the run's plan. Missing means `plan` has not committed: a bug in the stage
    /// order, so the error is permanent.
    pub async fn load(ctx: &RunContext) -> anyhow::Result<Self> {
        let key = ctx.layout.plan();
        match ctx
            .store
            .get_json::<Plan>(&key)
            .await
            .with_context(|| format!("reading {key}"))?
        {
            Some((plan, _)) => Ok(plan),
            None => Err(permanent(anyhow::anyhow!(
                "{key} does not exist: the plan stage has not run"
            ))),
        }
    }
}

/// The two input feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feed {
    /// `raw/organizations`.
    Organizations,
    /// `raw/persons`.
    Persons,
}

impl Feed {
    /// The feed's name in counters, quarantine records and logs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Organizations => "organizations",
            Self::Persons => "persons",
        }
    }
}

/// True for the keys `plan` takes as input files.
#[must_use]
pub fn is_input_key(key: &str) -> bool {
    key.ends_with(".ndjson.gz")
}

/// The index of a run: `<alias>-<run_id>`, lowercased because Elasticsearch index names must
/// be (run ids carry an uppercase `T` and `Z` from their timestamp).
#[must_use]
pub fn index_name(alias: &str, run_id: &str) -> String {
    format!("{alias}-{run_id}").to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_names_are_lowercase() {
        assert_eq!(
            index_name("persons", "20261015T120000Z-ab12"),
            "persons-20261015t120000z-ab12"
        );
        assert!(pi_es::validate_index_name(&index_name("persons", "20261015T120000Z-ab")).is_ok());
    }

    #[test]
    fn only_gzipped_ndjson_is_input() {
        assert!(is_input_key("raw/persons/part-00000.ndjson.gz"));
        assert!(!is_input_key("raw/persons/_SUCCESS"));
        assert!(!is_input_key("raw/expected.json"));
    }

    #[test]
    fn plan_json_round_trips() {
        let plan = Plan {
            run_id: "r1".into(),
            delivery: None,
            partitions: 4,
            alias: "persons".into(),
            index: "persons-r1".into(),
            organizations: vec![InputFile {
                key: "raw/organizations/a.ndjson.gz".into(),
                size: 10,
                version: ObjectVersion {
                    e_tag: Some("\"e1\"".into()),
                    version: None,
                },
            }],
            persons: vec![],
            total_bytes: 10,
            schema: Some(SchemaPin::builtin()),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let json = serde_json::to_string(&plan).unwrap();
        assert_eq!(serde_json::from_str::<Plan>(&json).unwrap(), plan);
        assert_eq!(plan.files(Feed::Organizations).len(), 1);
        assert!(plan.partitioner().is_ok());
    }

    #[test]
    fn a_plan_refuses_another_schema_or_mapping_and_an_old_plan_means_the_builtin_ones() {
        let old = r#"{"run_id":"r1","partitions":4,"alias":"persons","index":"persons-r1",
            "organizations":[],"persons":[],"total_bytes":0,"created_at":"2026-10-01T00:00:00Z"}"#;
        let old: Plan = serde_json::from_str(old).unwrap();
        assert_eq!(old.schema, None);
        assert_eq!(old.schema_pin(), SchemaPin::builtin());
        old.check_schema(&StagesConfig::default().schema_pin())
            .unwrap();

        let schema = FeedSchema::from_json(r#"{"envelope": {"id": "key"}}"#).unwrap();
        let custom = SchemaPin::of(&schema, &pi_es::mapping());
        assert_eq!(custom.mapping_sha256, SchemaPin::builtin().mapping_sha256);
        let err = old.check_schema(&custom).unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        let text = format!("{err:#}");
        assert!(
            text.contains("run r1 was planned with another feed schema"),
            "{text}"
        );
        assert!(
            text.contains("the feed schema (PI_SCHEMA_FILE) has sha256"),
            "{text}"
        );
        assert!(!text.contains("PI_MAPPING_FILE) has"), "{text}");

        let pinned = Plan {
            schema: Some(custom.clone()),
            ..old.clone()
        };
        pinned.check_schema(&custom).unwrap();
        let mapping = serde_json::json!({"dynamic": false, "properties": {}});
        let err = pinned
            .check_schema(&SchemaPin::of(&schema, &mapping))
            .unwrap_err();
        assert!(format!("{err:#}").contains("the index mapping (PI_MAPPING_FILE) has"));
        assert!(pinned.check_schema(&SchemaPin::builtin()).is_err());
        // Key order and whitespace do not change a digest.
        let respelled: Value =
            serde_json::from_str(&serde_json::to_string_pretty(&pi_es::mapping()).unwrap())
                .unwrap();
        assert_eq!(
            SchemaPin::of(FeedSchema::builtin(), &respelled),
            SchemaPin::builtin()
        );
    }

    #[test]
    fn an_input_without_a_version_still_parses() {
        let file: InputFile =
            serde_json::from_str(r#"{"key":"raw/persons/a.ndjson.gz","size":3}"#).unwrap();
        assert!(file.version.is_empty());
    }
}
