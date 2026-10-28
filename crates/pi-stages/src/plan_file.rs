//! [`Plan`]: the content of `runs/<id>/plan.json`, fixed once per run.
//!
//! Everything every participant must agree on lives here: the input files and their order
//! (which is feed order, and so decides last-wins), the partition count and the index name.
//! It is written with create-if-absent, so a retried or duplicated `plan` task can never
//! change it under stages that already read it.

use anyhow::Context as _;
use pi_coord::{RunContext, permanent};
use pi_model::Partitioner;
use pi_store::ObjectVersion;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

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
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let json = serde_json::to_string(&plan).unwrap();
        assert_eq!(serde_json::from_str::<Plan>(&json).unwrap(), plan);
        assert_eq!(plan.files(Feed::Organizations).len(), 1);
        assert!(plan.partitioner().is_ok());
    }

    #[test]
    fn an_input_without_a_version_still_parses() {
        let file: InputFile =
            serde_json::from_str(r#"{"key":"raw/persons/a.ndjson.gz","size":3}"#).unwrap();
        assert!(file.version.is_empty());
    }
}
