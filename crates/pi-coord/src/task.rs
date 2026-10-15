//! Tasks: what a stage is split into, the id of one try at a task, and what a task reports.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use pi_store::{LayoutError, validate_id};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::stage_name::StageName;

/// One unit of work of a stage: a partition, an input file, or the whole run.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskSpec {
    /// The stage the task belongs to.
    pub stage: StageName,
    /// Unique within the stage; one path segment (`[A-Za-z0-9._=-]`), since commit and lease
    /// keys are named after it.
    pub task_id: String,
    /// The partition this task owns, for partitioned stages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
    /// The input object this task reads, for per-file stages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
}

impl TaskSpec {
    /// A task with a caller-chosen id, validated as a path segment.
    pub fn new(stage: StageName, task_id: impl Into<String>) -> Result<Self, LayoutError> {
        let task_id = task_id.into();
        validate_id("task id", &task_id)?;
        Ok(Self {
            stage,
            task_id,
            partition: None,
            input: None,
        })
    }

    /// The single task of a run-wide stage (`plan`, `prepare-index`, `publish`): id `run`.
    #[must_use]
    pub fn single(stage: StageName) -> Self {
        Self {
            stage,
            task_id: "run".to_owned(),
            partition: None,
            input: None,
        }
    }

    /// The task of partition `p`: id `part-00007`. Zero-padded so ids sort in partition order.
    #[must_use]
    pub fn for_partition(stage: StageName, partition: u32) -> Self {
        Self {
            stage,
            task_id: format!("part-{partition:05}"),
            partition: Some(partition),
            input: None,
        }
    }

    /// The task of the `index`-th input file (in key order): id `file-00003`.
    #[must_use]
    pub fn for_input(stage: StageName, index: usize, input: impl Into<String>) -> Self {
        Self {
            stage,
            task_id: format!("file-{index:05}"),
            partition: None,
            input: Some(input.into()),
        }
    }
}

/// The id of one try at a task: a UUID v7, so ids sort by creation time and never collide
/// across workers. Output files carry it, which keeps two tries from overwriting each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptId(Uuid);

impl AttemptId {
    /// A fresh attempt id.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// The UUID.
    #[must_use]
    pub fn uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for AttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AttemptId {
    /// 32 lowercase hex digits, no hyphens: it sits inside `<task>-<attempt>-<n>.seg`, where
    /// hyphens already separate the parts.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.simple())
    }
}

impl FromStr for AttemptId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl Serialize for AttemptId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for AttemptId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// What a finished task hands to its commit: the exact output keys and its counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskReport {
    /// Every object the task wrote that readers should see. Readers list commits, never data
    /// prefixes, so anything not listed here is an orphan.
    #[serde(default)]
    pub outputs: Vec<String>,
    /// Named counts (`records`, `malformed`, `documents`, ...), summed per stage.
    #[serde(default)]
    pub counters: BTreeMap<String, u64>,
    /// Bytes read.
    #[serde(default)]
    pub bytes_in: u64,
    /// Bytes written.
    #[serde(default)]
    pub bytes_out: u64,
    /// Wall time of the attempt; the executor fills it in when the stage leaves it zero.
    #[serde(default, rename = "duration_ms", with = "duration_ms")]
    pub duration: Duration,
}

impl TaskReport {
    /// Adds `n` to a counter.
    pub fn add(&mut self, counter: &str, n: u64) {
        *self.counters.entry(counter.to_owned()).or_default() += n;
    }

    /// A counter's value, zero when absent.
    #[must_use]
    pub fn counter(&self, counter: &str) -> u64 {
        self.counters.get(counter).copied().unwrap_or(0)
    }
}

/// Durations as whole milliseconds in JSON: readable, and exact enough for task timings.
mod duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        u64::deserialize(d).map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_make_valid_ids() {
        assert_eq!(TaskSpec::single(StageName::Plan).task_id, "run");
        let t = TaskSpec::for_partition(StageName::Join, 7);
        assert_eq!(t.task_id, "part-00007");
        assert_eq!(t.partition, Some(7));
        let t = TaskSpec::for_input(StageName::ExtractOrgs, 3, "raw/organizations/a.ndjson.gz");
        assert_eq!(t.task_id, "file-00003");
        assert!(TaskSpec::new(StageName::Join, "a/b").is_err());
        assert!(TaskSpec::new(StageName::Join, "p=1").is_ok());
    }

    #[test]
    fn attempt_ids_are_unique_ordered_and_round_trip() {
        let a = AttemptId::new();
        let b = AttemptId::new();
        assert_ne!(a, b);
        assert!(a < b, "v7 ids sort by creation");
        let s = a.to_string();
        assert_eq!(s.len(), 32);
        assert!(validate_id("attempt", &s).is_ok());
        assert_eq!(s.parse::<AttemptId>().unwrap(), a);
        let json = serde_json::to_string(&a).unwrap();
        assert_eq!(serde_json::from_str::<AttemptId>(&json).unwrap(), a);
    }

    #[test]
    fn report_json_shape() {
        let mut r = TaskReport {
            outputs: vec!["k".into()],
            bytes_in: 10,
            bytes_out: 5,
            duration: Duration::from_millis(1500),
            ..TaskReport::default()
        };
        r.add("records", 2);
        r.add("records", 3);
        assert_eq!(r.counter("records"), 5);
        assert_eq!(r.counter("absent"), 0);
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["duration_ms"], 1500);
        assert_eq!(serde_json::from_value::<TaskReport>(v).unwrap(), r);
        let spec = TaskSpec::single(StageName::Plan);
        let v = serde_json::to_value(&spec).unwrap();
        assert!(v.get("partition").is_none(), "absent options are omitted");
    }
}
