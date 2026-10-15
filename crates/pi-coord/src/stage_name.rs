//! [`StageName`]: the stages of a run, in the order they run.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// One stage of the pipeline. The declaration order is the run order, so `Ord` sorts stages
/// the way the driver runs them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StageName {
    /// Lists the inputs and fixes the partition count and the index name.
    Plan,
    /// Splits organization files into organization partitions.
    ExtractOrgs,
    /// Splits person files into person partitions and edge partitions.
    ExtractPersons,
    /// Joins each organization partition with its edges.
    Join,
    /// Builds the final documents of each person partition.
    Assemble,
    /// Creates the run's index with load-time settings.
    PrepareIndex,
    /// Sends each document partition to the index.
    Load,
    /// Restores settings, checks counts and moves the alias.
    Publish,
}

impl StageName {
    /// Every stage, in run order.
    pub const ALL: [StageName; 8] = [
        StageName::Plan,
        StageName::ExtractOrgs,
        StageName::ExtractPersons,
        StageName::Join,
        StageName::Assemble,
        StageName::PrepareIndex,
        StageName::Load,
        StageName::Publish,
    ];

    /// The kebab-case name used in paths, logs and the CLI.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::ExtractOrgs => "extract-orgs",
            Self::ExtractPersons => "extract-persons",
            Self::Join => "join",
            Self::Assemble => "assemble",
            Self::PrepareIndex => "prepare-index",
            Self::Load => "load",
            Self::Publish => "publish",
        }
    }

    /// The stage after this one, or `None` after `publish`.
    #[must_use]
    pub fn next(self) -> Option<StageName> {
        let i = Self::ALL.iter().position(|s| *s == self)?;
        Self::ALL.get(i + 1).copied()
    }
}

impl fmt::Display for StageName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A string that names no stage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "unknown stage {0:?} (expected one of plan, extract-orgs, extract-persons, join, assemble, prepare-index, load, publish)"
)]
pub struct UnknownStage(pub String);

impl FromStr for StageName {
    type Err = UnknownStage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|stage| stage.as_str() == s)
            .ok_or_else(|| UnknownStage(s.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_names_and_round_trip() {
        let mut sorted = StageName::ALL;
        sorted.sort();
        assert_eq!(sorted, StageName::ALL, "Ord is run order");
        for stage in StageName::ALL {
            assert_eq!(stage.as_str().parse::<StageName>().unwrap(), stage);
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{}\"", stage.as_str()));
            assert!(pi_store::validate_id("stage", stage.as_str()).is_ok());
        }
        assert_eq!(StageName::Plan.next(), Some(StageName::ExtractOrgs));
        assert_eq!(StageName::Publish.next(), None);
        assert!("extract".parse::<StageName>().is_err());
    }
}
