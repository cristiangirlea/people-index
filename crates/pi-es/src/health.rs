//! Cluster and index health as an ordered value, so "at least yellow" is a comparison.

use std::fmt;
use std::str::FromStr;

use crate::error::EsError;

/// `red < yellow < green`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealthStatus {
    /// Some primary is unassigned.
    Red,
    /// Every primary is assigned, some replica is not.
    Yellow,
    /// Every copy is assigned.
    Green,
}

impl HealthStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Red => "red",
            Self::Yellow => "yellow",
            Self::Green => "green",
        }
    }
}

impl fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for HealthStatus {
    type Err = EsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "red" => Ok(Self::Red),
            "yellow" => Ok(Self::Yellow),
            "green" => Ok(Self::Green),
            other => Err(EsError::Config(format!("health status {other:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_and_parsed() {
        assert!(HealthStatus::Red < HealthStatus::Yellow);
        assert!(HealthStatus::Yellow < HealthStatus::Green);
        assert_eq!(
            "green".parse::<HealthStatus>().ok(),
            Some(HealthStatus::Green)
        );
        assert!("blue".parse::<HealthStatus>().is_err());
    }
}
