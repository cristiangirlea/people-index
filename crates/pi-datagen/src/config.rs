//! What to generate and how messy it should be.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::MAX_RECORDS;

/// The most files one feed may have: names are `part-NNNNN`, five digits, so lexical order
/// (the order the pipeline reads them in) equals numeric order.
pub const MAX_FILES: u32 = 100_000;

/// Size and realism knobs for one generated data set.
///
/// Every rate is a probability in `[0, 1]`. The defaults describe a feed that is mostly
/// clean but has every defect the pipeline must survive.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GenConfig {
    /// Distinct person ids (= documents in the index).
    pub persons: u64,
    /// Distinct organization ids.
    pub organizations: u64,
    /// Files per feed (`part-00000.ndjson.gz` ...). More files give more parallelism to both
    /// the generator and the pipeline's extract stages.
    pub files_per_feed: u32,
    /// Seed of every random choice. Same config and seed, same bytes.
    pub seed: u64,
    /// Relative weights of 0, 1, ..., 8 roles per person.
    pub roles_weights: [u32; 9],
    /// Zipf exponent of organization popularity (how often an organization is a role's
    /// employer). 1.0 gives a long tail with a few very large employers.
    pub org_popularity_exponent: f64,
    /// Zipf exponent of job title popularity. 0.8 keeps the most common title near a fifth
    /// of all persons while the rarest still occur.
    pub title_popularity_exponent: f64,
    /// Share of roles whose `organization_id` is null.
    pub null_org_rate: f64,
    /// Share of non-null role organization ids that are absent from the organization feed.
    pub unresolved_rate: f64,
    /// Share of lines that are malformed (a malformed line is inserted before a valid one
    /// with this probability): truncated JSON, a non-object record, or a bad id.
    pub malformed_rate: f64,
    /// Share of organizations that appear twice; the later record has a different name.
    pub duplicate_org_rate: f64,
    /// Share of persons that appear twice; the later record has different roles.
    pub duplicate_person_rate: f64,
    /// Share of records that carry fields the schema does not know.
    pub extra_field_rate: f64,
    /// Share of envelopes whose `id` is a decimal string instead of a number.
    pub string_id_rate: f64,
    /// gzip level, 0 (store) to 9.
    pub gzip_level: u32,
}

impl Default for GenConfig {
    fn default() -> Self {
        Self {
            persons: 10_000,
            organizations: 2_000,
            files_per_feed: 4,
            seed: 1,
            roles_weights: [8, 22, 22, 17, 12, 8, 5, 3, 3],
            org_popularity_exponent: 1.0,
            title_popularity_exponent: 0.8,
            null_org_rate: 0.33,
            unresolved_rate: 0.02,
            malformed_rate: 0.0005,
            duplicate_org_rate: 0.01,
            duplicate_person_rate: 0.001,
            extra_field_rate: 0.05,
            string_id_rate: 0.02,
            gzip_level: 6,
        }
    }
}

impl GenConfig {
    /// Checks that the configuration can be generated.
    pub fn validate(&self) -> Result<()> {
        let fail = |msg: String| Err(Error::Config(msg));
        if self.persons > MAX_RECORDS || self.organizations > MAX_RECORDS {
            return fail(format!("at most {MAX_RECORDS} records per feed"));
        }
        if self.files_per_feed == 0 || self.files_per_feed > MAX_FILES {
            return fail(format!("files_per_feed must be in 1..={MAX_FILES}"));
        }
        if self.roles_weights.iter().all(|&w| w == 0) {
            return fail("roles_weights must not all be zero".into());
        }
        if self
            .roles_weights
            .iter()
            .map(|&w| u64::from(w))
            .sum::<u64>()
            > u64::from(u32::MAX)
        {
            return fail("roles_weights must sum to at most u32::MAX".into());
        }
        for (name, exponent) in [
            ("org_popularity_exponent", self.org_popularity_exponent),
            ("title_popularity_exponent", self.title_popularity_exponent),
        ] {
            if !(exponent.is_finite() && exponent >= 0.0) {
                return fail(format!("{name} must be a finite number >= 0"));
            }
        }
        for (name, rate) in [
            ("null_org_rate", self.null_org_rate),
            ("unresolved_rate", self.unresolved_rate),
            ("malformed_rate", self.malformed_rate),
            ("duplicate_org_rate", self.duplicate_org_rate),
            ("duplicate_person_rate", self.duplicate_person_rate),
            ("extra_field_rate", self.extra_field_rate),
            ("string_id_rate", self.string_id_rate),
        ] {
            if !(0.0..=1.0).contains(&rate) {
                return fail(format!("{name} must be in [0, 1], got {rate}"));
            }
        }
        if self.malformed_rate >= 1.0 {
            return fail("malformed_rate must be below 1".into());
        }
        if self.gzip_level > 9 {
            return fail("gzip_level must be in 0..=9".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_documented_rates() {
        let cfg = GenConfig::default();
        assert!(cfg.validate().is_ok());
        assert!((cfg.null_org_rate - 0.33).abs() < f64::EPSILON);
        assert!((cfg.unresolved_rate - 0.02).abs() < f64::EPSILON);
        assert!((cfg.malformed_rate - 0.0005).abs() < f64::EPSILON);
        assert!((cfg.duplicate_org_rate - 0.01).abs() < f64::EPSILON);
        assert!((cfg.duplicate_person_rate - 0.001).abs() < f64::EPSILON);
    }

    #[test]
    fn bad_configs_are_refused() {
        let bad = [
            GenConfig {
                files_per_feed: 0,
                ..GenConfig::default()
            },
            GenConfig {
                files_per_feed: MAX_FILES + 1,
                ..GenConfig::default()
            },
            GenConfig {
                null_org_rate: 1.5,
                ..GenConfig::default()
            },
            GenConfig {
                malformed_rate: 1.0,
                ..GenConfig::default()
            },
            GenConfig {
                roles_weights: [0; 9],
                ..GenConfig::default()
            },
            GenConfig {
                org_popularity_exponent: f64::NAN,
                ..GenConfig::default()
            },
            GenConfig {
                gzip_level: 10,
                ..GenConfig::default()
            },
            GenConfig {
                persons: MAX_RECORDS + 1,
                ..GenConfig::default()
            },
        ];
        for cfg in bad {
            assert!(cfg.validate().is_err(), "{cfg:?}");
        }
    }

    #[test]
    fn config_round_trips_through_json() {
        let cfg: GenConfig = serde_json::from_str(r#"{"persons": 5, "seed": 9}"#).unwrap();
        assert_eq!(cfg.persons, 5);
        assert_eq!(cfg.organizations, GenConfig::default().organizations);
        assert!(serde_json::from_str::<GenConfig>(r#"{"nope": 1}"#).is_err());
    }
}
