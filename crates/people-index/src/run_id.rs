//! Generated run ids: `20261015T120000Z-a1b2c3`.
//!
//! The UTC start time comes first so the greatest id is the newest run (`worker` without
//! `--run-id` follows it); the random suffix keeps two coordinators started in the same
//! second apart.

use time::OffsetDateTime;
use time::macros::format_description;

/// A fresh run id for a run starting now.
#[must_use]
pub fn new_run_id() -> String {
    run_id_at(OffsetDateTime::now_utc(), rand::random::<u32>())
}

/// The run id for a start time and a random value (only its low 24 bits are used).
#[must_use]
pub fn run_id_at(at: OffsetDateTime, random: u32) -> String {
    let stamp = at
        .to_offset(time::UtcOffset::UTC)
        .format(format_description!(
            "[year][month][day]T[hour][minute][second]Z"
        ))
        // Formatting a UTC date with numeric components only cannot fail for years 0..=9999.
        .unwrap_or_else(|_| "00000000T000000Z".to_owned());
    format!("{stamp}-{:06x}", random & 0x00ff_ffff)
}

/// Derives the scheduled UTC minute and a stable suffix from a `CronJob` Job name.
pub fn from_job_name(name: &str) -> anyhow::Result<String> {
    let (prefix, minutes) = name
        .rsplit_once('-')
        .ok_or_else(|| anyhow::anyhow!("job name must end in a numeric minutes suffix"))?;
    if prefix.is_empty() || minutes.is_empty() || !minutes.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!("job name must end in a numeric minutes suffix");
    }
    let seconds = minutes
        .parse::<i64>()?
        .checked_mul(60)
        .ok_or_else(|| anyhow::anyhow!("job name minutes out of range"))?;
    if seconds < 1_577_836_800 {
        anyhow::bail!("job name minutes must be on or after 2020-01-01 UTC");
    }
    let at = OffsetDateTime::from_unix_timestamp(seconds)?;
    // FNV-1a gives the same suffix on every pod and distinguishes CronJob names.
    let hash = name.bytes().fold(2_166_136_261_u32, |h, b| {
        (h ^ u32::from(b)).wrapping_mul(16_777_619)
    });
    Ok(run_id_at(at, hash))
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn job_names_are_stable_and_validated() {
        assert!(from_job_name("pi-0").is_err());
        assert!(
            from_job_name("pi-26297280")
                .unwrap()
                .starts_with("20200101T000000Z-")
        );
        assert_eq!(
            from_job_name("pi-29876543").unwrap(),
            from_job_name("pi-29876543").unwrap()
        );
        assert!(from_job_name("pi-nope").is_err());
        assert!(from_job_name("pi-999999999999999999999").is_err());
    }

    #[test]
    fn scheduled_ids_have_valid_stamps_and_distinct_names() {
        let id = from_job_name("pi-schedule-29876543").unwrap();
        assert_eq!(id, "20261021T142300Z-9b5323");
        assert_ne!(id, from_job_name("other-29876543").unwrap());
        assert!(pi_store::RunLayout::new(&id).is_ok());
        assert!(pi_es::run_stamp("persons", &format!("persons-{}", id.to_lowercase())).is_some());
        for name in ["-123", "pi-", "nodash", "pi-26297279"] {
            assert!(from_job_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn ids_sort_by_time_and_are_valid() {
        let a = run_id_at(datetime!(2026-10-15 12:00:00 UTC), 0xabcd_ef12);
        assert_eq!(a, "20261015T120000Z-cdef12");
        let b = run_id_at(datetime!(2026-10-15 12:00:01 UTC), 0);
        assert!(b > a);
        assert!(pi_store::validate_id("run id", &new_run_id()).is_ok());
    }
}
