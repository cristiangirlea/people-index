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

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn ids_sort_by_time_and_are_valid() {
        let a = run_id_at(datetime!(2026-10-15 12:00:00 UTC), 0xabcd_ef12);
        assert_eq!(a, "20261015T120000Z-cdef12");
        let b = run_id_at(datetime!(2026-10-15 12:00:01 UTC), 0);
        assert!(b > a);
        assert!(pi_store::validate_id("run id", &new_run_id()).is_ok());
    }
}
