//! The line format: `{"id": ..., "updated_at": "...", "record": {...}}` plus a newline.

use serde::Serialize;

/// An envelope id as written: usually a JSON number, sometimes a decimal string (both are
/// valid per the contract, and real feeds mix them).
#[derive(Serialize)]
#[serde(untagged)]
enum WireId {
    Number(u64),
    Text(String),
}

#[derive(Serialize)]
struct Envelope<'a, R: Serialize> {
    id: WireId,
    updated_at: &'a str,
    record: &'a R,
}

/// Appends one envelope line (with its trailing `\n`) to `out`.
pub(crate) fn write_line<R: Serialize>(
    out: &mut Vec<u8>,
    id: u64,
    id_as_string: bool,
    updated_at: &str,
    record: &R,
) -> serde_json::Result<()> {
    let id = if id_as_string {
        WireId::Text(id.to_string())
    } else {
        WireId::Number(id)
    };
    serde_json::to_writer(
        &mut *out,
        &Envelope {
            id,
            updated_at,
            record,
        },
    )?;
    out.push(b'\n');
    Ok(())
}

/// Formats Unix seconds as an RFC 3339 UTC timestamp (`2026-01-31T12:00:00Z`).
///
/// Hand-rolled (days-to-civil, H. Hinnant) because it is infallible for the range used and
/// keeps a date library out of the generator.
pub(crate) fn rfc3339(unix_seconds: u64) -> String {
    let days = unix_seconds / 86_400;
    let rem = unix_seconds % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// `(year, month, day)` for days since 1970-01-01.
pub(crate) fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + u64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_format_known_instants() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339(1_769_860_800), "2026-01-31T12:00:00Z");
        assert_eq!(rfc3339(4_102_444_799), "2099-12-31T23:59:59Z");
    }

    #[test]
    fn lines_are_envelopes() {
        #[derive(Serialize)]
        struct R {
            id: u64,
        }
        let mut out = Vec::new();
        write_line(&mut out, 7, false, "t", &R { id: 7 }).unwrap();
        write_line(&mut out, 8, true, "t", &R { id: 8 }).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"id\":7,\"updated_at\":\"t\",\"record\":{\"id\":7}}\n\
             {\"id\":\"8\",\"updated_at\":\"t\",\"record\":{\"id\":8}}\n"
        );
    }
}
