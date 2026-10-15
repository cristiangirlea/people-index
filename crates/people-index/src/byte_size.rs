//! Byte sizes on the command line: `268435456`, `256MiB`, `64M`, `8MB`.

/// Parses a byte size. Binary suffixes (`K`, `KiB`, `M`, `MiB`, `G`, `GiB`, `T`, `TiB`) are
/// powers of 1024; decimal ones (`KB`, `MB`, `GB`, `TB`) powers of 1000; `B` or none is bytes.
pub fn parse_byte_size(text: &str) -> Result<u64, String> {
    let t = text.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (digits, unit) = t.split_at(split);
    if digits.is_empty() {
        return Err(format!("{text:?} is not a byte size (e.g. 256MiB)"));
    }
    let n: u64 = digits
        .parse()
        .map_err(|e| format!("{text:?} is not a byte size: {e}"))?;
    let multiplier: u64 = match unit.trim() {
        "" | "B" => 1,
        "K" | "KiB" => 1 << 10,
        "M" | "MiB" => 1 << 20,
        "G" | "GiB" => 1 << 30,
        "T" | "TiB" => 1 << 40,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        other => {
            return Err(format!(
                "{text:?}: unknown unit {other:?} (use B, KiB, MiB, GiB, TiB)"
            ));
        }
    };
    n.checked_mul(multiplier)
        .ok_or_else(|| format!("{text:?} is too large"))
}

/// [`parse_byte_size`] for sizes held in memory (`usize`).
pub fn parse_byte_size_usize(text: &str) -> Result<usize, String> {
    let n = parse_byte_size(text)?;
    usize::try_from(n).map_err(|_| format!("{text:?} does not fit in memory on this platform"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units() {
        assert_eq!(parse_byte_size("1234").unwrap(), 1234);
        assert_eq!(parse_byte_size("256MiB").unwrap(), 256 << 20);
        assert_eq!(parse_byte_size("64M").unwrap(), 64 << 20);
        assert_eq!(parse_byte_size("8MB").unwrap(), 8_000_000);
        assert_eq!(parse_byte_size(" 2 GiB ").unwrap(), 2 << 30);
        assert_eq!(parse_byte_size("1B").unwrap(), 1);
        assert_eq!(parse_byte_size_usize("4KiB").unwrap(), 4096);
    }

    #[test]
    fn rejects() {
        for bad in [
            "",
            "MiB",
            "12XB",
            "-1",
            "1.5MiB",
            "99999999999999999999",
            "20000000TiB",
        ] {
            assert!(parse_byte_size(bad).is_err(), "{bad}");
        }
    }
}
