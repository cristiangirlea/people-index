//! Parsing of store URLs (`PI_STORE_URL`, `PI_INPUT_URL`) into a backend and a key prefix.

use std::path::PathBuf;

use crate::error::{StoreError, StoreResult};

/// Where a store lives, parsed from its URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreLocation {
    /// `s3://bucket/optional/prefix/`.
    S3 { bucket: String, prefix: String },
    /// `file:///abs/dir/`, `file://./relative/dir`, or a bare filesystem path.
    Local { root: PathBuf },
    /// `memory://`: a process-local store for tests and dry runs.
    Memory,
}

impl StoreLocation {
    /// Parses a store URL.
    ///
    /// A bare path (no `scheme://`) is taken as a local directory so `--store ./store` works.
    pub fn parse(url: &str) -> StoreResult<Self> {
        let invalid = |reason: &str| StoreError::InvalidUrl {
            url: url.to_owned(),
            reason: reason.to_owned(),
        };
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return Err(invalid("empty"));
        }
        if let Some(rest) = trimmed.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(invalid("no bucket name"));
            }
            return Ok(Self::S3 {
                bucket: bucket.to_owned(),
                prefix: prefix.trim_matches('/').to_owned(),
            });
        }
        if let Some(rest) = trimmed.strip_prefix("file://") {
            if rest.is_empty() {
                return Err(invalid("no path"));
            }
            return Ok(Self::Local {
                root: PathBuf::from(strip_windows_drive_slash(rest)),
            });
        }
        if trimmed == "memory://" || trimmed == "memory:" {
            return Ok(Self::Memory);
        }
        if let Some((scheme, _)) = trimmed.split_once("://") {
            return Err(invalid(&format!(
                "unsupported scheme {scheme:?} (use s3://, file:// or memory://)"
            )));
        }
        Ok(Self::Local {
            root: PathBuf::from(trimmed),
        })
    }
}

/// `file:///C:/data` names `C:/data`, not `/C:/data`: drop the slash before a drive letter.
fn strip_windows_drive_slash(path: &str) -> &str {
    let bytes = path.as_bytes();
    if bytes.len() >= 3 && bytes[0] == b'/' && bytes[1].is_ascii_alphabetic() && bytes[2] == b':' {
        &path[1..]
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_with_and_without_prefix() {
        assert_eq!(
            StoreLocation::parse("s3://bucket/a/b/").unwrap(),
            StoreLocation::S3 {
                bucket: "bucket".into(),
                prefix: "a/b".into()
            }
        );
        assert_eq!(
            StoreLocation::parse("s3://bucket").unwrap(),
            StoreLocation::S3 {
                bucket: "bucket".into(),
                prefix: String::new()
            }
        );
        assert!(StoreLocation::parse("s3:///x").is_err());
    }

    #[test]
    fn file_urls_and_bare_paths() {
        assert_eq!(
            StoreLocation::parse("file:///data/store/").unwrap(),
            StoreLocation::Local {
                root: PathBuf::from("/data/store/")
            }
        );
        assert_eq!(
            StoreLocation::parse("file://./store").unwrap(),
            StoreLocation::Local {
                root: PathBuf::from("./store")
            }
        );
        assert_eq!(
            StoreLocation::parse("file:///C:/data").unwrap(),
            StoreLocation::Local {
                root: PathBuf::from("C:/data")
            }
        );
        assert_eq!(
            StoreLocation::parse("./store").unwrap(),
            StoreLocation::Local {
                root: PathBuf::from("./store")
            }
        );
    }

    #[test]
    fn memory_and_rejections() {
        assert_eq!(
            StoreLocation::parse("memory://").unwrap(),
            StoreLocation::Memory
        );
        assert!(StoreLocation::parse("gs://bucket").is_err());
        assert!(StoreLocation::parse("  ").is_err());
        assert!(StoreLocation::parse("file://").is_err());
    }
}
