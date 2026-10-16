//! Object versions: what a conditional update must present to win.

use std::fmt;

use object_store::{PutResult, UpdateVersion};
use serde::{Deserialize, Serialize};

/// The version of an object as the store reported it on a put, get or head.
///
/// Stores differ in which half they fill (S3 and `MinIO` an etag, versioned buckets also a
/// version id), so both are kept and both are sent back on an update.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct ObjectVersion {
    /// The entity tag, quotes included, exactly as the store returned it.
    pub e_tag: Option<String>,
    /// The object version id, on stores that keep versions.
    pub version: Option<String>,
}

impl ObjectVersion {
    /// True when the store returned nothing a conditional update could use.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.e_tag.is_none() && self.version.is_none()
    }

    pub(crate) fn to_update(&self) -> UpdateVersion {
        UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }
}

/// `etag "abc"`, `etag "abc", version v1`, or `none`, for errors and logs.
impl fmt::Display for ObjectVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.e_tag, &self.version) {
            (Some(e_tag), Some(version)) => write!(f, "etag {e_tag}, version {version}"),
            (Some(e_tag), None) => write!(f, "etag {e_tag}"),
            (None, Some(version)) => write!(f, "version {version}"),
            (None, None) => f.write_str("none"),
        }
    }
}

impl From<PutResult> for ObjectVersion {
    fn from(value: PutResult) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<&object_store::ObjectMeta> for ObjectVersion {
    fn from(meta: &object_store::ObjectMeta) -> Self {
        Self {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_names_what_the_store_returned() {
        let both = ObjectVersion {
            e_tag: Some("\"abc\"".into()),
            version: Some("v1".into()),
        };
        assert_eq!(both.to_string(), "etag \"abc\", version v1");
        let etag = ObjectVersion {
            version: None,
            ..both.clone()
        };
        assert_eq!(etag.to_string(), "etag \"abc\"");
        let version = ObjectVersion {
            e_tag: None,
            ..both
        };
        assert_eq!(version.to_string(), "version v1");
        assert_eq!(ObjectVersion::default().to_string(), "none");
    }
}
