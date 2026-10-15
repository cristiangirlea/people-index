//! Object versions: what a conditional update must present to win.

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
