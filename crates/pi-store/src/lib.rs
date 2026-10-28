//! pi-store: the object store seam of people-index.
//!
//! [`Store`] wraps `object_store` (S3 / `MinIO`, a local directory, memory) with the operations
//! the commit and lease protocol needs as plain values: create-if-absent, update-if-match,
//! reads that return the version to update against. [`RunLayout`] builds every key of the
//! storage layout described in docs/ARCHITECTURE.md.

mod error;
mod layout;
mod location;
mod options;
mod store;
mod version;

pub use error::{StoreError, StoreResult};
pub use layout::{Dataset, LayoutError, RUNS_ROOT, RunLayout, alias_lease, validate_id};
pub use location::StoreLocation;
pub use options::StoreOptions;
pub use store::{
    Backend, ByteStream, CreateOutcome, ObjectInfo, ObjectReader, ObjectWriter, Store, StoredObject,
};
pub use version::ObjectVersion;
