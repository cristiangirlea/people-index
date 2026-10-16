//! [`Store`]: the one way the pipeline talks to the object store.
//!
//! It is a thin layer over `object_store` that does three things the raw API does not:
//! string keys validated in one place, the conditional-write outcomes the commit and lease
//! protocol branch on as plain values (`CreateOutcome`, `PreconditionFailed`), and one
//! constructor for every backend the deployment uses (S3 / `MinIO`, a local directory, memory).

use std::fmt;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::io::StreamReader;

use crate::error::{StoreError, StoreResult};
use crate::location::StoreLocation;
use crate::options::StoreOptions;
use crate::version::ObjectVersion;

/// Which kind of backend a [`Store`] talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    /// S3 or an S3-compatible server (`MinIO`, R2, Ceph RGW).
    S3,
    /// A directory on the local filesystem.
    Local,
    /// Process memory (tests, dry runs).
    Memory,
}

impl Backend {
    /// A short lowercase name for logs and errors.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Local => "local",
            Self::Memory => "memory",
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The result of a create-if-absent put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome {
    /// This call created the object; the version is what a later update must present.
    Created(ObjectVersion),
    /// Someone else created it first; nothing was written.
    AlreadyExists,
}

impl CreateOutcome {
    /// True when this call wrote the object.
    #[must_use]
    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }
}

/// An object read whole, with the version a conditional update must present.
#[derive(Debug, Clone)]
pub struct StoredObject {
    /// The object body.
    pub bytes: Bytes,
    /// The version the store reported with this body.
    pub version: ObjectVersion,
}

/// Metadata of one object, as listed or headed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    /// The key relative to the store's root, no leading slash.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// The version the store reported.
    pub version: ObjectVersion,
    /// When the object was last written, by the store's clock.
    pub last_modified: SystemTime,
}

impl ObjectInfo {
    fn from_meta(meta: &object_store::ObjectMeta) -> Self {
        Self {
            key: meta.location.to_string(),
            size: meta.size,
            version: ObjectVersion::from(meta),
            last_modified: SystemTime::from(meta.last_modified),
        }
    }
}

/// An object body as a stream of chunks.
pub type ByteStream = BoxStream<'static, StoreResult<Bytes>>;

/// An object body as an `AsyncBufRead`, for line readers and decompressors.
pub type ObjectReader = StreamReader<BoxStream<'static, std::io::Result<Bytes>>, Bytes>;

/// A streaming upload: write with `AsyncWriteExt`, then `shutdown()` to complete it. Large
/// bodies go up as a multipart upload, small ones as one put; dropping it unfinished aborts.
pub type ObjectWriter = BufWriter;

/// A handle to one object store prefix. Cheap to clone: clones share the client.
#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
    backend: Backend,
    url: Arc<str>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store")
            .field("backend", &self.backend)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Store {
    /// Opens the store a URL names (`s3://bucket/prefix`, `file:///dir`, `memory://`).
    pub fn open(url: &str, options: &StoreOptions) -> StoreResult<Self> {
        match StoreLocation::parse(url)? {
            StoreLocation::S3 { bucket, prefix } => Self::s3(&bucket, &prefix, options),
            StoreLocation::Local { root } => Self::local(root),
            StoreLocation::Memory => Ok(Self::memory()),
        }
    }

    /// A fresh, empty in-memory store. Clones of the returned handle share the same objects.
    #[must_use]
    pub fn memory() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            backend: Backend::Memory,
            url: Arc::from("memory://"),
        }
    }

    /// A store rooted at a local directory, created if missing.
    ///
    /// Supports create-if-absent (a hard link, atomic) but not update-if-match, so it serves
    /// the local executor only.
    pub fn local(root: impl AsRef<FsPath>) -> StoreResult<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let fs = LocalFileSystem::new_with_prefix(root)
            .map_err(|e| StoreError::from_backend(&root.display().to_string(), e))?
            // Removing the last object of a directory removes the directory, so a cleaned-up
            // run leaves no empty tree behind.
            .with_automatic_cleanup(true);
        Ok(Self {
            inner: Arc::new(fs),
            backend: Backend::Local,
            url: Arc::from(format!("file://{}", root.display())),
        })
    }

    /// A store on an S3-compatible bucket, optionally under a key prefix.
    pub fn s3(bucket: &str, prefix: &str, options: &StoreOptions) -> StoreResult<Self> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_allow_http(options.allow_http_resolved());
        if let Some(region) = &options.s3_region {
            builder = builder.with_region(region);
        } else if std::env::var_os("AWS_REGION").is_none()
            && std::env::var_os("AWS_DEFAULT_REGION").is_none()
        {
            builder = builder.with_region("us-east-1");
        }
        if let Some(endpoint) = &options.s3_endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(options.virtual_hosted_style.unwrap_or(false));
        } else if let Some(virtual_hosted) = options.virtual_hosted_style {
            builder = builder.with_virtual_hosted_style_request(virtual_hosted);
        }
        let s3 = builder.build().map_err(|e| StoreError::InvalidUrl {
            url: format!("s3://{bucket}/{prefix}"),
            reason: e.to_string(),
        })?;
        let prefix = prefix.trim_matches('/');
        let inner: Arc<dyn ObjectStore> = if prefix.is_empty() {
            Arc::new(s3)
        } else {
            let path = Path::parse(prefix).map_err(|e| StoreError::InvalidKey {
                key: prefix.to_owned(),
                reason: e.to_string(),
            })?;
            Arc::new(PrefixStore::new(s3, path))
        };
        Ok(Self {
            inner,
            backend: Backend::S3,
            url: Arc::from(if prefix.is_empty() {
                format!("s3://{bucket}/")
            } else {
                format!("s3://{bucket}/{prefix}/")
            }),
        })
    }

    /// Wraps any `object_store` implementation (for tests that inject faults).
    #[must_use]
    pub fn from_object_store(inner: Arc<dyn ObjectStore>, backend: Backend, url: &str) -> Self {
        Self {
            inner,
            backend,
            url: Arc::from(url),
        }
    }

    /// The backend kind.
    #[must_use]
    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// The URL this store was opened from (normalised), for logs.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// True when [`Store::put_update_if_match`] works, which leases need.
    #[must_use]
    pub fn supports_conditional_update(&self) -> bool {
        !matches!(self.backend, Backend::Local)
    }

    /// The underlying `object_store`, for callers that need an API this type does not wrap.
    #[must_use]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    /// One put, retried a few times on the local backend when Windows reports a transient
    /// sharing violation: renaming over a file another handle holds for a moment (an antivirus
    /// scan of the file just written, a concurrent reader) fails with "Access is denied". Other
    /// errors, other backends and other platforms go straight back to the caller.
    ///
    /// A retried create-if-absent whose first attempt did land reports `AlreadyExists`; the
    /// commit protocol treats that as "someone committed" and reads the commit, so it is safe.
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        const ATTEMPTS: u32 = 6;
        let mut attempt = 1;
        loop {
            match self
                .inner
                .put_opts(path, payload.clone(), opts.clone())
                .await
            {
                Err(e) if attempt < ATTEMPTS && self.is_transient_local_error(&e) => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 << attempt)).await;
                    attempt += 1;
                }
                other => return other,
            }
        }
    }

    fn is_transient_local_error(&self, error: &object_store::Error) -> bool {
        if !cfg!(windows) || self.backend != Backend::Local {
            return false;
        }
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(e) = source {
            if let Some(io) = e.downcast_ref::<std::io::Error>() {
                return io.kind() == std::io::ErrorKind::PermissionDenied;
            }
            source = e.source();
        }
        false
    }

    /// Writes an object, replacing any previous one.
    pub async fn put(&self, key: &str, body: impl Into<Bytes>) -> StoreResult<ObjectVersion> {
        let path = object_path(key)?;
        let result = self
            .put_opts(&path, PutPayload::from(body.into()), PutOptions::default())
            .await
            .map_err(|e| StoreError::from_backend(key, e))?;
        Ok(ObjectVersion::from(result))
    }

    /// Writes an object only if no object exists at `key`. Atomic on every backend: exactly
    /// one of several concurrent callers gets `Created`.
    pub async fn put_create_if_absent(
        &self,
        key: &str,
        body: impl Into<Bytes>,
    ) -> StoreResult<CreateOutcome> {
        let path = object_path(key)?;
        let opts = PutOptions::from(PutMode::Create);
        match self
            .put_opts(&path, PutPayload::from(body.into()), opts)
            .await
        {
            Ok(result) => Ok(CreateOutcome::Created(ObjectVersion::from(result))),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(CreateOutcome::AlreadyExists),
            Err(e) => Err(self.unsupported_or(key, "create-if-absent put", e)),
        }
    }

    /// Replaces an object only if its current version is `expected`. Returns the new version,
    /// or [`StoreError::PreconditionFailed`] when the object changed or no longer exists.
    pub async fn put_update_if_match(
        &self,
        key: &str,
        body: impl Into<Bytes>,
        expected: &ObjectVersion,
    ) -> StoreResult<ObjectVersion> {
        if !self.supports_conditional_update() {
            return Err(StoreError::Unsupported {
                operation: "update-if-match put",
                backend: self.backend.as_str(),
            });
        }
        if expected.is_empty() {
            // Without an etag or version the backend cannot check anything; refusing here keeps
            // a missing etag from turning into an unconditional overwrite or an opaque error.
            return Err(StoreError::PreconditionFailed {
                key: key.to_owned(),
            });
        }
        let path = object_path(key)?;
        let opts = PutOptions::from(PutMode::Update(expected.to_update()));
        self.put_opts(&path, PutPayload::from(body.into()), opts)
            .await
            .map(ObjectVersion::from)
            .map_err(|e| self.unsupported_or(key, "update-if-match put", e))
    }

    /// Reads a whole object.
    pub async fn get(&self, key: &str) -> StoreResult<StoredObject> {
        let path = object_path(key)?;
        let result = self
            .inner
            .get(&path)
            .await
            .map_err(|e| StoreError::from_backend(key, e))?;
        let version = ObjectVersion::from(&result.meta);
        let bytes = result
            .bytes()
            .await
            .map_err(|e| StoreError::from_backend(key, e))?;
        Ok(StoredObject { bytes, version })
    }

    /// Reads a whole object, or `None` when it does not exist.
    pub async fn get_opt(&self, key: &str) -> StoreResult<Option<StoredObject>> {
        match self.get(key).await {
            Ok(obj) => Ok(Some(obj)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Streams an object's body in chunks, without buffering it whole.
    pub async fn get_stream(&self, key: &str) -> StoreResult<ByteStream> {
        self.stream_opts(key, GetOptions::default()).await
    }

    /// Streams an object's body only if its current version is `expected` (an `If-Match` on
    /// the etag, and the version id when there is one). Returns
    /// [`StoreError::PreconditionFailed`] when the object changed since that version was
    /// read, [`StoreError::NotFound`] when it is gone. Works on every backend: the local
    /// filesystem's etag is its inode, modification time and size.
    pub async fn get_stream_if_match(
        &self,
        key: &str,
        expected: &ObjectVersion,
    ) -> StoreResult<ByteStream> {
        if expected.is_empty() {
            // Without an etag or version nothing can be checked; refusing keeps a missing etag
            // from turning into an unconditional read.
            return Err(StoreError::PreconditionFailed {
                key: key.to_owned(),
            });
        }
        let opts = GetOptions {
            if_match: expected.e_tag.clone(),
            version: expected.version.clone(),
            ..GetOptions::default()
        };
        self.stream_opts(key, opts).await
    }

    async fn stream_opts(&self, key: &str, opts: GetOptions) -> StoreResult<ByteStream> {
        let path = object_path(key)?;
        let result = self
            .inner
            .get_opts(&path, opts)
            .await
            .map_err(|e| StoreError::from_backend(key, e))?;
        let owned_key = key.to_owned();
        Ok(result
            .into_stream()
            .map_err(move |e| StoreError::from_backend(&owned_key, e))
            .boxed())
    }

    /// An object's body as an `AsyncBufRead`.
    pub async fn get_reader(&self, key: &str) -> StoreResult<ObjectReader> {
        let stream = self
            .get_stream(key)
            .await?
            .map_err(std::io::Error::other)
            .boxed();
        Ok(StreamReader::new(stream))
    }

    /// An object's metadata, or `None` when it does not exist.
    pub async fn head(&self, key: &str) -> StoreResult<Option<ObjectInfo>> {
        let path = object_path(key)?;
        match self.inner.head(&path).await {
            Ok(meta) => Ok(Some(ObjectInfo::from_meta(&meta))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StoreError::from_backend(key, e)),
        }
    }

    /// Every object under a directory-style prefix (`runs/r1/join/_commit`), recursively,
    /// sorted by key. The prefix matches whole path segments, so `a/b` does not match `a/bc`.
    pub async fn list(&self, prefix: &str) -> StoreResult<Vec<ObjectInfo>> {
        let path = prefix_path(prefix)?;
        let mut out: Vec<ObjectInfo> = self
            .inner
            .list(path.as_ref())
            .map_ok(|meta| ObjectInfo::from_meta(&meta))
            .try_collect()
            .await
            .map_err(|e| StoreError::from_backend(prefix, e))?;
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    /// The keys under a prefix, sorted. See [`Store::list`].
    pub async fn list_keys(&self, prefix: &str) -> StoreResult<Vec<String>> {
        Ok(self
            .list(prefix)
            .await?
            .into_iter()
            .map(|o| o.key)
            .collect())
    }

    /// The immediate child "directories" of a prefix (`runs` gives `runs/r1`, `runs/r2`),
    /// sorted, without a trailing slash.
    pub async fn list_prefixes(&self, prefix: &str) -> StoreResult<Vec<String>> {
        let path = prefix_path(prefix)?;
        let result = self
            .inner
            .list_with_delimiter(path.as_ref())
            .await
            .map_err(|e| StoreError::from_backend(prefix, e))?;
        let mut out: Vec<String> = result
            .common_prefixes
            .iter()
            .map(ToString::to_string)
            .collect();
        out.sort();
        Ok(out)
    }

    /// Deletes an object. Deleting an absent object succeeds, so cleanup is retry-safe.
    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        let path = object_path(key)?;
        match self.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(StoreError::from_backend(key, e)),
        }
    }

    /// A streaming writer for a large object (segments). Call `shutdown()` to finish it.
    pub fn writer(&self, key: &str) -> StoreResult<ObjectWriter> {
        Ok(BufWriter::new(Arc::clone(&self.inner), object_path(key)?))
    }

    /// Writes a value as JSON, replacing any previous object.
    pub async fn put_json<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
    ) -> StoreResult<ObjectVersion> {
        self.put(key, encode_json(key, value)?).await
    }

    /// Writes a value as JSON only if nothing exists at `key`.
    pub async fn put_json_create_if_absent<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
    ) -> StoreResult<CreateOutcome> {
        self.put_create_if_absent(key, encode_json(key, value)?)
            .await
    }

    /// Replaces a JSON object only if its version is still `expected`.
    pub async fn put_json_update_if_match<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        expected: &ObjectVersion,
    ) -> StoreResult<ObjectVersion> {
        self.put_update_if_match(key, encode_json(key, value)?, expected)
            .await
    }

    /// Reads and decodes a JSON object with its version, or `None` when it does not exist.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> StoreResult<Option<(T, ObjectVersion)>> {
        let Some(obj) = self.get_opt(key).await? else {
            return Ok(None);
        };
        let value = serde_json::from_slice(&obj.bytes).map_err(|source| StoreError::Json {
            key: key.to_owned(),
            source,
        })?;
        Ok(Some((value, obj.version)))
    }

    /// Maps "not implemented" to [`StoreError::Unsupported`], everything else as usual.
    fn unsupported_or(
        &self,
        key: &str,
        operation: &'static str,
        err: object_store::Error,
    ) -> StoreError {
        match err {
            object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. } => StoreError::Unsupported {
                operation,
                backend: self.backend.as_str(),
            },
            other => StoreError::from_backend(key, other),
        }
    }
}

fn encode_json<T: Serialize + ?Sized>(key: &str, value: &T) -> StoreResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|source| StoreError::Json {
        key: key.to_owned(),
        source,
    })
}

/// Validates an object key: non-empty, no empty segments, no `.`/`..`, no control characters.
fn object_path(key: &str) -> StoreResult<Path> {
    let invalid = |reason: String| StoreError::InvalidKey {
        key: key.to_owned(),
        reason,
    };
    if key.is_empty() || key.ends_with('/') {
        return Err(invalid("an object key must name a file".to_owned()));
    }
    Path::parse(key).map_err(|e| invalid(e.to_string()))
}

/// A listing prefix: empty means the whole store.
fn prefix_path(prefix: &str) -> StoreResult<Option<Path>> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    Path::parse(trimmed)
        .map(Some)
        .map_err(|e| StoreError::InvalidKey {
            key: prefix.to_owned(),
            reason: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_validated() {
        assert!(object_path("runs/r1/plan.json").is_ok());
        assert!(object_path("runs/r1/join/_commit/p=3.json").is_ok());
        assert!(object_path("").is_err());
        assert!(object_path("runs//plan.json").is_err());
        assert!(object_path("runs/../plan.json").is_err());
        assert!(object_path("runs/").is_err());
        assert!(prefix_path("").unwrap().is_none());
        assert!(prefix_path("/runs/r1/").unwrap().is_some());
    }

    #[test]
    fn local_store_refuses_update_if_match_up_front() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::local(dir.path()).unwrap();
        assert!(!store.supports_conditional_update());
        assert!(Store::memory().supports_conditional_update());
    }
}
