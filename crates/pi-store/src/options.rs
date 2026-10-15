//! Connection options for S3-compatible stores.

/// How to reach an S3-compatible endpoint. Unset fields fall back to the standard `AWS_*`
/// environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, ...).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreOptions {
    /// A custom endpoint (`PI_S3_ENDPOINT`), e.g. `http://minio:9000`. Setting one switches to
    /// path-style requests, which local S3 servers expect.
    pub s3_endpoint: Option<String>,
    /// The region; S3-compatible servers accept any, so it defaults to `us-east-1`.
    pub s3_region: Option<String>,
    /// Allow plain `http://` endpoints. Defaults to true when the endpoint is `http://`.
    pub allow_http: Option<bool>,
    /// Force virtual-hosted-style requests (`bucket.host`). Defaults to false (path style)
    /// when an endpoint is set, and to the AWS default otherwise.
    pub virtual_hosted_style: Option<bool>,
}

impl StoreOptions {
    /// Options for a custom endpoint, with the defaults local S3 servers need.
    #[must_use]
    pub fn with_endpoint(endpoint: impl Into<String>) -> Self {
        Self {
            s3_endpoint: Some(endpoint.into()),
            ..Self::default()
        }
    }

    pub(crate) fn allow_http_resolved(&self) -> bool {
        self.allow_http.unwrap_or_else(|| {
            self.s3_endpoint
                .as_deref()
                .is_some_and(|e| e.starts_with("http://"))
        })
    }
}
