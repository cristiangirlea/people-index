//! Connection settings for [`crate::EsClient`]. The binary fills these from `PI_ES_*`.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use crate::retry::RetryPolicy;

/// A string that never shows up in `Debug` output or logs.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The value itself, for building the request header and nothing else.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

/// How requests prove who they are.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Auth {
    /// No credentials (a local development cluster).
    #[default]
    None,
    /// `PI_ES_USERNAME` / `PI_ES_PASSWORD`.
    Basic { username: String, password: Secret },
    /// `PI_ES_API_KEY`: the base64 `id:key` string Elasticsearch hands out as `encoded`.
    ApiKey(Secret),
}

/// Everything [`crate::EsClient::new`] needs.
#[derive(Debug, Clone)]
pub struct EsConfig {
    /// `PI_ES_URL`: an `http(s)` origin, optionally with a path prefix (behind a proxy), never
    /// with credentials in it.
    pub url: String,
    pub auth: Auth,
    /// `PI_ES_CA_CERT`: a PEM file whose certificates are trusted on top of the system roots
    /// (an ECK or self-signed cluster CA).
    pub ca_cert: Option<PathBuf>,
    pub connect_timeout: Duration,
    /// The longest any single request may take, bulk included.
    pub request_timeout: Duration,
    /// Gzip bulk bodies on the wire (`Content-Encoding: gzip`): fewer network bytes for some
    /// CPU on our side. Off by default; worth it when the network, not the cluster, is the limit.
    pub gzip_requests: bool,
    /// The largest response body read into memory.
    pub response_limit: usize,
    /// Retries for whole requests (admin calls and bulk requests alike).
    pub retry: RetryPolicy,
}

impl EsConfig {
    /// Defaults for everything but the URL.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth: Auth::None,
            ca_cert: None,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            gzip_requests: false,
            response_limit: 64 << 20,
            retry: RetryPolicy::default(),
        }
    }
}
