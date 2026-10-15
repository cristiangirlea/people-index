//! The HTTP edge: one `reqwest` client (rustls, pooled connections) shared by every caller in
//! the process, with auth, an extra CA, timeouts, bounded response reads and request retries.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderValue};
use reqwest::{Certificate, Method, Url};
use serde_json::Value;

use crate::config::{Auth, EsConfig};
use crate::error::{EsError, Result, is_retryable_status, snippet};
use crate::retry::RetryPolicy;

/// A request body, typed by what it carries so the right headers go with it.
#[derive(Debug, Clone)]
pub enum Body {
    Empty,
    /// A JSON document (`application/json`).
    Json(Vec<u8>),
    /// A bulk body (`application/x-ndjson`), already gzipped when `gzipped` is set.
    Ndjson {
        bytes: Bytes,
        gzipped: bool,
    },
}

/// A complete HTTP answer: the status and the body, read up to the configured limit.
#[derive(Debug, Clone)]
pub struct RawResponse {
    pub status: u16,
    pub body: Bytes,
}

/// A cheap-to-clone Elasticsearch client. Clones share the connection pool.
#[derive(Clone)]
pub struct EsClient {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    base: String,
    auth: Auth,
    api_key_header: Option<HeaderValue>,
    gzip_requests: bool,
    response_limit: usize,
    retry: RetryPolicy,
}

impl std::fmt::Debug for EsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EsClient")
            .field("base", &self.inner.base)
            .field("auth", &self.inner.auth)
            .field("gzip_requests", &self.inner.gzip_requests)
            .finish_non_exhaustive()
    }
}

impl EsClient {
    /// Builds the client. Reads the CA file once, here, at startup (a small blocking read
    /// before any request is in flight).
    pub fn new(config: &EsConfig) -> Result<Self> {
        let base = validate_base_url(&config.url)?;
        let mut builder = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .pool_idle_timeout(Duration::from_secs(60))
            .tcp_nodelay(true)
            .user_agent(concat!("people-index/", env!("CARGO_PKG_VERSION")));
        if let Some(path) = &config.ca_cert {
            let pem = std::fs::read(path).map_err(|e| {
                EsError::Config(format!("cannot read CA file {}: {e}", path.display()))
            })?;
            let certs = Certificate::from_pem_bundle(&pem).map_err(|e| {
                EsError::Config(format!("CA file {} is not PEM: {e}", path.display()))
            })?;
            if certs.is_empty() {
                return Err(EsError::Config(format!(
                    "CA file {} holds no certificate",
                    path.display()
                )));
            }
            builder = builder.tls_certs_merge(certs);
        }
        let http = builder
            .build()
            .map_err(|e| EsError::Config(format!("cannot build the HTTP client: {e}")))?;
        let api_key_header = match &config.auth {
            Auth::ApiKey(key) => {
                let mut value = HeaderValue::from_str(&format!("ApiKey {}", key.expose()))
                    .map_err(|_| EsError::Config("the API key is not a valid header".into()))?;
                value.set_sensitive(true);
                Some(value)
            }
            _ => None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                http,
                base,
                auth: config.auth.clone(),
                api_key_header,
                gzip_requests: config.gzip_requests,
                response_limit: config.response_limit,
                retry: config.retry,
            }),
        })
    }

    /// The retry policy requests made through this client follow.
    #[must_use]
    pub fn retry_policy(&self) -> RetryPolicy {
        self.inner.retry
    }

    /// Whether bulk bodies are gzipped on the wire.
    #[must_use]
    pub fn gzip_requests(&self) -> bool {
        self.inner.gzip_requests
    }

    /// One attempt, whatever the status. `path` starts with `/` and carries any query string.
    pub async fn send(&self, method: Method, path: &str, body: Body) -> Result<RawResponse> {
        let url = format!("{}{}", self.inner.base, path);
        let mut request = self.inner.http.request(method.clone(), url);
        request = match &self.inner.auth {
            Auth::None => request,
            Auth::Basic { username, password } => {
                request.basic_auth(username, Some(password.expose()))
            }
            Auth::ApiKey(_) => match &self.inner.api_key_header {
                Some(value) => request.header(AUTHORIZATION, value.clone()),
                None => request,
            },
        };
        request = match body {
            Body::Empty => request,
            Body::Json(bytes) => request.header(CONTENT_TYPE, "application/json").body(bytes),
            Body::Ndjson { bytes, gzipped } => {
                let request = request.header(CONTENT_TYPE, "application/x-ndjson");
                let request = if gzipped {
                    request.header(CONTENT_ENCODING, "gzip")
                } else {
                    request
                };
                request.body(bytes)
            }
        };
        let transport = |e: reqwest::Error| EsError::Transport {
            method: method.to_string(),
            path: path.to_owned(),
            // Without the URL: it is ours and adds nothing, and the error stays short.
            message: e.without_url().to_string(),
        };
        let mut response = request.send().await.map_err(transport)?;
        let status = response.status().as_u16();
        let limit = self.inner.response_limit;
        let too_large = || EsError::ResponseTooLarge {
            path: path.to_owned(),
            limit,
        };
        if response
            .content_length()
            .is_some_and(|len| len > limit as u64)
        {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if body.len() + chunk.len() > limit {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(RawResponse {
            status,
            body: Bytes::from(body),
        })
    }

    /// [`Self::send`] with the client's retry policy for retryable statuses and transport
    /// errors. Returns the last answer, whatever its status.
    pub async fn send_with_retry(
        &self,
        method: Method,
        path: &str,
        body: Body,
    ) -> Result<RawResponse> {
        let policy = self.inner.retry;
        let mut attempt = 1;
        loop {
            let outcome = self.send(method.clone(), path, body.clone()).await;
            let retryable = match &outcome {
                Ok(response) => is_retryable_status(response.status),
                Err(error) => error.is_retryable(),
            };
            if !retryable || !policy.allows_retry_after(attempt) {
                return outcome;
            }
            let delay = policy.delay(attempt - 1);
            tracing::debug!(%method, path, attempt, ?delay, "retrying Elasticsearch request");
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    /// A JSON call that must succeed (2xx), with retries.
    pub async fn json(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value> {
        let (_, value) = self.json_allowing(method, path, body, &[]).await?;
        Ok(value)
    }

    /// A JSON call that succeeds on 2xx or on one of `allowed` (such as 404 for "no alias").
    /// Returns the status with the parsed body (`Value::Null` for an empty body).
    pub async fn json_allowing(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        allowed: &[u16],
    ) -> Result<(u16, Value)> {
        let request_body = match body {
            Some(value) => Body::Json(serde_json::to_vec(value).map_err(|e| EsError::Decode {
                path: path.to_owned(),
                message: e.to_string(),
            })?),
            None => Body::Empty,
        };
        let response = self
            .send_with_retry(method.clone(), path, request_body)
            .await?;
        if !(200..300).contains(&response.status) && !allowed.contains(&response.status) {
            return Err(EsError::Http {
                method: method.to_string(),
                path: path.to_owned(),
                status: response.status,
                body: snippet(&response.body),
            });
        }
        let value = if response.body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&response.body).map_err(|e| EsError::Decode {
                path: path.to_owned(),
                message: e.to_string(),
            })?
        };
        Ok((response.status, value))
    }
}

/// An `http(s)` origin with an optional path prefix, without credentials, query or fragment.
/// Returned without a trailing slash so request paths can be appended as they are.
fn validate_base_url(raw: &str) -> Result<String> {
    let url = Url::parse(raw).map_err(|e| EsError::Config(format!("PI_ES_URL {raw:?}: {e}")))?;
    let bad = |why: &str| Err(EsError::Config(format!("PI_ES_URL {why}")));
    if !matches!(url.scheme(), "http" | "https") {
        return bad("must be http or https");
    }
    if url.host_str().is_none_or(str::is_empty) {
        return bad("needs a host");
    }
    if !url.username().is_empty() || url.password().is_some() {
        return bad(
            "must not carry credentials; use PI_ES_USERNAME/PI_ES_PASSWORD or PI_ES_API_KEY",
        );
    }
    if url.query().is_some() || url.fragment().is_some() {
        return bad("must not have a query or fragment");
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_urls_are_checked() {
        assert_eq!(
            validate_base_url("http://localhost:9200/").ok().as_deref(),
            Some("http://localhost:9200")
        );
        assert_eq!(
            validate_base_url("https://es.example/prefix/")
                .ok()
                .as_deref(),
            Some("https://es.example/prefix")
        );
        for bad in [
            "ftp://x",
            "http://user:pw@x:9200",
            "http://x:9200/?a=b",
            "not a url",
        ] {
            assert!(validate_base_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn debug_hides_secrets() {
        let mut config = EsConfig::new("http://localhost:9200");
        config.auth = Auth::Basic {
            username: "elastic".into(),
            password: crate::Secret::new("hunter2"),
        };
        let client = EsClient::new(&config).expect("client");
        let shown = format!("{client:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
    }

    #[test]
    fn missing_or_empty_ca_files_are_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = EsConfig::new("https://localhost:9200");
        config.ca_cert = Some(dir.path().join("absent.pem"));
        assert!(matches!(EsClient::new(&config), Err(EsError::Config(_))));
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, b"no certificates here\n").expect("write");
        config.ca_cert = Some(empty);
        assert!(matches!(EsClient::new(&config), Err(EsError::Config(_))));
    }
}
