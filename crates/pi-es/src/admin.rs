//! Index lifecycle calls: everything `prepare-index`, `publish` and `verify` ask of the cluster
//! besides bulk writes and searches.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{Value, json};

use crate::client::EsClient;
use crate::error::{EsError, Result};
use crate::health::HealthStatus;
use crate::names::validate_index_name;
use crate::settings::IndexSpec;
use crate::shards::{ShardCounts, require_complete};

/// The longest single cluster-health wait sent to Elasticsearch: well under the request
/// timeout, so a long overall wait is many short requests rather than one that a proxy or the
/// client timeout cuts off.
const HEALTH_POLL: Duration = Duration::from_secs(30);

/// Node roles that receive a new index's shards: `data` (every tier) and `data_content` (the
/// tier new indices are allocated to by default). Hot/warm/cold/frozen-only nodes do not hold
/// a freshly created index, so they must not inflate the shard count.
const CONTENT_ROLES: [&str; 2] = ["data", "data_content"];

/// Whether `create_index` made the index or found it already there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    /// The index exists already: a retried or concurrent `prepare-index` of the same run. The
    /// run id makes the name unique to the run, so this is the same index, not a stranger's.
    AlreadyExists,
}

/// Index administration over an [`EsClient`].
#[derive(Debug, Clone)]
pub struct IndexAdmin {
    client: EsClient,
}

impl IndexAdmin {
    #[must_use]
    pub fn new(client: EsClient) -> Self {
        Self { client }
    }

    /// The client underneath, for callers that need a raw call.
    #[must_use]
    pub fn client(&self) -> &EsClient {
        &self.client
    }

    /// The number of nodes that hold content shards, for the default primary count.
    pub async fn data_node_count(&self) -> Result<u32> {
        let path = "/_nodes/settings?filter_path=_nodes,nodes.*.roles";
        let response = self.client.json(Method::GET, path, None).await?;
        if let Some(failed) = response["_nodes"]["failed"].as_u64().filter(|n| *n > 0) {
            tracing::warn!(failed, "some nodes did not answer the nodes info request");
        }
        let nodes = response["nodes"]
            .as_object()
            .ok_or_else(|| EsError::Decode {
                path: path.to_owned(),
                message: "no nodes object".into(),
            })?;
        let count = nodes
            .values()
            .filter(|node| {
                node["roles"].as_array().is_some_and(|roles| {
                    roles
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|role| CONTENT_ROLES.contains(&role))
                })
            })
            .count();
        u32::try_from(count).map_err(|_| EsError::Decode {
            path: path.to_owned(),
            message: format!("{count} data nodes"),
        })
    }

    /// Creates `name` with the mapping and the load-time settings of `spec`.
    pub async fn create_index(&self, name: &str, spec: &IndexSpec) -> Result<CreateOutcome> {
        validate_index_name(name)?;
        let path = format!("/{name}");
        let (status, response) = self
            .client
            .json_allowing(Method::PUT, &path, Some(&spec.create_body()), &[400])
            .await?;
        if status == 400 {
            if response["error"]["type"] == "resource_already_exists_exception" {
                return Ok(CreateOutcome::AlreadyExists);
            }
            return Err(EsError::Http {
                method: "PUT".into(),
                path,
                status,
                body: crate::error::snippet(response.to_string().as_bytes()),
            });
        }
        if response["acknowledged"] != true || response["shards_acknowledged"] != true {
            return Err(EsError::NotAcknowledged(format!("creating {name}")));
        }
        Ok(CreateOutcome::Created)
    }

    /// Whether `name` resolves to anything (an index, an alias or a data stream).
    pub async fn exists(&self, name: &str) -> Result<bool> {
        validate_index_name(name)?;
        let path = format!("/{name}");
        let response = self
            .client
            .send_with_retry(Method::HEAD, &path, crate::client::Body::Empty)
            .await?;
        match response.status {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(EsError::Http {
                method: "HEAD".into(),
                path,
                status,
                body: String::new(),
            }),
        }
    }

    /// Applies index settings (`PUT /<index>/_settings`).
    pub async fn update_settings(&self, index: &str, settings: &Value) -> Result<()> {
        validate_index_name(index)?;
        let path = format!("/{index}/_settings");
        let response = self.client.json(Method::PUT, &path, Some(settings)).await?;
        if response["acknowledged"] != true {
            return Err(EsError::NotAcknowledged(format!("settings of {index}")));
        }
        Ok(())
    }

    /// Makes everything indexed so far searchable; every shard copy must answer.
    pub async fn refresh(&self, index: &str) -> Result<ShardCounts> {
        validate_index_name(index)?;
        let path = format!("/{index}/_refresh");
        let response = self.client.json(Method::POST, &path, None).await?;
        require_complete(&path, &response)
    }

    /// Commits segments and trims the translog, so replicas recover by copying files rather
    /// than replaying operations; every shard copy must answer.
    pub async fn flush(&self, index: &str) -> Result<ShardCounts> {
        validate_index_name(index)?;
        let path = format!("/{index}/_flush?wait_if_ongoing=true");
        let response = self.client.json(Method::POST, &path, None).await?;
        require_complete(&path, &response)
    }

    /// Documents matching `query` (all when `None`) in `target`, only from a complete answer.
    pub async fn count(&self, target: &str, query: Option<&Value>) -> Result<u64> {
        validate_index_name(target)?;
        let path = format!("/{target}/_count");
        let body = query.map(|q| json!({ "query": q }));
        let response = self.client.json(Method::POST, &path, body.as_ref()).await?;
        require_complete(&path, &response)?;
        response["count"].as_u64().ok_or_else(|| EsError::Decode {
            path,
            message: "no count".into(),
        })
    }

    /// The indices `alias` points at, sorted; empty when the alias does not exist.
    pub async fn alias_targets(&self, alias: &str) -> Result<Vec<String>> {
        validate_index_name(alias)?;
        let path = format!("/_alias/{alias}");
        let (status, response) = self
            .client
            .json_allowing(Method::GET, &path, None, &[404])
            .await?;
        if status == 404 {
            return Ok(Vec::new());
        }
        let indices = response.as_object().ok_or_else(|| EsError::Decode {
            path: path.clone(),
            message: "alias response is not an object".into(),
        })?;
        let mut names: Vec<String> = indices.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    /// Points `alias` at `index` and away from each of `previous` in one `_aliases` request:
    /// readers see the old index or the new one, never neither and never both.
    pub async fn swap_alias(&self, alias: &str, index: &str, previous: &[String]) -> Result<()> {
        validate_index_name(alias)?;
        validate_index_name(index)?;
        let mut actions: Vec<Value> = Vec::with_capacity(previous.len() + 1);
        for old in previous.iter().filter(|old| old.as_str() != index) {
            validate_index_name(old)?;
            actions.push(json!({"remove": {"index": old, "alias": alias}}));
        }
        actions.push(json!({"add": {"index": index, "alias": alias}}));
        let response = self
            .client
            .json(
                Method::POST,
                "/_aliases",
                Some(&json!({ "actions": actions })),
            )
            .await?;
        if response["acknowledged"] != true {
            return Err(EsError::NotAcknowledged(format!("moving alias {alias}")));
        }
        Ok(())
    }

    /// Deletes one index; an index that is already gone counts as deleted.
    pub async fn delete_index(&self, index: &str) -> Result<()> {
        validate_index_name(index)?;
        let path = format!("/{index}");
        let (status, response) = self
            .client
            .json_allowing(Method::DELETE, &path, None, &[404])
            .await?;
        if status == 200 && response["acknowledged"] != true {
            return Err(EsError::NotAcknowledged(format!("deleting {index}")));
        }
        Ok(())
    }

    /// Waits until the cluster (or `target`, an index or alias) is at least `wanted`, polling
    /// in short server-side waits until `timeout`. Returns the status reached.
    pub async fn wait_for_health(
        &self,
        target: Option<&str>,
        wanted: HealthStatus,
        timeout: Duration,
    ) -> Result<HealthStatus> {
        let scope = match target {
            Some(name) => {
                validate_index_name(name)?;
                format!("/{name}")
            }
            None => String::new(),
        };
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let wait = left.min(HEALTH_POLL).max(Duration::from_secs(1));
            let path = format!(
                "/_cluster/health{scope}?wait_for_status={wanted}&timeout={}ms",
                wait.as_millis()
            );
            // 408 is how Elasticsearch says the wait ran out; the body still has the status.
            let (_, response) = self
                .client
                .json_allowing(Method::GET, &path, None, &[408])
                .await?;
            let status: HealthStatus = response["status"]
                .as_str()
                .ok_or_else(|| EsError::Decode {
                    path: path.clone(),
                    message: "no status".into(),
                })?
                .parse()?;
            if status >= wanted {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(EsError::HealthTimeout {
                    target: target.unwrap_or("cluster").to_owned(),
                    wanted: wanted.to_string(),
                    last: status.to_string(),
                });
            }
        }
    }
}
