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
    /// The index already exists and belongs to the same run.
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
    pub async fn create_index(
        &self,
        name: &str,
        spec: &IndexSpec,
        run_id: &str,
    ) -> Result<CreateOutcome> {
        validate_index_name(name)?;
        let path = format!("/{name}");
        let mut body = spec.create_body();
        let mappings = body["mappings"]
            .as_object_mut()
            .ok_or_else(|| EsError::Config("index mapping must be an object".into()))?;
        set_owner(mappings, run_id)?;
        let (status, response) = self
            .client
            .json_allowing(Method::PUT, &path, Some(&body), &[400])
            .await?;
        if status == 400 {
            if response["error"]["type"] == "resource_already_exists_exception" {
                let mapping_path = format!("/{name}/_mapping");
                let mapping = self.client.json(Method::GET, &mapping_path, None).await?;
                let owner = mapping[name]["mappings"]["_meta"]["people_index"]["run_id"].as_str();
                if owner != Some(run_id) {
                    return Err(EsError::IndexOwnership {
                        index: name.to_owned(),
                        run_id: run_id.to_owned(),
                        owner: owner.map(str::to_owned),
                    });
                }
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

    /// Checks the effective settings and mapping after creation or reuse.
    pub async fn check_index(&self, name: &str, spec: &IndexSpec, run_id: &str) -> Result<()> {
        validate_index_name(name)?;
        let settings_path = format!("/{name}/_settings");
        let settings = self.client.json(Method::GET, &settings_path, None).await?;
        let settings_object = settings
            .get(name)
            .and_then(|v| v.get("settings"))
            .and_then(Value::as_object)
            .ok_or_else(|| EsError::Decode {
                path: settings_path.clone(),
                message: "no settings object".into(),
            })?;
        let mut flat = Vec::new();
        flatten_settings(settings_object, "", &mut flat);
        if !settings_object.contains_key("index")
            && !settings_object.keys().any(|k| k.starts_with("index."))
        {
            return Err(EsError::Decode {
                path: settings_path,
                message: "no index settings object".into(),
            });
        }
        if settings_object.get("index").is_some_and(|v| !v.is_object()) {
            return Err(EsError::Decode {
                path: settings_path,
                message: "index settings is not an object".into(),
            });
        }
        let mut forbidden_keys = Vec::new();
        for (key, value) in flat {
            let forbidden = [
                "index.analysis.analyzer",
                "index.analysis.normalizer",
                "index.analysis.tokenizer",
                "index.analysis.char_filter",
                "index.analysis.filter",
                "index.similarity",
            ]
            .iter()
            .any(|prefix| key == *prefix || key.starts_with(&format!("{prefix}.")))
                || matches!(
                    key.as_str(),
                    "index.mapping.coerce"
                        | "index.mapping.ignore_malformed"
                        | "index.mapping.ignore_above"
                )
                || (matches!(
                    key.as_str(),
                    "index.default_pipeline" | "index.final_pipeline"
                ) && value.as_str() != Some("_none"))
                || (key == "index.mode" && value.as_str() != Some("standard"))
                || (key == "index.mapping.source.mode"
                    && !value
                        .as_str()
                        .is_some_and(|mode| mode.eq_ignore_ascii_case("stored")));
            if forbidden {
                forbidden_keys.push(key);
            }
        }
        if !forbidden_keys.is_empty() {
            return Err(EsError::Config(format!(
                "{} are not permitted on {name}; check the index or an index template matching {}-*",
                forbidden_keys.join(", "),
                alias_for(name, run_id)
            )));
        }
        let mapping_path = format!("/{name}/_mapping");
        let response = self.client.json(Method::GET, &mapping_path, None).await?;
        let actual = response
            .get(name)
            .and_then(|v| v.get("mappings"))
            .filter(|v| v.is_object())
            .ok_or_else(|| EsError::Decode {
                path: mapping_path,
                message: "no mappings object".into(),
            })?;
        let mut expected = spec.mapping.clone();
        let object = expected
            .as_object_mut()
            .ok_or_else(|| EsError::Config("index mapping must be an object".into()))?;
        set_owner(object, run_id)?;
        let mut actual = actual.clone();
        normalize_mapping(&mut expected);
        normalize_mapping(&mut actual);
        let difference = if actual.pointer("/_meta/people_index/run_id")
            == expected.pointer("/_meta/people_index/run_id")
        {
            mapping_difference(&actual, &expected, "", false)
        } else {
            Some("_meta.people_index.run_id: value differs".to_owned())
        };
        if let Some(difference) = difference {
            return Err(EsError::Config(format!(
                "effective mapping of {name} differs from {}: {difference}; check the index or an index template matching {}-*",
                spec.mapping_source,
                alias_for(name, run_id)
            )));
        }
        Ok(())
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

    /// The indices named `<prefix>*` that no alias points at, sorted (open and closed alike).
    /// One `GET /<prefix>*/_alias`, which lists every matching index with its aliases and needs
    /// only `view_index_metadata` on the pattern.
    pub async fn unaliased_indices(&self, prefix: &str) -> Result<Vec<String>> {
        validate_index_name(prefix)?;
        let path = format!("/{prefix}*/_alias");
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
        let mut names = Vec::new();
        for (name, entry) in indices {
            let aliases = entry["aliases"]
                .as_object()
                .ok_or_else(|| EsError::Decode {
                    path: path.clone(),
                    message: format!("no aliases object for {name}"),
                })?;
            if aliases.is_empty() {
                names.push(name.clone());
            }
        }
        names.sort();
        Ok(names)
    }

    /// Points `alias` at `index` and away from each of `previous` in one `_aliases` request:
    /// readers see the old index or the new one, never neither and never both.
    ///
    /// The removes carry `"must_exist": false`, so the request is safe to send twice: when the
    /// first copy was applied but its answer lost (a 502, a reset), the retried copy finds the
    /// alias already gone from the old indexes and still succeeds.
    pub async fn swap_alias(&self, alias: &str, index: &str, previous: &[String]) -> Result<()> {
        validate_index_name(alias)?;
        validate_index_name(index)?;
        let mut actions: Vec<Value> = Vec::with_capacity(previous.len() + 1);
        for old in previous.iter().filter(|old| old.as_str() != index) {
            validate_index_name(old)?;
            actions.push(json!({"remove": {"index": old, "alias": alias, "must_exist": false}}));
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

// Elasticsearch 9.5.5 renders dynamic booleans as strings, omits explicit object
// types when properties are present, and renders copy_to strings as arrays.
fn normalize_mapping(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(Value::Bool(dynamic)) = object.get("dynamic") {
        object.insert("dynamic".into(), Value::String(dynamic.to_string()));
    }
    if object.get("type").and_then(Value::as_str) == Some("object") {
        object.remove("type");
    }
    if let Some(Value::String(target)) = object.get("copy_to") {
        object.insert("copy_to".into(), json!([target]));
    }
    for key in ["properties", "fields"] {
        if let Some(children) = object.get_mut(key).and_then(Value::as_object_mut) {
            for child in children.values_mut() {
                normalize_mapping(child);
            }
        }
    }
}

fn set_owner(mapping: &mut serde_json::Map<String, Value>, run_id: &str) -> Result<()> {
    let meta = mapping.entry("_meta").or_insert_with(|| json!({}));
    let meta = meta
        .as_object_mut()
        .ok_or_else(|| EsError::Config("mapping _meta must be an object".into()))?;
    meta.insert("people_index".into(), json!({"run_id": run_id}));
    Ok(())
}

fn alias_for<'a>(name: &'a str, run_id: &str) -> &'a str {
    let suffix = format!("-{}", run_id.to_lowercase());
    name.strip_suffix(&suffix).unwrap_or(name)
}

fn flatten_settings<'a>(
    object: &'a serde_json::Map<String, Value>,
    prefix: &str,
    out: &mut Vec<(String, &'a Value)>,
) {
    for (key, value) in object {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if let Some(children) = value.as_object().filter(|o| !o.is_empty()) {
            flatten_settings(children, &path, out);
        } else {
            out.push((path, value));
        }
    }
}

fn mapping_difference(actual: &Value, expected: &Value, path: &str, names: bool) -> Option<String> {
    if let (Some(a), Some(e)) = (actual.as_object(), expected.as_object()) {
        for (key, value) in a {
            let child = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            let Some(wanted) = e.get(key) else {
                return Some(format!("{child}: present in the index, not in the mapping"));
            };
            if let Some(diff) = mapping_difference(
                value,
                wanted,
                &child,
                key == "properties" || key == "fields",
            ) {
                return Some(diff);
            }
        }
        for key in ["properties", "fields"] {
            if !a.contains_key(key)
                && let Some(children) = e.get(key).and_then(Value::as_object)
                && let Some(name) = children.keys().next()
            {
                let parent = if path.is_empty() {
                    key.to_owned()
                } else {
                    format!("{path}.{key}")
                };
                return Some(format!(
                    "{parent}.{name}: present in the mapping, not in the index"
                ));
            }
        }
        if names {
            for key in e.keys() {
                if !a.contains_key(key) {
                    return Some(format!(
                        "{path}.{key}: present in the mapping, not in the index"
                    ));
                }
            }
        }
        None
    } else if actual != expected {
        Some(format!("{path}: value differs"))
    } else {
        None
    }
}
