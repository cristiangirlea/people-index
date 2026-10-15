//! The acceptance check: the published alias against `expected.json`, and an optional content
//! digest over every document.

use futures::{StreamExt as _, TryStreamExt as _};
use reqwest::Method;
use serde::Serialize;
use serde_json::{Value, json};

use crate::admin::IndexAdmin;
use crate::digest::{DigestReport, DocumentDigest};
use crate::error::{EsError, Result};
use crate::expected::Expected;
use crate::names::validate_index_name;
use crate::shards::require_complete;

/// How long Elasticsearch keeps the point in time open between two pages.
const PIT_KEEP_ALIVE: &str = "5m";

/// One acceptance check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Check {
    /// What was checked, e.g. `count`, `role_title:Engineer`.
    pub name: String,
    pub expected: u64,
    pub actual: u64,
}

impl Check {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.expected == self.actual
    }
}

/// Every check of one verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifyReport {
    pub target: String,
    pub checks: Vec<Check>,
}

impl VerifyReport {
    /// True when every check matched.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.checks.iter().all(Check::passed)
    }

    /// The checks that did not match.
    #[must_use]
    pub fn failures(&self) -> Vec<&Check> {
        self.checks.iter().filter(|c| !c.passed()).collect()
    }
}

/// Runs the acceptance queries and the digest against a cluster.
#[derive(Debug, Clone)]
pub struct Verifier {
    admin: IndexAdmin,
    concurrency: usize,
}

impl Verifier {
    /// Four count queries in flight at once: quick, without crowding a serving cluster.
    #[must_use]
    pub fn new(admin: IndexAdmin) -> Self {
        Self {
            admin,
            concurrency: 4,
        }
    }

    /// Runs every check of `expected` against `target` (the alias). A check that cannot run is
    /// an error; a check that runs and differs is a failed entry in the report.
    pub async fn verify(&self, target: &str, expected: &Expected) -> Result<VerifyReport> {
        validate_index_name(target)?;
        let mut queries: Vec<(String, u64, Option<Value>)> = vec![
            ("count".to_owned(), expected.persons, None),
            (
                "persons_with_unresolved".to_owned(),
                expected.persons_with_unresolved,
                Some(json!({"exists": {"field": "unresolved_organization_ids"}})),
            ),
        ];
        for (title, count) in &expected.role_title_counts {
            queries.push((
                format!("role_title:{title}"),
                *count,
                Some(json!({"term": {"roles.title.keyword": title}})),
            ));
        }
        for (name, count) in &expected.organization_name_counts {
            queries.push((
                format!("organization_name:{name}"),
                *count,
                Some(json!({"term": {"organizations.name.keyword": name}})),
            ));
        }
        let checks = futures::stream::iter(queries)
            .map(|(name, expected, query)| async move {
                let actual = self.admin.count(target, query.as_ref()).await?;
                Ok::<_, EsError>(Check {
                    name,
                    expected,
                    actual,
                })
            })
            .buffered(self.concurrency)
            .try_collect()
            .await?;
        Ok(VerifyReport {
            target: target.to_owned(),
            checks,
        })
    }

    /// Reads every document of `target` through a point in time, `page_size` at a time, and
    /// folds `_id` and `_source` into an order-independent digest (see [`crate::digest`]).
    /// Pages are read in `_shard_doc` order, the cheapest stable order; the digest does not
    /// depend on it. Hashing runs on the blocking pool, off the reactor.
    pub async fn digest(&self, target: &str, page_size: u32) -> Result<DigestReport> {
        validate_index_name(target)?;
        let client = self.admin.client();
        let open_path = format!("/{target}/_pit?keep_alive={PIT_KEEP_ALIVE}");
        let opened = client.json(Method::POST, &open_path, None).await?;
        let mut pit = opened["id"]
            .as_str()
            .ok_or_else(|| EsError::Decode {
                path: open_path,
                message: "no point-in-time id".into(),
            })?
            .to_owned();
        let outcome = self.scan(&mut pit, page_size.max(1)).await;
        // Best effort: an unclosed point in time expires after the keep-alive anyway.
        if let Err(error) = client
            .json(Method::DELETE, "/_pit", Some(&json!({ "id": pit })))
            .await
        {
            tracing::warn!(%error, "could not close the point in time");
        }
        outcome
    }

    async fn scan(&self, pit: &mut String, page_size: u32) -> Result<DigestReport> {
        let client = self.admin.client();
        let path = "/_search?filter_path=pit_id,timed_out,_shards,hits.hits._id,hits.hits._source,hits.hits.sort";
        let mut digest = DocumentDigest::new();
        let mut search_after: Option<Value> = None;
        loop {
            let mut body = json!({
                "size": page_size,
                "pit": {"id": pit.as_str(), "keep_alive": PIT_KEEP_ALIVE},
                "sort": [{"_shard_doc": "asc"}],
                "track_total_hits": false,
            });
            if let Some(after) = &search_after {
                body["search_after"] = after.clone();
            }
            let mut response = client.json(Method::POST, path, Some(&body)).await?;
            require_complete(path, &response)?;
            if let Some(id) = response["pit_id"].as_str() {
                id.clone_into(pit);
            }
            let hits = match response["hits"]["hits"].take() {
                Value::Array(hits) => hits,
                // filter_path drops `hits` entirely when there are none.
                Value::Null => Vec::new(),
                _ => {
                    return Err(EsError::Decode {
                        path: path.to_owned(),
                        message: "hits is not an array".into(),
                    });
                }
            };
            let Some(last) = hits.last() else {
                break;
            };
            search_after = Some(last["sort"].clone());
            let page = tokio::task::spawn_blocking(move || fold_page(&hits))
                .await
                .map_err(|e| EsError::Task(format!("digest: {e}")))??;
            digest.merge(&page);
        }
        Ok(digest.finish())
    }
}

fn fold_page(hits: &[Value]) -> Result<DocumentDigest> {
    let mut page = DocumentDigest::new();
    for hit in hits {
        let id = hit["_id"].as_str().ok_or_else(|| EsError::Decode {
            path: "/_search".into(),
            message: "a hit has no _id".into(),
        })?;
        page.add(id, &hit["_source"]);
    }
    Ok(page)
}
