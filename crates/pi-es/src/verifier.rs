//! The acceptance check: the published alias against `expected.json` (datagen) or against what
//! a run published (its `summary.json`), and an optional content digest over every document.

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
    /// `actual` only has to reach `expected` (a query that must find something), instead of
    /// equalling it.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub at_least: bool,
}

impl Check {
    #[must_use]
    pub fn passed(&self) -> bool {
        if self.at_least {
            self.actual >= self.expected
        } else {
            self.expected == self.actual
        }
    }
}

/// Where the alias points, against the index a run published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AliasCheck {
    /// The index the alias should point at, alone.
    pub expected: String,
    /// The indexes it points at.
    pub actual: Vec<String>,
}

impl AliasCheck {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.actual == [self.expected.as_str()]
    }
}

/// What a run published, as its `summary.json` records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    /// The run's index.
    pub index: String,
    /// The documents `publish` counted in it.
    pub docs: u64,
}

/// Every check of one verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifyReport {
    pub target: String,
    /// Where the alias points (summary mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<AliasCheck>,
    pub checks: Vec<Check>,
}

impl VerifyReport {
    /// True when every check matched.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.alias.as_ref().is_none_or(AliasCheck::passed) && self.checks.iter().all(Check::passed)
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
                    at_least: false,
                })
            })
            .buffered(self.concurrency)
            .try_collect()
            .await?;
        Ok(VerifyReport {
            target: target.to_owned(),
            alias: None,
            checks,
        })
    }

    /// Checks `alias` against what a run published, with nothing from datagen:
    ///
    /// - `alias`: it points at the run's index and nothing else, both before and after the
    ///   queries below (which read the index itself, so a publish in between cannot make them
    ///   answer for another run);
    /// - `count`: the documents equal what `publish` counted;
    /// - `sample:ids`: one document, read back by its `_id`, is found exactly once (ids are
    ///   unique and retrievable);
    /// - `sample:full_name`, when the sample has a first or last name: a match on those names
    ///   finds at least one document (full-text search works);
    /// - `sample:organization_id`, when the sample carries a joined organization with an id
    ///   the mapping can index: a term query on it finds at least one document.
    ///
    /// The sample prefers a document with both a last name and an organization, so the
    /// conditional checks run whenever the index has such a document; neither is required,
    /// since a real feed may lack names or resolve no organization. A non-empty index that
    /// returns no sample fails `sample:ids`; an empty one (`docs == 0`) gets only `alias` and
    /// `count`. A check that cannot run is an error; one that runs and differs
    /// is a failed entry in the report.
    pub async fn verify_published(
        &self,
        alias: &str,
        published: &Published,
    ) -> Result<VerifyReport> {
        validate_index_name(alias)?;
        let index = published.index.as_str();
        validate_index_name(index)?;
        let before = self.admin.alias_targets(alias).await?;
        let mut checks = vec![Check {
            name: "count".to_owned(),
            expected: published.docs,
            actual: self.admin.count(index, None).await?,
            at_least: false,
        }];
        if published.docs > 0 {
            // An index publish counted as non-empty that returns no document is a failure.
            let Some(sample) = self.sample(index).await? else {
                checks.push(Check {
                    name: "sample:ids".to_owned(),
                    expected: 1,
                    actual: 0,
                    at_least: false,
                });
                return self.finish(alias, index, before, checks).await;
            };
            let by_id = json!({"ids": {"values": [sample.id]}});
            checks.push(Check {
                name: "sample:ids".to_owned(),
                expected: 1,
                actual: self.admin.count(index, Some(&by_id)).await?,
                at_least: false,
            });
            // Names that analyse to no terms at all (punctuation) match everything rather than
            // nothing: the check is that real names find their document.
            if !sample.names.is_empty() {
                let by_name = json!({"match": {"full_name": {
                    "query": sample.names, "operator": "and", "zero_terms_query": "all",
                }}});
                checks.push(
                    self.at_least_one(index, "sample:full_name", &by_name)
                        .await?,
                );
            }
            if let Some(organization) = sample.organization_id {
                let by_organization = json!({"term": {"organizations.id": organization}});
                checks.push(
                    self.at_least_one(index, "sample:organization_id", &by_organization)
                        .await?,
                );
            }
        }
        self.finish(alias, index, before, checks).await
    }

    /// Reads the alias again and completes the report of `verify_published`.
    async fn finish(
        &self,
        alias: &str,
        index: &str,
        before: Vec<String>,
        checks: Vec<Check>,
    ) -> Result<VerifyReport> {
        let after = self.admin.alias_targets(alias).await?;
        let expected = [index];
        // Report the reading that fails, so a move in between shows where the alias went.
        let actual = if before == expected { after } else { before };
        Ok(VerifyReport {
            target: alias.to_owned(),
            alias: Some(AliasCheck {
                expected: index.to_owned(),
                actual,
            }),
            checks,
        })
    }

    async fn at_least_one(&self, index: &str, name: &str, query: &Value) -> Result<Check> {
        Ok(Check {
            name: name.to_owned(),
            expected: 1,
            actual: self.admin.count(index, Some(query)).await?,
            at_least: true,
        })
    }

    /// One document of `index`, preferring one with a last name and a joined organization;
    /// `None` when the index is empty.
    async fn sample(&self, index: &str) -> Result<Option<Sample>> {
        let path = format!(
            "/{index}/_search?filter_path=timed_out,_shards,hits.hits._id,hits.hits._source"
        );
        // Every document matches (match_all: a bool of only should clauses would need one of
        // them); each should clause it meets adds to its score, so the best-scored one has
        // both when any document does.
        let body = json!({
            "size": 1,
            "query": {"bool": {
                "must": {"match_all": {}},
                "should": [
                    {"exists": {"field": "last_name"}},
                    {"exists": {"field": "organizations.id"}},
                ],
                "minimum_should_match": 0,
            }},
            "_source": ["first_name", "last_name", "organizations.id"],
            "track_total_hits": false,
        });
        let response = self
            .admin
            .client()
            .json(Method::POST, &path, Some(&body))
            .await?;
        require_complete(&path, &response)?;
        let Some(hit) = response["hits"]["hits"].get(0) else {
            return Ok(None);
        };
        let id = hit["_id"].as_str().ok_or_else(|| EsError::Decode {
            path: path.clone(),
            message: "a hit has no _id".into(),
        })?;
        let source = &hit["_source"];
        let names: Vec<&str> = ["first_name", "last_name"]
            .iter()
            .filter_map(|field| name_text(&source[field]))
            .collect();
        let organization_id = match &source["organizations"] {
            Value::Array(organizations) => organizations
                .iter()
                .find_map(|organization| indexable_id(&organization["id"])),
            organization => indexable_id(&organization["id"]),
        };
        Ok(Some(Sample {
            id: id.to_owned(),
            names: names.join(" "),
            organization_id,
        }))
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

/// The document `verify_published` samples.
struct Sample {
    id: String,
    /// First and last name, joined by a space; empty when neither is text.
    names: String,
    /// The first joined organization id the mapping can index.
    organization_id: Option<i64>,
}

/// An id as the `long` mapping indexes it: a number or a string of digits up to `i64::MAX`.
/// Larger ids are dropped by `ignore_malformed`, so a term query could not find them.
fn indexable_id(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64().filter(|id| *id >= 0),
        Value::String(text) if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) => {
            text.parse().ok()
        }
        _ => None,
    }
}

/// A name field as text: a string, or the first string of an array (`_source` keeps what the
/// feed sent); `None` for anything else or blank.
fn name_text(value: &Value) -> Option<&str> {
    let text = match value {
        Value::String(text) => text.as_str(),
        Value::Array(items) => items.iter().find_map(Value::as_str)?,
        _ => return None,
    };
    let text = text.trim();
    (!text.is_empty()).then_some(text)
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
