//! `Verifier` against a mock Elasticsearch.

mod common;

use common::{client, shards};
use pi_es::{DocumentDigest, EsError, Expected, IndexAdmin, Verifier};
use serde_json::{Value, json};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn expected() -> Expected {
    Expected::from_json(
        br#"{
            "persons": 100,
            "organizations": 20,
            "malformed": {"persons": 2, "organizations": 1},
            "persons_with_unresolved": 7,
            "role_title_counts": {"Engineer": 40, "Lighthouse Keeper": 1},
            "organization_name_counts": {"Acme": 12}
        }"#,
    )
    .expect("expected")
}

async fn mount_count(server: &MockServer, query: Option<Value>, count: u64) {
    let mut mock = Mock::given(method("POST")).and(path("/persons/_count"));
    if let Some(query) = query {
        mock = mock.and(body_json(json!({ "query": query })));
    }
    mock.respond_with(
        ResponseTemplate::new(200).set_body_json(json!({"count": count, "_shards": shards(4)})),
    )
    .mount(server)
    .await;
}

/// Answers every count query the verifier sends, with `title_engineer` for the Engineer check.
async fn mount_all(server: &MockServer, title_engineer: u64) {
    mount_count(
        server,
        Some(json!({"exists": {"field": "unresolved_organization_ids"}})),
        7,
    )
    .await;
    mount_count(
        server,
        Some(json!({"term": {"roles.title.keyword": "Engineer"}})),
        title_engineer,
    )
    .await;
    mount_count(
        server,
        Some(json!({"term": {"roles.title.keyword": "Lighthouse Keeper"}})),
        1,
    )
    .await;
    mount_count(
        server,
        Some(json!({"term": {"organizations.name.keyword": "Acme"}})),
        12,
    )
    .await;
    // The match-all count has no body; mounted last so the query mocks win.
    Mock::given(method("POST"))
        .and(path("/persons/_count"))
        .and(EmptyBody)
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"count": 100, "_shards": shards(4)})),
        )
        .mount(server)
        .await;
}

struct EmptyBody;

impl wiremock::Match for EmptyBody {
    fn matches(&self, request: &Request) -> bool {
        request.body.is_empty()
    }
}

#[tokio::test]
async fn verifier_passes_when_every_count_matches() {
    let server = MockServer::start().await;
    mount_all(&server, 40).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify("persons", &expected())
        .await
        .expect("verified");
    assert!(report.passed(), "{report:?}");
    assert_eq!(report.checks.len(), 5);
    assert_eq!(report.failures(), [] as [&pi_es::Check; 0]);
}

#[tokio::test]
async fn verifier_reports_a_differing_count() {
    let server = MockServer::start().await;
    mount_all(&server, 39).await;
    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .verify("persons", &expected())
        .await
        .expect("verified");
    assert!(!report.passed());
    let failures = report.failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].name, "role_title:Engineer");
    assert_eq!(failures[0].expected, 40);
    assert_eq!(failures[0].actual, 39);
}

#[tokio::test]
async fn verifier_errors_when_a_count_is_incomplete() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/persons/_count"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "count": 100, "_shards": {"total": 4, "successful": 2, "failed": 2}
        })))
        .mount(&server)
        .await;
    let outcome = Verifier::new(IndexAdmin::new(client(&server)))
        .verify("persons", &expected())
        .await;
    assert!(
        matches!(outcome, Err(EsError::IncompleteShards { .. })),
        "{outcome:?}"
    );
}

/// Serves `docs` two per page through a point in time, keyed on `search_after`.
struct PitPages {
    docs: Vec<(String, Value)>,
}

impl Respond for PitPages {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).expect("search body");
        assert_eq!(body["pit"]["id"], "pit-1");
        assert_eq!(body["sort"], json!([{"_shard_doc": "asc"}]));
        let start = body["search_after"][0]
            .as_u64()
            .map_or(0, |after| usize::try_from(after).expect("small") + 1);
        let hits: Vec<Value> = self
            .docs
            .iter()
            .enumerate()
            .skip(start)
            .take(2)
            .map(|(n, (id, source))| json!({"_id": id, "_source": source, "sort": [n]}))
            .collect();
        let mut response = json!({"pit_id": "pit-1", "timed_out": false, "_shards": shards(2)});
        if !hits.is_empty() {
            response["hits"] = json!({ "hits": hits });
        }
        ResponseTemplate::new(200).set_body_json(response)
    }
}

#[tokio::test]
async fn digest_reads_every_document_and_closes_the_pit() {
    let server = MockServer::start().await;
    let docs: Vec<(String, Value)> = (1..=5)
        .map(|n| {
            (
                n.to_string(),
                json!({"id": n, "first_name": format!("p{n}")}),
            )
        })
        .collect();
    Mock::given(method("POST"))
        .and(path("/persons/_pit"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "pit-1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_search"))
        .respond_with(PitPages { docs: docs.clone() })
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_pit"))
        .and(body_json(json!({"id": "pit-1"})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"succeeded": true, "num_freed": 1})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let report = Verifier::new(IndexAdmin::new(client(&server)))
        .digest("persons", 2)
        .await
        .expect("digest");

    let mut local = DocumentDigest::new();
    for (id, source) in docs.iter().rev() {
        local.add(id, source);
    }
    assert_eq!(report, local.finish());
    assert_eq!(report.docs, 5);
}
