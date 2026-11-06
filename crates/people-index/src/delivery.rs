//! `PI_DELIVERY=latest`: which delivery a new run plans, and whether it runs at all
//! (docs/ARCHITECTURE.md, "Feed delivery contract"; docs/OPERATIONS.md, "Scheduled runs").
//!
//! The coordinator decides once per run, under the alias run lease and before it writes any
//! run state: a run with a plan keeps the delivery the plan recorded; a new run takes the
//! newest delivery under the input root that has its `_COMPLETE` marker, by name in byte
//! order, unless the alias already serves that delivery, in which case it writes
//! `runs/<id>/skipped.json` instead of a plan and does nothing.

use std::future::Future;

use anyhow::Context as _;
use pi_coord::{RunSkipped, RunState, permanent};
use pi_es::IndexAdmin;
use pi_stages::Plan;
use pi_store::{RunLayout, Store};

use crate::settings::LATEST_DELIVERY;

/// The supplier's readiness marker in a delivery's root.
pub const COMPLETE_MARKER: &str = "_COMPLETE";

/// Where `inputs`' `root` is, for messages.
fn describe(inputs: &Store, root: &str) -> String {
    let url = inputs.url().trim_end_matches('/');
    if root.is_empty() {
        url.to_owned()
    } else {
        format!("{url}/{root}")
    }
}

fn child(root: &str, name: &str) -> String {
    if root.is_empty() {
        name.to_owned()
    } else {
        format!("{root}/{name}")
    }
}

/// The newest complete delivery directly under `root` of `inputs`: of the directories that
/// hold a `_COMPLETE` object, the greatest name in byte order (so delivery names must sort in
/// time order, `batch-20261102` or `20261102T0200Z`). The feed directories of the layout
/// without deliveries (`persons`, `organizations`) and names that are not a delivery segment
/// are passed over. A directory named `latest` fails for good, since `PI_DELIVERY=latest`
/// could never name it; so does finding no complete delivery.
pub async fn newest_complete(inputs: &Store, root: &str) -> anyhow::Result<String> {
    let place = describe(inputs, root);
    let listed = inputs
        .list_prefixes(root)
        .await
        .with_context(|| format!("listing the deliveries under {place}"))?;
    let mut names = Vec::new();
    for dir in &listed {
        let name = if root.is_empty() {
            Some(dir.as_str())
        } else {
            dir.strip_prefix(root).and_then(|d| d.strip_prefix('/'))
        };
        let Some(name) = name.map(|n| n.trim_end_matches('/')) else {
            continue;
        };
        if name == LATEST_DELIVERY {
            return Err(permanent(anyhow::anyhow!(
                "{}: a delivery cannot be named {LATEST_DELIVERY:?} while PI_DELIVERY=latest \
                 selects the newest one; rename it",
                child(&place, name)
            )));
        }
        if name.eq_ignore_ascii_case("persons") || name.eq_ignore_ascii_case("organizations") {
            continue;
        }
        if let Err(e) = pi_store::validate_id("delivery", name) {
            tracing::warn!(error = %e, "not a delivery name; passed over");
            continue;
        }
        names.push(name.to_owned());
    }
    names.sort_unstable();
    for name in names.iter().rev() {
        let marker = child(&child(root, name), COMPLETE_MARKER);
        if inputs
            .head(&marker)
            .await
            .with_context(|| format!("reading {}", child(&place, &child(name, COMPLETE_MARKER))))?
            .is_some()
        {
            return Ok(name.clone());
        }
        tracing::info!(delivery = %name, "delivery has no {COMPLETE_MARKER} yet; looking at older ones");
    }
    Err(permanent(anyhow::anyhow!(
        "no complete delivery under {place}: PI_DELIVERY=latest plans the newest directory there \
         that holds a {COMPLETE_MARKER} object, and none does"
    )))
}

/// What the alias serves now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Serving {
    /// No alias yet: the first publish.
    Nothing,
    /// The run whose index the alias points at, and the delivery its plan recorded (`None`:
    /// the inputs without a delivery).
    Run {
        /// The run.
        run_id: String,
        /// Its plan's delivery.
        delivery: Option<String>,
    },
    /// The alias serves something no plan in this store explains; why.
    Unknown(String),
}

/// What the alias serves: the index it points at, the run that index records as its owner
/// (`_meta.people_index.run_id`, written by `prepare-index`), and that run's plan in `runs`.
///
/// The alias, not the newest published run in the store, is the reference: it is what readers
/// get. A rollback or a restore re-points it at an older run's index and a manual alias change
/// can point it anywhere; then the newest published run names the wrong delivery. The owner
/// recorded in the index survives a snapshot restore, and the plan says which delivery that
/// run read.
pub async fn serving(admin: &IndexAdmin, runs: &Store, alias: &str) -> anyhow::Result<Serving> {
    let targets = admin
        .alias_targets(alias)
        .await
        .map_err(|e| anyhow::anyhow!("reading alias {alias}: {e}"))?;
    let index = match targets.as_slice() {
        [] => return Ok(Serving::Nothing),
        [index] => index,
        many => {
            return Ok(Serving::Unknown(format!(
                "alias {alias} points at {} indexes ({})",
                many.len(),
                many.join(", ")
            )));
        }
    };
    let Some(run_id) = admin
        .run_owner(index)
        .await
        .map_err(|e| anyhow::anyhow!("reading the owner of {index}: {e}"))?
    else {
        return Ok(Serving::Unknown(format!(
            "{index} records no run (_meta.people_index.run_id)"
        )));
    };
    let Ok(layout) = RunLayout::new(&run_id) else {
        return Ok(Serving::Unknown(format!(
            "{index} records the run {run_id:?}, which is not a run id"
        )));
    };
    let key = layout.plan();
    match runs
        .get_json::<Plan>(&key)
        .await
        .with_context(|| format!("reading {key}"))?
    {
        Some((plan, _)) => Ok(Serving::Run {
            run_id,
            delivery: plan.delivery,
        }),
        None => Ok(Serving::Unknown(format!(
            "{index} belongs to run {run_id}, which has no {key} in {}",
            runs.url()
        ))),
    }
}

/// Following the newest delivery: where deliveries are, and whether an unchanged one runs.
#[derive(Debug, Clone, Copy)]
pub struct Follow<'a> {
    /// The input store.
    pub inputs: &'a Store,
    /// The prefix deliveries are directories of.
    pub root: &'a str,
    /// `PI_REPUBLISH_UNCHANGED_DELIVERY`: plan the newest delivery even when the alias
    /// serves it.
    pub republish_unchanged: bool,
}

/// What the coordinator does with a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Choice {
    /// Run with the delivery `PI_DELIVERY` names (or none): the behaviour without `latest`.
    Configured,
    /// The run has a plan: resume it with the delivery the plan recorded.
    Recorded(Option<String>),
    /// Plan this delivery, the newest complete one.
    Newest(String),
    /// Do nothing: the run was skipped (now, or by an earlier coordinator of the same id).
    Skip(RunSkipped),
}

/// Decides what the coordinator of `layout`'s run does; call it holding the alias run lease.
/// `follow` is `None` unless `PI_DELIVERY=latest`. `serving` is awaited only when a new run
/// would plan a delivery the alias might already serve.
pub async fn choose(
    runs: &Store,
    layout: &RunLayout,
    follow: Option<Follow<'_>>,
    serving: impl Future<Output = anyhow::Result<Serving>>,
) -> anyhow::Result<Choice> {
    let plan_key = layout.plan();
    if let Some((plan, _)) = runs
        .get_json::<Plan>(&plan_key)
        .await
        .with_context(|| format!("reading {plan_key}"))?
    {
        // The plan is what every task reads; `latest` is never resolved again for this run.
        return Ok(match follow {
            Some(_) => Choice::Recorded(plan.delivery),
            None => Choice::Configured,
        });
    }
    if let Some(skipped) = RunSkipped::load(runs, layout)
        .await
        .with_context(|| format!("reading {}", layout.skipped()))?
    {
        return Ok(Choice::Skip(skipped));
    }
    let Some(follow) = follow else {
        return Ok(Choice::Configured);
    };
    let newest = newest_complete(follow.inputs, follow.root).await?;
    if follow.republish_unchanged {
        return Ok(Choice::Newest(newest));
    }
    if RunState::load(runs, layout)
        .await
        .with_context(|| format!("reading {}", layout.state()))?
        .is_some()
    {
        // An earlier coordinator of this run got as far as its state but not its plan: the
        // run is under way, and a skip now would leave that state behind.
        return Ok(Choice::Newest(newest));
    }
    // Not older: the same delivery, or one that sorts after the newest complete one (a hand run
    // of a named delivery without its marker); planning the newest would publish older data.
    let (reason, run_id) = match serving.await? {
        Serving::Run {
            run_id,
            delivery: Some(served),
        } if served >= newest => {
            let reason = if served == newest {
                format!("no new delivery: the alias already serves {newest}, from run {run_id}")
            } else {
                format!(
                    "the alias serves {served}, newer than the newest complete delivery \
                     {newest}, from run {run_id}"
                )
            };
            (reason, run_id)
        }
        Serving::Unknown(why) => {
            tracing::warn!(reason = %why, delivery = %newest, "cannot tell which delivery the alias serves; planning the newest complete one");
            return Ok(Choice::Newest(newest));
        }
        Serving::Nothing | Serving::Run { .. } => return Ok(Choice::Newest(newest)),
    };
    let skipped = RunSkipped::new(layout.run_id(), reason, newest, Some(run_id))
        .record(runs, layout)
        .await
        .with_context(|| format!("writing {}", layout.skipped()))?;
    Ok(Choice::Skip(skipped))
}

#[cfg(test)]
mod tests {
    use pi_store::ObjectVersion;
    use serde_json::json;
    use time::OffsetDateTime;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    async fn delivery(store: &Store, root: &str, name: &str, complete: bool) {
        let dir = child(root, name);
        store
            .put(&format!("{dir}/persons/a.ndjson.gz"), vec![1])
            .await
            .unwrap();
        if complete {
            store
                .put(&format!("{dir}/{COMPLETE_MARKER}"), Vec::new())
                .await
                .unwrap();
        }
    }

    fn plan(run_id: &str, delivery: Option<&str>) -> Plan {
        Plan {
            run_id: run_id.into(),
            delivery: delivery.map(str::to_owned),
            partitions: 16,
            alias: "persons".into(),
            index: pi_stages::index_name("persons", run_id),
            organizations: Vec::new(),
            persons: vec![pi_stages::InputFile {
                key: "raw/b1/persons/a.ndjson.gz".into(),
                size: 1,
                version: ObjectVersion {
                    e_tag: Some("\"e\"".into()),
                    version: None,
                },
            }],
            total_bytes: 1,
            schema: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn follow(store: &Store, republish_unchanged: bool) -> Follow<'_> {
        Follow {
            inputs: store,
            root: "raw",
            republish_unchanged,
        }
    }

    /// For the cases that must not read the alias: awaiting it fails the choice.
    fn not_read() -> std::future::Ready<anyhow::Result<Serving>> {
        std::future::ready(Err(anyhow::anyhow!("the alias was read")))
    }

    fn serves(run_id: &str, delivery: &str) -> Serving {
        Serving::Run {
            run_id: run_id.into(),
            delivery: Some(delivery.into()),
        }
    }

    #[tokio::test]
    async fn the_newest_complete_delivery_by_name() {
        let store = Store::memory();
        let err = newest_complete(&store, "raw").await.unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        assert!(
            format!("{err:#}").contains("no complete delivery under memory:/raw"),
            "{err:#}"
        );
        // The layout without deliveries is not a delivery, complete marker or not.
        store.put("raw/persons/a.ndjson.gz", vec![1]).await.unwrap();
        store.put("raw/_COMPLETE", Vec::new()).await.unwrap();
        assert!(newest_complete(&store, "raw").await.is_err());
        delivery(&store, "raw", "batch-20261031", true).await;
        delivery(&store, "raw", "batch-20261101", true).await;
        // Newer by name but still being written.
        delivery(&store, "raw", "batch-20261102", false).await;
        // Byte order: an upper-case name sorts before every lower-case one.
        delivery(&store, "raw", "Batch-20261231", true).await;
        // Not delivery names (PI_DELIVERY could not select them), however they sort.
        delivery(&store, "raw", "batch 20261231", true).await;
        delivery(&store, "raw", "batch+20261231", true).await;
        assert_eq!(
            newest_complete(&store, "raw").await.unwrap(),
            "batch-20261101"
        );
        store
            .put("raw/batch-20261102/_COMPLETE", Vec::new())
            .await
            .unwrap();
        assert_eq!(
            newest_complete(&store, "raw").await.unwrap(),
            "batch-20261102"
        );
        // A separate input store keeps deliveries at its top.
        let inputs = Store::memory();
        delivery(&inputs, "", "20261102T0200Z", true).await;
        delivery(&inputs, "", "20261101T0200Z", true).await;
        assert_eq!(
            newest_complete(&inputs, "").await.unwrap(),
            "20261102T0200Z"
        );
    }

    #[tokio::test]
    async fn a_delivery_named_latest_is_refused() {
        let store = Store::memory();
        delivery(&store, "raw", "batch-1", true).await;
        delivery(&store, "raw", LATEST_DELIVERY, false).await;
        let err = newest_complete(&store, "raw").await.unwrap_err();
        assert!(pi_coord::is_permanent(&err));
        let text = format!("{err:#}");
        assert!(text.contains("raw/latest"), "{text}");
        assert!(text.contains("rename it"), "{text}");
    }

    #[tokio::test]
    async fn without_latest_nothing_changes() {
        let store = Store::memory();
        let layout = RunLayout::new("r1").unwrap();
        assert_eq!(
            choose(&store, &layout, None, not_read()).await.unwrap(),
            Choice::Configured
        );
        assert_eq!(store.list_keys("").await.unwrap(), Vec::<String>::new());
        store
            .put_json(&layout.plan(), &plan("r1", Some("b0")))
            .await
            .unwrap();
        assert_eq!(
            choose(&store, &layout, None, not_read()).await.unwrap(),
            Choice::Configured
        );
    }

    #[tokio::test]
    async fn a_new_run_plans_the_newest_unless_the_alias_serves_it() {
        let store = Store::memory();
        delivery(&store, "raw", "b1", true).await;
        delivery(&store, "raw", "b2", true).await;
        let layout = RunLayout::new("r1").unwrap();
        for serving in [
            Ok(Serving::Nothing),
            Ok(serves("r0", "b1")),
            Ok(Serving::Run {
                run_id: "r0".into(),
                delivery: None,
            }),
            Ok(Serving::Unknown("two indexes".into())),
        ] {
            let choice = choose(&store, &layout, Some(follow(&store, false)), async move {
                serving
            })
            .await
            .unwrap();
            assert_eq!(choice, Choice::Newest("b2".into()));
        }
        assert_eq!(store.list_keys("runs").await.unwrap(), Vec::<String>::new());
        // The alias cannot be read: no decision, no run state.
        let err = choose(&store, &layout, Some(follow(&store, false)), async {
            Err(anyhow::anyhow!("connection refused"))
        })
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("connection refused"));
        assert_eq!(store.list_keys("runs").await.unwrap(), Vec::<String>::new());

        // The alias serves a delivery newer than every complete one (a hand run of a delivery
        // without its marker): planning b2 would publish older data over it, so the run skips.
        let Choice::Skip(skipped) = choose(&store, &layout, Some(follow(&store, false)), async {
            Ok(serves("r0", "b3"))
        })
        .await
        .unwrap() else {
            panic!("expected a skip")
        };
        assert_eq!(
            skipped.reason,
            "the alias serves b3, newer than the newest complete delivery b2, from run r0"
        );
        assert_eq!(skipped.delivery, "b2");
        assert_eq!(skipped.serving_run.as_deref(), Some("r0"));
        assert_eq!(
            store.list_keys("runs").await.unwrap(),
            vec![layout.skipped()]
        );
        // Asked to republish, the run plans the newest complete one all the same.
        let next = RunLayout::new("r2").unwrap();
        let choice = choose(&store, &next, Some(follow(&store, true)), not_read())
            .await
            .unwrap();
        assert_eq!(choice, Choice::Newest("b2".into()));
    }

    #[tokio::test]
    async fn an_unchanged_delivery_skips_the_run_and_the_skip_is_final() {
        let store = Store::memory();
        delivery(&store, "raw", "b1", true).await;
        let layout = RunLayout::new("r1").unwrap();
        let Choice::Skip(skipped) = choose(&store, &layout, Some(follow(&store, false)), async {
            Ok(serves("r0", "b1"))
        })
        .await
        .unwrap() else {
            panic!("expected a skip")
        };
        assert_eq!(skipped.delivery, "b1");
        assert_eq!(skipped.serving_run.as_deref(), Some("r0"));
        assert!(
            skipped.reason.contains("no new delivery"),
            "{}",
            skipped.reason
        );
        assert_eq!(
            store.list_keys("runs").await.unwrap(),
            vec![layout.skipped()],
            "no plan and no run state"
        );
        // A newer delivery does not undo it for this run id, with or without latest.
        delivery(&store, "raw", "b2", true).await;
        for follow in [
            Some(follow(&store, false)),
            Some(follow(&store, true)),
            None,
        ] {
            let choice = choose(&store, &layout, follow, async { Ok(Serving::Nothing) })
                .await
                .unwrap();
            assert_eq!(choice, Choice::Skip(skipped.clone()));
        }
        // The next run plans it.
        let next = RunLayout::new("r2").unwrap();
        let choice = choose(&store, &next, Some(follow(&store, false)), async {
            Ok(serves("r0", "b1"))
        })
        .await
        .unwrap();
        assert_eq!(choice, Choice::Newest("b2".into()));
    }

    #[tokio::test]
    async fn republishing_an_unchanged_delivery_does_not_ask_the_alias() {
        let store = Store::memory();
        delivery(&store, "raw", "b1", true).await;
        let layout = RunLayout::new("r1").unwrap();
        let choice = choose(&store, &layout, Some(follow(&store, true)), not_read())
            .await
            .unwrap();
        assert_eq!(choice, Choice::Newest("b1".into()));
        assert_eq!(RunSkipped::load(&store, &layout).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_planned_run_keeps_its_delivery_after_a_newer_one_arrives() {
        let store = Store::memory();
        delivery(&store, "raw", "b1", true).await;
        let layout = RunLayout::new("r1").unwrap();
        store
            .put_json(&layout.plan(), &plan("r1", Some("b1")))
            .await
            .unwrap();
        delivery(&store, "raw", "b2", true).await;
        assert_eq!(
            choose(&store, &layout, Some(follow(&store, false)), not_read())
                .await
                .unwrap(),
            Choice::Recorded(Some("b1".into()))
        );
        // Not even when every delivery is gone.
        store.delete("raw/b1/_COMPLETE").await.unwrap();
        store.delete("raw/b2/_COMPLETE").await.unwrap();
        assert_eq!(
            choose(&store, &layout, Some(follow(&store, false)), not_read())
                .await
                .unwrap(),
            Choice::Recorded(Some("b1".into()))
        );
    }

    #[tokio::test]
    async fn a_started_run_without_a_plan_is_not_skipped() {
        let store = Store::memory();
        delivery(&store, "raw", "b1", true).await;
        let layout = RunLayout::new("r1").unwrap();
        RunState::new("r1").save(&store, &layout).await.unwrap();
        let choice = choose(&store, &layout, Some(follow(&store, false)), async {
            Ok(serves("r0", "b1"))
        })
        .await
        .unwrap();
        assert_eq!(choice, Choice::Newest("b1".into()));
        assert_eq!(RunSkipped::load(&store, &layout).await.unwrap(), None);
    }

    fn admin(server: &MockServer) -> IndexAdmin {
        IndexAdmin::new(pi_es::EsClient::new(&pi_es::EsConfig::new(server.uri())).unwrap())
    }

    async fn alias(server: &MockServer, indexes: &[&str]) {
        let body: serde_json::Map<String, serde_json::Value> = indexes
            .iter()
            .map(|i| ((*i).to_owned(), json!({"aliases": {"persons": {}}})))
            .collect();
        Mock::given(method("GET"))
            .and(path("/_alias/persons"))
            .respond_with(if indexes.is_empty() {
                ResponseTemplate::new(404).set_body_json(json!({"status": 404}))
            } else {
                ResponseTemplate::new(200).set_body_json(body)
            })
            .mount(server)
            .await;
    }

    async fn owner(server: &MockServer, index: &str, run_id: Option<&str>) {
        let meta = run_id.map_or(json!({}), |id| json!({"people_index": {"run_id": id}}));
        Mock::given(method("GET"))
            .and(path(format!("/{index}/_mapping")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({index: {"mappings": {"_meta": meta}}})),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn the_serving_delivery_comes_from_the_alias_owner_and_its_plan() {
        let runs = Store::memory();
        runs.put_json(
            &RunLayout::new("20261101T020000Z-aa").unwrap().plan(),
            &plan("20261101T020000Z-aa", Some("b1")),
        )
        .await
        .unwrap();

        let server = MockServer::start().await;
        alias(&server, &[]).await;
        assert_eq!(
            serving(&admin(&server), &runs, "persons").await.unwrap(),
            Serving::Nothing
        );

        let server = MockServer::start().await;
        alias(&server, &["persons-20261101t020000z-aa"]).await;
        owner(
            &server,
            "persons-20261101t020000z-aa",
            Some("20261101T020000Z-aa"),
        )
        .await;
        assert_eq!(
            serving(&admin(&server), &runs, "persons").await.unwrap(),
            Serving::Run {
                run_id: "20261101T020000Z-aa".into(),
                delivery: Some("b1".into())
            }
        );

        for (indexes, owned_by, why) in [
            (&["persons-a", "persons-b"][..], None, "points at 2 indexes"),
            (&["persons-a"][..], None, "records no run"),
            (&["persons-a"][..], Some("../x"), "not a run id"),
            (
                &["persons-a"][..],
                Some("gone"),
                "has no runs/gone/plan.json",
            ),
        ] {
            let server = MockServer::start().await;
            alias(&server, indexes).await;
            owner(&server, "persons-a", owned_by).await;
            let Serving::Unknown(text) = serving(&admin(&server), &runs, "persons").await.unwrap()
            else {
                panic!("expected unknown for {indexes:?}")
            };
            assert!(text.contains(why), "{text}");
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_alias/persons"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let err = serving(&admin(&server), &runs, "persons")
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("reading alias persons"),
            "{err:#}"
        );

        // A serving run whose plan cannot be read fails the decision rather than guessing.
        runs.put("runs/broken/plan.json", b"{not json".to_vec())
            .await
            .unwrap();
        let server = MockServer::start().await;
        alias(&server, &["persons-broken"]).await;
        owner(&server, "persons-broken", Some("broken")).await;
        let err = serving(&admin(&server), &runs, "persons")
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("reading runs/broken/plan.json"),
            "{err:#}"
        );
    }
}
