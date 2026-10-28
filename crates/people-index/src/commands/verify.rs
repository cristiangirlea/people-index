//! `people-index verify`: the acceptance check against the published alias, either against
//! datagen's `expected.json` or against a run's own `summary.json` (real feeds).

use std::path::Path;

use anyhow::Context as _;
use pi_es::{DigestReport, EsClient, Expected, IndexAdmin, Published, Verifier, VerifyReport};
use pi_stages::{Summary, index_name};
use pi_store::{RunLayout, Store};

use crate::cli::{VerifyArgs, VerifySource};
use crate::settings::{Stores, es_config, store_options};

/// What `verify` found.
#[derive(Debug)]
pub struct VerifyOutcome {
    /// The run whose summary was checked (`--source summary`).
    pub run_id: Option<String>,
    /// The checks.
    pub report: VerifyReport,
    /// The digest, when asked for.
    pub digest: Option<DigestReport>,
}

/// Reads `expected.json` from a local path, a store URL (`s3://bucket/key`, `file:///...`),
/// or by default from beside the inputs.
pub async fn load_expected(args: &VerifyArgs) -> anyhow::Result<Expected> {
    let options = store_options(&args.store);
    let bytes = match args.expected.as_deref() {
        Some(url) if url.contains("://") => {
            let (dir, name) = url
                .trim_end_matches('/')
                .rsplit_once('/')
                .ok_or_else(|| anyhow::anyhow!("{url} names no file"))?;
            let store = Store::open(&format!("{dir}/"), &options)
                .with_context(|| format!("opening {dir}"))?;
            store
                .get(name)
                .await
                .with_context(|| format!("reading {url}"))?
                .bytes
        }
        Some(path) => {
            return Expected::load(Path::new(path))
                .await
                .map_err(|e| anyhow::anyhow!("{path}: {e}"));
        }
        None => {
            let stores = Stores::open(&args.store)?;
            let key = stores
                .layout("verify")?
                .input_key(pi_datagen::EXPECTED_FILE);
            let store = stores.inputs.clone().unwrap_or_else(|| stores.runs.clone());
            store
                .get(&key)
                .await
                .with_context(|| format!("reading {key} from {}", store.url()))?
                .bytes
        }
    };
    Expected::from_json(&bytes).map_err(|e| anyhow::anyhow!("expected counts: {e}"))
}

/// The run to check: `--run-id`, or (when it is absent or empty, as compose passes an unset
/// variable) the run whose index the alias points at alone. Index names are lowercased run
/// ids (`20261015T120000Z-ab12` becomes `persons-20261015t120000z-ab12`), so the run is found
/// by listing the runs in the store, not by reading the id back from the index name.
async fn summary_run(
    args: &VerifyArgs,
    admin: &IndexAdmin,
    runs: &Store,
) -> anyhow::Result<String> {
    if let Some(run_id) = args.run_id.as_deref().filter(|id| !id.is_empty()) {
        return Ok(run_id.to_owned());
    }
    let targets = admin
        .alias_targets(&args.alias)
        .await
        .map_err(|e| anyhow::anyhow!("reading alias {}: {e}", args.alias))?;
    let index = match targets.as_slice() {
        [index] => index,
        [] => anyhow::bail!("alias {} does not exist; pass --run-id", args.alias),
        many => anyhow::bail!(
            "alias {} points at {} indexes ({}); pass --run-id",
            args.alias,
            many.len(),
            many.join(", ")
        ),
    };
    let dirs = runs
        .list_prefixes(RunLayout::runs_root())
        .await
        .with_context(|| format!("listing the runs in {}", runs.url()))?;
    let matching: Vec<&str> = dirs
        .iter()
        .filter_map(|dir| RunLayout::run_id_from_dir(dir))
        .filter(|run_id| index_name(&args.alias, run_id) == *index)
        .collect();
    match matching.as_slice() {
        [run_id] => Ok((*run_id).to_owned()),
        [] => anyhow::bail!(
            "alias {} points at {index}, which no run in {} names; pass --run-id",
            args.alias,
            runs.url()
        ),
        many => anyhow::bail!(
            "runs {} all name index {index}; pass --run-id",
            many.join(", ")
        ),
    }
}

/// Reads `runs/<run_id>/summary.json` from the run store; it exists only once the run published.
pub async fn load_summary(
    args: &VerifyArgs,
    runs: &Store,
    run_id: &str,
) -> anyhow::Result<Summary> {
    let layout = RunLayout::new(run_id).map_err(|e| anyhow::anyhow!("run id {run_id:?}: {e}"))?;
    let key = layout.summary();
    let bytes = runs
        .get(&key)
        .await
        .with_context(|| {
            format!(
                "reading {key} from {} (has run {run_id} published?)",
                runs.url()
            )
        })?
        .bytes;
    let summary: Summary =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {key}"))?;
    anyhow::ensure!(
        summary.alias == args.alias,
        "run {run_id} published alias {}, not {}",
        summary.alias,
        args.alias
    );
    Ok(summary)
}

/// Runs the checks (and the digest when asked). In summary mode the digest reads the run's
/// index itself, like the checks.
pub async fn run(args: VerifyArgs) -> anyhow::Result<VerifyOutcome> {
    let client = EsClient::new(&es_config(&args.es)?).map_err(|e| anyhow::anyhow!("{e}"))?;
    let admin = IndexAdmin::new(client);
    let verifier = Verifier::new(admin.clone());
    let (run_id, report, digest_target) = match args.source {
        VerifySource::Expected => {
            let expected = load_expected(&args).await?;
            let report = verifier
                .verify(&args.alias, &expected)
                .await
                .map_err(|e| anyhow::anyhow!("verifying {}: {e}", args.alias))?;
            (None, report, args.alias.clone())
        }
        VerifySource::Summary => {
            let stores = Stores::open(&args.store)?;
            let run_id = summary_run(&args, &admin, &stores.runs).await?;
            let summary = load_summary(&args, &stores.runs, &run_id).await?;
            let published = Published {
                builtin_names: summary.builtin_schema(),
                index: summary.index,
                docs: summary.publish.docs,
            };
            let report = verifier
                .verify_published(&args.alias, &published)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("verifying {} against run {run_id}: {e}", args.alias)
                })?;
            (Some(run_id), report, published.index)
        }
    };
    let digest = if args.digest {
        Some(
            verifier
                .digest(&digest_target, args.page_size)
                .await
                .map_err(|e| anyhow::anyhow!("digest of {digest_target}: {e}"))?,
        )
    } else {
        None
    };
    Ok(VerifyOutcome {
        run_id,
        report,
        digest,
    })
}
