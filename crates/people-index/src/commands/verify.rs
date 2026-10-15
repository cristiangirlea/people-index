//! `people-index verify`: the acceptance check against the published alias.

use std::path::Path;

use anyhow::Context as _;
use pi_es::{DigestReport, EsClient, Expected, IndexAdmin, Verifier, VerifyReport};
use pi_store::Store;

use crate::cli::VerifyArgs;
use crate::settings::{Stores, es_config, store_options};

/// What `verify` found.
#[derive(Debug)]
pub struct VerifyOutcome {
    /// The count checks.
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
            let (store, key) = match &stores.inputs {
                Some(inputs) => (inputs.clone(), pi_datagen::EXPECTED_FILE.to_owned()),
                None => (
                    stores.runs.clone(),
                    format!("raw/{}", pi_datagen::EXPECTED_FILE),
                ),
            };
            store
                .get(&key)
                .await
                .with_context(|| format!("reading {key} from {}", store.url()))?
                .bytes
        }
    };
    Expected::from_json(&bytes).map_err(|e| anyhow::anyhow!("expected counts: {e}"))
}

/// Runs the checks (and the digest when asked).
pub async fn run(args: VerifyArgs) -> anyhow::Result<VerifyOutcome> {
    let expected = load_expected(&args).await?;
    let client = EsClient::new(&es_config(&args.es)?).map_err(|e| anyhow::anyhow!("{e}"))?;
    let verifier = Verifier::new(IndexAdmin::new(client));
    let report = verifier
        .verify(&args.alias, &expected)
        .await
        .map_err(|e| anyhow::anyhow!("verifying {}: {e}", args.alias))?;
    let digest = if args.digest {
        Some(
            verifier
                .digest(&args.alias, args.page_size)
                .await
                .map_err(|e| anyhow::anyhow!("digest of {}: {e}", args.alias))?,
        )
    } else {
        None
    };
    Ok(VerifyOutcome { report, digest })
}
