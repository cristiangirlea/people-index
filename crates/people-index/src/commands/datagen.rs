//! `people-index datagen`: both feeds and `expected.json`, written into a store.

use anyhow::Context as _;
use pi_datagen::{Expected, GenConfig};
use pi_store::Store;

use crate::cli::DatagenArgs;
use crate::settings::store_options;
use crate::store_sink::StoreSink;

/// The generator settings: the `--config` file (or the defaults), then the flags.
pub fn gen_config(args: &DatagenArgs) -> anyhow::Result<GenConfig> {
    let mut cfg = match &args.config {
        Some(path) => {
            let text =
                std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
            serde_json::from_slice(&text).with_context(|| format!("parsing {}", path.display()))?
        }
        None => GenConfig::default(),
    };
    if let Some(n) = args.persons {
        cfg.persons = n;
    }
    if let Some(n) = args.organizations {
        cfg.organizations = n;
    }
    if let Some(n) = args.files {
        cfg.files_per_feed = n;
    }
    if let Some(n) = args.seed {
        cfg.seed = n;
    }
    cfg.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(cfg)
}

/// Where the output goes: `--out`, else the input prefix, else `<store>/raw`.
pub fn destination(args: &DatagenArgs) -> anyhow::Result<(Store, String)> {
    let options = store_options(&args.store);
    if let Some(url) = args.out.as_deref().or(args.store.input_url.as_deref()) {
        let store = Store::open(url, &options).with_context(|| format!("opening {url}"))?;
        return Ok((store, String::new()));
    }
    let store = Store::open(&args.store.store_url, &options)
        .with_context(|| format!("opening {}", args.store.store_url))?;
    Ok((store, "raw".to_owned()))
}

/// Generates the feeds and returns the expected counts.
pub async fn run(args: DatagenArgs) -> anyhow::Result<Expected> {
    let cfg = gen_config(&args)?;
    let (store, prefix) = destination(&args)?;
    tracing::info!(
        persons = cfg.persons,
        organizations = cfg.organizations,
        files = cfg.files_per_feed,
        seed = cfg.seed,
        out = %format!("{}/{prefix}", store.url().trim_end_matches('/')),
        "generating"
    );
    let handle = tokio::runtime::Handle::current();
    let expected = tokio::task::spawn_blocking(move || {
        let mut sink = StoreSink::new(store, &prefix, handle);
        pi_datagen::generate(&cfg, &mut sink)
    })
    .await
    .context("the generator panicked")?
    .map_err(|e| anyhow::anyhow!("generating: {e}"))?;
    tracing::info!(
        persons = expected.persons,
        organizations = expected.organizations,
        "generated"
    );
    Ok(expected)
}
