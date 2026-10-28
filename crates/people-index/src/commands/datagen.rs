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

/// Where the output goes: `--out`, else the input prefix including the selected delivery.
pub fn destination(args: &DatagenArgs) -> anyhow::Result<(Store, String)> {
    let options = store_options(&args.store);
    if let Some(url) = args.out.as_deref() {
        let store = Store::open(url, &options).with_context(|| format!("opening {url}"))?;
        return Ok((store, String::new()));
    }
    let prefix = crate::settings::input_root(&args.store)?;
    let url = args
        .store
        .input_url
        .as_deref()
        .unwrap_or(&args.store.store_url);
    let store = Store::open(url, &options).with_context(|| format!("opening {url}"))?;
    Ok((store, prefix))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::Parser;

    #[test]
    fn delivery_destination_uses_only_the_selected_store() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("unopened-run-store");
        let run = missing.to_str().unwrap();
        for separate in [true, false] {
            let mut argv = vec![
                "people-index",
                "datagen",
                "--store-url",
                run,
                "--delivery",
                "a/b",
            ];
            if separate {
                argv.extend(["--input-url", "memory://"]);
            }
            let Command::Datagen(parsed) = Cli::try_parse_from(argv).unwrap().command else {
                panic!("datagen")
            };
            let (_, prefix) = destination(&parsed).unwrap();
            assert_eq!(prefix, if separate { "a/b" } else { "raw/a/b" });
            if separate {
                assert!(!missing.exists());
            }
        }
    }
    #[test]
    fn destinations_without_delivery_and_with_explicit_out() {
        for (args, expected) in [
            (
                vec!["people-index", "datagen", "--store-url", "memory://"],
                "raw",
            ),
            (
                vec![
                    "people-index",
                    "datagen",
                    "--store-url",
                    "memory://",
                    "--input-url",
                    "memory://",
                ],
                "",
            ),
            (
                vec![
                    "people-index",
                    "datagen",
                    "--store-url",
                    "memory://",
                    "--delivery",
                    "batch",
                    "--out",
                    "memory://",
                ],
                "",
            ),
        ] {
            let Command::Datagen(parsed) = Cli::try_parse_from(args).unwrap().command else {
                panic!("datagen")
            };
            let (store, prefix) = destination(&parsed).unwrap();
            assert_eq!(prefix, expected);
            assert_eq!(store.url(), "memory://");
        }
    }
}
