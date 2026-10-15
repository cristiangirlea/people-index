//! [`build_registry`]: every stage, built from one [`StagesConfig`].

use std::sync::Arc;

use pi_coord::StageRegistry;
use pi_es::EsClient;

use crate::assemble::AssembleStage;
use crate::commits::CommitCache;
use crate::config::StagesConfig;
use crate::extract_orgs::ExtractOrgsStage;
use crate::extract_persons::ExtractPersonsStage;
use crate::join::JoinStage;
use crate::load::LoadStage;
use crate::metered::Metered;
use crate::plan::PlanStage;
use crate::prepare_index::PrepareIndexStage;
use crate::publish::PublishStage;

/// The registry of all eight stages, each wrapped in [`Metered`]. The Elasticsearch client is
/// built here (no request is sent), so a bad CA file or URL fails at startup.
pub fn build_registry(config: StagesConfig) -> anyhow::Result<StageRegistry> {
    config.validate()?;
    let client = EsClient::new(&config.es.client)
        .map_err(|e| anyhow::anyhow!("Elasticsearch client: {e}"))?;
    let config = Arc::new(config);
    let cache = Arc::new(CommitCache::new());
    Ok(StageRegistry::new()
        .with(Arc::new(Metered(PlanStage::new(Arc::clone(&config)))))
        .with(Arc::new(Metered(ExtractOrgsStage::new(Arc::clone(
            &config,
        )))))
        .with(Arc::new(Metered(ExtractPersonsStage::new(Arc::clone(
            &config,
        )))))
        .with(Arc::new(Metered(JoinStage::new(
            Arc::clone(&config),
            Arc::clone(&cache),
        ))))
        .with(Arc::new(Metered(AssembleStage::new(
            Arc::clone(&config),
            Arc::clone(&cache),
        ))))
        .with(Arc::new(Metered(PrepareIndexStage::new(
            Arc::clone(&config),
            client.clone(),
        ))))
        .with(Arc::new(Metered(LoadStage::new(
            Arc::clone(&config),
            Arc::clone(&cache),
            client.clone(),
        ))))
        .with(Arc::new(Metered(PublishStage::new(config, cache, client)))))
}

#[cfg(test)]
mod tests {
    use pi_coord::StageName;

    use super::*;

    #[test]
    fn every_stage_is_registered_in_order() {
        let registry = build_registry(StagesConfig::default()).unwrap();
        assert_eq!(registry.names(), StageName::ALL);
        let bad = StagesConfig {
            upload_concurrency: 0,
            ..StagesConfig::default()
        };
        assert!(build_registry(bad).is_err());
    }
}
