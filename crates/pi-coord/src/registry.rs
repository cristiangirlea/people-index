//! [`StageRegistry`]: the stages a binary knows, looked up by name.
//!
//! Workers find the current stage by name in the run state; the registry turns that name
//! back into the code that runs it.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::stage::Stage;
use crate::stage_name::StageName;

/// Stages by name.
#[derive(Clone, Default)]
pub struct StageRegistry {
    stages: BTreeMap<StageName, Arc<dyn Stage>>,
}

impl std::fmt::Debug for StageRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.stages.keys()).finish()
    }
}

impl StageRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a stage under its own name, returning the one it replaced.
    pub fn register(&mut self, stage: Arc<dyn Stage>) -> Option<Arc<dyn Stage>> {
        self.stages.insert(stage.name(), stage)
    }

    /// Builder form of [`StageRegistry::register`].
    #[must_use]
    pub fn with(mut self, stage: Arc<dyn Stage>) -> Self {
        self.register(stage);
        self
    }

    /// The stage of that name.
    #[must_use]
    pub fn get(&self, name: StageName) -> Option<Arc<dyn Stage>> {
        self.stages.get(&name).cloned()
    }

    /// Every registered stage, in run order.
    #[must_use]
    pub fn ordered(&self) -> Vec<Arc<dyn Stage>> {
        self.stages.values().cloned().collect()
    }

    /// The registered names, in run order.
    #[must_use]
    pub fn names(&self) -> Vec<StageName> {
        self.stages.keys().copied().collect()
    }

    /// True when no stage is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }
}
