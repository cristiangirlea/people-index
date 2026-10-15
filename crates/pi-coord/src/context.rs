//! [`RunContext`]: what every stage and executor call gets about the run.

use pi_store::{LayoutError, RunLayout, Store};
use tokio_util::sync::CancellationToken;

/// The run a call works on: its id, the store and the layout of keys in it.
///
/// Deliberately small: stage-specific settings (partition targets, Elasticsearch, bulk sizes)
/// live in the stage structs, so this type does not grow with every stage. Cheap to clone.
#[derive(Debug, Clone)]
pub struct RunContext {
    /// The run id.
    pub run_id: String,
    /// The object store holding the run (and, unless moved, the inputs).
    pub store: Store,
    /// Every key of the run.
    pub layout: RunLayout,
    /// Cancelled when the work this context was handed to must stop: process shutdown for
    /// the root context, a failed sibling or a lost lease for a task's child context. Stages
    /// that loop for long (or run blocking work) should check it; executors also drop the
    /// task's future when it fires.
    pub cancel: CancellationToken,
}

impl RunContext {
    /// A context for run `run_id` with the default layout (inputs under `raw/`).
    pub fn new(run_id: impl Into<String>, store: Store) -> Result<Self, LayoutError> {
        let run_id = run_id.into();
        let layout = RunLayout::new(run_id.clone())?;
        Ok(Self {
            run_id,
            store,
            layout,
            cancel: CancellationToken::new(),
        })
    }

    /// A context with an explicit layout (inputs moved with `RunLayout::with_input_root`).
    #[must_use]
    pub fn with_layout(store: Store, layout: RunLayout) -> Self {
        Self {
            run_id: layout.run_id().to_owned(),
            store,
            layout,
            cancel: CancellationToken::new(),
        }
    }

    /// Replaces the cancellation token (to tie the run to a shutdown signal).
    #[must_use]
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// A copy whose token is a child of this one: cancelling the child stops only the work it
    /// was handed, cancelling this context stops the child too.
    #[must_use]
    pub fn child(&self) -> Self {
        let mut ctx = self.clone();
        ctx.cancel = self.cancel.child_token();
        ctx
    }

    /// True once the context has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_tokens_cascade_one_way() {
        let ctx = RunContext::new("r1", Store::memory()).unwrap();
        let a = ctx.child();
        let b = ctx.child();
        a.cancel.cancel();
        assert!(a.is_cancelled());
        assert!(!b.is_cancelled() && !ctx.is_cancelled());
        ctx.cancel.cancel();
        assert!(b.is_cancelled());
        assert!(RunContext::new("bad/id", Store::memory()).is_err());
    }
}
