//! Error markers the executors branch on.
//!
//! Stages return `anyhow::Error`; these types ride inside it so an executor can tell a task
//! worth retrying from one that never will succeed, and a cancellation from a failure.

use crate::stage_name::StageName;

/// Wraps an error that retrying cannot fix (a malformed plan, a mapping conflict, too many
/// malformed lines). The executor fails the stage at once instead of retrying.
#[derive(Debug, thiserror::Error)]
#[error("{0:#}")]
pub struct PermanentError(pub anyhow::Error);

/// Marks an error as permanent: `return Err(permanent(anyhow!("...")))`.
pub fn permanent(err: impl Into<anyhow::Error>) -> anyhow::Error {
    anyhow::Error::new(PermanentError(err.into()))
}

/// True when the error (or anything it wraps) was marked [`permanent`].
#[must_use]
pub fn is_permanent(err: &anyhow::Error) -> bool {
    err.downcast_ref::<PermanentError>().is_some()
        || err
            .chain()
            .any(<dyn std::error::Error>::is::<PermanentError>)
}

/// The work was cancelled (shutdown, a failed sibling, a lost lease); not a failure of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cancelled")]
pub struct Cancelled;

/// True when the error (or anything it wraps) is a [`Cancelled`].
#[must_use]
pub fn is_cancelled(err: &anyhow::Error) -> bool {
    err.downcast_ref::<Cancelled>().is_some()
        || err.chain().any(<dyn std::error::Error>::is::<Cancelled>)
}

/// Another participant failed the run (its failure marker exists).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("run {run_id} failed{}: {reason}", stage.map(|s| format!(" in stage {s}")).unwrap_or_default())]
pub struct RunFailed {
    /// The run.
    pub run_id: String,
    /// The stage that failed, when known.
    pub stage: Option<StageName>,
    /// The recorded error.
    pub reason: String,
}

/// The coordinator gave up waiting for a stage's commits.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("stage {stage} timed out after {waited:?} with {missing} of {total} tasks uncommitted")]
pub struct StageTimeout {
    /// The stage.
    pub stage: StageName,
    /// How long the coordinator waited.
    pub waited: std::time::Duration,
    /// Tasks without a commit.
    pub missing: usize,
    /// Tasks in the stage.
    pub total: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_survive_context() {
        let e = permanent(anyhow::anyhow!("bad plan")).context("task file-00001");
        assert!(is_permanent(&e));
        assert!(!is_cancelled(&e));
        let e = anyhow::Error::new(Cancelled).context("join part-00001");
        assert!(is_cancelled(&e));
        assert!(!is_permanent(&anyhow::anyhow!("transient")));
        let f = RunFailed {
            run_id: "r".into(),
            stage: Some(StageName::Join),
            reason: "boom".into(),
        };
        assert_eq!(f.to_string(), "run r failed in stage join: boom");
    }
}
