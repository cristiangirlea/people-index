//! [`Coordination`]: commits, leases and the failure marker behind one seam.
//!
//! The executors only talk to this trait, so the protocol can move to another backend (a
//! database, etcd) without touching them. [`StoreCoordination`] implements it on the object
//! store, which is all the deployment needs.

use std::collections::BTreeSet;

use async_trait::async_trait;
use pi_store::CreateOutcome;
use tokio_util::sync::CancellationToken;

use crate::commit::{CommitLog, CommitOutcome, CommitRecord};
use crate::context::RunContext;
use crate::failure::RunFailure;
use crate::lease::{LeaseConfig, LeaseGuard, LeaseManager};
use crate::stage_name::StageName;

/// A claim on a task, held while it runs.
#[async_trait]
pub trait TaskLease: Send + Sync {
    /// Cancelled when the claim is lost; the task must stop.
    fn lost_token(&self) -> CancellationToken;
    /// True once the claim is lost.
    fn is_lost(&self) -> bool;
    /// Lets go of the claim so another worker could take the task at once.
    async fn release(self: Box<Self>) -> anyhow::Result<()>;
}

/// Everything the executors need to agree on who ran what.
#[async_trait]
pub trait Coordination: Send + Sync {
    /// The name this participant records in commits, leases and failures.
    fn worker_id(&self) -> &str;

    /// True when this backend can arbitrate leases for the run (the object store needs
    /// update-if-match, which a local directory does not have).
    fn supports_leases(&self, ctx: &RunContext) -> bool;

    /// Commits a task unless it already has a commit; the first commit wins.
    async fn commit(
        &self,
        ctx: &RunContext,
        record: &CommitRecord,
    ) -> anyhow::Result<CommitOutcome>;

    /// The commit of a task, if any.
    async fn committed(
        &self,
        ctx: &RunContext,
        stage: StageName,
        task_id: &str,
    ) -> anyhow::Result<Option<CommitRecord>>;

    /// The ids of a stage's committed tasks.
    async fn committed_task_ids(
        &self,
        ctx: &RunContext,
        stage: StageName,
    ) -> anyhow::Result<BTreeSet<String>>;

    /// Every commit of a stage, sorted by task id.
    async fn commits(
        &self,
        ctx: &RunContext,
        stage: StageName,
    ) -> anyhow::Result<Vec<CommitRecord>>;

    /// Tries to claim a task; `None` when another participant holds it.
    async fn try_claim(
        &self,
        ctx: &RunContext,
        stage: StageName,
        task_id: &str,
    ) -> anyhow::Result<Option<Box<dyn TaskLease>>>;

    /// Records that the run failed. Returns false when a failure was already recorded (the
    /// first one is kept).
    async fn mark_failed(&self, ctx: &RunContext, failure: &RunFailure) -> anyhow::Result<bool>;

    /// The recorded failure, if any.
    async fn failure(&self, ctx: &RunContext) -> anyhow::Result<Option<RunFailure>>;

    /// Removes the failure record, so a resumed run starts clean.
    async fn clear_failure(&self, ctx: &RunContext) -> anyhow::Result<()>;
}

/// [`Coordination`] on the run's object store: the layout in docs/ARCHITECTURE.md.
#[derive(Debug, Clone)]
pub struct StoreCoordination {
    worker: String,
    lease: LeaseConfig,
}

impl StoreCoordination {
    /// Coordination for participant `worker` with the given lease timing.
    #[must_use]
    pub fn new(worker: impl Into<String>, lease: LeaseConfig) -> Self {
        Self {
            worker: worker.into(),
            lease,
        }
    }

    fn commits_of(ctx: &RunContext) -> CommitLog {
        CommitLog::new(ctx.store.clone(), ctx.layout.clone())
    }

    /// The lease manager of a run, for callers that want the lease objects themselves.
    #[must_use]
    pub fn leases_of(&self, ctx: &RunContext) -> LeaseManager {
        LeaseManager::new(
            ctx.store.clone(),
            ctx.layout.clone(),
            self.worker.clone(),
            self.lease,
        )
    }
}

#[async_trait]
impl TaskLease for LeaseGuard {
    fn lost_token(&self) -> CancellationToken {
        LeaseGuard::lost_token(self)
    }

    fn is_lost(&self) -> bool {
        LeaseGuard::is_lost(self)
    }

    async fn release(self: Box<Self>) -> anyhow::Result<()> {
        LeaseGuard::release(*self).await.map_err(Into::into)
    }
}

#[async_trait]
impl Coordination for StoreCoordination {
    fn worker_id(&self) -> &str {
        &self.worker
    }

    fn supports_leases(&self, ctx: &RunContext) -> bool {
        ctx.store.supports_conditional_update()
    }

    async fn commit(
        &self,
        ctx: &RunContext,
        record: &CommitRecord,
    ) -> anyhow::Result<CommitOutcome> {
        Ok(Self::commits_of(ctx).commit(record).await?)
    }

    async fn committed(
        &self,
        ctx: &RunContext,
        stage: StageName,
        task_id: &str,
    ) -> anyhow::Result<Option<CommitRecord>> {
        Ok(Self::commits_of(ctx).get(stage, task_id).await?)
    }

    async fn committed_task_ids(
        &self,
        ctx: &RunContext,
        stage: StageName,
    ) -> anyhow::Result<BTreeSet<String>> {
        Ok(Self::commits_of(ctx).committed_task_ids(stage).await?)
    }

    async fn commits(
        &self,
        ctx: &RunContext,
        stage: StageName,
    ) -> anyhow::Result<Vec<CommitRecord>> {
        Ok(Self::commits_of(ctx).list(stage).await?)
    }

    async fn try_claim(
        &self,
        ctx: &RunContext,
        stage: StageName,
        task_id: &str,
    ) -> anyhow::Result<Option<Box<dyn TaskLease>>> {
        Ok(self
            .leases_of(ctx)
            .try_claim(stage, task_id)
            .await?
            .map(|g| Box::new(g) as Box<dyn TaskLease>))
    }

    async fn mark_failed(&self, ctx: &RunContext, failure: &RunFailure) -> anyhow::Result<bool> {
        let key = ctx.layout.failure_marker();
        Ok(matches!(
            ctx.store.put_json_create_if_absent(&key, failure).await?,
            CreateOutcome::Created(_)
        ))
    }

    async fn failure(&self, ctx: &RunContext) -> anyhow::Result<Option<RunFailure>> {
        Ok(ctx
            .store
            .get_json(&ctx.layout.failure_marker())
            .await?
            .map(|(f, _)| f))
    }

    async fn clear_failure(&self, ctx: &RunContext) -> anyhow::Result<()> {
        Ok(ctx.store.delete(&ctx.layout.failure_marker()).await?)
    }
}
