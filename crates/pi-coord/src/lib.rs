//! pi-coord: how a run's stages become committed tasks, on one node or many.
//!
//! - [`Stage`] lists a stage's tasks and runs one; [`Executor`] runs a whole stage.
//! - [`LocalExecutor`] runs tasks in this process; [`DistributedExecutor`] (coordinator) and
//!   [`Worker`] split them across nodes with leases.
//! - [`Coordination`] hides commits, leases and the failure marker behind one seam;
//!   [`StoreCoordination`] implements it on the object store with [`CommitLog`] and
//!   [`LeaseManager`].
//! - [`RunDriver`] runs the stages in order and records [`RunState`], so a run id that is run
//!   again resumes: completed stages and committed tasks are skipped.
//!
//! The protocol is in docs/ARCHITECTURE.md (Commits, Leases).

mod commit;
mod context;
mod coordination;
mod distributed;
mod driver;
mod error;
mod executor;
mod failure;
mod lease;
mod local;
mod registry;
mod retry;
mod run_state;
mod runner;
mod stage;
mod stage_name;
mod stage_spec;
mod task;
mod worker;

pub use commit::{CommitLog, CommitOutcome, CommitRecord, sum_counters};
pub use context::RunContext;
pub use coordination::{Coordination, StoreCoordination, TaskLease};
pub use distributed::DistributedExecutor;
pub use driver::{RunDriver, RunOutcome};
pub use error::{
    Cancelled, PermanentError, RunFailed, StageTimeout, is_cancelled, is_permanent, permanent,
};
pub use executor::{Executor, StageReport};
pub use failure::RunFailure;
pub use lease::{Lease, LeaseConfig, LeaseGuard, LeaseManager};
pub use local::LocalExecutor;
pub use registry::StageRegistry;
pub use retry::RetryPolicy;
pub use run_state::{RunState, RunStatus, latest_run_id};
pub use stage::Stage;
pub use stage_name::{StageName, UnknownStage};
pub use stage_spec::{RunBy, StageSpec};
pub use task::{AttemptId, TaskReport, TaskSpec};
pub use worker::{Worker, WorkerExit, WorkerReport};

/// Re-exported so stages and binaries need no direct `tokio-util` dependency.
pub use tokio_util::sync::CancellationToken;
