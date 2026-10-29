//! `people-index stage`: one task (or every task) of one stage, for debugging.
//!
//! It goes through the local executor, so it follows the commit protocol: a committed task is
//! skipped, and a successful one commits. The stage's task list file is rewritten with the
//! tasks run, so do not point it at a run that distributed workers are following.

use pi_coord::{CancellationToken, StageName, StageReport};

use crate::cli::StageArgs;
use crate::settings::{Stores, coordination, local_executor, stages_config};

/// Runs the selected tasks of the stage and returns their report.
pub async fn run(args: StageArgs, cancel: CancellationToken) -> anyhow::Result<StageReport> {
    let name: StageName = args.name.parse()?;
    let stores = Stores::open(&args.store)?;
    let ctx = stores.context(&args.run_id, cancel)?;
    let registry = pi_stages::build_registry(stages_config(
        &args.pipeline,
        &args.es,
        &args.exec,
        &stores,
    )?)?;
    let stage = registry
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("stage {name} is not registered"))?;
    let mut tasks = stage.tasks(&ctx).await?;
    if let Some(id) = &args.task {
        tasks.retain(|t| &t.task_id == id);
        if tasks.is_empty() {
            anyhow::bail!("stage {name} of run {} has no task {id}", args.run_id);
        }
    }
    let executor = local_executor(&args.exec, coordination(&args.exec));
    executor.run_tasks(stage, &ctx, tasks).await
}
