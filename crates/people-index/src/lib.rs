//! people-index: the command line (docs/ARCHITECTURE.md, "Commands").
//!
//! The binary is a thin `main`; everything is here so the integration tests can drive the
//! same code: argument parsing ([`cli`]), settings ([`settings`]), the choice of delivery
//! ([`delivery`]), the datagen store sink ([`store_sink`]) and the subcommands ([`commands`]).

pub mod byte_size;
pub mod cli;
pub mod commands;
pub mod delivery;
pub mod run_id;
pub mod settings;
pub mod shutdown;
pub mod store_sink;
pub mod telemetry;

use std::process::ExitCode;

use clap::Parser as _;
use pi_coord::{CancellationToken, WorkerExit, is_cancelled};
use serde_json::json;

use crate::cli::{Cli, Command};

/// Parses the command line, runs the subcommand and returns the process exit code: 0 on
/// success, 1 on failure (or failed checks, or a setting refused after parsing), 2 when the
/// command line does not parse (clap exits) or a scheduled worker fails, 143 after a shutdown
/// signal (`run`, `worker` and `stage`).
#[must_use]
pub fn run_cli() -> ExitCode {
    let mut argv: Vec<_> = std::env::args_os().collect();
    let scheduled_worker = match rewrite_scheduled(
        &mut argv,
        std::env::var("JOB_COMPLETION_INDEX").ok().as_deref(),
    ) {
        Ok(worker) => worker,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let cli = Cli::parse_from(argv);
    telemetry::init_logging(cli.log_format);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(error = %e, "cannot start the async runtime");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(dispatch(cli.command));
    scheduled_exit_code(scheduled_worker, code)
}

fn scheduled_exit_code(scheduled_worker: bool, code: ExitCode) -> ExitCode {
    if scheduled_worker && code == ExitCode::FAILURE {
        ExitCode::from(2)
    } else {
        code
    }
}

fn rewrite_scheduled(argv: &mut [std::ffi::OsString], index: Option<&str>) -> anyhow::Result<bool> {
    if argv.get(1).is_some_and(|arg| arg == "scheduled") {
        let index = index.and_then(|s| s.parse::<u32>().ok()).ok_or_else(|| {
            anyhow::anyhow!("JOB_COMPLETION_INDEX must be a non-negative integer")
        })?;
        argv[1] = if index == 0 {
            "run".into()
        } else {
            "worker".into()
        };
        return Ok(index != 0);
    }
    Ok(false)
}

async fn dispatch(command: Command) -> ExitCode {
    let cancel = CancellationToken::new();
    let result = match command {
        Command::Run(args) => {
            if let Err(e) = start_service(&args.metrics, &cancel) {
                return failure(&e);
            }
            commands::run::run(args, cancel.clone()).await.map(|end| {
                print_json(&run_json(&end));
                true
            })
        }
        Command::Worker(args) => {
            if let Err(e) = start_service(&args.metrics, &cancel) {
                return failure(&e);
            }
            commands::worker::run(args, cancel.clone())
                .await
                .map(|report| {
                    tracing::info!(exit = ?report.exit, committed = report.committed, duplicates = report.duplicates, lost = report.lost, "worker done");
                    // Published, failed or skipped all end a worker's job (the coordinator
                    // reports the run's fate); only a shutdown is not a clean exit.
                    !matches!(report.exit, WorkerExit::Cancelled)
                })
        }
        Command::Datagen(args) => commands::datagen::run(args).await.map(|expected| {
            print_json(&serde_json::to_value(&expected).unwrap_or_default());
            true
        }),
        Command::Verify(args) => commands::verify::run(args).await.map(|outcome| {
            let passed = outcome.report.passed();
            let mut printed = json!({
                "passed": passed,
                "report": outcome.report,
                "digest": outcome.digest,
            });
            if let Some(run_id) = outcome.run_id {
                printed["run_id"] = json!(run_id);
            }
            print_json(&printed);
            if !passed {
                tracing::error!(
                    failures = outcome.report.failures().len(),
                    alias_passed = outcome
                        .report
                        .alias
                        .as_ref()
                        .is_none_or(pi_es::AliasCheck::passed),
                    "verification failed"
                );
            }
            passed
        }),
        Command::Stage(args) => {
            if let Err(e) = start_service(&args.metrics, &cancel) {
                return failure(&e);
            }
            commands::stage::run(args, cancel.clone())
                .await
                .map(|report| {
                    print_json(&json!({
                        "stage": report.stage,
                        "tasks": report.tasks(),
                        "ran": report.ran,
                        "skipped": report.skipped,
                        "counters": report.counters(),
                        "bytes_in": report.bytes_in(),
                        "bytes_out": report.bytes_out(),
                    }));
                    true
                })
        }
    };
    match result {
        Ok(true) if cancel.is_cancelled() => ExitCode::from(shutdown::EXIT_SIGNALLED),
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => {
            if cancel.is_cancelled() {
                ExitCode::from(shutdown::EXIT_SIGNALLED)
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) if is_cancelled(&e) || cancel.is_cancelled() => {
            tracing::warn!(error = %format!("{e:#}"), "stopped by a shutdown signal");
            ExitCode::from(shutdown::EXIT_SIGNALLED)
        }
        Err(e) => failure(&e),
    }
}

/// What `run` prints. A skipped run (nothing new to publish) is a success, so the scheduled
/// Job completes.
fn run_json(end: &commands::run::RunEnd) -> serde_json::Value {
    match end {
        commands::run::RunEnd::Published(outcome) => json!({
            "run_id": outcome.run_id,
            "status": format!("{:?}", outcome.status).to_lowercase(),
            "stages_run": outcome.stages.iter().map(|s| json!({
                "stage": s.stage,
                "tasks": s.tasks(),
                "ran": s.ran,
                "skipped": s.skipped,
                "duration_ms": u64::try_from(s.duration.as_millis()).unwrap_or(u64::MAX),
            })).collect::<Vec<_>>(),
            "stages_skipped": outcome.skipped,
        }),
        commands::run::RunEnd::Skipped(skipped) => json!({
            "run_id": skipped.run_id,
            "status": "skipped",
            "delivery": skipped.delivery,
            "serving_run": skipped.serving_run,
            "reason": skipped.reason,
        }),
    }
}

/// Signal handling and the metrics endpoint, for the long-running commands.
fn start_service(metrics: &cli::MetricsArgs, cancel: &CancellationToken) -> anyhow::Result<()> {
    shutdown::install(cancel.clone());
    telemetry::init_metrics(metrics.addr()?)
}

fn failure(err: &anyhow::Error) -> ExitCode {
    tracing::error!(error = %format!("{err:#}"), "failed");
    ExitCode::FAILURE
}

fn print_json(value: &serde_json::Value) {
    // stdout carries the command's result; logs go to stderr.
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

#[cfg(test)]
mod scheduled_tests {
    use super::*;
    #[test]
    fn scheduled_exit_codes() {
        assert_eq!(
            scheduled_exit_code(true, ExitCode::FAILURE),
            ExitCode::from(2)
        );
        assert_eq!(
            scheduled_exit_code(false, ExitCode::FAILURE),
            ExitCode::FAILURE
        );
        assert_eq!(
            scheduled_exit_code(true, ExitCode::from(shutdown::EXIT_SIGNALLED)),
            ExitCode::from(143)
        );
    }
    #[test]
    fn rewrites_indexed_jobs() {
        for (index, expected, worker) in [
            ("0", "run", false),
            ("1", "worker", true),
            ("00", "run", false),
        ] {
            let mut args = vec!["pi".into(), "scheduled".into()];
            assert_eq!(rewrite_scheduled(&mut args, Some(index)).unwrap(), worker);
            assert_eq!(args[1], expected);
        }
        for index in [None, Some("bad")] {
            assert!(rewrite_scheduled(&mut ["pi".into(), "scheduled".into()], index).is_err());
        }
    }

    #[test]
    fn a_skipped_run_prints_its_status_delivery_and_serving_run() {
        let skipped = pi_coord::RunSkipped::new(
            "r1",
            "no new delivery: the alias already serves b1, from run r0",
            "b1",
            Some("r0".into()),
        );
        assert_eq!(
            run_json(&commands::run::RunEnd::Skipped(skipped)),
            json!({
                "run_id": "r1",
                "status": "skipped",
                "delivery": "b1",
                "serving_run": "r0",
                "reason": "no new delivery: the alias already serves b1, from run r0",
            })
        );
        let published = pi_coord::RunOutcome {
            run_id: "r2".into(),
            status: pi_coord::RunStatus::Published,
            stages: Vec::new(),
            skipped: vec![pi_coord::StageName::Plan],
        };
        assert_eq!(
            run_json(&commands::run::RunEnd::Published(published)),
            json!({"run_id": "r2", "status": "published", "stages_run": [], "stages_skipped": ["plan"]})
        );
    }
}
