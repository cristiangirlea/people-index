//! Logs (tracing) and metrics (a Prometheus endpoint).

use std::net::SocketAddr;

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::EnvFilter;

use crate::cli::LogFormat;

/// Installs the global log subscriber, writing to stderr. `RUST_LOG` filters (default
/// `info`). Safe to call once per process; a second call is ignored.
pub fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()));
    // An error only means a subscriber is already installed (tests), which is fine.
    let _ = match format {
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .try_init(),
        LogFormat::Pretty => builder.compact().try_init(),
    };
}

/// Starts the Prometheus endpoint on `addr` (must run inside the tokio runtime). The
/// pi-coord executors and pi-stages record into it: tasks committed, retried and failed,
/// task durations, bytes and counters, documents indexed, malformed lines.
pub fn init_metrics(addr: Option<SocketAddr>) -> anyhow::Result<()> {
    let Some(addr) = addr else {
        return Ok(());
    };
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| anyhow::anyhow!("starting the metrics endpoint on {addr}: {e}"))?;
    tracing::info!(%addr, "metrics endpoint listening");
    Ok(())
}

/// Records what a finished stage took, for dashboards.
pub fn record_stage(report: &pi_coord::StageReport) {
    let stage = report.stage.as_str();
    metrics::gauge!("pi_stage_duration_seconds", "stage" => stage)
        .set(report.duration.as_secs_f64());
    #[allow(clippy::cast_precision_loss, reason = "task counts are small")]
    metrics::gauge!("pi_stage_tasks", "stage" => stage).set(report.tasks() as f64);
}
