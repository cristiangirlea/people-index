//! Graceful shutdown: the first SIGTERM (or Ctrl-C) cancels the root token, so running tasks
//! stop without committing and the executors release their leases; a second one exits at once.

use pi_coord::CancellationToken;

/// Exit code after a shutdown signal (128 + SIGTERM's 15, what Kubernetes expects).
pub const EXIT_SIGNALLED: u8 = 143;

/// Cancels `token` on the first shutdown signal and exits the process on the second.
pub fn install(token: CancellationToken) {
    tokio::spawn(async move {
        signal().await;
        tracing::warn!("shutdown requested: abandoning running tasks without committing");
        token.cancel();
        signal().await;
        tracing::error!("second shutdown signal: exiting now");
        std::process::exit(i32::from(EXIT_SIGNALLED));
    });
}

/// Waits for SIGTERM or SIGINT (Ctrl-C).
async fn signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
                return;
            }
            Err(e) => tracing::warn!(error = %e, "cannot listen for SIGTERM; Ctrl-C only"),
        }
    }
    if tokio::signal::ctrl_c().await.is_err() {
        // No signal handling at all: never resolve rather than cancel at once.
        std::future::pending::<()>().await;
    }
}
