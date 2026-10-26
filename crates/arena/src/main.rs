//! The `tcab-arena` binary entrypoint.
//!
//! Resolves configuration from the environment, builds the service, resumes the
//! unfinished tournaments the backend records, and serves the Axum router until
//! `SIGTERM`. Then it answers new work with `503`, waits within its grace for the
//! match each tournament is on to finish and be recorded, and exits, abandoning a
//! match still running (see [`test_cabinet_arena::shutdown`]). Every route but the
//! health probes needs an
//! identity: the Access assertion every request through the edge carries, verified
//! against `TCAB_ACCESS_TEAM_DOMAIN` and `TCAB_ACCESS_AUDIENCE` (see
//! [`test_cabinet_arena::auth`]). A configuration naming only one of the two
//! `TCAB_ACCESS_*` variables is refused at startup.

use std::future::IntoFuture as _;
use std::process::ExitCode;

use test_cabinet_arena::config::Config;

#[tokio::main]
async fn main() -> ExitCode {
    // Load `.env.arena` beside the project before anything reads the environment.
    // A missing file is fine (variables can be exported instead); `dotenvy` never
    // overrides already-set variables. This runs before telemetry init so any
    // `OTEL_*`/`TCAB_ENV` configured in the file is visible to it.
    let _ = dotenvy::from_filename(".env.arena");

    let telemetry = match test_cabinet_telemetry::init(test_cabinet_telemetry::Config::new(
        "tcab-arena",
        env!("CARGO_PKG_VERSION"),
        "info,test_cabinet_arena=info",
    )) {
        Ok(guard) => guard,
        Err(err) => {
            eprintln!("telemetry init error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("configuration error: {err}");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(
        backend = %config.backend_url,
        max_concurrent = config.max_concurrent_matches,
        verifies_access = config.access.is_some(),
        "arena service configuration resolved"
    );

    let service = test_cabinet_arena::build(config);
    let state = service.state.clone();

    let listener = match tokio::net::TcpListener::bind(&service.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("could not bind {}: {err}", service.bind);
            return ExitCode::FAILURE;
        }
    };
    tracing::info!("arena service listening on {}", service.bind);

    // The unfinished tournaments resume in the background, each queueing for a
    // capacity permit, so the server answers while the backend is still being
    // listed.
    tokio::spawn(test_cabinet_arena::drive::resume_unfinished(state.clone()));

    // The listener stays open while the drives finish, so new work meets a `503`
    // rather than a refused connection; it closes once the last drive ends.
    let shutdown = state.shutdown.clone();
    let drives = state.drives.clone();
    let mut server = tokio::spawn(
        axum::serve(listener, service.router)
            .with_graceful_shutdown(async move {
                shutdown.draining().await;
                drives.idle().await;
            })
            .into_future(),
    );
    tokio::select! {
        () = terminated() => {}
        served = &mut server => {
            // The server ended with no signal, which only a failure does.
            match served {
                Ok(Ok(())) => eprintln!("server stopped"),
                Ok(Err(err)) => eprintln!("server error: {err}"),
                Err(err) => eprintln!("server task failed: {err}"),
            }
            return ExitCode::FAILURE;
        }
    }

    tracing::info!(
        drives = state.drives.active(),
        grace_seconds = service.shutdown_grace.as_secs(),
        "shutting down: finishing the match each tournament is on"
    );
    state.shutdown.begin();
    let drained = tokio::time::timeout(service.shutdown_grace, async {
        state.drives.idle().await;
        let _ = server.await;
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            drives = state.drives.active(),
            "the shutdown grace ended; the matches still running are abandoned unrecorded"
        );
    }
    // A match still running holds a blocking thread the runtime's drop would wait
    // for, so the process exits without dropping it, flushing telemetry first.
    drop(telemetry);
    std::process::exit(0);
}

/// Resolve on `SIGTERM` or Ctrl-C.
async fn terminated() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = sigterm.recv() => {}
                    _ = tokio::signal::ctrl_c() => {}
                }
            }
            Err(err) => {
                eprintln!("could not listen for SIGTERM: {err}");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
