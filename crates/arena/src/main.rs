//! The `tcab-arena` binary entrypoint.
//!
//! Resolves configuration from the environment, builds the service, resumes the
//! unfinished tournaments the backend records, and serves the Axum router until
//! it is told to stop. Every route but the two probes needs an identity: the
//! Access assertion every request through the edge carries, verified against
//! `TCAB_ACCESS_TEAM_DOMAIN` and `TCAB_ACCESS_AUDIENCE` (see
//! [`test_cabinet_arena::auth`]). A configuration naming only one of the two
//! `TCAB_ACCESS_*` variables is refused at startup.
//!
//! On `SIGTERM` it shuts down as [`test_cabinet_arena::shutdown`] describes and
//! exits 0 within `TCAB_ARENA_SHUTDOWN_GRACE_SECONDS`; an interrupt (`Ctrl-C`)
//! skips the drain delay.

use std::process::ExitCode;

use test_cabinet_arena::config::Config;
use test_cabinet_arena::shutdown::{DRAIN_DELAY, Stop, stop_signal};
use tokio::sync::oneshot;
use tokio::time::Instant;

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("could not start the async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(serve());
    // A match still running past the shutdown grace runs on a blocking thread,
    // which dropping the runtime would wait for; it is abandoned instead, and the
    // next arena plays it again.
    runtime.shutdown_background();
    code
}

/// Serve the arena until it is told to stop, then shut it down.
async fn serve() -> ExitCode {
    // First, so a stop requested while the arena starts is held for the drain
    // rather than killing the process.
    let stop_requested = match stop_signal() {
        Ok(stop_requested) => stop_requested,
        Err(err) => {
            eprintln!("could not listen for SIGTERM and the interrupt: {err}");
            return ExitCode::FAILURE;
        }
    };

    // Load `.env.arena` beside the project before anything reads the environment.
    // A missing file is fine (variables can be exported instead); `dotenvy` never
    // overrides already-set variables. This runs before telemetry init so any
    // `OTEL_*`/`TCAB_ENV` configured in the file is visible to it.
    let _ = dotenvy::from_filename(".env.arena");

    // Dropped when `serve` returns, which flushes the telemetry before the exit.
    let _telemetry = match test_cabinet_telemetry::init(test_cabinet_telemetry::Config::new(
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
        shutdown_grace_secs = config.shutdown_grace.as_secs(),
        verifies_access = config.access.is_some(),
        "arena service configuration resolved"
    );

    let service = test_cabinet_arena::build(config);
    let state = service.state.clone();
    let grace = service.shutdown_grace;

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

    // The moment the stop was requested, from which the shutdown grace runs.
    let (signalled_tx, signalled_rx) = oneshot::channel::<Instant>();
    let shutdown = {
        let state = state.clone();
        async move {
            let stop = stop_requested.await;
            let _ = signalled_tx.send(Instant::now());
            state.shutdown.begin();
            tracing::info!(
                drives = state.drives.active(),
                "stopping: readiness fails, and each tournament stops once the match \
                 it is on is recorded"
            );
            if stop == Stop::Terminate {
                tracing::info!(
                    delay_secs = DRAIN_DELAY.as_secs(),
                    "serving through the drain delay"
                );
                tokio::time::sleep(DRAIN_DELAY).await;
            } else {
                tracing::info!("interrupted: skipping the drain delay");
            }
            state.shutdown.refuse();
            tracing::info!("no longer accepting; finishing the work in flight");
        }
    };

    let drives = state.drives.clone();
    let stopped = async move {
        axum::serve(listener, service.router)
            .with_graceful_shutdown(shutdown)
            .await?;
        // Every request has finished; a tournament may still be recording the
        // match it is on.
        drives.idle().await;
        Ok::<(), std::io::Error>(())
    };
    let deadline = async move {
        match signalled_rx.await {
            Ok(signalled) => tokio::time::sleep_until(signalled + grace).await,
            // The server ended without a stop being requested: only its own result
            // decides.
            Err(_) => std::future::pending().await,
        }
    };

    tokio::select! {
        result = stopped => match result {
            Ok(()) => {
                tracing::info!("arena stopped");
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("server error: {err}");
                ExitCode::FAILURE
            }
        },
        () = deadline => {
            tracing::warn!(
                grace_secs = grace.as_secs(),
                drives = state.drives.active(),
                "the shutdown grace ended; the matches still running are abandoned unrecorded"
            );
            ExitCode::SUCCESS
        }
    }
}
