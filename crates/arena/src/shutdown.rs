//! Shutting the arena down on `SIGTERM`.
//!
//! The arena follows the control plane's "Graceful termination"
//! (<https://docs.testcabinet.ai/deployment/kubernetes/control-plane/#graceful-termination>)
//! and its own page's "Shutdown"
//! (<https://docs.testcabinet.ai/components/arena/overview/#shutdown>):
//!
//! 1. [`Shutdown::begin`] fails `/readyz` at once (`/healthz` stays `200`), ends
//!    the live tail of every tournament progress stream so its client reconnects
//!    to a ready arena, and tells every tournament to stop once the match it is on
//!    has finished and been recorded. A resumed tournament still waiting for its
//!    permit stops waiting.
//! 2. The server keeps accepting, and serving, for a further [`DRAIN_DELAY`], so a
//!    request routed by an endpoint list not yet updated still lands and runs. A
//!    tournament accepted then is recorded before its `202`, and left queued for
//!    the next arena to resume.
//! 3. [`Shutdown::refuse`] ends the delay: the server stops accepting connections,
//!    a new match or tournament still arriving on a connection already open is
//!    answered `503` `stopping`, every request in flight finishes, and every
//!    tournament the arena is driving ([`Drives`]) stops.
//! 4. It exits with status 0, at the latest the shutdown grace
//!    (`TCAB_ARENA_SHUTDOWN_GRACE_SECONDS`) after the signal, abandoning a match
//!    still running then; the next arena plays it again.
//!
//! An interrupt (`Ctrl-C`) skips the drain delay. The binary's `main` sequences
//! the steps; this module holds the shared signal, the drive count, the admission
//! middleware, the signal handlers and the drain delay.
//!
//! Whether a request is refused is decided by when it **arrived**: the outermost
//! layer, [`stamp_arrival`], records the phase at that moment, before the identity
//! check, so a request that arrived in time is served however long verifying its
//! assertion takes.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::watch;

use crate::api::AppState;
use crate::error::ApiError;

/// How long the arena keeps accepting after `SIGTERM`, answering readiness with
/// `503`, before it stops accepting connections.
pub const DRAIN_DELAY: Duration = Duration::from_secs(5);

/// How far the arena is in stopping. Each phase follows the one before and is
/// never left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// Serving: not told to stop.
    Serving,
    /// Told to stop: readiness fails, live streams end and the drives stop, while
    /// new work is still served through the drain delay.
    Draining,
    /// The drain delay has ended: new work is refused.
    Refusing,
}

/// The arena's shutdown signal: the one phase every clone shares, which only
/// moves forward.
#[derive(Debug, Clone)]
pub struct Shutdown {
    phase: Arc<watch::Sender<Phase>>,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    /// A signal that has not begun.
    pub fn new() -> Self {
        Self {
            phase: Arc::new(watch::Sender::new(Phase::Serving)),
        }
    }

    /// Move to `phase`, unless the arena is already there or past it.
    fn advance(&self, phase: Phase) {
        self.phase.send_if_modified(|current| {
            let moved = *current < phase;
            if moved {
                *current = phase;
            }
            moved
        });
    }

    /// Begin shutting down: fail readiness, end every live stream and stop every
    /// tournament after its match in progress. Every waiter on [`Self::draining`]
    /// wakes. New work is still served. Idempotent.
    pub fn begin(&self) {
        self.advance(Phase::Draining);
    }

    /// End the drain delay: refuse a new match or tournament from now on. Implies
    /// [`Self::begin`]. Idempotent.
    pub fn refuse(&self) {
        self.advance(Phase::Refusing);
    }

    /// Whether shutdown has begun.
    pub fn is_draining(&self) -> bool {
        *self.phase.borrow() >= Phase::Draining
    }

    /// Whether the drain delay has ended, so new work is refused.
    pub fn is_refusing(&self) -> bool {
        *self.phase.borrow() >= Phase::Refusing
    }

    /// A future that resolves once shutdown has begun, at once when it already
    /// has. It owns what it waits on, so a long-lived stream can end on it.
    pub fn draining(&self) -> impl Future<Output = ()> + Send + use<> {
        let mut receiver = self.phase.subscribe();
        async move {
            // The sender lives as long as any clone, so the wait never closes early.
            let _ = receiver.wait_for(|phase| *phase >= Phase::Draining).await;
        }
    }
}

/// What the arena was doing when a request arrived, recorded by [`stamp_arrival`].
#[derive(Debug, Clone, Copy)]
pub struct Arrival {
    /// Whether the drain delay had already ended.
    refusing: bool,
}

/// The outermost layer: record in the request's extensions whether new work was
/// already refused when it arrived, so the decision [`refuse_new_work`] makes does
/// not depend on how long the layers in between take.
pub async fn stamp_arrival(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    request.extensions_mut().insert(Arrival {
        refusing: state.shutdown.is_refusing(),
    });
    next.run(request).await
}

/// Middleware in front of the routes that start CPU-bound work (a match, a
/// tournament): answer `503` `stopping` to a request that arrived after the drain
/// delay ended, without reading its body. A request that arrived earlier is
/// served.
pub async fn refuse_new_work(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let refusing = request
        .extensions()
        .get::<Arrival>()
        .map_or_else(|| state.shutdown.is_refusing(), |arrival| arrival.refusing);
    if refusing {
        return ApiError::stopping().into_response();
    }
    next.run(request).await
}

/// What asked the arena to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// `SIGTERM`, which Kubernetes sends a pod it stops: drain in full.
    Terminate,
    /// An interrupt (`Ctrl-C`): skip the drain delay.
    Interrupt,
}

/// Install the handlers for `SIGTERM` and the interrupt, and answer a future that
/// resolves with whichever arrives first.
///
/// The handlers are installed by this call, not when the future is first polled, so
/// a signal delivered at any point afterwards is held for the future rather than
/// ending the process by the signal's default action. Call it before anything that
/// can take time, so a stop requested during startup is never lost.
pub fn stop_signal() -> std::io::Result<impl Future<Output = Stop> + Send + 'static> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! {
                _ = terminate.recv() => Stop::Terminate,
                _ = interrupt.recv() => Stop::Interrupt,
            }
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async move {
            // Only an interrupt reaches a process here. An error listening for it
            // leaves no way to be asked to stop, so wait forever then.
            match tokio::signal::ctrl_c().await {
                Ok(()) => Stop::Interrupt,
                Err(err) => {
                    tracing::warn!(error = %err, "could not listen for an interrupt");
                    std::future::pending::<Stop>().await
                }
            }
        })
    }
}

/// The tournaments the arena is driving, counted so a shutdown can wait for each
/// to stop. Cloning shares the count.
#[derive(Debug, Clone)]
pub struct Drives {
    active: Arc<watch::Sender<usize>>,
}

impl Default for Drives {
    fn default() -> Self {
        Self::new()
    }
}

impl Drives {
    /// No drive under way.
    pub fn new() -> Self {
        Self {
            active: Arc::new(watch::Sender::new(0)),
        }
    }

    /// Count one drive until the returned guard drops.
    pub fn enter(&self) -> DriveGuard {
        self.active.send_modify(|active| *active += 1);
        DriveGuard {
            active: self.active.clone(),
        }
    }

    /// How many drives are under way.
    pub fn active(&self) -> usize {
        *self.active.borrow()
    }

    /// Resolve once no drive is under way.
    pub async fn idle(&self) {
        let mut receiver = self.active.subscribe();
        let _ = receiver.wait_for(|active| *active == 0).await;
    }
}

/// One counted drive; dropping it ends the count.
#[derive(Debug)]
pub struct DriveGuard {
    active: Arc<watch::Sender<usize>>,
}

impl Drop for DriveGuard {
    fn drop(&mut self) {
        self.active.send_modify(|active| *active -= 1);
    }
}

#[cfg(test)]
#[path = "shutdown.test.rs"]
mod tests;
