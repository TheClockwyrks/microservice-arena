//! Shutting the arena down on `SIGTERM`.
//!
//! The arena follows the control plane's "Graceful termination"
//! (<https://docs.testcabinet.ai/deployment/kubernetes/control-plane/#graceful-termination>)
//! and its own page's "Shutdown"
//! (<https://docs.testcabinet.ai/components/arena/overview/#shutdown>):
//!
//! 1. [`Shutdown::begin`] fails `/readyz` at once (`/healthz` stays `200`), ends
//!    the live tail of every tournament progress stream so its client reconnects
//!    to a ready arena, answers a new match or tournament with `503`, and tells
//!    every tournament to stop once the match it is on has finished and been
//!    recorded. A resumed tournament still waiting for its permit stops waiting.
//! 2. The server keeps accepting for a further [`DRAIN_DELAY`], so a request routed
//!    by an endpoint list not yet updated still lands.
//! 3. It stops accepting, finishes every request in flight and waits for every
//!    tournament it is driving ([`Drives`]) to stop.
//! 4. It exits with status 0, at the latest the shutdown grace
//!    (`TCAB_ARENA_SHUTDOWN_GRACE_SECONDS`) after the signal, abandoning a match
//!    still running then; the next arena plays it again.
//!
//! An interrupt (`Ctrl-C`) skips the drain delay. The binary's `main` sequences
//! the steps; this module holds the shared signal, the drive count, the admission
//! middleware and the drain delay.

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

/// The arena's shutdown signal: a latch shared by every clone, set once and never
/// cleared.
#[derive(Debug, Clone)]
pub struct Shutdown {
    draining: Arc<watch::Sender<bool>>,
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
            draining: Arc::new(watch::Sender::new(false)),
        }
    }

    /// Begin shutting down: fail readiness, refuse new work, end every live stream
    /// and stop every tournament after its match in progress. Every waiter on
    /// [`Self::draining`] wakes. Idempotent.
    pub fn begin(&self) {
        self.draining.send_replace(true);
    }

    /// Whether shutdown has begun.
    pub fn is_draining(&self) -> bool {
        *self.draining.borrow()
    }

    /// A future that resolves once shutdown has begun, at once when it already
    /// has. It owns what it waits on, so a long-lived stream can end on it.
    pub fn draining(&self) -> impl Future<Output = ()> + Send + use<> {
        let mut receiver = self.draining.subscribe();
        async move {
            // The sender lives as long as any clone, so the wait never closes early.
            let _ = receiver.wait_for(|draining| *draining).await;
        }
    }
}

/// Middleware in front of the routes that start CPU-bound work (a match, a
/// tournament): once shutdown has begun, answer `503` without reading the
/// request's body. It runs when a request's head arrives, so a request that
/// arrived before the signal is let through and finishes.
pub async fn refuse_new_work(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if state.shutdown.is_draining() {
        return ApiError::stopping().into_response();
    }
    next.run(request).await
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
