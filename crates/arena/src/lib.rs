//! The Test Cabinet arena service: the execution host for adversarial **matches**
//! and **tournaments**.
//!
//! The adversarial test type pits controller WASM programs head-to-head. Running
//! those matches is **CPU-bound in-process wasm** (via the shared `foray-host`
//! sandbox the engine reuses) — fast, but heavy — so it lives in its own dedicated
//! service rather than the single-replica control-plane backend. This is the same
//! control-plane/data-plane split the whole service topology is built on: the
//! backend owns the data (controller inputs, published tournaments, stored
//! replays); the arena owns the *execution*.
//!
//! The service is **stateless and decoupled over HTTP**. It holds no database and no
//! disk: it fetches every controller input from the backend (resolve a version,
//! baseline `references/<id>.wasm`, a pushed run's `controller.wasm`, the
//! pushed-controller listing) and records every tournament it accepts, each match
//! and its replay through the backend as it plays, all through the shared
//! [`HttpBackendClient`](test_cabinet_backend_client::HttpBackendClient). So an
//! accepted tournament outlives the process, which resumes the unfinished ones on
//! start ([`drive::resume_unfinished`]).
//!
//! The pieces:
//!
//! - [`api`] — the HTTP surface: list pittable controllers, run one transient match,
//!   submit a tournament, poll its status, and stream live per-match progress. These
//!   are the worker's old **execution** endpoints, ported verbatim so the console's
//!   hand-typed transport reads them unchanged.
//! - [`arena_resolve`] — resolving a controller's wasm against the backend
//!   (baselines + pushed runs only; run-local controllers are not resolvable here).
//! - [`drive`] — driving a recorded tournament: recording each match as it
//!   completes, its standings at the end, and resuming the unfinished ones.
//! - [`tournaments`] — the tournaments this process drives and their live NDJSON
//!   progress channels. Per-pod, so the Deployment runs a single replica.
//! - [`shutdown`] — `SIGTERM`: no new work, the match in progress recorded within
//!   the grace, then exit.
//! - [`executor`] — the capacity guard: a semaphore bounding concurrent CPU-bound
//!   work, rejecting with `503` (not queueing) at capacity.
//! - [`auth`] — who is calling: the Access assertion every request through the edge
//!   carries (checked by [`access`]), with no route reachable without an identity
//!   but the health probe.
//! - [`access`] — the verification of the Cloudflare Access assertion.
//! - [`config`] — the environment-sourced configuration.
//!
//! Every route is served at the root and under the edge's `/arena` prefix.

pub mod access;
pub mod api;
pub mod arena_resolve;
pub mod auth;
pub mod config;
pub mod drive;
pub mod error;
pub mod executor;
pub mod shutdown;
pub mod tournaments;

#[cfg(test)]
#[path = "stub_backend.test.rs"]
mod stub_backend;

use std::sync::Arc;

use crate::access::AccessVerifier;
use crate::api::AppState;
use crate::config::Config;
use crate::drive::RetryPolicy;
use crate::executor::MatchExecutor;
use crate::shutdown::{Drives, Shutdown};
use crate::tournaments::TournamentRegistry;

/// A fully wired, runnable arena service: the Axum router, its resolved bind
/// address, and the state the binary resumes tournaments and shuts down through.
pub struct ArenaService {
    /// The Axum router, ready to be served.
    pub router: axum::Router,
    /// The bind address resolved from configuration.
    pub bind: String,
    /// The shared state: [`drive::resume_unfinished`] runs over it, and its
    /// [`Shutdown`] and [`Drives`] are what the binary drains on `SIGTERM`.
    pub state: AppState,
    /// How long a shutdown waits for the drives before the process exits.
    pub shutdown_grace: std::time::Duration,
}

/// Assemble the arena service from a configuration: an empty tournament registry, a
/// capacity-bounded executor, the backend URL the handlers resolve and record
/// against, and the Access verifier. It opens no store and reaches no disk — it is
/// stateless — so this never fails.
pub fn build(config: Config) -> ArenaService {
    let bind = config.bind.clone();
    let shutdown_grace = config.shutdown_grace;
    let state = AppState {
        backend_url: Arc::new(config.backend_url),
        backend_service_token: config.backend_service_token.map(Arc::new),
        tournaments: TournamentRegistry::new(),
        executor: Arc::new(MatchExecutor::new(config.max_concurrent_matches)),
        access: config
            .access
            .as_ref()
            .map(|access| Arc::new(AccessVerifier::new(access, reqwest::Client::new()))),
        shutdown: Shutdown::new(),
        drives: Drives::new(),
        runner: drive::wasm_runner(),
        retry: RetryPolicy::default(),
    };
    ArenaService {
        router: api::router(state.clone()),
        bind,
        state,
        shutdown_grace,
    }
}
