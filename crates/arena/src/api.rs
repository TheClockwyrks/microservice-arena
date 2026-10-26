//! The arena service's HTTP surface: list the controllers a case can pit, run one
//! head-to-head match, submit a tournament, and observe a tournament live.
//!
//! All of these are the **execution** endpoints the console expects — the
//! run-and-stream surface, not the gallery's reads (succeeded tournaments, stored
//! replays, and controller listings are read from the backend). A tournament's
//! status and its stream's backlog are read from the backend's record, so they
//! answer the same from any arena process (the arena overview's "Durable
//! tournaments").
//!
//! Every route but the health probe needs an identity, resolved by
//! [`crate::auth::identify`]: the Access assertion every request through the edge
//! carries, verified as the backend verifies it. Every route is served at the root
//! and under the edge's [`/arena`](crate::auth::EDGE_PREFIX) prefix, which
//! `cloudflared` forwards unchanged. The CPU-bound wasm execution behind the routes
//! is bounded by the [`MatchExecutor`] capacity guard.

use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use futures_util::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::broadcast::error::RecvError;
use tower_http::cors::{AllowHeaders, CorsLayer};
use tower_http::trace::TraceLayer;

use foray_core::replay::Replay;
use test_cabinet_backend_client::{BackendClient, HttpBackendClient};
use test_cabinet_match_play::{
    ControllerRef, MatchSummary, PendingTournament, RecordedTournament, ResolvedController,
    TournamentRecord, TournamentState, run_quick_match, tournament_pairings,
};

use crate::arena_resolve::{list_controllers, resolve_controller, with_pushed_controllers};
use crate::auth::{AccessMode, EDGE_PREFIX};
use crate::drive::{DriveStart, RetryPolicy, TournamentRunner, spawn_drive};
use crate::error::ApiError;
use crate::executor::MatchExecutor;
use crate::shutdown::{Drives, Shutdown};
use crate::tournaments::{StreamItem, TournamentProgress, TournamentRegistry, TournamentStatus};

/// The shared handler state: the backend URL the service fetches inputs from and
/// records tournaments through, the live registry of the tournaments this process
/// drives, the capacity guard the CPU-bound work runs through, the shutdown signal,
/// and the Access verifier.
#[derive(Clone)]
pub struct AppState {
    /// The backend base URL (no trailing slash). Every handler builds a fresh
    /// [`HttpBackendClient`] from it to resolve controllers and publish tournaments.
    pub backend_url: Arc<String>,
    /// The shared service token presented on every backend call, or `None` when
    /// the service was configured without one.
    pub backend_service_token: Option<Arc<String>>,
    /// The tournaments this process is driving and their live channels.
    pub tournaments: TournamentRegistry,
    /// Bounds concurrent CPU-bound match/tournament execution.
    pub executor: Arc<MatchExecutor>,
    /// The Access verifier a request's assertion is checked with, or `None` when
    /// the arena takes a person's identity from the local identity header.
    pub access: AccessMode,
    /// Begun on `SIGTERM`: no new match or tournament starts.
    pub shutdown: Shutdown,
    /// The tournament drives under way, which a shutdown waits for.
    pub drives: Drives,
    /// What plays a tournament's pairings.
    pub runner: Arc<dyn TournamentRunner>,
    /// How a write to the backend is retried before a drive is abandoned.
    pub retry: RetryPolicy,
}

impl AppState {
    /// A backend client pointed at the configured backend, presenting the service
    /// token on every call: reads need an identity too, and recording a tournament
    /// takes the service token.
    pub fn backend(&self) -> HttpBackendClient {
        HttpBackendClient::new((*self.backend_url).clone())
            .with_token(self.backend_service_token.as_deref().cloned())
    }
}

/// Every route of the arena, served by [`router`] at the root and under the edge
/// prefix.
fn routes() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/matches/controllers", get(controllers))
        .route("/matches", post(run_match_handler))
        .route("/tournaments", post(submit_tournament))
        .route("/tournaments/{id}", get(tournament_status))
        .route("/tournaments/{id}/events", get(tournament_events))
}

/// Build the arena service's Axum router: every route at the root and under the
/// edge's [`EDGE_PREFIX`], behind the identity layer. The trace middleware
/// continues an inbound W3C trace; CORS is permissive so a browser console reaching
/// the service is never blocked — the same posture as the backend and artifact
/// services.
pub fn router(state: AppState) -> Router {
    Router::new()
        .merge(routes())
        .nest(EDGE_PREFIX, routes())
        // Innermost, so the trace layer records a refused request, and inside CORS,
        // so a browser's preflight (which carries no credential) is answered by it.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::auth::identify,
        ))
        .layer(axum::middleware::from_fn(accept_trace))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive().allow_headers(AllowHeaders::mirror_request()))
        .with_state(state)
}

/// Continue any inbound W3C trace context so spans stitch across the call (a no-op
/// when no propagator is installed, i.e. telemetry disabled).
async fn accept_trace(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    test_cabinet_telemetry::propagation::accept_inbound(request.headers());
    next.run(request).await
}

/// Liveness probe: `200` while the process serves, shutting down included.
async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Readiness probe: `503` once the arena is shutting down, so its endpoint leaves
/// the `Service`.
async fn readyz(State(state): State<AppState>) -> Response {
    if state.shutdown.is_draining() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "status": "shutting_down" })),
        )
            .into_response()
    } else {
        Json(serde_json::json!({ "status": "ok" })).into_response()
    }
}

// --- Matches ----------------------------------------------------------------

/// `POST /matches` — run one head-to-head match between two controllers and return
/// its replay (for immediate browser playback) plus the summary. Transient: nothing
/// is persisted.
async fn run_match_handler(
    State(state): State<AppState>,
    Json(body): Json<MatchBody>,
) -> Result<Json<MatchResponse>, ApiError> {
    if state.shutdown.is_draining() {
        return Err(ApiError::shutting_down());
    }
    let client = state.backend();
    let test_case = client
        .resolve_version(&body.test_case, &body.version)
        .await
        .map_err(|err| {
            ApiError::bad_request(format!(
                "resolving {}@{}: {err}",
                body.test_case, body.version
            ))
        })?;

    let red = resolve_controller(&client, &body.test_case, &body.version, &body.red)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let blue = resolve_controller(&client, &body.test_case, &body.version, &body.blue)
        .await
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    // A match is CPU-bound wasm execution; run it off the async runtime under the
    // capacity guard (which rejects with `503` at capacity rather than queueing).
    let outcome = state
        .executor
        .run_match(move || run_quick_match(&test_case, &red, &blue))
        .await?
        .map_err(|err| ApiError::internal(format!("running the match: {err}")))?;

    Ok(Json(MatchResponse {
        replay: outcome.replay,
        summary: outcome.summary,
    }))
}

/// `GET /matches/controllers?testCase=` — the controllers available to pit for a
/// case: the committed arena opponents (the model-facing baselines plus the hidden
/// references) and the case's **pushed** adversarial controllers resolved from the
/// backend.
async fn controllers(
    State(state): State<AppState>,
    Query(params): Query<ControllersParams>,
) -> Json<ControllersResponse> {
    let baselines = list_controllers(&params.test_case);
    let client = state.backend();
    let controllers = with_pushed_controllers(&client, &params.test_case, baselines).await;
    Json(ControllersResponse { controllers })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchBody {
    pub test_case: String,
    pub version: String,
    pub red: ControllerRef,
    pub blue: ControllerRef,
}

#[derive(Debug, Serialize)]
pub struct MatchResponse {
    /// The browser-playable replay, or `null` when a controller failed to load.
    replay: Option<Replay>,
    summary: MatchSummary,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllersParams {
    pub test_case: String,
}

#[derive(Debug, Serialize)]
pub struct ControllersResponse {
    controllers: Vec<ControllerRef>,
}

// --- Tournaments ------------------------------------------------------------

/// `POST /tournaments` — submit a tournament. Resolves every participant's
/// controller, acquires a capacity permit, records the tournament through the
/// backend with every pairing as a match without a result, and only then answers
/// `202 Accepted` and drives it in the background. At capacity, or while shutting
/// down, the submit is rejected with `503` rather than queued, so the console sees
/// the rejection.
async fn submit_tournament(
    State(state): State<AppState>,
    Json(body): Json<TournamentBody>,
) -> Result<Response, ApiError> {
    if state.shutdown.is_draining() {
        return Err(ApiError::shutting_down());
    }
    if body.participants.len() < 2 {
        return Err(ApiError::bad_request(
            "a tournament needs at least two participants",
        ));
    }
    let mut ids = HashSet::new();
    if let Some(repeated) = body
        .participants
        .iter()
        .find(|c| !ids.insert(c.id.as_str()))
    {
        return Err(ApiError::bad_request(format!(
            "the field names controller `{}` more than once",
            repeated.id
        )));
    }

    let client = state.backend();
    let test_case = client
        .resolve_version(&body.test_case, &body.version)
        .await
        .map_err(|err| {
            ApiError::bad_request(format!(
                "resolving {}@{}: {err}",
                body.test_case, body.version
            ))
        })?;

    // Resolve every controller up front so a bad participant fails the submit rather
    // than a half-run tournament.
    let mut participants: Vec<ResolvedController> = Vec::with_capacity(body.participants.len());
    for controller in &body.participants {
        participants.push(
            resolve_controller(&client, &body.test_case, &body.version, controller)
                .await
                .map_err(|err| ApiError::bad_request(err.to_string()))?,
        );
    }

    // Acquire the capacity permit before accepting the tournament: at capacity the
    // submit is rejected with `503` so the console can retry, rather than queueing
    // CPU-bound work the pod can't keep up with. The permit is held across the
    // whole drive.
    let permit = state.executor.acquire()?;

    let id = cuid2::create_id();
    let variant = if body.variant.trim().is_empty() {
        "base".to_string()
    } else {
        body.variant
    };
    let pending = PendingTournament {
        id: id.clone(),
        created_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|err| ApiError::internal(format!("formatting the time: {err}")))?,
        test_case_slug: test_case.slug.clone(),
        test_case_version: test_case.version.clone(),
        variant,
        participants: body.participants,
        pairings: tournament_pairings(
            &participants
                .iter()
                .map(|p| p.controller.clone())
                .collect::<Vec<_>>(),
        ),
    };

    // Register before recording, so a resume listing the new tournament as queued
    // finds it already driven here.
    let live = state
        .tournaments
        .register(&id)
        .ok_or_else(|| ApiError::internal(format!("tournament `{id}` is already driven")))?;
    // The `202` promises the tournament outlives any restart, so it is answered
    // only once the record is stored.
    client
        .record_pending_tournament(&pending)
        .await
        .map_err(|err| ApiError::backend_unavailable(format!("recording the tournament: {err}")))?;

    let tournament = RecordedTournament {
        record: TournamentRecord {
            id: pending.id.clone(),
            created_at: pending.created_at.clone(),
            test_case_slug: pending.test_case_slug.clone(),
            test_case_version: pending.test_case_version.clone(),
            variant: pending.variant.clone(),
            participants: pending.participants.clone(),
            standings: Vec::new(),
            matches: Vec::new(),
        },
        state: TournamentState::Queued,
        detail: None,
        pairings: pending.pairings,
        published_at: pending.created_at,
    };
    spawn_drive(
        state.clone(),
        tournament,
        live,
        DriveStart::Accepted {
            test_case: Box::new(test_case),
            participants,
            permit,
        },
    );

    let ack = SubmitAck {
        tournament_id: id.clone(),
        status_url: format!("/tournaments/{id}"),
        events_url: format!("/tournaments/{id}/events"),
    };
    Ok((StatusCode::ACCEPTED, Json(ack)).into_response())
}

/// `GET /tournaments/{id}` — the tournament's recorded status, and the finished
/// record once it has succeeded.
async fn tournament_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TournamentStatus>, ApiError> {
    Ok(Json(recorded(&state, &id).await?.into()))
}

/// `GET /tournaments/{id}/events` — the recorded matches as a backlog, then each
/// match as this process records it, as NDJSON. The stream ends with the backlog
/// when the tournament is finished or this process is not driving it, and
/// otherwise when the drive ends.
async fn tournament_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    // Subscribe before reading the record, so a match recorded in between is in
    // one or the other; one in both is skipped from the live tail.
    let receiver = state.tournaments.subscribe(&id);
    let tournament = recorded(&state, &id).await?;
    let receiver = receiver.filter(|_| !tournament.state.is_terminal());
    let body = Body::from_stream(progress_stream(&tournament, receiver));
    Ok((
        [
            (header::CONTENT_TYPE, "application/x-ndjson"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response())
}

/// Read the backend's record of tournament `id`: `404` when it records none,
/// `502` when it cannot be read.
async fn recorded(state: &AppState, id: &str) -> Result<RecordedTournament, ApiError> {
    state
        .backend()
        .recorded_tournament(id)
        .await
        .map_err(|err| ApiError::backend_unavailable(format!("reading the tournament: {err}")))?
        .ok_or_else(|| ApiError::not_found(format!("tournament `{id}` not found")))
}

/// Build the NDJSON byte stream for a tournament: its recorded matches as the
/// backlog, followed by the live tail when there is one, ending when the drive
/// does.
fn progress_stream(
    tournament: &RecordedTournament,
    receiver: Option<tokio::sync::broadcast::Receiver<StreamItem>>,
) -> impl Stream<Item = Result<Bytes, std::convert::Infallible>> + use<> {
    let total = tournament.pairings.len();
    let backlog: Vec<TournamentProgress> = tournament
        .record
        .matches
        .iter()
        .enumerate()
        .map(|(index, summary)| TournamentProgress {
            played: index + 1,
            total,
            summary: summary.clone(),
        })
        .collect();
    let seen: HashSet<String> = backlog
        .iter()
        .map(|progress| progress.summary.match_id.clone())
        .collect();
    let backlog = stream::iter(backlog.into_iter().map(|p| Ok(encode_line(&p))));
    let live = stream::unfold((receiver, seen), |(receiver, seen)| async move {
        let mut receiver = receiver?;
        loop {
            match receiver.recv().await {
                Ok(StreamItem::Progress(progress)) => {
                    if seen.contains(&progress.summary.match_id) {
                        continue;
                    }
                    return Some((Ok(encode_line(&progress)), (Some(receiver), seen)));
                }
                Ok(StreamItem::Done) | Err(RecvError::Closed) => return None,
                Err(RecvError::Lagged(_)) => continue,
            }
        }
    });
    backlog.chain(live)
}

/// Encode one progress item as a `\n`-terminated NDJSON line. A progress item's
/// fields are plain JSON-safe scalars, so a defensive empty line stands in for the
/// impossible serialization error rather than aborting the stream.
fn encode_line(progress: &TournamentProgress) -> Bytes {
    match serde_json::to_string(progress) {
        Ok(mut line) => {
            line.push('\n');
            Bytes::from(line)
        }
        Err(_) => Bytes::from_static(b"\n"),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TournamentBody {
    pub test_case: String,
    pub version: String,
    #[serde(default)]
    pub variant: String,
    pub participants: Vec<ControllerRef>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitAck {
    /// The id of the accepted tournament (also the recorded tournament's id).
    pub tournament_id: String,
    /// Where to poll the tournament's status.
    pub status_url: String,
    /// Where to stream live per-match progress (NDJSON).
    pub events_url: String,
}

#[cfg(test)]
#[path = "api.test.rs"]
mod tests;
