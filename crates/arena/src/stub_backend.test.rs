//! A stub backend the arena's tests drive the real router and the real drives
//! against, over HTTP. It answers the endpoints the arena fetches controller
//! inputs from, and records tournaments the way the backend's tournament routes
//! do, in memory, so a test can inspect what was recorded, refuse a write, and
//! hand the same record to a second arena as a restarted pod would find it.
//!
//! - `GET /test-cases/{slug}/versions/{version}` (a minimal but valid adversarial
//!   case carrying `[contract]`, `[sandbox]`, `[simulation]`),
//! - `GET /test-cases/{slug}/versions/{version}/artifacts/references/{id}.wasm`
//!   (a baseline's wasm: deliberately invalid bytes, so a match played through
//!   the real runner forfeits cleanly),
//! - `GET /adversarial/controllers?testCase=` and `GET /runs/{id}/controller.wasm`,
//! - the tournament routes: `PUT /tournaments/{id}`,
//!   `PUT /tournaments/{id}/matches/{matchId}`, `POST /tournaments/{id}/state`,
//!   `GET /tournaments/{id}`, `GET /tournaments/unfinished`, and
//!   `POST /tournaments/{id}/matches/{matchId}/replay.json`.
//!
//! Every route takes [`SERVICE_TOKEN`] only.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use test_cabinet_contracts::test_case::TestCaseVersion;
use test_cabinet_contracts::validation::AdversarialOutcome;
use test_cabinet_match_play::{
    MatchSummary, PendingTournament, RecordedTournament, ResolvedController, TournamentPairing,
    TournamentRecord, TournamentState, TournamentTransition,
};

use crate::api::AppState;
use crate::drive::{CompletedMatch, RetryPolicy, TournamentRunner};
use crate::executor::MatchExecutor;
use crate::shutdown::{Drives, Shutdown};
use crate::tournaments::TournamentRegistry;

/// The service token the stub backend takes.
pub const SERVICE_TOKEN: &str = "svc-token";

/// A minimal but valid adversarial `VersionBody` (camelCase): enough for
/// `canonical_match_setup` to build a match (it needs `contract`, `sandbox` with
/// `fuelPerTick`, and `simulation`).
pub fn version_json(slug: &str, version: &str) -> serde_json::Value {
    serde_json::json!({
        "slug": slug,
        "version": version,
        "name": "Pacman Arena",
        "difficulty": "medium",
        "tags": ["adversarial"],
        "summary": null,
        "description": null,
        "maxRuntimeSeconds": 600,
        "engines": [{ "slug": "none" }],
        "testType": "adversarial",
        "contract": { "entry": "controller", "world": "world.wit", "action": "action.wit" },
        "sandbox": { "fuelPerTick": 1_000_000, "maxMemoryBytes": 16_777_216u64 },
        "simulation": { "timestepMs": 16, "maxTicks": 1024 },
        "promptTemplate": "build a controller",
        "commonSpecs": [],
        "assets": [],
        "variants": [],
        "commonReferences": [],
        "checks": []
    })
}

/// What the stub backend holds, shared with the test.
#[derive(Debug, Default)]
pub struct Recorded {
    /// Every recorded tournament by id.
    pub tournaments: HashMap<String, RecordedTournament>,
    /// Every stored replay by `(tournament, match)`.
    pub replays: HashMap<(String, String), Vec<u8>>,
    /// The matches whose summary write is refused with `500`.
    pub refuse_summary: HashSet<String>,
    /// Refuse recording a pending tournament with `500`.
    pub refuse_pending: bool,
    /// Answer resolving a test case version with this status rather than the
    /// version.
    pub refuse_version: Option<StatusCode>,
    /// Every replay and summary write, in order, refused ones included:
    /// `replay:<match>` and `summary:<match>`.
    pub writes: Vec<String>,
}

/// The stub backend's shared state.
pub type Backend = Arc<Mutex<Recorded>>;

/// Refuse a request that does not present [`SERVICE_TOKEN`] with `401`, as the
/// backend refuses a caller without an identity.
async fn require_service_token(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let expected = format!("Bearer {SERVICE_TOKEN}");
    let presented = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if presented == Some(expected.as_str()) {
        next.run(request).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

fn refused(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({ "error": { "code": "stub", "message": message } })),
    )
        .into_response()
}

async fn record_pending(
    State(backend): State<Backend>,
    Path(id): Path<String>,
    Json(pending): Json<PendingTournament>,
) -> Response {
    let mut backend = backend.lock().unwrap();
    if backend.refuse_pending {
        return refused(StatusCode::INTERNAL_SERVER_ERROR, "refused");
    }
    if backend.tournaments.contains_key(&id) {
        return StatusCode::OK.into_response();
    }
    backend.tournaments.insert(
        id,
        RecordedTournament {
            record: TournamentRecord {
                id: pending.id,
                created_at: pending.created_at.clone(),
                test_case_slug: pending.test_case_slug,
                test_case_version: pending.test_case_version,
                variant: pending.variant,
                participants: pending.participants,
                standings: Vec::new(),
                matches: Vec::new(),
            },
            state: TournamentState::Queued,
            detail: None,
            pairings: pending.pairings,
            published_at: pending.created_at,
        },
    );
    StatusCode::CREATED.into_response()
}

async fn record_match(
    State(backend): State<Backend>,
    Path((id, match_id)): Path<(String, String)>,
    Json(summary): Json<MatchSummary>,
) -> Response {
    let mut backend = backend.lock().unwrap();
    backend.writes.push(format!("summary:{match_id}"));
    if backend.refuse_summary.contains(&match_id) {
        return refused(StatusCode::INTERNAL_SERVER_ERROR, "refused");
    }
    let Some(tournament) = backend.tournaments.get_mut(&id) else {
        return refused(StatusCode::NOT_FOUND, "no tournament");
    };
    if tournament.state.is_terminal() {
        return refused(StatusCode::CONFLICT, "terminal");
    }
    let order: Vec<String> = tournament
        .pairings
        .iter()
        .map(|p| p.match_id.clone())
        .collect();
    tournament.record.matches.retain(|m| m.match_id != match_id);
    tournament.record.matches.push(summary);
    tournament.record.matches.sort_by_key(|m| {
        order
            .iter()
            .position(|id| *id == m.match_id)
            .unwrap_or(usize::MAX)
    });
    if tournament.state == TournamentState::Queued {
        tournament.state = TournamentState::Running;
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn transition(
    State(backend): State<Backend>,
    Path(id): Path<String>,
    Json(transition): Json<TournamentTransition>,
) -> Response {
    let mut backend = backend.lock().unwrap();
    let Some(tournament) = backend.tournaments.get_mut(&id) else {
        return refused(StatusCode::NOT_FOUND, "no tournament");
    };
    if tournament.state.is_terminal() {
        return if tournament.state == transition.state() {
            StatusCode::NO_CONTENT.into_response()
        } else {
            refused(StatusCode::CONFLICT, "terminal")
        };
    }
    match transition {
        TournamentTransition::Running => tournament.state = TournamentState::Running,
        TournamentTransition::Failed { detail } => {
            tournament.state = TournamentState::Failed;
            tournament.detail = Some(detail);
        }
        TournamentTransition::Succeeded { standings } => {
            if !tournament.unplayed_pairings().is_empty() {
                return refused(StatusCode::CONFLICT, "pairings unplayed");
            }
            tournament.state = TournamentState::Succeeded;
            tournament.record.standings = standings;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

async fn read_tournament(State(backend): State<Backend>, Path(id): Path<String>) -> Response {
    match backend.lock().unwrap().tournaments.get(&id) {
        Some(tournament) => Json(tournament.clone()).into_response(),
        None => refused(StatusCode::NOT_FOUND, "no tournament"),
    }
}

async fn unfinished(State(backend): State<Backend>) -> Response {
    let mut tournaments: Vec<RecordedTournament> = backend
        .lock()
        .unwrap()
        .tournaments
        .values()
        .filter(|t| !t.state.is_terminal())
        .cloned()
        .collect();
    tournaments.sort_by(|a, b| a.record.created_at.cmp(&b.record.created_at));
    Json(serde_json::json!({ "tournaments": tournaments })).into_response()
}

async fn store_replay(
    State(backend): State<Backend>,
    Path((id, match_id)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Response {
    let mut backend = backend.lock().unwrap();
    backend.writes.push(format!("replay:{match_id}"));
    backend.replays.insert((id, match_id), body.to_vec());
    StatusCode::NO_CONTENT.into_response()
}

/// Spawn a stub backend on an ephemeral port holding `backend`, and answer its base
/// URL.
pub async fn spawn_stub_backend_with(backend: Backend) -> String {
    let app =
        Router::new()
            .route(
                "/test-cases/{slug}/versions/{version}",
                get(
                    |State(backend): State<Backend>,
                     Path((slug, version)): Path<(String, String)>| async move {
                        match backend.lock().unwrap().refuse_version {
                            Some(status) => refused(status, "refused"),
                            None => Json(version_json(&slug, &version)).into_response(),
                        }
                    },
                ),
            )
            .route(
                // A baseline's wasm, addressed as references/{id}.wasm. Invalid bytes:
                // the match forfeits cleanly rather than needing a real module.
                "/test-cases/{slug}/versions/{version}/artifacts/{*key}",
                get(
                    |Path((_slug, _version, _key)): Path<(String, String, String)>| async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "application/wasm")],
                            b"\0not-wasm".to_vec(),
                        )
                    },
                ),
            )
            .route(
                "/runs/{id}/controller.wasm",
                get(|Path(_id): Path<String>| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/wasm")],
                        b"\0not-wasm".to_vec(),
                    )
                }),
            )
            .route(
                "/adversarial/controllers",
                get(|Query(params): Query<HashMap<String, String>>| async move {
                    let _ = params;
                    Json(serde_json::json!({
                        "controllers": [
                            { "id": "run-xyz", "kind": "pushed", "label": "claude" }
                        ]
                    }))
                }),
            )
            .route("/tournaments/unfinished", get(unfinished))
            .route(
                "/tournaments/{id}",
                get(read_tournament).put(record_pending),
            )
            .route("/tournaments/{id}/matches/{matchId}", put(record_match))
            .route("/tournaments/{id}/state", post(transition))
            .route(
                "/tournaments/{id}/matches/{matchId}/replay.json",
                post(store_replay),
            )
            .layer(axum::middleware::from_fn(require_service_token))
            .with_state(backend);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Spawn a stub backend holding nothing.
pub async fn spawn_stub_backend() -> String {
    spawn_stub_backend_with(Backend::default()).await
}

/// A runner standing in for the wasm hosts: Red wins every pairing, and a played
/// match's replay is a small JSON document naming it. It logs every pairing it
/// plays, and runs `hook` before each, which a test uses to stop or hold a
/// tournament at a chosen match.
#[derive(Clone, Default)]
pub struct ScriptedRunner {
    /// Every match id played, in order.
    pub played: Arc<Mutex<Vec<String>>>,
    /// Run before each pairing is played, with how many this runner has played.
    #[allow(clippy::type_complexity)]
    pub hook: Option<Arc<dyn Fn(usize, &TournamentPairing) + Send + Sync>>,
    /// Fail with this error instead of playing.
    pub fail: Option<String>,
}

impl ScriptedRunner {
    /// The match ids played so far.
    pub fn played(&self) -> Vec<String> {
        self.played.lock().unwrap().clone()
    }
}

impl TournamentRunner for ScriptedRunner {
    fn run(
        &self,
        _test_case: &TestCaseVersion,
        _participants: &[ResolvedController],
        pairings: &[TournamentPairing],
        on_match: &mut dyn FnMut(CompletedMatch) -> ControlFlow<()>,
    ) -> Result<(), String> {
        if let Some(err) = &self.fail {
            return Err(err.clone());
        }
        for pairing in pairings {
            let count = self.played.lock().unwrap().len();
            if let Some(hook) = &self.hook {
                hook(count, pairing);
            }
            self.played.lock().unwrap().push(pairing.match_id.clone());
            let summary = MatchSummary {
                match_id: pairing.match_id.clone(),
                red_id: pairing.red_id.clone(),
                blue_id: pairing.blue_id.clone(),
                winner: Some(pairing.red_id.clone()),
                win_type: "swept".to_string(),
                outcome_for_red: AdversarialOutcome::Win,
                red_score: 3,
                blue_score: 0,
                ticks: 10,
                red_kills: 1,
                blue_kills: 0,
                red_fuel: 5,
                blue_fuel: 6,
                replay_key: Some(pairing.match_id.clone()),
                detail: None,
            };
            let replay = serde_json::json!({ "match": pairing.match_id }).to_string();
            if on_match(CompletedMatch {
                summary,
                replay: Some(replay.into_bytes()),
            })
            .is_break()
            {
                break;
            }
        }
        Ok(())
    }
}

/// A retry policy that gives up at once, so a refused write abandons the drive
/// without waiting.
pub fn no_retry() -> RetryPolicy {
    RetryPolicy {
        attempts: 1,
        initial: Duration::from_millis(1),
        max: Duration::from_millis(1),
        timeout: Duration::from_secs(5),
    }
}

/// The state of an arena pointed at `backend_url` with the executor cap `cap`,
/// playing through `runner`, as a fresh process starts with.
pub fn arena_state(
    backend_url: &str,
    cap: usize,
    token: Option<&str>,
    runner: Arc<dyn TournamentRunner>,
) -> AppState {
    AppState {
        backend_url: Arc::new(backend_url.trim_end_matches('/').to_string()),
        backend_service_token: token.map(|t| Arc::new(t.to_string())),
        tournaments: TournamentRegistry::new(),
        executor: Arc::new(MatchExecutor::new(cap)),
        access: None,
        shutdown: Shutdown::new(),
        drives: Drives::new(),
        runner,
        retry: no_retry(),
    }
}

/// Poll `condition` until it holds, failing the test after ten seconds.
pub async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
