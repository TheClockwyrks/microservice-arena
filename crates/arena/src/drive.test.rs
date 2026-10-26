//! End-to-end tests for durable tournaments: the real router and the real drives
//! against the [stub backend](crate::stub_backend), with a scripted runner standing
//! in for the wasm hosts. A second arena state over the same stub is a restarted
//! pod: it holds nothing but what the backend recorded.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use futures_util::StreamExt as _;
use test_cabinet_match_play::TournamentState;
use tower::ServiceExt;

use super::*;
use crate::auth::LOCAL_IDENTITY_HEADER;
use crate::shutdown::Shutdown;
use crate::stub_backend::{
    Backend, SERVICE_TOKEN, ScriptedRunner, arena_state, eventually, spawn_stub_backend_with,
};

/// The four committed arena opponents: a field of six pairings.
const FIELD: [&str; 4] = ["border-soldier", "greedy-raider", "random", "fuel-probe"];

fn arena(backend_url: &str, cap: usize, runner: &ScriptedRunner) -> AppState {
    arena_state(
        backend_url,
        cap,
        Some(SERVICE_TOKEN),
        Arc::new(runner.clone()),
    )
}

fn router(state: &AppState) -> Router {
    crate::api::router(state.clone())
}

async fn send(app: &Router, method: &str, uri: &str, body: Option<serde_json::Value>) -> Response {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(LOCAL_IDENTITY_HEADER, "dev@local");
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap())),
        None => builder.body(Body::empty()),
    }
    .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

async fn body_bytes(response: Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

async fn json(response: Response) -> serde_json::Value {
    serde_json::from_slice(&body_bytes(response).await).unwrap()
}

/// Submit a tournament of `field`, answering the response.
async fn submit(app: &Router, field: &[&str]) -> Response {
    let participants: Vec<serde_json::Value> = field
        .iter()
        .map(|id| serde_json::json!({ "id": id, "kind": "baseline" }))
        .collect();
    send(
        app,
        "POST",
        "/tournaments",
        Some(serde_json::json!({
            "testCase": "pacman",
            "version": "1.0.0",
            "participants": participants,
        })),
    )
    .await
}

/// Submit the four-controller field and answer the accepted tournament's id.
async fn submit_field(app: &Router) -> String {
    let response = submit(app, &FIELD).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    json(response).await["tournamentId"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Every NDJSON line of a tournament's event stream, read to its end.
async fn events(app: &Router, id: &str) -> Vec<serde_json::Value> {
    let response = send(app, "GET", &format!("/tournaments/{id}/events"), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    String::from_utf8(body_bytes(response).await)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Assert `id` succeeded on `backend` with every pairing recorded, and every
/// replay its matches name stored.
fn assert_succeeded_with_every_replay(backend: &Backend, id: &str) {
    let backend = backend.lock().unwrap();
    let tournament = &backend.tournaments[id];
    assert_eq!(tournament.state, TournamentState::Succeeded);
    assert_eq!(tournament.record.matches.len(), 6);
    assert_eq!(tournament.record.standings.len(), 4);
    for summary in &tournament.record.matches {
        let key = summary
            .replay_key
            .clone()
            .expect("a played match names its replay");
        assert!(
            backend.replays.contains_key(&(id.to_string(), key.clone())),
            "the replay of `{key}` is stored"
        );
    }
}

#[tokio::test]
async fn a_submitted_tournament_records_each_match_replay_first_and_then_its_standings() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let runner = ScriptedRunner::default();
    let state = arena(&url, 2, &runner);
    let app = router(&state);

    let id = submit_field(&app).await;
    state.drives.idle().await;

    assert_succeeded_with_every_replay(&backend, &id);
    let pairings: Vec<String> = backend.lock().unwrap().tournaments[&id]
        .pairings
        .iter()
        .map(|p| p.match_id.clone())
        .collect();
    assert_eq!(
        runner.played(),
        pairings,
        "every pairing played once, in order"
    );
    let expected: Vec<String> = pairings
        .iter()
        .flat_map(|m| [format!("replay:{m}"), format!("summary:{m}")])
        .collect();
    assert_eq!(backend.lock().unwrap().writes, expected);
    let standings = backend.lock().unwrap().tournaments[&id]
        .record
        .standings
        .clone();
    assert_eq!(
        standings[0].participant_id, "border-soldier",
        "Red wins every match"
    );
    assert_eq!(standings[0].wins, 3);

    let status = json(send(&app, "GET", &format!("/tournaments/{id}"), None).await).await;
    assert_eq!(status["state"], "succeeded");
    assert_eq!(status["record"]["matches"].as_array().unwrap().len(), 6);
    let lines = events(&app, &id).await;
    let played: Vec<u64> = lines
        .iter()
        .map(|l| l["played"].as_u64().unwrap())
        .collect();
    assert_eq!(played, vec![1, 2, 3, 4, 5, 6]);
    assert!(lines.iter().all(|l| l["total"] == 6));
}

#[tokio::test]
async fn a_tournament_killed_between_a_replay_and_its_summary_plays_that_match_again() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;

    // The first arena stores the third match's replay and is refused its summary,
    // which leaves the tournament as a pod killed between the two writes would.
    let first_runner = ScriptedRunner::default();
    let first = arena(&url, 2, &first_runner);
    let pairings = test_cabinet_match_play::tournament_pairings(
        &FIELD
            .iter()
            .map(|id| test_cabinet_match_play::ControllerRef {
                id: id.to_string(),
                kind: test_cabinet_match_play::ControllerKind::Baseline,
                label: None,
            })
            .collect::<Vec<_>>(),
    );
    let third = pairings[2].match_id.clone();
    backend.lock().unwrap().refuse_summary.insert(third.clone());
    let id = submit_field(&router(&first)).await;
    first.drives.idle().await;
    {
        let backend = backend.lock().unwrap();
        let tournament = &backend.tournaments[&id];
        assert_eq!(
            tournament.state,
            TournamentState::Running,
            "left unfinished"
        );
        assert_eq!(tournament.record.matches.len(), 2);
        assert!(backend.replays.contains_key(&(id.clone(), third.clone())));
    }
    assert!(!first.tournaments.is_live(&id), "the abandoned drive ended");

    // A restarted arena resumes it and plays the third match once more.
    backend.lock().unwrap().refuse_summary.clear();
    let second_runner = ScriptedRunner::default();
    let second = arena(&url, 2, &second_runner);
    crate::drive::resume_unfinished(second.clone()).await;
    second.drives.idle().await;

    let resumed: Vec<String> = pairings[2..].iter().map(|p| p.match_id.clone()).collect();
    assert_eq!(
        second_runner.played(),
        resumed,
        "only the unrecorded pairings"
    );
    assert_succeeded_with_every_replay(&backend, &id);
}

#[tokio::test]
async fn a_shutdown_records_the_match_in_progress_and_a_restart_resumes_the_rest() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;

    // `SIGTERM` arrives while the second match is being played.
    let shutdown = Shutdown::new();
    let signal = shutdown.clone();
    let first_runner = ScriptedRunner {
        hook: Some(Arc::new(move |count, _| {
            if count == 1 {
                signal.begin();
            }
        })),
        ..ScriptedRunner::default()
    };
    let mut first = arena(&url, 2, &first_runner);
    first.shutdown = shutdown;
    let first_app = router(&first);
    let id = submit_field(&first_app).await;
    first.drives.idle().await;

    {
        let backend = backend.lock().unwrap();
        let tournament = &backend.tournaments[&id];
        assert_eq!(tournament.state, TournamentState::Running);
        assert_eq!(
            tournament.record.matches.len(),
            2,
            "the match in progress is recorded, and no other starts"
        );
    }
    assert_eq!(first_runner.played().len(), 2);

    // A shutting-down arena accepts no new work and fails its readiness probe.
    assert_eq!(
        submit(&first_app, &FIELD).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let quick = send(
        &first_app,
        "POST",
        "/matches",
        Some(serde_json::json!({
            "testCase": "pacman",
            "version": "1.0.0",
            "red": { "id": "random", "kind": "baseline" },
            "blue": { "id": "greedy-raider", "kind": "baseline" }
        })),
    )
    .await;
    assert_eq!(quick.status(), StatusCode::SERVICE_UNAVAILABLE);
    let ready = send(&first_app, "GET", "/readyz", None).await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let health = send(&first_app, "GET", "/healthz", None).await;
    assert_eq!(health.status(), StatusCode::OK);

    // The restarted arena answers the same recorded state before it resumes.
    let second_runner = ScriptedRunner::default();
    let second = arena(&url, 2, &second_runner);
    let second_app = router(&second);
    let before =
        body_bytes(send(&first_app, "GET", &format!("/tournaments/{id}"), None).await).await;
    let after =
        body_bytes(send(&second_app, "GET", &format!("/tournaments/{id}"), None).await).await;
    assert_eq!(before, after);
    let status: serde_json::Value = serde_json::from_slice(&after).unwrap();
    assert_eq!(status["state"], "running");
    assert_eq!(
        events(&first_app, &id).await,
        events(&second_app, &id).await
    );
    assert_eq!(
        events(&second_app, &id).await.len(),
        2,
        "the recorded backlog"
    );

    crate::drive::resume_unfinished(second.clone()).await;
    second.drives.idle().await;
    assert_eq!(second_runner.played().len(), 4, "only the four unplayed");
    assert_succeeded_with_every_replay(&backend, &id);
    let before =
        body_bytes(send(&first_app, "GET", &format!("/tournaments/{id}"), None).await).await;
    let after =
        body_bytes(send(&second_app, "GET", &format!("/tournaments/{id}"), None).await).await;
    assert_eq!(before, after);
    let played: Vec<u64> = events(&second_app, &id)
        .await
        .iter()
        .map(|l| l["played"].as_u64().unwrap())
        .collect();
    assert_eq!(played, vec![1, 2, 3, 4, 5, 6]);
}

/// Record a queued tournament of the four-controller field on `backend` through
/// the first arena, refusing every summary so nothing is played to a result.
async fn a_queued_tournament(url: &str, backend: &Backend) -> String {
    backend.lock().unwrap().refuse_pending = false;
    let runner = ScriptedRunner {
        fail: Some("held".to_string()),
        ..ScriptedRunner::default()
    };
    let state = arena(url, 1, &runner);
    // A shutdown before the submit's drive starts leaves the tournament queued.
    state.shutdown.begin();
    let app = router(&state);
    // The submit itself is refused while draining, so record it directly.
    let _ = app;
    let pending = test_cabinet_match_play::PendingTournament {
        id: "queued".to_string(),
        created_at: "2026-10-26T00:00:00Z".to_string(),
        test_case_slug: "pacman".to_string(),
        test_case_version: "1.0.0".to_string(),
        variant: "base".to_string(),
        participants: FIELD
            .iter()
            .map(|id| test_cabinet_match_play::ControllerRef {
                id: id.to_string(),
                kind: test_cabinet_match_play::ControllerKind::Baseline,
                label: None,
            })
            .collect(),
        pairings: Vec::new(),
    };
    let pairings = test_cabinet_match_play::tournament_pairings(&pending.participants);
    let pending = test_cabinet_match_play::PendingTournament {
        pairings,
        ..pending
    };
    state
        .backend()
        .record_pending_tournament(&pending)
        .await
        .unwrap();
    pending.id
}

#[tokio::test]
async fn a_resumed_tournament_waits_for_a_capacity_permit() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let id = a_queued_tournament(&url, &backend).await;

    let runner = ScriptedRunner::default();
    let state = arena(&url, 1, &runner);
    let held = state.executor.acquire().expect("the only permit");
    crate::drive::resume_unfinished(state.clone()).await;
    assert!(state.tournaments.is_live(&id), "resumed");
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(runner.played().is_empty(), "no match without a permit");
    assert_eq!(
        backend.lock().unwrap().tournaments[&id].state,
        TournamentState::Queued
    );

    drop(held);
    state.drives.idle().await;
    assert_eq!(runner.played().len(), 6);
    assert_succeeded_with_every_replay(&backend, &id);
}

#[tokio::test]
async fn a_shutdown_ends_a_drive_still_waiting_for_its_permit() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let id = a_queued_tournament(&url, &backend).await;

    let runner = ScriptedRunner::default();
    let state = arena(&url, 1, &runner);
    let _held = state.executor.acquire().expect("the only permit");
    crate::drive::resume_unfinished(state.clone()).await;
    assert_eq!(state.drives.active(), 1);
    state.shutdown.begin();
    state.drives.idle().await;
    assert!(runner.played().is_empty());
    assert!(!state.tournaments.is_live(&id));
    assert_eq!(
        backend.lock().unwrap().tournaments[&id].state,
        TournamentState::Queued
    );
}

#[tokio::test]
async fn a_resume_the_backend_cannot_resolve_yet_leaves_the_tournament_for_the_next_start() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let id = a_queued_tournament(&url, &backend).await;
    backend.lock().unwrap().refuse_version = Some(StatusCode::SERVICE_UNAVAILABLE);

    let runner = ScriptedRunner::default();
    let state = arena(&url, 1, &runner);
    crate::drive::resume_unfinished(state.clone()).await;
    state.drives.idle().await;
    assert!(runner.played().is_empty());
    assert!(!state.tournaments.is_live(&id));
    assert_eq!(
        backend.lock().unwrap().tournaments[&id].state,
        TournamentState::Queued
    );
}

#[tokio::test]
async fn a_resume_whose_test_case_no_longer_exists_marks_the_tournament_failed() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let id = a_queued_tournament(&url, &backend).await;
    backend.lock().unwrap().refuse_version = Some(StatusCode::NOT_FOUND);

    let runner = ScriptedRunner::default();
    let state = arena(&url, 1, &runner);
    crate::drive::resume_unfinished(state.clone()).await;
    state.drives.idle().await;
    assert!(runner.played().is_empty());
    let recorded = backend.lock().unwrap().tournaments[&id].clone();
    assert_eq!(recorded.state, TournamentState::Failed);
    assert!(
        recorded
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("pacman@1.0.0"),
        "{:?}",
        recorded.detail
    );
}

#[tokio::test]
async fn a_tournament_whose_engine_fails_is_marked_failed_with_its_reason() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let runner = ScriptedRunner {
        fail: Some("the wasm engine could not be built".to_string()),
        ..ScriptedRunner::default()
    };
    let state = arena(&url, 2, &runner);
    let app = router(&state);
    let id = submit_field(&app).await;
    state.drives.idle().await;

    let status = json(send(&app, "GET", &format!("/tournaments/{id}"), None).await).await;
    assert_eq!(status["state"], "failed");
    assert!(status.get("record").is_none());
    assert!(
        status["detail"]
            .as_str()
            .unwrap()
            .contains("the wasm engine could not be built"),
        "{status}"
    );
    assert!(events(&app, &id).await.is_empty());
}

#[tokio::test]
async fn a_field_naming_a_controller_twice_is_refused() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    let state = arena(&url, 2, &ScriptedRunner::default());
    let response = submit(&router(&state), &["random", "greedy-raider", "random"]).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(backend.lock().unwrap().tournaments.is_empty());
}

#[tokio::test]
async fn a_tournament_the_backend_does_not_record_is_not_accepted() {
    let backend = Backend::default();
    backend.lock().unwrap().refuse_pending = true;
    let url = spawn_stub_backend_with(backend.clone()).await;
    let runner = ScriptedRunner::default();
    let state = arena(&url, 1, &runner);
    let app = router(&state);
    let response = submit(&app, &FIELD).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(state.drives.active(), 0, "nothing is driven");
    assert!(runner.played().is_empty());
    // The permit and the live slot were given back.
    backend.lock().unwrap().refuse_pending = false;
    assert_eq!(submit(&app, &FIELD).await.status(), StatusCode::ACCEPTED);
    state.drives.idle().await;
}

#[tokio::test]
async fn the_event_stream_replays_the_recorded_matches_then_tails_the_rest() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend.clone()).await;
    // Hold the tournament before its third match until the test releases it.
    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let reached_tx = Arc::new(std::sync::Mutex::new(reached_tx));
    let runner = ScriptedRunner {
        hook: Some(Arc::new(move |count, _| {
            if count == 2 {
                reached_tx.lock().unwrap().send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        })),
        ..ScriptedRunner::default()
    };
    let state = arena(&url, 2, &runner);
    let app = router(&state);
    let id = submit_field(&app).await;
    tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
        .await
        .unwrap();
    eventually("the two matches recorded", || {
        backend.lock().unwrap().tournaments[&id]
            .record
            .matches
            .len()
            == 2
    })
    .await;

    let response = send(&app, "GET", &format!("/tournaments/{id}/events"), None).await;
    let mut body = response.into_body().into_data_stream();
    let mut text = String::new();
    while text.lines().count() < 2 {
        let chunk = body.next().await.unwrap().unwrap();
        text.push_str(std::str::from_utf8(&chunk).unwrap());
    }
    release_tx.send(()).unwrap();
    while let Some(chunk) = body.next().await {
        text.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
    }
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let played: Vec<u64> = lines
        .iter()
        .map(|l| l["played"].as_u64().unwrap())
        .collect();
    assert_eq!(played, vec![1, 2, 3, 4, 5, 6], "each match once, in order");
    state.drives.idle().await;
    assert_succeeded_with_every_replay(&backend, &id);
}

#[tokio::test]
async fn an_unknown_tournament_is_not_found() {
    let backend = Backend::default();
    let url = spawn_stub_backend_with(backend).await;
    let app = router(&arena(&url, 1, &ScriptedRunner::default()));
    assert_eq!(
        send(&app, "GET", "/tournaments/nope", None).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&app, "GET", "/tournaments/nope/events", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn the_retry_policy_tries_until_a_call_succeeds_or_its_attempts_run_out() {
    let policy = RetryPolicy {
        attempts: 3,
        initial: std::time::Duration::from_millis(1),
        max: std::time::Duration::from_millis(2),
        timeout: std::time::Duration::from_secs(5),
    };
    let mut calls = 0;
    let answered = policy
        .run("a call", || {
            calls += 1;
            let ok = calls == 2;
            async move { if ok { Ok(7) } else { Err("refused") } }
        })
        .await;
    assert_eq!(answered, Ok(7));
    let mut calls = 0;
    let refused: Result<(), String> = policy
        .run("a call", || {
            calls += 1;
            async { Err::<(), _>("refused") }
        })
        .await;
    assert_eq!(calls, 3);
    assert_eq!(refused, Err("a call: refused".to_string()));
}

#[tokio::test]
async fn a_call_that_does_not_answer_within_the_timeout_fails() {
    let policy = RetryPolicy {
        attempts: 2,
        initial: std::time::Duration::from_millis(1),
        max: std::time::Duration::from_millis(1),
        timeout: std::time::Duration::from_millis(20),
    };
    let mut calls = 0;
    let stalled: Result<(), String> = policy
        .run("a call", || {
            calls += 1;
            std::future::pending::<Result<(), String>>()
        })
        .await;
    assert_eq!(calls, 2);
    assert_eq!(stalled, Err("a call: no answer within 0s".to_string()));
}

#[tokio::test]
async fn a_refusal_is_not_retried() {
    let policy = RetryPolicy {
        attempts: 5,
        initial: std::time::Duration::from_millis(1),
        max: std::time::Duration::from_millis(1),
        timeout: std::time::Duration::from_secs(5),
    };
    let mut calls = 0;
    let refused: Result<(), ResolveError> = policy
        .run_while(
            "resolving",
            || {
                calls += 1;
                async { Err(ResolveError::Refused("unknown baseline `x`".to_string())) }
            },
            ResolveError::is_transient,
            |timeout| ResolveError::unanswered("resolving", timeout),
        )
        .await;
    assert_eq!(calls, 1);
    assert_eq!(
        refused,
        Err(ResolveError::Refused("unknown baseline `x`".to_string()))
    );
}
