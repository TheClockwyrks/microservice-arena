//! End-to-end tests for the arena service's match routes, driving the real router
//! against the [stub backend](crate::stub_backend).
//!
//! With invalid wasm a match still returns a `MatchSummary` (a forfeit, `replay:
//! null`), which is all the happy-path test asserts — no real wasm execution.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crate::auth::LOCAL_IDENTITY_HEADER;
use crate::stub_backend::{SERVICE_TOKEN, arena_state, spawn_stub_backend};

/// Build the arena router pointed at `backend_url`, with the given executor cap,
/// presenting [`SERVICE_TOKEN`] to the backend.
fn app(backend_url: &str, cap: usize) -> Router {
    app_with_token(backend_url, cap, Some(SERVICE_TOKEN))
}

/// Build the arena router pointed at `backend_url`, presenting `token` (or none),
/// playing matches through the real runner.
fn app_with_token(backend_url: &str, cap: usize, token: Option<&str>) -> Router {
    super::router(arena_state(
        backend_url,
        cap,
        token,
        crate::drive::wasm_runner(),
    ))
}

#[tokio::test]
async fn run_match_between_baselines_returns_a_summary() {
    let backend = spawn_stub_backend().await;
    let app = app(&backend, 2);

    let body = serde_json::json!({
        "testCase": "pacman",
        "version": "1.0.0",
        "red": { "id": "random", "kind": "baseline" },
        "blue": { "id": "greedy-raider", "kind": "baseline" }
    });
    let request = Request::builder()
        .method("POST")
        .uri("/matches")
        .header(LOCAL_IDENTITY_HEADER, "dev@local")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // The summary is always present; with invalid wasm the replay is null (a
    // clean forfeit), which is exactly the shape the console reads.
    assert!(parsed.get("summary").is_some(), "got: {parsed}");
    assert!(
        parsed.get("replay").is_some(),
        "replay key present: {parsed}"
    );
    assert!(
        parsed["replay"].is_null(),
        "invalid wasm forfeits: {parsed}"
    );
}

#[tokio::test]
async fn controllers_lists_baselines_and_pushed() {
    let backend = spawn_stub_backend().await;
    let app = app(&backend, 2);

    let request = Request::builder()
        .method("GET")
        .uri("/matches/controllers?testCase=pacman")
        .header(LOCAL_IDENTITY_HEADER, "dev@local")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let controllers = parsed["controllers"].as_array().unwrap();
    // The committed arena opponents (baselines + hidden references) plus the one
    // pushed controller the stub lists.
    let ids: Vec<&str> = controllers
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"random"), "baselines present: {ids:?}");
    assert!(ids.contains(&"run-xyz"), "pushed merged in: {ids:?}");
}

#[tokio::test]
async fn an_unknown_baseline_is_rejected_400() {
    let backend = spawn_stub_backend().await;
    let app = app(&backend, 2);

    let body = serde_json::json!({
        "testCase": "pacman",
        "version": "1.0.0",
        "red": { "id": "no-such-baseline", "kind": "baseline" },
        "blue": { "id": "random", "kind": "baseline" }
    });
    let request = Request::builder()
        .method("POST")
        .uri("/matches")
        .header(LOCAL_IDENTITY_HEADER, "dev@local")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_run_local_controller_is_rejected_400() {
    let backend = spawn_stub_backend().await;
    let app = app(&backend, 2);

    // A `run` (local-out-dir) controller is not resolvable in the service topology.
    let body = serde_json::json!({
        "testCase": "pacman",
        "version": "1.0.0",
        "red": { "id": "run-local", "kind": "run" },
        "blue": { "id": "random", "kind": "baseline" }
    });
    let request = Request::builder()
        .method("POST")
        .uri("/matches")
        .header(LOCAL_IDENTITY_HEADER, "dev@local")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_match_without_the_service_token_is_refused_by_the_backend() {
    // The backend answers no caller without an identity, reads included, so an arena
    // configured without the service token cannot even resolve the version.
    let backend = spawn_stub_backend().await;
    let app = app_with_token(&backend, 2, None);

    let body = serde_json::json!({
        "testCase": "pacman",
        "version": "1.0.0",
        "red": { "id": "random", "kind": "baseline" },
        "blue": { "id": "greedy-raider", "kind": "baseline" }
    });
    let request = Request::builder()
        .method("POST")
        .uri("/matches")
        .header(LOCAL_IDENTITY_HEADER, "dev@local")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        text.contains("401"),
        "the backend's refusal is surfaced: {text}"
    );
}
