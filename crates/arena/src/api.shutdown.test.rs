//! Tests for how the HTTP surface stops: readiness fails while liveness holds, and
//! a new match or tournament is refused with `503`, once shutdown begins. How a
//! live progress stream ends is tested with the drives, in `drive.test.rs`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::*;
use crate::auth::LOCAL_IDENTITY_HEADER;
use crate::shutdown::Shutdown;

/// The paths of the two routes that start CPU-bound work, at the root and under
/// the edge prefix.
const WORK: [&str; 4] = [
    "/matches",
    "/tournaments",
    "/arena/matches",
    "/arena/tournaments",
];

/// An arena whose backend is unreachable (no test here reaches it) with its
/// shutdown signal handed back.
fn arena() -> (Router, Shutdown) {
    let state = crate::stub_backend::arena_state(
        "http://127.0.0.1:0",
        1,
        None,
        crate::drive::wasm_runner(),
    );
    let shutdown = state.shutdown.clone();
    (router(state), shutdown)
}

async fn get_status(app: &Router, uri: &str) -> StatusCode {
    let request = Request::get(uri).body(Body::empty()).unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

/// `POST uri` with an empty JSON object, as a person, and the response's status
/// and error code (if any).
async fn post_work(app: &Router, uri: &str) -> (StatusCode, String) {
    let request = Request::post(uri)
        .header(LOCAL_IDENTITY_HEADER, "dev@local")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let code = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|body| body["error"]["code"].as_str().map(str::to_string))
        .unwrap_or_default();
    (status, code)
}

#[tokio::test]
async fn readiness_fails_once_shutdown_begins_while_liveness_holds() {
    let (app, shutdown) = arena();
    for uri in ["/readyz", "/arena/readyz", "/healthz", "/arena/healthz"] {
        assert_eq!(get_status(&app, uri).await, StatusCode::OK, "{uri}");
    }
    shutdown.begin();
    for uri in ["/readyz", "/arena/readyz"] {
        assert_eq!(
            get_status(&app, uri).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "{uri}"
        );
    }
    for uri in ["/healthz", "/arena/healthz"] {
        assert_eq!(get_status(&app, uri).await, StatusCode::OK, "{uri}");
    }
}

#[tokio::test]
async fn new_matches_and_tournaments_are_refused_once_shutdown_begins() {
    let (app, shutdown) = arena();
    for uri in WORK {
        // Served: the handler reads the body, which is not a match or tournament.
        let (status, code) = post_work(&app, uri).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {code}");
    }
    shutdown.begin();
    for uri in WORK {
        let (status, code) = post_work(&app, uri).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}");
        assert_eq!(code, "stopping", "{uri}");
    }
}

#[tokio::test]
async fn a_refusal_still_needs_an_identity() {
    let (app, shutdown) = arena();
    shutdown.begin();
    let request = Request::post("/matches").body(Body::from("{}")).unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
