//! Tests for how the HTTP surface stops: readiness fails while liveness holds, new
//! work is served through the drain delay and refused with `503` after it, and a
//! request is judged by when it arrived. How a live progress stream ends, and what
//! becomes of a tournament accepted during the delay, are tested with the drives,
//! in `drive.test.rs`.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use tokio::sync::{Notify, mpsc};
use tower::ServiceExt;

use super::*;
use crate::access::tests::keys::{KEY_A, key_set, person_claims};
use crate::access::{ASSERTION_HEADER, AccessVerifier};
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
    arena_with(None)
}

/// [`arena`] verifying assertions with `access` when given one.
fn arena_with(access: AccessMode) -> (Router, Shutdown) {
    let state = AppState {
        access,
        ..crate::stub_backend::arena_state(
            "http://127.0.0.1:0",
            1,
            None,
            crate::drive::wasm_runner(),
        )
    };
    let shutdown = state.shutdown.clone();
    (router(state), shutdown)
}

async fn get_status(app: &Router, uri: &str) -> StatusCode {
    let request = Request::get(uri).body(Body::empty()).unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

/// `POST uri` with an empty JSON object, carrying the identity header `identity`,
/// and the response's status and error code (if any).
async fn post_work(app: &Router, uri: &str, identity: (&str, &str)) -> (StatusCode, String) {
    let request = Request::post(uri)
        .header(identity.0, identity.1)
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
async fn new_work_is_served_through_the_drain_delay() {
    let (app, shutdown) = arena();
    shutdown.begin();
    for uri in WORK {
        // Served: the handler reads the body, which is not a match or tournament.
        let (status, code) = post_work(&app, uri, (LOCAL_IDENTITY_HEADER, "dev@local")).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {code}");
    }
}

#[tokio::test]
async fn new_work_is_refused_once_the_drain_delay_ends() {
    let (app, shutdown) = arena();
    shutdown.begin();
    shutdown.refuse();
    for uri in WORK {
        let (status, code) = post_work(&app, uri, (LOCAL_IDENTITY_HEADER, "dev@local")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{uri}");
        assert_eq!(code, "stopping", "{uri}");
    }
}

/// The audience the verifying arena checks for.
const AUDIENCE: &str = "test-audience-tag";

/// A team's certificates endpoint that reports each fetch on the receiver and
/// answers only once `release` is notified, holding an identity check open.
async fn serve_held_certs(release: Arc<Notify>) -> (String, mpsc::UnboundedReceiver<()>) {
    let (fetched_tx, fetched_rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/cdn-cgi/access/certs",
        get(move || {
            let release = release.clone();
            let fetched = fetched_tx.clone();
            async move {
                let _ = fetched.send(());
                release.notified().await;
                axum::Json(key_set(&[&KEY_A]))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/cdn-cgi/access/certs"), fetched_rx)
}

#[tokio::test]
async fn a_request_that_arrived_before_the_delay_ended_is_served() {
    let release = Arc::new(Notify::new());
    let (certs, mut fetched) = serve_held_certs(release.clone()).await;
    let (app, shutdown) = arena_with(Some(Arc::new(AccessVerifier::with_certs_url(
        certs,
        AUDIENCE,
        reqwest::Client::new(),
        Duration::ZERO,
    ))));
    shutdown.begin();

    let assertion = KEY_A.sign(&person_claims("ada@example.com", AUDIENCE));
    let request = tokio::spawn({
        let app = app.clone();
        async move { post_work(&app, "/matches", (ASSERTION_HEADER, &assertion)).await }
    });
    // The request has arrived and its identity check is waiting on the keys.
    tokio::time::timeout(Duration::from_secs(10), fetched.recv())
        .await
        .expect("the identity check fetches the keys")
        .unwrap();
    shutdown.refuse();
    release.notify_one();

    let (status, code) = request.await.unwrap();
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "served: {code}");

    // One arriving now is refused. Its check reads the cached keys; should it fetch
    // them again, a stored release answers it.
    release.notify_one();
    let assertion = KEY_A.sign(&person_claims("ada@example.com", AUDIENCE));
    let (status, code) = post_work(&app, "/matches", (ASSERTION_HEADER, &assertion)).await;
    assert_eq!(
        (status, code.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, "stopping")
    );
}

#[tokio::test]
async fn a_refusal_still_needs_an_identity() {
    let (app, shutdown) = arena();
    shutdown.refuse();
    let request = Request::post("/matches").body(Body::from("{}")).unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
