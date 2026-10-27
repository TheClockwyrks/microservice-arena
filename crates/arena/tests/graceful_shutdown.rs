//! The real `tcab-arena` process drains on `SIGTERM`: readiness answers `503`
//! through the drain delay while a slow request is in flight, a new match is
//! refused, the server then stops accepting, the request still completes, and the
//! process exits 0. An interrupt stops it with status 0 too.
//!
//! The slow request is `GET /matches/controllers`, which asks the backend for the
//! case's pushed controllers. The backend here is a stub that holds that answer
//! until the test releases it, so the request stays in flight exactly as long as
//! the test needs, with no timing assumption about how long anything takes.

#![cfg(unix)]

use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::routing::get;
use tokio::sync::{Notify, mpsc};

/// The identity header an arena with no Access application reads a person from.
const IDENTITY: &str = "cf-access-authenticated-user-email";

/// The id of the pushed controller the stub backend lists.
const PUSHED_ID: &str = "run-held-by-the-stub";

/// How long any one wait in this test may take before it fails. Generous: each
/// wait ends on an event, never on the bound.
const BOUND: Duration = Duration::from_secs(30);

/// The arena child process, killed if the test ends while it still runs.
struct Arena {
    child: Child,
    base: String,
}

impl Drop for Arena {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

impl Arena {
    /// Spawn the arena binary against `backend` on a free loopback port.
    fn spawn(backend: &str) -> Self {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let child = Command::new(env!("CARGO_BIN_EXE_tcab-arena"))
            // A directory with no `.env.arena`, so only this environment applies.
            .current_dir(env!("CARGO_TARGET_TMPDIR"))
            .env("TCAB_ARENA_BIND", format!("127.0.0.1:{port}"))
            .env("TCAB_BACKEND_URL", backend)
            .env_remove("TCAB_ACCESS_TEAM_DOMAIN")
            .env_remove("TCAB_ACCESS_AUDIENCE")
            .env_remove("TCAB_BACKEND_SERVICE_TOKEN")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("the arena binary starts");
        Self {
            child,
            base: format!("http://127.0.0.1:{port}"),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Send the process `signal` (`TERM` or `INT`).
    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("kill runs");
        assert!(status.success(), "kill -{signal} failed");
    }

    fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Wait for the process to exit, failing the test past [`BOUND`].
    async fn exit_status(&mut self) -> ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(started.elapsed() < BOUND, "the arena did not exit");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// A client that opens a fresh connection per request, so every probe tests
/// whether the arena still accepts.
fn fresh_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .timeout(BOUND)
        .build()
        .unwrap()
}

/// A stub backend whose pushed-controller listing reports each request's arrival on
/// the returned receiver and answers once `release` is notified.
async fn spawn_stub_backend(release: Arc<Notify>) -> (String, mpsc::UnboundedReceiver<()>) {
    let (arrived_tx, arrived_rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/adversarial/controllers",
        get(move || {
            let release = release.clone();
            let arrived = arrived_tx.clone();
            async move {
                let released = release.notified();
                let _ = arrived.send(());
                released.await;
                axum::Json(serde_json::json!({
                    "controllers": [{ "id": PUSHED_ID, "kind": "pushed" }]
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), arrived_rx)
}

/// Poll `GET path` on fresh connections until `done` accepts the outcome (a status,
/// or `None` for a connection the arena refused), failing past [`BOUND`].
async fn poll_until(
    arena: &Arena,
    path: &str,
    what: &str,
    done: impl Fn(Option<reqwest::StatusCode>) -> bool,
) {
    let client = fresh_client();
    let started = Instant::now();
    loop {
        let outcome = match client.get(arena.url(path)).send().await {
            Ok(response) => Some(response.status()),
            Err(err) if err.is_connect() => None,
            Err(err) => panic!("GET {path}: {err}"),
        };
        if done(outcome) {
            return;
        }
        assert!(started.elapsed() < BOUND, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_drains_the_request_in_flight_and_exits_zero() {
    let release = Arc::new(Notify::new());
    let (backend, mut arrived) = spawn_stub_backend(release.clone()).await;
    let mut arena = Arena::spawn(&backend);
    poll_until(&arena, "/readyz", "the arena to become ready", |status| {
        status == Some(reqwest::StatusCode::OK)
    })
    .await;

    // The slow request: in flight until the stub backend is released.
    let in_flight = tokio::spawn({
        let url = arena.url("/matches/controllers?testCase=pacman");
        async move {
            let response = fresh_client()
                .get(url)
                .header(IDENTITY, "dev@local")
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            Ok::<_, reqwest::Error>((status, body))
        }
    });
    tokio::time::timeout(BOUND, arrived.recv())
        .await
        .expect("the request reaches the backend")
        .expect("the stub backend is up");

    arena.signal("TERM");

    // Readiness fails at once while the arena still accepts: a 503, not a refused
    // connection, during the drain delay.
    poll_until(&arena, "/readyz", "readiness to answer 503", |status| {
        assert!(status.is_some(), "the arena stopped accepting before 503");
        status == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
    })
    .await;
    // Liveness holds, and a new match is refused, while it drains.
    let client = fresh_client();
    let health = client.get(arena.url("/healthz")).send().await.unwrap();
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    let refused = client
        .post(arena.url("/matches"))
        .header(IDENTITY, "dev@local")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let refused: serde_json::Value = refused.json().await.unwrap();
    assert_eq!(refused["error"]["code"], "stopping", "{refused}");

    // After the drain delay it stops accepting, yet stays up for the request in
    // flight.
    poll_until(&arena, "/readyz", "the arena to stop accepting", |status| {
        status.is_none()
    })
    .await;
    assert!(
        arena.is_running(),
        "the arena exited with a request in flight"
    );
    assert!(!in_flight.is_finished());

    release.notify_one();
    let (status, body) = tokio::time::timeout(BOUND, in_flight)
        .await
        .expect("the request in flight completes")
        .unwrap()
        .expect("the request in flight is answered");
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert!(
        body.contains(PUSHED_ID),
        "the backend's answer arrives: {body}"
    );

    let exit = arena.exit_status().await;
    assert!(exit.success(), "the arena exits 0, got {exit}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupt_stops_the_arena_with_status_zero() {
    let release = Arc::new(Notify::new());
    let (backend, _arrived) = spawn_stub_backend(release).await;
    let mut arena = Arena::spawn(&backend);
    poll_until(&arena, "/readyz", "the arena to become ready", |status| {
        status == Some(reqwest::StatusCode::OK)
    })
    .await;

    arena.signal("INT");
    let exit = arena.exit_status().await;
    assert!(exit.success(), "the arena exits 0, got {exit}");
}
