//! Driving a recorded tournament to its end, and resuming the unfinished ones on
//! start (the arena overview's "Durable tournaments", "Resuming after a restart"
//! and "Shutdown").
//!
//! A drive holds a capacity permit, waiting for one when it is a resumed
//! tournament, and plays the pairings without a recorded result. Each match is
//! recorded as it completes, its replay first and then its summary, and only then
//! published to the live channel. Once every pairing has a result the drive
//! records the standings, which marks the tournament succeeded. An engine failure,
//! or a participant that no longer resolves, marks it failed. A write the backend
//! keeps refusing abandons the drive and leaves the tournament unfinished for the
//! next start to resume, as does a shutdown, which lets the match in progress
//! finish and be recorded first.

use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use test_cabinet_backend_client::{BackendClient, HttpBackendClient};
use test_cabinet_contracts::test_case::TestCaseVersion;
use test_cabinet_match_play::{
    MatchSummary, RecordedTournament, ResolvedController, TournamentPairing, TournamentTransition,
    tournament_standings,
};
use tokio::sync::OwnedSemaphorePermit;
use tracing::Instrument as _;

use crate::api::AppState;
use crate::arena_resolve::{ResolveError, resolve_controller};
use crate::shutdown::DriveGuard;
use crate::tournaments::{LiveTournament, TournamentProgress};

/// One tournament match as the runner reports it: its summary, and its replay as
/// the JSON the backend stores, when one was produced.
#[derive(Debug, Clone)]
pub struct CompletedMatch {
    /// The match summary.
    pub summary: MatchSummary,
    /// The replay JSON, or `None` for a load forfeit.
    pub replay: Option<Vec<u8>>,
}

/// What plays a tournament's pairings. The arena plays them through the match
/// runner's wasm hosts ([`WasmRunner`]); a test stands in its own.
pub trait TournamentRunner: Send + Sync + 'static {
    /// Play `pairings` between `participants`, reporting each match to `on_match`
    /// as it completes and stopping when it answers [`ControlFlow::Break`]. An
    /// error is a failure the tournament cannot complete past.
    fn run(
        &self,
        test_case: &TestCaseVersion,
        participants: &[ResolvedController],
        pairings: &[TournamentPairing],
        on_match: &mut dyn FnMut(CompletedMatch) -> ControlFlow<()>,
    ) -> Result<(), String>;
}

/// The match runner over the wasm hosts.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasmRunner;

impl TournamentRunner for WasmRunner {
    fn run(
        &self,
        test_case: &TestCaseVersion,
        participants: &[ResolvedController],
        pairings: &[TournamentPairing],
        on_match: &mut dyn FnMut(CompletedMatch) -> ControlFlow<()>,
    ) -> Result<(), String> {
        test_cabinet_match_play::run_tournament(test_case, participants, pairings, |played| {
            on_match(CompletedMatch {
                summary: played.summary,
                replay: played.replay.map(|replay| replay.to_json().into_bytes()),
            })
        })
        .map_err(|err| err.to_string())
    }
}

/// How a call to the backend is retried: `attempts` tries, each given up after
/// `timeout`, waiting `initial` after the first failure and doubling up to `max`
/// after each later one.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// How many times a call is tried before it is given up.
    pub attempts: u32,
    /// The wait after the first failure.
    pub initial: Duration,
    /// The longest wait between two tries.
    pub max: Duration,
    /// How long one try may take before it counts as failed, so a backend that
    /// stops answering never holds a drive.
    pub timeout: Duration,
}

impl Default for RetryPolicy {
    /// About a minute of retrying: waits of 1, 2, 4, 8 and then 16 seconds.
    fn default() -> Self {
        Self {
            attempts: 8,
            initial: Duration::from_secs(1),
            max: Duration::from_secs(16),
            timeout: Duration::from_secs(60),
        }
    }
}

impl RetryPolicy {
    /// Call `call` until it succeeds or the attempts run out, answering the last
    /// error, prefixed with `what`.
    pub async fn run<T, E, F, Fut>(&self, what: &str, call: F) -> Result<T, String>
    where
        E: std::fmt::Display,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut call = call;
        self.run_while(
            what,
            || {
                let attempt = call();
                async move { attempt.await.map_err(|err| err.to_string()) }
            },
            |_| true,
            |timeout| format!("no answer within {}s", timeout.as_secs()),
        )
        .await
        .map_err(|err| format!("{what}: {err}"))
    }

    /// Call `call` until it succeeds, fails with an error `transient` does not
    /// accept, or the attempts run out, answering the last error. A try that
    /// takes longer than the timeout fails with `timed_out`'s error.
    pub async fn run_while<T, E, F, Fut>(
        &self,
        what: &str,
        mut call: F,
        transient: impl Fn(&E) -> bool,
        timed_out: impl Fn(Duration) -> E,
    ) -> Result<T, E>
    where
        E: std::fmt::Display,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut wait = self.initial;
        let mut attempt = 1;
        loop {
            let result = tokio::time::timeout(self.timeout, call())
                .await
                .unwrap_or_else(|_| Err(timed_out(self.timeout)));
            match result {
                Ok(value) => return Ok(value),
                Err(err) if attempt >= self.attempts.max(1) || !transient(&err) => {
                    return Err(err);
                }
                Err(err) => {
                    tracing::warn!(attempt, error = %err, "{what} failed; retrying");
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(self.max);
                    attempt += 1;
                }
            }
        }
    }
}

/// What a drive starts from: a tournament accepted just now, whose inputs the
/// submit already resolved and whose permit it already holds, or a recorded one
/// resumed on start, which resolves its inputs and waits for a permit itself.
pub enum DriveStart {
    /// Accepted by `POST /tournaments`.
    Accepted {
        /// The resolved test case.
        test_case: Box<TestCaseVersion>,
        /// The resolved participants.
        participants: Vec<ResolvedController>,
        /// The capacity permit the submit acquired.
        permit: OwnedSemaphorePermit,
    },
    /// Resumed from the backend's record.
    Resumed,
}

/// Spawn the drive of `tournament`, counted in the arena's drives until it ends.
pub fn spawn_drive(
    state: AppState,
    tournament: RecordedTournament,
    live: LiveTournament,
    start: DriveStart,
) {
    let guard = state.drives.enter();
    let span = tracing::info_span!(
        "tournament.drive",
        tournament_id = %tournament.record.id,
        test_case = %tournament.record.test_case_slug,
        participants = tournament.record.participants.len(),
    );
    tokio::spawn(drive(state, tournament, live, start, guard).instrument(span));
}

/// Drive `tournament` until it ends, is abandoned or the arena shuts down.
async fn drive(
    state: AppState,
    tournament: RecordedTournament,
    live: LiveTournament,
    start: DriveStart,
    _guard: DriveGuard,
) {
    let client = state.backend();
    let id = tournament.record.id.clone();
    let (test_case, participants, _permit) = match start {
        DriveStart::Accepted {
            test_case,
            participants,
            permit,
        } => (*test_case, participants, permit),
        DriveStart::Resumed => {
            // A resumed tournament queues for a permit like any other, so a restart
            // never runs more at once than the cap allows.
            let permit = tokio::select! {
                permit = state.executor.acquire_queued() => permit,
                () = state.shutdown.draining() => return,
            };
            let Some(permit) = permit else { return };
            let resolved = tokio::select! {
                resolved = resolve_inputs(&state, &client, &tournament) => resolved,
                () = state.shutdown.draining() => return,
            };
            match resolved {
                Ok((test_case, participants)) => (test_case, participants, permit),
                Err(ResolveError::Refused(reason)) => {
                    fail(&state, &client, &id, reason).await;
                    return;
                }
                Err(ResolveError::Unavailable(err)) => {
                    tracing::error!(error = %err, "abandoning the tournament; the next start resumes it");
                    return;
                }
            }
        }
    };
    if state.shutdown.is_draining() {
        return;
    }
    if let Err(err) = state
        .retry
        .run("marking the tournament running", || {
            client.transition_tournament(&id, &TournamentTransition::Running)
        })
        .await
    {
        tracing::error!(error = %err, "abandoning the tournament; the next start resumes it");
        return;
    }

    let total = tournament.pairings.len();
    let mut recorded = tournament.record.matches.clone();
    let unplayed = tournament.unplayed_pairings();
    if !unplayed.is_empty() {
        let played = play(
            &state,
            client.clone(),
            &id,
            test_case,
            participants,
            unplayed,
            recorded.len(),
            total,
            &live,
        )
        .await;
        recorded.extend(played.recorded);
        match played.end {
            PlayEnd::Played => {}
            PlayEnd::Failed(reason) => {
                fail(&state, &client, &id, reason).await;
                return;
            }
            PlayEnd::Abandoned(err) => {
                tracing::error!(error = %err, "abandoning the tournament; the next start resumes it");
                return;
            }
        }
    }
    if recorded.len() < total {
        // Stopped by a shutdown: the next start plays the rest.
        tracing::info!(
            recorded = recorded.len(),
            total,
            "stopping the tournament for shutdown"
        );
        return;
    }

    let standings = tournament_standings(&tournament.record.participants, &recorded);
    let succeeded = TournamentTransition::Succeeded { standings };
    if let Err(err) = state
        .retry
        .run("recording the standings", || {
            client.transition_tournament(&id, &succeeded)
        })
        .await
    {
        tracing::error!(error = %err, "abandoning the tournament; the next start resumes it");
    }
}

/// Resolve a resumed tournament's test case and participants, retrying a backend
/// that does not answer. A refusal is the reason the tournament cannot complete;
/// a backend that still does not answer leaves it for the next start.
async fn resolve_inputs(
    state: &AppState,
    client: &HttpBackendClient,
    tournament: &RecordedTournament,
) -> Result<(TestCaseVersion, Vec<ResolvedController>), ResolveError> {
    let record = &tournament.record;
    let what = format!(
        "resolving {}@{}",
        record.test_case_slug, record.test_case_version
    );
    let test_case = state
        .retry
        .run_while(
            &what,
            || async {
                client
                    .resolve_version(&record.test_case_slug, &record.test_case_version)
                    .await
                    .map_err(|err| ResolveError::from_client(&what, &err))
            },
            ResolveError::is_transient,
            |timeout| ResolveError::unanswered(&what, timeout),
        )
        .await?;
    let mut participants = Vec::with_capacity(record.participants.len());
    for controller in &record.participants {
        participants.push(
            state
                .retry
                .run_while(
                    "resolving a participant",
                    || {
                        resolve_controller(
                            client,
                            &record.test_case_slug,
                            &record.test_case_version,
                            controller,
                        )
                    },
                    ResolveError::is_transient,
                    |timeout| ResolveError::unanswered("resolving a participant", timeout),
                )
                .await?,
        );
    }
    Ok((test_case, participants))
}

/// How playing a tournament's pairings ended.
enum PlayEnd {
    /// Every pairing given was played and recorded, or a shutdown stopped it
    /// after recording the match in progress.
    Played,
    /// The runner failed; the tournament cannot complete.
    Failed(String),
    /// A write the backend kept refusing; the tournament is left unfinished.
    Abandoned(String),
}

/// What playing a tournament's pairings came to.
struct Played {
    /// The summaries recorded, in the order they were played.
    recorded: Vec<MatchSummary>,
    /// How it ended.
    end: PlayEnd,
}

/// Play `pairings` off the async runtime, recording each match as it completes
/// and publishing it to the live channel once recorded. Stops before the next
/// pairing once the arena is shutting down.
#[allow(clippy::too_many_arguments)]
async fn play(
    state: &AppState,
    client: HttpBackendClient,
    id: &str,
    test_case: TestCaseVersion,
    participants: Vec<ResolvedController>,
    pairings: Vec<TournamentPairing>,
    already_recorded: usize,
    total: usize,
    live: &LiveTournament,
) -> Played {
    let handle = tokio::runtime::Handle::current();
    let runner = state.runner.clone();
    let retry = state.retry;
    let shutdown = state.shutdown.clone();
    let id = id.to_string();
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let blocking = tokio::task::spawn_blocking(move || {
        let mut recorded: Vec<MatchSummary> = Vec::new();
        let mut abandoned: Option<String> = None;
        let result = runner.run(&test_case, &participants, &pairings, &mut |completed| {
            let written = handle.block_on(record_match(&client, &retry, &id, &completed));
            match written {
                Ok(()) => {
                    recorded.push(completed.summary.clone());
                    let _ = progress_tx.send(TournamentProgress {
                        played: already_recorded + recorded.len(),
                        total,
                        summary: completed.summary,
                    });
                    if shutdown.is_draining() {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                }
                Err(err) => {
                    abandoned = Some(err);
                    ControlFlow::Break(())
                }
            }
        });
        let end = match (abandoned, result) {
            (Some(err), _) => PlayEnd::Abandoned(err),
            (None, Err(err)) => PlayEnd::Failed(format!("running the tournament: {err}")),
            (None, Ok(())) => PlayEnd::Played,
        };
        Played { recorded, end }
    });
    // Publish each recorded match as the blocking task reports it; the channel
    // closes when the task ends.
    while let Some(progress) = progress_rx.recv().await {
        live.push(progress);
    }
    match blocking.await {
        Ok(played) => played,
        Err(err) => Played {
            recorded: Vec::new(),
            end: PlayEnd::Failed(format!("the tournament task panicked: {err}")),
        },
    }
}

/// Record one completed match: its replay first, then its summary, so a match
/// with a recorded result always has its replay stored.
async fn record_match(
    client: &HttpBackendClient,
    retry: &RetryPolicy,
    id: &str,
    completed: &CompletedMatch,
) -> Result<(), String> {
    let match_id = &completed.summary.match_id;
    if let Some(replay) = &completed.replay {
        retry
            .run(&format!("storing the replay of `{match_id}`"), || {
                client.record_tournament_replay(id, match_id, replay.clone())
            })
            .await?;
    }
    retry
        .run(&format!("recording `{match_id}`"), || {
            client.record_tournament_match(id, &completed.summary)
        })
        .await
}

/// Mark the tournament failed with `reason`. A backend that keeps refusing leaves
/// it unfinished, and the next start resumes it.
async fn fail(state: &AppState, client: &HttpBackendClient, id: &str, reason: String) {
    tracing::warn!(reason = %reason, "the tournament cannot complete");
    let failed = TournamentTransition::Failed { detail: reason };
    if let Err(err) = state
        .retry
        .run("marking the tournament failed", || {
            client.transition_tournament(id, &failed)
        })
        .await
    {
        tracing::error!(error = %err, "the tournament stays unfinished");
    }
}

/// Resume every tournament the backend holds as queued or running, retrying until
/// the backend answers or the arena shuts down. A tournament this process is
/// already driving is left to its drive.
pub async fn resume_unfinished(state: AppState) {
    let client = state.backend();
    let mut wait = state.retry.initial;
    let unfinished = loop {
        let listed = tokio::time::timeout(state.retry.timeout, client.unfinished_tournaments())
            .await
            .map_err(|_| format!("no answer within {}s", state.retry.timeout.as_secs()))
            .and_then(|listed| listed.map_err(|err| err.to_string()));
        match listed {
            Ok(unfinished) => break unfinished,
            Err(err) => {
                tracing::warn!(error = %err, "listing the unfinished tournaments failed; retrying");
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = state.shutdown.draining() => return,
                }
                wait = (wait * 2).min(state.retry.max);
            }
        }
    };
    let mut resumed = 0usize;
    for tournament in unfinished {
        let Some(live) = state.tournaments.register(&tournament.record.id) else {
            continue;
        };
        resumed += 1;
        spawn_drive(state.clone(), tournament, live, DriveStart::Resumed);
    }
    tracing::info!(resumed, "resumed the unfinished tournaments");
}

/// The default runner, shared.
pub fn wasm_runner() -> Arc<dyn TournamentRunner> {
    Arc::new(WasmRunner)
}

#[cfg(test)]
#[path = "drive.test.rs"]
mod tests;
