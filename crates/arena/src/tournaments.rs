//! The tournaments this arena process is driving, and their live progress.
//!
//! A tournament's record lives on the backend (the arena overview's "Durable
//! tournaments"): its state, its pairings and every recorded match. What lives
//! here is only what a process adds while it drives one: a live channel each
//! completed match is published to as it is recorded, which
//! `GET /tournaments/{id}/events` tails after replaying the recorded matches.
//! The registry is also how the arena knows which tournaments it is already
//! driving, so a resume never drives one twice.
//!
//! The channel is per-pod, which is one reason the Deployment runs a single
//! replica.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use test_cabinet_match_play::{
    MatchSummary, RecordedTournament, TournamentRecord, TournamentState,
};
use tokio::sync::broadcast;

/// One match's completion, streamed as the field plays out so the UI can show a
/// live tournament board. Carries the full [`MatchSummary`] plus how far along the
/// field is.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TournamentProgress {
    /// How many matches have a recorded result, this one included (1-based).
    pub played: usize,
    /// How many matches the field has in total (`C(n,2)`).
    pub total: usize,
    /// The match that just completed.
    pub summary: MatchSummary,
}

/// A tournament's status as `GET /tournaments/{id}` answers it, read from the
/// backend's record.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TournamentStatus {
    /// The tournament id.
    pub id: String,
    /// Lifecycle state.
    pub state: TournamentState,
    /// The finished record, present once `state` is `succeeded`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<TournamentRecord>,
    /// A failure reason, present when `state` is `failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl From<RecordedTournament> for TournamentStatus {
    fn from(recorded: RecordedTournament) -> Self {
        Self {
            id: recorded.record.id.clone(),
            state: recorded.state,
            record: (recorded.state == TournamentState::Succeeded).then_some(recorded.record),
            detail: recorded.detail,
        }
    }
}

/// An item delivered to a tournament's live stream.
#[derive(Debug, Clone)]
pub enum StreamItem {
    /// A recorded match.
    Progress(Box<TournamentProgress>),
    /// This process stopped driving the tournament; no further items will arrive.
    Done,
}

/// The capacity of each tournament's live progress channel.
const PROGRESS_CHANNEL_CAPACITY: usize = 1024;

/// The tournaments this process is driving, keyed by id. `Arc`-backed so the
/// router state and each drive share one registry.
#[derive(Debug, Clone, Default)]
pub struct TournamentRegistry {
    live: Arc<Mutex<HashMap<String, broadcast::Sender<StreamItem>>>>,
}

impl TournamentRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Start driving tournament `id`: open its live channel and answer the guard
    /// that publishes to it and closes it on drop. `None` when this process is
    /// already driving it.
    pub fn register(&self, id: &str) -> Option<LiveTournament> {
        let mut live = self
            .live
            .lock()
            .expect("tournament registry mutex poisoned");
        if live.contains_key(id) {
            return None;
        }
        let (tx, _rx) = broadcast::channel(PROGRESS_CHANNEL_CAPACITY);
        live.insert(id.to_string(), tx.clone());
        Some(LiveTournament {
            id: id.to_string(),
            tx,
            registry: self.clone(),
        })
    }

    /// Subscribe to tournament `id`'s live channel, or `None` when this process is
    /// not driving it. Taken under the registry's lock, which closing a channel
    /// also takes, so a subscriber either receives [`StreamItem::Done`] or finds no
    /// channel.
    pub fn subscribe(&self, id: &str) -> Option<broadcast::Receiver<StreamItem>> {
        self.live
            .lock()
            .expect("tournament registry mutex poisoned")
            .get(id)
            .map(broadcast::Sender::subscribe)
    }

    /// Whether this process is driving tournament `id`.
    pub fn is_live(&self, id: &str) -> bool {
        self.live
            .lock()
            .expect("tournament registry mutex poisoned")
            .contains_key(id)
    }

    /// Close tournament `id`'s channel: remove it and tell every subscriber.
    fn close(&self, id: &str) {
        let mut live = self
            .live
            .lock()
            .expect("tournament registry mutex poisoned");
        if let Some(tx) = live.remove(id) {
            let _ = tx.send(StreamItem::Done);
        }
    }
}

/// The live channel of a tournament this process is driving. Dropping it closes
/// the channel, so every way a drive ends ends its streams.
#[derive(Debug)]
pub struct LiveTournament {
    id: String,
    tx: broadcast::Sender<StreamItem>,
    registry: TournamentRegistry,
}

impl LiveTournament {
    /// The tournament id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Publish a recorded match to every live subscriber.
    pub fn push(&self, progress: TournamentProgress) {
        let _ = self.tx.send(StreamItem::Progress(Box::new(progress)));
    }
}

impl Drop for LiveTournament {
    fn drop(&mut self) {
        self.registry.close(&self.id);
    }
}

#[cfg(test)]
#[path = "tournaments.test.rs"]
mod tests;
