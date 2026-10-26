//! Tests for the live-tournament registry.

use test_cabinet_contracts::validation::AdversarialOutcome;
use test_cabinet_match_play::match_id;

use super::*;

fn summary() -> MatchSummary {
    MatchSummary {
        match_id: match_id("a", "b"),
        red_id: "a".to_string(),
        blue_id: "b".to_string(),
        winner: None,
        win_type: "time_limit".to_string(),
        outcome_for_red: AdversarialOutcome::Draw,
        red_score: 0,
        blue_score: 0,
        ticks: 1,
        red_kills: 0,
        blue_kills: 0,
        red_fuel: 0,
        blue_fuel: 0,
        replay_key: None,
        detail: None,
    }
}

#[test]
fn a_tournament_is_driven_once_per_process() {
    let registry = TournamentRegistry::new();
    let live = registry.register("t").expect("first registration");
    assert!(registry.is_live("t"));
    assert!(registry.register("t").is_none(), "already driven");
    drop(live);
    assert!(!registry.is_live("t"));
    assert!(registry.register("t").is_some(), "free once it ends");
}

#[tokio::test]
async fn a_subscriber_receives_each_match_and_then_done() {
    let registry = TournamentRegistry::new();
    let live = registry.register("t").unwrap();
    let mut receiver = registry.subscribe("t").expect("live");
    live.push(TournamentProgress {
        played: 1,
        total: 1,
        summary: summary(),
    });
    drop(live);
    assert!(matches!(
        receiver.recv().await.unwrap(),
        StreamItem::Progress(progress) if progress.played == 1
    ));
    assert!(matches!(receiver.recv().await.unwrap(), StreamItem::Done));
    assert!(registry.subscribe("t").is_none(), "no channel once closed");
}
