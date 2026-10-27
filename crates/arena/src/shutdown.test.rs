//! Tests for the shutdown signal and the drive count.

use std::time::Duration;

use super::*;

#[tokio::test]
async fn a_waiter_wakes_when_shutdown_begins() {
    let shutdown = Shutdown::new();
    assert!(!shutdown.is_draining());
    let waiter = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { shutdown.draining().await }
    });
    shutdown.begin();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the waiter wakes")
        .unwrap();
    assert!(shutdown.is_draining());
    // A wait begun after the signal resolves at once.
    tokio::time::timeout(Duration::from_secs(5), shutdown.draining())
        .await
        .expect("already draining");
}

#[test]
fn shutdown_latches_for_every_clone() {
    let shutdown = Shutdown::new();
    let clone = shutdown.clone();
    clone.begin();
    assert!(shutdown.is_draining());
    clone.begin();
    assert!(shutdown.is_draining());
}

#[test]
fn refusing_comes_after_draining_and_implies_it() {
    let shutdown = Shutdown::new();
    shutdown.begin();
    assert!(!shutdown.is_refusing());
    shutdown.refuse();
    assert!(shutdown.is_refusing());
    // A later `begin` never moves it back.
    shutdown.begin();
    assert!(shutdown.is_refusing());

    let fresh = Shutdown::new();
    fresh.refuse();
    assert!(fresh.is_refusing());
    assert!(fresh.is_draining());
}

#[tokio::test]
async fn a_waiter_wakes_when_the_delay_ends_without_a_begin() {
    let shutdown = Shutdown::new();
    let waiter = tokio::spawn(shutdown.draining());
    shutdown.refuse();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the waiter wakes")
        .unwrap();
}

#[tokio::test]
async fn the_drives_are_idle_once_every_guard_drops() {
    let drives = Drives::new();
    let first = drives.enter();
    let second = drives.enter();
    assert_eq!(drives.active(), 2);
    let idle = tokio::spawn({
        let drives = drives.clone();
        async move { drives.idle().await }
    });
    drop(first);
    assert_eq!(drives.active(), 1);
    assert!(!idle.is_finished());
    drop(second);
    tokio::time::timeout(Duration::from_secs(5), idle)
        .await
        .expect("idle once both drop")
        .unwrap();
}
