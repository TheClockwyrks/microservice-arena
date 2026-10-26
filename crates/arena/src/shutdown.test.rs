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
