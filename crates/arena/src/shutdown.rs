//! Shutting the arena down on `SIGTERM` (the arena overview's "Shutdown").
//!
//! [`Shutdown`] is the one signal every part of the arena reads: once it begins,
//! the routes that start work answer `503`, `/readyz` fails, a tournament stops
//! before its next match and a queued one stops waiting for its permit.
//! [`Drives`] counts the tournaments still being driven, so the binary waits for
//! the match each one is on, within its grace, before it exits.

use std::sync::Arc;

use tokio::sync::watch;

/// The arena's shutdown signal. Cloning shares it.
#[derive(Debug, Clone)]
pub struct Shutdown {
    draining: Arc<watch::Sender<bool>>,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    /// A signal that has not begun.
    pub fn new() -> Self {
        Self {
            draining: Arc::new(watch::Sender::new(false)),
        }
    }

    /// Begin shutting down. Every waiter on [`Self::draining`] wakes.
    pub fn begin(&self) {
        self.draining.send_replace(true);
    }

    /// Whether shutdown has begun.
    pub fn is_draining(&self) -> bool {
        *self.draining.borrow()
    }

    /// Resolve once shutdown has begun.
    pub async fn draining(&self) {
        let mut receiver = self.draining.subscribe();
        // The sender lives as long as `self`, so the wait never closes early.
        let _ = receiver.wait_for(|draining| *draining).await;
    }
}

/// The tournaments the arena is driving, counted so a shutdown can wait for each
/// to stop. Cloning shares the count.
#[derive(Debug, Clone)]
pub struct Drives {
    active: Arc<watch::Sender<usize>>,
}

impl Default for Drives {
    fn default() -> Self {
        Self::new()
    }
}

impl Drives {
    /// No drive under way.
    pub fn new() -> Self {
        Self {
            active: Arc::new(watch::Sender::new(0)),
        }
    }

    /// Count one drive until the returned guard drops.
    pub fn enter(&self) -> DriveGuard {
        self.active.send_modify(|active| *active += 1);
        DriveGuard {
            active: self.active.clone(),
        }
    }

    /// How many drives are under way.
    pub fn active(&self) -> usize {
        *self.active.borrow()
    }

    /// Resolve once no drive is under way.
    pub async fn idle(&self) {
        let mut receiver = self.active.subscribe();
        let _ = receiver.wait_for(|active| *active == 0).await;
    }
}

/// One counted drive; dropping it ends the count.
#[derive(Debug)]
pub struct DriveGuard {
    active: Arc<watch::Sender<usize>>,
}

impl Drop for DriveGuard {
    fn drop(&mut self) {
        self.active.send_modify(|active| *active -= 1);
    }
}

#[cfg(test)]
#[path = "shutdown.test.rs"]
mod tests;
