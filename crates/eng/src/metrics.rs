//! Per-task metrics and shared coarse clock.
//!
//! The [`CoarseClock`] avoids per-message syscalls by updating a shared
//! atomic timestamp from a background tokio task at a configurable interval.
//! Tasks read it with a single `AtomicI64` load — zero overhead.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// CoarseClock
// ---------------------------------------------------------------------------

/// A shared, low-overhead clock updated by a background task.
///
/// Resolution is determined by `interval` (default 100 ms).
/// All consumers share the same `Arc<AtomicI64>` — reading is a single
/// relaxed atomic load with no syscall.
///
/// The background updater stops automatically when every `CoarseClock`
/// clone is dropped (it holds a `Weak` reference).
#[derive(Clone)]
pub struct CoarseClock {
    millis: Arc<AtomicI64>,
}

impl CoarseClock {
    /// Create and start a coarse clock with the given update interval.
    ///
    /// Spawns a background tokio task.  The task terminates when all
    /// clones of this `CoarseClock` are dropped.
    pub fn start(interval: Duration) -> Self {
        let millis = Arc::new(AtomicI64::new(Self::system_millis()));
        let weak = Arc::downgrade(&millis);

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                match weak.upgrade() {
                    Some(m) => m.store(Self::system_millis(), Ordering::Relaxed),
                    None => break, // all clones dropped
                }
            }
        });

        Self { millis }
    }

    /// Current coarse timestamp (ms since Unix epoch).
    #[inline]
    pub fn now_millis(&self) -> i64 {
        self.millis.load(Ordering::Relaxed)
    }

    /// One-shot syscall: current system time in ms since epoch.
    fn system_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

// ---------------------------------------------------------------------------
// TaskMetrics
// ---------------------------------------------------------------------------

/// Per-task counters updated automatically by [`Input`] and [`Output`].
///
/// All fields are atomic — safe to read from any thread while the task
/// is running.
pub struct TaskMetrics {
    /// Total messages received on input channels
    pub messages_in: AtomicU64,
    /// Total messages sent on output channels
    pub messages_out: AtomicU64,
    /// Total errors encountered
    pub errors: AtomicU64,
    /// Coarse timestamp (ms since epoch) of the last received message
    pub last_message_at: AtomicI64,
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            messages_in: AtomicU64::new(0),
            messages_out: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_message_at: AtomicI64::new(0),
        }
    }

    /// Take a point-in-time snapshot (cheap — four relaxed loads).
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages_in: self.messages_in.load(Ordering::Relaxed),
            messages_out: self.messages_out.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_message_at: self.last_message_at.load(Ordering::Relaxed),
        }
    }
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MetricsSnapshot
// ---------------------------------------------------------------------------

/// A serialisable point-in-time copy of [`TaskMetrics`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub messages_in: u64,
    pub messages_out: u64,
    pub errors: u64,
    /// Milliseconds since Unix epoch of the last received message,
    /// or 0 if no message has been received yet.
    pub last_message_at: i64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coarse_clock_ticks() {
        let clock = CoarseClock::start(Duration::from_millis(10));

        let t0 = clock.now_millis();
        assert!(t0 > 0, "clock should be initialised");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let t1 = clock.now_millis();
        assert!(t1 >= t0, "clock should not go backwards");
    }

    #[tokio::test]
    async fn test_coarse_clock_stops_on_drop() {
        let clock = CoarseClock::start(Duration::from_millis(10));
        let _t = clock.now_millis();
        drop(clock);

        // If the background task leaked it would keep running;
        // we just verify no panic and the test finishes.
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    #[test]
    fn test_metrics_snapshot() {
        let m = TaskMetrics::new();
        m.messages_in.fetch_add(10, Ordering::Relaxed);
        m.messages_out.fetch_add(5, Ordering::Relaxed);
        m.errors.fetch_add(1, Ordering::Relaxed);
        m.last_message_at.store(1234567890, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.messages_in, 10);
        assert_eq!(snap.messages_out, 5);
        assert_eq!(snap.errors, 1);
        assert_eq!(snap.last_message_at, 1234567890);
    }
}
