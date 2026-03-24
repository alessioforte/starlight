//! Timer Task
//!
//! Source task that emits events at regular intervals or on a cron-like schedule.
//! Each tick produces a JSON object with tick metadata.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use chrono::Utc;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How the timer determines when to fire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerMode {
    /// Fixed interval in milliseconds.
    Interval(u64),
    /// Cron expression (7-field: sec min hour day-of-month month day-of-week year).
    /// Examples:
    /// - `"0 */5 * * * * *"` — every 5 minutes
    /// - `"0 0 9 * * Mon-Fri *"` — weekdays at 09:00
    /// - `"*/10 * * * * * *"` — every 10 seconds
    Cron(String),
}

/// Optional static payload attached to every tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    /// Include a fixed JSON value in every tick under the `"data"` key.
    Static(Value),
    /// No extra payload — only tick metadata is emitted.
    None,
}

impl Default for Payload {
    fn default() -> Self {
        Self::None
    }
}

/// Timer task parameters.
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Firing strategy.
    pub mode: TimerMode,
    /// Optional: maximum number of ticks (default: unlimited).
    #[serde(default)]
    pub count: Option<u64>,
    /// Optional: static payload to attach to each tick.
    #[serde(default)]
    pub payload: Payload,
    /// Whether to emit immediately on start before waiting for the first interval/cron.
    /// Default: `false`.
    #[serde(default)]
    pub immediate: bool,
}

/// Timer state.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    ticks: AtomicU64,
}

// ---------------------------------------------------------------------------
// Task implementation
// ---------------------------------------------------------------------------

/// Timer task
///
/// Source task — no inputs. Emits tick events to `"out"`.
///
/// # Output
///
/// ```json
/// {
///   "tick": 0,
///   "timestamp": "2025-03-18T12:00:00Z",
///   "data": { ... }
/// }
/// ```
///
/// The `"data"` key is only present when a static payload is configured.
///
/// # Example Configurations
///
/// ## Fixed interval
/// ```json
/// {
///   "mode": { "interval": 1000 },
///   "count": 100,
///   "immediate": true
/// }
/// ```
///
/// ## Cron schedule
/// ```json
/// {
///   "mode": { "cron": "0 */5 * * * * *" },
///   "payload": { "static": { "source": "timer" } }
/// }
/// ```
pub struct Timer {
    base: BaseTask<Params, State>,
}

impl Timer {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        // Validate cron expression at creation time
        let base: BaseTask<Params, State> = BaseTask::new(id, params)?;
        if let TimerMode::Cron(ref expr) = base.params.mode {
            Schedule::from_str(expr).map_err(|e| {
                crate::err::EngineError::invalid_params(
                    &base.id,
                    format!("Invalid cron expression '{}': {}", expr, e),
                )
            })?;
        }
        Ok(Box::new(Self { base }))
    }

    /// Build the tick payload.
    fn make_tick(&self, index: u64) -> Value {
        let mut obj = json!({
            "tick": index,
            "timestamp": Utc::now().to_rfc3339(),
        });

        if let Payload::Static(ref data) = self.base.params.payload {
            obj["data"] = data.clone();
        }

        obj
    }

    /// Check if we've reached the tick limit.
    fn limit_reached(&self) -> bool {
        self.base
            .params
            .count
            .is_some_and(|max| self.base.state.ticks.load(Ordering::Relaxed) >= max)
    }
}

#[async_trait]
impl Task for Timer {
    fn name(&self) -> &str {
        "Timer"
    }

    fn set_status_handle(&mut self, status: Arc<tokio::sync::RwLock<crate::task::TaskStatus>>) {
        self.base.status = Some(status);
    }

    fn get_info(&self) -> TaskInfo {
        let current_status = if let Some(status_lock) = &self.base.status {
            status_lock.try_read().ok().map(|s| format!("{:?}", *s))
        } else {
            None
        };

        TaskInfo {
            id: self.base.id.clone(),
            params: serde_json::to_value(&self.base.params).unwrap_or(json!({})),
            state: serde_json::to_value(&self.base.state).unwrap_or(json!({})),
            status: current_status,
            metrics: None,
        }
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let output = ctx.output("out")?;

        match &self.base.params.mode {
            TimerMode::Interval(ms) => {
                let interval = tokio::time::Duration::from_millis(*ms);

                tracing::info!(
                    "Timer [{}]: interval={}ms, count={:?}, immediate={}",
                    self.base.id,
                    ms,
                    self.base.params.count,
                    self.base.params.immediate,
                );

                // Optionally emit immediately
                if self.base.params.immediate && !self.limit_reached() {
                    let tick_idx = self.base.state.ticks.fetch_add(1, Ordering::Relaxed);
                    output.send(self.make_tick(tick_idx).into()).await?;
                }

                while ctx.running().await && !self.limit_reached() {
                    tokio::time::sleep(interval).await;

                    // Re-check after sleep (could have been stopped/paused)
                    if !ctx.is_running() || self.limit_reached() {
                        break;
                    }

                    let tick_idx = self.base.state.ticks.fetch_add(1, Ordering::Relaxed);
                    output.send(self.make_tick(tick_idx).into()).await?;
                }
            }

            TimerMode::Cron(expr) => {
                let schedule = Schedule::from_str(expr).unwrap(); // validated in create()

                tracing::info!(
                    "Timer [{}]: cron=\"{}\", count={:?}",
                    self.base.id,
                    expr,
                    self.base.params.count,
                );

                while ctx.running().await && !self.limit_reached() {
                    let now = Utc::now();
                    let next = match schedule.upcoming(Utc).next() {
                        Some(t) => t,
                        None => {
                            tracing::info!(
                                "Timer [{}]: No more upcoming cron events",
                                self.base.id
                            );
                            break;
                        }
                    };

                    let wait = (next - now).to_std().unwrap_or(std::time::Duration::ZERO);

                    if !wait.is_zero() {
                        tokio::time::sleep(wait).await;
                    }

                    // Re-check after sleep
                    if !ctx.is_running() || self.limit_reached() {
                        break;
                    }

                    let tick_idx = self.base.state.ticks.fetch_add(1, Ordering::Relaxed);
                    output.send(self.make_tick(tick_idx).into()).await?;
                }
            }
        }

        let total = self.base.state.ticks.load(Ordering::Relaxed);
        tracing::info!("Timer [{}]: Finished after {} ticks", self.base.id, total,);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- Factory / deserialization ---

    #[test]
    fn test_timer_create_interval() {
        let params = json!({
            "mode": { "interval": 500 },
            "count": 10
        });
        assert!(Timer::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_timer_create_cron() {
        let params = json!({
            "mode": { "cron": "*/10 * * * * * *" }
        });
        assert!(Timer::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_timer_create_cron_invalid() {
        let params = json!({
            "mode": { "cron": "not a cron" }
        });
        assert!(Timer::create("test".into(), params).is_err());
    }

    #[test]
    fn test_timer_create_invalid_params() {
        assert!(Timer::create("test".into(), json!({"wrong": true})).is_err());
    }

    #[test]
    fn test_timer_create_with_payload() {
        let params = json!({
            "mode": { "interval": 1000 },
            "payload": { "static": { "source": "heartbeat" } }
        });
        assert!(Timer::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_timer_create_with_immediate() {
        let params = json!({
            "mode": { "interval": 1000 },
            "immediate": true,
            "count": 5
        });
        assert!(Timer::create("test".into(), params).is_ok());
    }

    // --- make_tick ---

    #[test]
    fn test_make_tick_no_payload() {
        let timer = Timer {
            base: BaseTask {
                id: "t".into(),
                params: Params {
                    mode: TimerMode::Interval(1000),
                    count: None,
                    payload: Payload::None,
                    immediate: false,
                },
                state: State::default(),
                status: None,
            },
        };

        let tick = timer.make_tick(0);
        assert_eq!(tick["tick"], 0);
        assert!(tick["timestamp"].is_string());
        assert!(tick.get("data").is_none());
    }

    #[test]
    fn test_make_tick_with_payload() {
        let timer = Timer {
            base: BaseTask {
                id: "t".into(),
                params: Params {
                    mode: TimerMode::Interval(1000),
                    count: None,
                    payload: Payload::Static(json!({"source": "heartbeat"})),
                    immediate: false,
                },
                state: State::default(),
                status: None,
            },
        };

        let tick = timer.make_tick(5);
        assert_eq!(tick["tick"], 5);
        assert_eq!(tick["data"]["source"], "heartbeat");
    }

    #[test]
    fn test_make_tick_increments() {
        let timer = Timer {
            base: BaseTask {
                id: "t".into(),
                params: Params {
                    mode: TimerMode::Interval(100),
                    count: None,
                    payload: Payload::None,
                    immediate: false,
                },
                state: State::default(),
                status: None,
            },
        };

        let t0 = timer.make_tick(0);
        let t1 = timer.make_tick(1);
        let t2 = timer.make_tick(2);
        assert_eq!(t0["tick"], 0);
        assert_eq!(t1["tick"], 1);
        assert_eq!(t2["tick"], 2);
    }

    // --- limit_reached ---

    #[test]
    fn test_limit_reached_none() {
        let timer = Timer {
            base: BaseTask {
                id: "t".into(),
                params: Params {
                    mode: TimerMode::Interval(100),
                    count: None,
                    payload: Payload::None,
                    immediate: false,
                },
                state: State::default(),
                status: None,
            },
        };
        assert!(!timer.limit_reached());
    }

    #[test]
    fn test_limit_reached_not_yet() {
        let timer = Timer {
            base: BaseTask {
                id: "t".into(),
                params: Params {
                    mode: TimerMode::Interval(100),
                    count: Some(5),
                    payload: Payload::None,
                    immediate: false,
                },
                state: State::default(),
                status: None,
            },
        };
        timer.base.state.ticks.store(3, Ordering::Relaxed);
        assert!(!timer.limit_reached());
    }

    #[test]
    fn test_limit_reached_exact() {
        let timer = Timer {
            base: BaseTask {
                id: "t".into(),
                params: Params {
                    mode: TimerMode::Interval(100),
                    count: Some(5),
                    payload: Payload::None,
                    immediate: false,
                },
                state: State::default(),
                status: None,
            },
        };
        timer.base.state.ticks.store(5, Ordering::Relaxed);
        assert!(timer.limit_reached());
    }

    // --- Cron expression parsing ---

    #[test]
    fn test_cron_every_5_minutes() {
        let params = json!({
            "mode": { "cron": "0 */5 * * * * *" }
        });
        assert!(Timer::create("test".into(), params).is_ok());
    }

    #[test]
    fn test_cron_weekdays_9am() {
        let params = json!({
            "mode": { "cron": "0 0 9 * * Mon-Fri *" }
        });
        assert!(Timer::create("test".into(), params).is_ok());
    }

    // --- TimerMode serde ---

    #[test]
    fn test_timer_mode_interval_serde() {
        let mode: TimerMode = serde_json::from_value(json!({"interval": 500})).unwrap();
        assert!(matches!(mode, TimerMode::Interval(500)));
    }

    #[test]
    fn test_timer_mode_cron_serde() {
        let mode: TimerMode = serde_json::from_value(json!({"cron": "*/10 * * * * * *"})).unwrap();
        assert!(matches!(mode, TimerMode::Cron(_)));
    }
}
