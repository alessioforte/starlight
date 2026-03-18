//! Number Generator Task
//!
//! Generates random numbers at a specified interval.

use crate::ctx::TaskContext;
use crate::err::Result;
use crate::task::{BaseTask, Task, TaskInfo};
use async_trait::async_trait;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Number Generator parameters
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Minimum value (inclusive)
    pub min: i64,
    /// Maximum value (inclusive)
    pub max: i64,
    /// Interval between generations in milliseconds (default: 1000)
    #[serde(default = "default_interval")]
    pub interval_ms: u64,
    /// Optional: Maximum number of values to generate (default: unlimited)
    #[serde(default)]
    pub count: Option<usize>,
    /// Optional: Seed for reproducible random generation
    #[serde(default)]
    pub seed: Option<u64>,
}

fn default_interval() -> u64 {
    1000
}

/// Number Generator state
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    /// Number of values generated so far
    generated: AtomicUsize,
}

/// Number Generator task
///
/// Generates random numbers and emits them as JSON objects.
/// This is an example of a source task (no inputs).
///
/// # Output
///
/// Sends JSON objects with the generated number to the "out" channel:
/// ```json
/// {
///   "value": 42,
///   "index": 0
/// }
/// ```
///
/// # Example Configuration
///
/// ```json
/// {
///   "min": 1,
///   "max": 100,
///   "interval_ms": 500,
///   "count": 10
/// }
/// ```
pub struct NumberGenerator {
    base: BaseTask<Params, State>,
}

impl NumberGenerator {
    /// Create a new number generator task
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        Ok(Box::new(Self {
            base: BaseTask::new(id, params)?,
        }))
    }
}

#[async_trait]
impl Task for NumberGenerator {
    fn name(&self) -> &str {
        "NumberGenerator"
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
        let params = &self.base.params;
        let state = &self.base.state;

        // Validate parameters
        if params.min > params.max {
            return Err(crate::err::EngineError::invalid_params(
                &self.base.id,
                format!("min ({}) must be <= max ({})", params.min, params.max),
            ));
        }

        // Get output channel
        let output = ctx.output("out")?;

        // Initialize RNG
        let mut rng = if let Some(seed) = params.seed {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            StdRng::from_rng(&mut rng)
        } else {
            StdRng::from_os_rng()
        };

        let interval = tokio::time::Duration::from_millis(params.interval_ms);
        let _start_count = state.generated.load(Ordering::Relaxed);

        tracing::info!(
            "Number Generator [{}]: Starting generation (range: {}-{}, interval: {}ms)",
            self.base.id,
            params.min,
            params.max,
            params.interval_ms
        );

        // Generate numbers
        while ctx.running().await {
            let current_count = state.generated.load(Ordering::Relaxed);

            // Check if we've reached the limit
            if let Some(max_count) = params.count {
                if current_count >= max_count {
                    tracing::info!(
                        "Number Generator [{}]: Reached count limit ({})",
                        self.base.id,
                        max_count
                    );
                    break;
                }
            }

            // Generate random number
            let number = rng.random_range(params.min..=params.max);

            // Create output object (timestamp comes from the coarse clock — no syscall)
            let data = json!({
                "value": number,
                "index": current_count,
            });

            // Send to output (async — applies backpressure)
            output.send(data).await?;

            // Update state
            state.generated.fetch_add(1, Ordering::Relaxed);

            // Log progress periodically
            if current_count > 0 && current_count % 100 == 0 {
                tracing::debug!(
                    "Number Generator [{}]: Generated {} numbers",
                    self.base.id,
                    current_count
                );
            }

            // Wait for next interval
            tokio::time::sleep(interval).await;
        }

        let final_count = state.generated.load(Ordering::Relaxed);
        tracing::info!(
            "Number Generator [{}]: Stopped after generating {} numbers",
            self.base.id,
            final_count
        );

        Ok(())
    }

    async fn on_start(&self, _ctx: Arc<TaskContext>) -> Result<()> {
        tracing::info!(
            "Number Generator [{}]: Starting from count {}",
            self.base.id,
            self.base.state.generated.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_number_generator_creation() {
        let params = json!({
            "min": 1,
            "max": 100,
            "interval_ms": 100,
            "count": 10
        });

        let result = NumberGenerator::create("test".to_string(), params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_range() {
        let params = json!({
            "min": 100,
            "max": 1,
            "interval_ms": 100
        });

        let result = NumberGenerator::create("test".to_string(), params);
        assert!(result.is_ok()); // Creation succeeds, validation happens in execute
    }

    #[test]
    fn test_with_seed() {
        let params = json!({
            "min": 1,
            "max": 100,
            "interval_ms": 100,
            "seed": 42
        });

        let result = NumberGenerator::create("test".to_string(), params);
        assert!(result.is_ok());
    }
}
