//! Simulator Task
//!
//! Source task that generates simulated time-series data using the `simulator` crate.
//! Demonstrates external task registration via the dynamic `TaskRegistry`.

use eng::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use simulator::Simulator;
use simulator::models::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Model configuration (JSON → simulator models)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelConfig {
    Sine {
        amplitude: f64,
        frequency: f64,
        #[serde(default)]
        phase: f64,
    },
    Random {
        #[serde(default)]
        mean: f64,
        #[serde(default = "default_stddev")]
        stddev: f64,
        #[serde(default)]
        seed: Option<u64>,
    },
    RandomWalk {
        #[serde(default)]
        start: f64,
        #[serde(default)]
        drift: f64,
        #[serde(default = "default_volatility")]
        volatility: f64,
        #[serde(default)]
        seed: Option<u64>,
    },
    Trend {
        #[serde(flatten)]
        kind: TrendConfig,
    },
    Anomaly {
        base: Box<ModelConfig>,
        #[serde(default = "default_probability")]
        probability: f64,
        #[serde(default = "default_min_magnitude")]
        min_magnitude: f64,
        #[serde(default = "default_max_magnitude")]
        max_magnitude: f64,
        #[serde(default)]
        bidirectional: bool,
        #[serde(default)]
        seed: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TrendConfig {
    Linear {
        #[serde(default)]
        slope: f64,
        #[serde(default)]
        intercept: f64,
    },
    Exponential {
        #[serde(default = "default_one")]
        initial: f64,
        #[serde(default)]
        rate: f64,
    },
}

fn default_stddev() -> f64 {
    1.0
}
fn default_volatility() -> f64 {
    1.0
}
fn default_probability() -> f64 {
    0.05
}
fn default_min_magnitude() -> f64 {
    5.0
}
fn default_max_magnitude() -> f64 {
    10.0
}
fn default_one() -> f64 {
    1.0
}

impl ModelConfig {
    fn build(self) -> Box<dyn simulator::models::Model> {
        match self {
            ModelConfig::Sine {
                amplitude,
                frequency,
                phase,
            } => Box::new(SineModel::new(amplitude, frequency, phase)),
            ModelConfig::Random { mean, stddev, seed } => {
                if let Some(s) = seed {
                    Box::new(RandomModel::with_seed(mean, stddev, s))
                } else {
                    Box::new(RandomModel::new(mean, stddev))
                }
            }
            ModelConfig::RandomWalk {
                start,
                drift,
                volatility,
                seed,
            } => {
                if let Some(s) = seed {
                    Box::new(RandomWalkModel::with_seed(start, drift, volatility, s))
                } else {
                    Box::new(RandomWalkModel::new(start, drift, volatility))
                }
            }
            ModelConfig::Trend { kind } => match kind {
                TrendConfig::Linear { slope, intercept } => {
                    Box::new(TrendModel::linear(slope, intercept))
                }
                TrendConfig::Exponential { initial, rate } => {
                    Box::new(TrendModel::exponential(initial, rate))
                }
            },
            ModelConfig::Anomaly {
                base,
                probability,
                min_magnitude,
                max_magnitude,
                bidirectional,
                seed,
            } => {
                let base_model = base.build();
                if let Some(s) = seed {
                    Box::new(AnomalyModel::with_seed(
                        base_model,
                        probability,
                        min_magnitude,
                        max_magnitude,
                        bidirectional,
                        s,
                    ))
                } else {
                    Box::new(AnomalyModel::new(
                        base_model,
                        probability,
                        min_magnitude,
                        max_magnitude,
                        bidirectional,
                    ))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Task parameters & state
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    /// Models to compose (their outputs are summed by the Simulator).
    pub models: Vec<ModelConfig>,
    /// Emission interval in milliseconds.
    #[serde(default = "default_interval")]
    pub interval_ms: u64,
    /// Simulation time-step in milliseconds (how much simulated time advances per tick).
    /// Defaults to `interval_ms`.
    #[serde(default)]
    pub step_ms: Option<u64>,
    /// Maximum number of ticks (default: unlimited).
    #[serde(default)]
    pub count: Option<u64>,
    /// JSON field name for the simulated value.
    #[serde(default = "default_field")]
    pub field: String,
}

fn default_interval() -> u64 {
    1000
}
fn default_field() -> String {
    "value".to_string()
}

#[derive(Debug, Default)]
pub struct State {
    ticks: AtomicU64,
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

pub struct SimulatorTask {
    id: String,
    params: Params,
    state: State,
    sim: Mutex<Simulator>,
    status: Option<Arc<tokio::sync::RwLock<TaskStatus>>>,
}

impl SimulatorTask {
    pub fn create(id: String, params: Value) -> Result<Box<dyn Task>> {
        let p: Params = serde_json::from_value(params)
            .map_err(|e| EngineError::invalid_params(&id, e.to_string()))?;

        let models: Vec<Box<dyn simulator::models::Model>> =
            p.models.iter().cloned().map(|m| m.build()).collect();

        let step_ms = p.step_ms.unwrap_or(p.interval_ms) as u128;
        let sim = Simulator::with_step(models, 0, step_ms);

        Ok(Box::new(SimulatorTask {
            id,
            params: p,
            state: State::default(),
            sim: Mutex::new(sim),
            status: None,
        }))
    }

    fn limit_reached(&self) -> bool {
        self.params
            .count
            .is_some_and(|max| self.state.ticks.load(Ordering::Relaxed) >= max)
    }
}

#[async_trait]
impl Task for SimulatorTask {
    fn name(&self) -> &str {
        "Simulator"
    }

    fn required_outputs(&self) -> &'static [&'static str] {
        &["out"]
    }

    fn set_status_handle(&mut self, status: Arc<tokio::sync::RwLock<TaskStatus>>) {
        self.status = Some(status);
    }

    fn get_info(&self) -> TaskInfo {
        let current_status = self
            .status
            .as_ref()
            .and_then(|s| s.try_read().ok().map(|s| format!("{:?}", *s)));

        TaskInfo {
            id: self.id.clone(),
            params: serde_json::to_value(&self.params).unwrap_or(json!({})),
            state: json!({
                "ticks": self.state.ticks.load(Ordering::Relaxed),
            }),
            status: current_status,
            metrics: None,
        }
    }

    async fn execute(&self, ctx: Arc<TaskContext>) -> Result<()> {
        let output = ctx.output("out")?;
        let interval = tokio::time::Duration::from_millis(self.params.interval_ms);
        let field = &self.params.field;

        tracing::info!(
            "Simulator [{}]: interval={}ms, step={}ms, models={}, count={:?}",
            self.id,
            self.params.interval_ms,
            self.params.step_ms.unwrap_or(self.params.interval_ms),
            self.params.models.len(),
            self.params.count,
        );

        while ctx.running().await && !self.limit_reached() {
            let (value, time_ms) = {
                let mut sim = self.sim.lock().await;
                let v = sim.tick();
                let t = sim.current_time_ms();
                (v, t)
            };

            let tick_idx = self.state.ticks.fetch_add(1, Ordering::Relaxed);

            let msg = json!({
                "tick": tick_idx,
                "time_ms": time_ms,
                field: value,
            });

            output.send(msg.into()).await?;

            tokio::time::sleep(interval).await;
        }

        let total = self.state.ticks.load(Ordering::Relaxed);
        tracing::info!("Simulator [{}]: Finished after {} ticks", self.id, total);

        Ok(())
    }
}
