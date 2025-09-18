use super::{HandleTarget, Status, Task, Wiring, Worker};
use crate::ext::duration::DurationExt;
use crate::task;
use async_trait::async_trait;
use jb::JsonBuilder;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering::Relaxed},
};
use std::thread;
use std::time::SystemTime;
use tokio::time::Duration;

const NAME: &str = "Simulator";

#[derive(Debug, Clone, Deserialize)]
pub struct Model {
    name: Models,
    params: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Simulator {
    key: String,
    interval: String,
    models: Vec<Model>,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    Simulator,
    State,
    async fn execute(&self, _channel: Option<&HandleTarget>) {
        let out_txs = self.wiring.out_txs.clone();
        let key = self.params.key.clone();
        let duration = Duration::parse(&self.params.interval).unwrap_or(Duration::from_millis(1000));
        let models = self.params.models.clone();
        let running = self.running();
        let mut simulator = init_simulator(models);
        rayon::spawn(move || {
            while running.load(Relaxed) {
                let data = Value::Object(Map::new());
                let mut jb = JsonBuilder::new(data);
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap().as_millis();
                let value = simulator.generate();
                let _ = jb.set_value("timestamp", serde_json::json!(now));
                let _ = jb.set_value(&key, serde_json::json!(value));
                let data = jb.data();
                let out = out_txs.get("out").unwrap();
                for handle in out {
                    let _ = handle.tx.send(data.clone());
                }
                thread::sleep(duration);
            }
            log::info!("Simulator task stopped");
        });

        // Sleep to avoid busy-waiting
        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Models {
    Random,
    RandomWalk,
    Sine,
}

#[derive(Debug, Deserialize, Clone)]
struct RandomModel {
    mean: f64,
    stddev: f64,
}

#[derive(Debug, Deserialize, Clone)]
struct RandomWalkModel {
    current: f64,
    drift: f64,
    volatility: f64,
}

#[derive(Debug, Deserialize, Clone)]
struct SineModel {
    amplitude: f64,
    frequency: f64,
    phase: f64,
}

fn init_simulator(models: Vec<Model>) -> simulator::Simulator {
    let mut simulator = simulator::Simulator::default();
    for model in models {
        match model.name {
            Models::Random => {
                let RandomModel { mean, stddev } = serde_json::from_value(model.params).unwrap();
                simulator.add_model(Box::new(simulator::models::RandomModel::new(mean, stddev)));
            }
            Models::RandomWalk => {
                let RandomWalkModel {
                    current,
                    drift,
                    volatility,
                } = serde_json::from_value(model.params).unwrap();
                simulator.add_model(Box::new(simulator::models::RandomWalkModel::new(
                    current, drift, volatility,
                )));
            }
            Models::Sine => {
                let SineModel {
                    amplitude,
                    frequency,
                    phase,
                } = serde_json::from_value(model.params).unwrap();
                simulator.add_model(Box::new(simulator::models::SineModel::new(
                    amplitude, frequency, phase,
                )));
            }
        }
    }
    simulator
}
