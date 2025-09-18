use super::{HandleTarget, Status, Task, Wiring, Worker};
use crate::ext::duration::DurationExt;
use crate::task;
use async_trait::async_trait;
use jb::JsonBuilder;
use rand::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering::Relaxed},
};
use std::thread;
use std::time::SystemTime;
use tokio::time::Duration;

const NAME: &str = "RandomNumbersGenerator";

#[derive(Debug, Clone, Deserialize)]
pub struct RandomNumbersGenerator {
    keys: Vec<String>,
    min: f64,
    max: f64,
    interval: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    RandomNumbersGenerator,
    State,
    async fn execute(&self, _channel: Option<&HandleTarget>) {
        let out_txs = self.wiring.out_txs.clone();
        let keys = self.params.keys.clone();
        let min = self.params.min;
        let max = self.params.max;
        let duration = Duration::parse(&self.params.interval).unwrap_or(Duration::from_millis(1000));
        let running = self.running();

        rayon::spawn(move || {
            let mut rng = rand::rng();
            while running.load(Relaxed) {
                let data = Value::Object(Map::new());
                let mut manipulator = JsonBuilder::new(data);
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap().as_millis();
                let _ = manipulator.set_value("timestamp", serde_json::json!(now));
                for k in &keys {
                    let value = rng.random_range(min..max);
                    let value = serde_json::json!(value);
                    let _ = manipulator.set_value(k, value);
                }
                let data = manipulator.data();

                let data = data.clone();

                let out = out_txs.get("out").unwrap();
                for handle in out {
                    let _ = handle.tx.send(data.clone());
                }

                thread::sleep(duration);
            }
        });

        // // Sleep to avoid busy-waiting
        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}

// /// Generates a new timestamp representing the current seconds since epoch.
// fn gen_timestamp() -> u32 {
//     #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
//     let timestamp: u32 = (js_sys::Date::now() / 1000.0) as u32;
//     #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
//     let timestamp: u32 = SystemTime::now()
//         .duration_since(SystemTime::UNIX_EPOCH)
//         .expect("system clock is before 1970")
//         .as_secs()
//         .try_into()
//         .unwrap(); // will succeed until 2106 since timestamp is unsigned

//     timestamp
// }
