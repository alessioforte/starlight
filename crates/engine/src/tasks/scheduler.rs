use super::{Activator, Task, Wiring};
use crate::task;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::atomic::AtomicBool;
use std::sync::{
    atomic::{AtomicBool, Ordering::Relaxed},
    Arc,
};

const NAME: &str = "Scheduler";

#[derive(Debug, Clone, Deserialize)]
pub struct Scheduler {}

task! {
    Activator,
    Scheduler,

    async fn execute(&self, running: Arc<AtomicBool>) {
        let out_txs = self.wiring.out_txs.clone();
        while running.load(Relaxed) {
            // if let Some(tx) = tx.as_ref() {
            //     let data = serde_json::json!({
            //         "timestamp": std::time::SystemTime::now()
            //             .duration_since(std::time::SystemTime::UNIX_EPOCH)
            //             .unwrap().as_millis(),
            //     });
            //     let _ = tx.send(data.clone());
            // }
        }
    }
}
