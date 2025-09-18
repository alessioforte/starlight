use super::{HandleTarget, Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "StdoutLogger";

#[derive(Debug, Clone, Deserialize)]
pub struct StdoutLogger {
    format: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    StdoutLogger,
    State,
    async fn execute(&self, channel: Option<&HandleTarget>) {
        let mut rx = channel.unwrap().rx.lock().await;
        let format = self.params.format.clone();
        while let Some(value) = rx.recv().await {
            let formatted = format.replace("{}", &value.to_string());
            println!("{formatted}");
        }
    }
}
