use super::{Status, Task, Wiring, Worker};
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
    async fn execute(&self, id: Option<&str>) {
        let id = id.unwrap();
        let mut rx = self.subscribe(id);
        let format = self.params.format.clone();
        while let Ok(value) = rx.recv().await {
            let formatted = format.replace("{}", &value.to_string());
            println!("{formatted}");
        }
    }
}
