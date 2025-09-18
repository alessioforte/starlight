use crate::engine::task::{Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use serde::Deserialize;

const NAME: &str = "HttpRequest";

#[derive(Debug, Clone, Deserialize)]
pub struct HttpRequest {}

task! {
    HttpRequest,
    async fn execute(&self, running: bool) {
        let tx = self.wiring.out_tx.clone();
        while running {
            if let Some(tx) = tx.as_ref() {
                let _ = tx.send(/* pass to next task */);
            }
        }
    }
}
