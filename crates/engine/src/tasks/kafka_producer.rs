use super::{Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "KafkaProducer";

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaProducer {
    key: String,
    topic: String,
    bootstrap_servers: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    KafkaProducer,
    State,
    async fn execute(&self, id: Option<&str>) {
        let producer: FutureProducer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &self.params.bootstrap_servers)
            .create()
            .expect("Producer creation failed");

        let id = id.unwrap();
        let mut rx = self.subscribe(id);
        while let Ok(payload) = rx.recv().await {
            let key = self.params.key.clone();
            let topic = self.params.topic.clone();
            let payload = serde_json::to_string(&payload)
                .expect("Failed to serialize payload to JSON");
            let record = FutureRecord::to(&topic)
                .key(&key)
                .payload(&payload);

            let _ = producer.send(
                record,
                std::time::Duration::from_secs(0),
            )
            .await;
        }
    }
}
