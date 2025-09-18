use super::{HandleTarget, Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "KafkaConsumer";

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConsumer {
    topic: String,
    group_id: String,
    bootstrap_servers: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    KafkaConsumer,
    State,
    async fn execute(&self, _channel: Option<&HandleTarget>) {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &self.params.group_id)
            .set("bootstrap.servers", &self.params.bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer");
        consumer.subscribe(&[&self.params.topic]).expect("Can't subscribe");
        let out_txs = self.wiring.out_txs.clone();
        let out = out_txs.get("out").unwrap();
        loop {
            match consumer.recv().await {
                Ok(m) => {
                    let paylaod = m.payload().map(|p| String::from_utf8_lossy(p));
                    // for handle in out {
                    //     let _ = handle.tx.send(data);
                    // }
                },
                Err(e) => eprintln!("Error receiving message: {:?}", e),
            }
        }
    }
}
