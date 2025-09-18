use super::{HandleTarget, Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use jb::JsonBuilder;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "Splitter";

#[derive(Deserialize, Debug, Clone)]
struct Rule {
    mapping: Value,
    enrichments: Option<Value>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Splitter {
    rules: Vec<Rule>,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    Splitter,
    State,
    async fn execute(&self, channel: Option<&HandleTarget>) {
        let mut rx = channel.unwrap().rx.lock().await;
        while let Some(payload) = rx.recv().await {
            let rules = self.params.rules.clone();
            let obj = payload.as_object().unwrap();
            for rule in rules.iter() {
                let mapping = rule.mapping.as_object().unwrap();
                let mut jb = JsonBuilder::new(Value::Object(Map::new()));

                if let Some(enrichments) = rule.enrichments.as_ref() {
                    let enrichments = enrichments.as_object().unwrap();
                    for (key, value) in enrichments.iter() {
                        jb.set_value(key, value.clone()).unwrap();
                    }
                }

                for (key, path) in mapping.iter() {
                    let path = path.as_str().unwrap();
                    if let Some(value) = obj.get(key) {
                        jb.set_value(path, value.clone()).unwrap();
                    }
                }

                let out_txs = self.wiring.out_txs.clone();
                let out = out_txs.get("out").unwrap();
                for handle in out {
                    let _ = handle.tx.send(jb.data().clone());
                }
            }
        }
    }
}
