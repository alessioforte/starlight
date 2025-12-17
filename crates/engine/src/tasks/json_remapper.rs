use super::{Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use jb::JsonBuilder;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "JsonRemapper";

#[derive(Deserialize, Debug, Clone)]
pub struct JsonRemapper {
    mapping: Value,
    enrichments: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    JsonRemapper,
    State,
    async fn execute(&self, id: Option<&str>) {
        let id = id.unwrap();
        let wiring = self.wiring();
        let mut rx = self.subscribe(id);
        while let Ok(payload) = rx.recv().await {
            let mapping = self.params.mapping.as_object().unwrap();
            let obj = payload.as_object().unwrap();
            let mut jb = JsonBuilder::new(Value::Object(Map::new()));

            if let Some(enrichments) = self.params.enrichments.as_ref() {
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
            let out = wiring.out_txs.get("out").unwrap();
            out.iter().for_each(|handle| {
                let _ = handle.tx.send(jb.data().clone());
            });
        }
    }
}
