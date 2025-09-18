use super::{HandleTarget, Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use evalexpr::*;
use indexmap::IndexMap;
use jb::JsonBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "MathExpEval";

#[derive(Deserialize, Debug, Clone)]
pub struct MathExpEval {
    vars: HashMap<String, String>,
    consts: Option<HashMap<String, f64>>,
    expressions: IndexMap<String, String>,
    mapping: HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    MathExpEval,
    State,
    async fn execute(&self, channel: Option<&HandleTarget>) {
        let vars = self.params.vars.clone();
        let expressions = self.params.expressions.clone();
        let mapping = self.params.mapping.clone();
        let mut rx = channel.unwrap().rx.lock().await;
        while let Some(payload) = rx.recv().await {
            let constants = self.params.consts.clone();
            let mut jb = JsonBuilder::new(payload.clone());
            let mut context = HashMapContext::<DefaultNumericTypes>::new();
            let _ = context.set_value("consts::PI".into(), Value::from_float(std::f64::consts::PI));
            for (key, value) in vars.iter() {
                if let Ok(v) = jb.get_value(value) {
                    let num = v.as_f64().unwrap();
                    let _ = context.set_value(key.into(), Value::from_float(num));
                }
            }
            if let Some(c) = constants {
                for (key, value) in c.iter() {
                    let _ = context.set_value(key.into(), Value::from_float(*value));
                }
            }

            let mut results: HashMap<String, f64> = HashMap::new();
            for (key, exp) in expressions.iter() {
                let result = match eval_with_context(&exp, &context) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("Error while evaluating expression: {:?}", e);
                        continue;
                    }
                };
                let num = result.to_string().parse::<f64>().unwrap();
                results.insert(key.clone(), num);
                let _ = context.set_value(key.into(), Value::from_float(num));
            }

            for (key, path) in mapping.iter() {
                if let Some(value) = results.get(key) {
                    jb.set_value(path, serde_json::json!(value)).unwrap();
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
