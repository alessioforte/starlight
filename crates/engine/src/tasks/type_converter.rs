use super::{Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use jb::JsonBuilder;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "TypeConverter";

#[derive(Deserialize, Debug, Clone)]
pub struct Rule {
    key: String,
    #[serde(rename = "type")]
    kind: String,
    format: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TypeConverter {
    pub rules: Vec<Rule>,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    TypeConverter,
    State,
    async fn execute(&self, id: Option<&str>) {
        let id = id.unwrap();
        let wiring = self.wiring();
        let mut rx = self.subscribe(id);
        while let Ok(payload) = rx.recv().await {
            let rules = self.params.rules.clone();
            let record = payload.as_object().unwrap();
            let mut jb = JsonBuilder::new(Value::Object(Map::new()));
            for rule in rules.iter() {
                let key = rule.key.clone();
                let kind = rule.kind.clone();

                if let Some(v) = record.get(&key) {
                    match v {
                        Value::String(value) => {
                            let cast_value = match kind.as_str() {
                                "timestamp" => {
                                    let format = rule.format.clone();
                                    let value = match chrono::NaiveDateTime::parse_from_str(value, format.as_ref().unwrap()) {
                                        Ok(value) => value.and_utc().timestamp(),
                                        Err(_) => 0,
                                    };
                                    json!(value)
                                },
                                "int" => {
                                    let number = value.parse::<f64>().unwrap();
                                    let value = number as i64;
                                    json!(value)
                                },
                                "float" => json!(value.parse::<f64>().unwrap()),
                                "bool" => json!(value.parse::<bool>().unwrap()),
                                _ => json!(value),
                            };
                            let _ = jb.set_value(&key, cast_value);
                        }
                        Value::Number(_) => {
                            let cast_value = match kind.as_str() {
                                "int" => {
                                    let number = v.as_f64().unwrap();
                                    let value = number as i64;
                                    json!(value)
                                },
                                "float" => json!(v.as_f64().unwrap()),
                                "bool" => json!(v.as_bool().unwrap()),
                                _ => v.clone(),
                            };
                            let _ = jb.set_value(&key, cast_value);
                        }
                        Value::Bool(_) => {
                            let cast_value = match kind.as_str() {
                                "int" => {
                                    let number = v.as_f64().unwrap();
                                    let value = number as i64;
                                    json!(value)
                                },
                                "float" => json!(v.as_f64().unwrap()),
                                "bool" => json!(v.as_bool().unwrap()),
                                _ => v.clone(),
                            };
                            let _ = jb.set_value(&key, cast_value);
                        }
                        Value::Null => {
                            let cast_value = match kind.as_str() {
                                "int" => json!(0),
                                "float" => json!(0.0),
                                "bool" => json!(false),
                                _ => v.clone(),
                            };
                            let _ = jb.set_value(&key, cast_value);
                        }
                        _ => {
                            let cast_value = match kind.as_str() {
                                "int" => {
                                    let number = v.as_f64().unwrap();
                                    let value = number as i64;
                                    json!(value)
                                },
                                "float" => json!(v.as_f64().unwrap()),
                                "bool" => json!(v.as_bool().unwrap()),
                                _ => v.clone(),
                            };
                            let _ = jb.set_value(&key, cast_value);
                        }
                    }
                }
            }
            let out = wiring.out_txs.get("out").unwrap();
            out.iter().for_each(|handle| {
                let _ = handle.tx.send(jb.data().clone());
            });
        }
    }
}
