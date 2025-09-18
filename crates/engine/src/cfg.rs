use super::tasks::Tasks;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub tasks: Vec<Component>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Component {
    pub id: String,
    pub dependencies: Vec<String>,
    pub handles: HashMap<String, Vec<String>>,
    pub params: Value,
    #[serde(rename = "type")]
    pub kind: Tasks,
}
