use serde_json::{Value, json};

#[derive(Debug)]
pub enum JsonPathError {
    InvalidPath,
    NotFound,
}

pub struct JsonBuilder {
    data: Value,
}

impl JsonBuilder {
    pub fn new(data: Value) -> Self {
        JsonBuilder { data }
    }

    pub fn data(&self) -> &Value {
        &self.data
    }

    pub fn get_value(&self, path: &str) -> Result<&Value, JsonPathError> {
        path.split('.').try_fold(&self.data, |value, key| {
            if key.is_empty() {
                Err(JsonPathError::InvalidPath)
            } else {
                value.get(key).ok_or(JsonPathError::NotFound)
            }
        })
    }

    pub fn set_value(&mut self, path: &str, value: Value) -> Result<&Value, JsonPathError> {
        let mut data = &mut self.data;
        let keys: Vec<&str> = path.split('.').collect();
        for key in &keys[..keys.len() - 1] {
            data = match data {
                Value::Object(map) => map.entry(key.to_string()).or_insert(json!({})),
                _ => return Err(JsonPathError::InvalidPath),
            };
        }
        if let Value::Object(map) = data {
            map.insert(keys[keys.len() - 1].to_string(), value);
            Ok(&self.data)
        } else {
            Err(JsonPathError::InvalidPath)
        }
    }

    pub fn get_type(&self, path: &str) -> Result<&str, JsonPathError> {
        let value = self.get_value(path)?;
        match value {
            Value::Null => Ok("null"),
            Value::Bool(_) => Ok("bool"),
            Value::Number(_) => Ok("number"),
            Value::String(_) => Ok("string"),
            Value::Array(_) => Ok("array"),
            Value::Object(_) => Ok("object"),
        }
    }

    pub fn merge(&mut self, value: &Value) {
        Self::merge_values(&mut self.data, value);
    }

    fn merge_values(target: &mut Value, source: &Value) {
        match (target, source) {
            (Value::Object(target_map), Value::Object(source_map)) => {
                for (key, value) in source_map {
                    Self::merge_values(target_map.entry(key.clone()).or_insert(json!({})), value);
                }
            }
            (target, source) => {
                *target = source.clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_value() {
        let data = json!({
            "header": {
                "input": ["input1", "input2"],
                "next_task": {
                    "on_success": {
                        "next_task": "task1"
                    }
                }
            }
        });
        let builder = JsonBuilder::new(data.clone());
        assert_eq!(
            builder.get_value("header.input").unwrap(),
            &json!(["input1", "input2"])
        );
        assert_eq!(
            builder
                .get_value("header.next_task.on_success.next_task")
                .unwrap(),
            &json!("task1")
        );
    }

    #[test]
    fn test_set_value() {
        let data = json!({
            "header": {
                "input": ["input1", "input2"],
                "next_task": {
                    "on_success": {
                        "next_task": "task1"
                    }
                }
            }
        });
        let mut builder = JsonBuilder::new(data.clone());
        let inputs = builder.get_value("header.input").unwrap();
        let mut inputs = inputs.as_array().unwrap().clone();
        inputs.push(json!("input3"));
        builder
            .set_value("header.input", Value::Array(inputs))
            .unwrap();
        builder
            .set_value("header.next_task.on_success.next_task", json!("task2"))
            .unwrap();
        assert_eq!(
            builder.get_value("header.input").unwrap(),
            &json!(["input1", "input2", "input3"])
        );
        assert_eq!(
            builder
                .get_value("header.next_task.on_success.next_task")
                .unwrap(),
            &json!("task2")
        );
    }

    #[test]
    fn test_get_type() {
        let data = json!({
            "header": {
                "input": ["input1", "input2"],
                "next_task": {
                    "on_success": {
                        "next_task": "task1"
                    }
                }
            }
        });
        let builder = JsonBuilder::new(data.clone());
        assert_eq!(builder.get_type("header.input").unwrap(), "array");
        assert_eq!(
            builder
                .get_type("header.next_task.on_success.next_task")
                .unwrap(),
            "string"
        );
    }
}
