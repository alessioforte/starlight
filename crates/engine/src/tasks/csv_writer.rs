use super::{Status, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const NAME: &str = "CsvWriter";

#[derive(Debug, Clone, Deserialize)]
pub struct CsvWriter {
    filename: String,
    // delimiter: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct State {}

task! {
    CsvWriter,
    State,
    async fn execute(&self, id: Option<&str>) {
        let base_path = "./.starlight/data/";
        let filename = self.params.filename.clone();
        let path = format!("{}/{}", base_path, filename);
        let mut header_written = std::path::Path::new(&path).exists();
        // let delimiter = self.params.delimiter.clone().unwrap_or(",".to_string());

        let id = id.unwrap();
        let mut rx = self.subscribe(id);
        while let Ok(payload) = rx.recv().await {
            if let Value::Object(map) = payload {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .expect("file not found");

                let mut wtr = csv::Writer::from_writer(BufWriter::new(file));

                if !header_written {
                    let keys: Vec<String> = map.keys().cloned().collect();
                    wtr.write_record(keys).expect("error writing headers");
                    header_written = true;
                }

                // Serialize values properly
                let row: Vec<String> = map.values()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),                // Write strings as-is
                        Value::Number(num) => num.to_string(),        // Convert numbers directly
                        Value::Bool(b) => b.to_string(),              // Convert booleans directly
                        _ => v.to_string(),                           // Fallback to default `to_string()`
                    })
                    .collect();

                wtr.write_record(row).expect("error writing record");
                wtr.flush().expect("error flushing writer"); // Make sure data is written to the file
            }
        }
    }
}
