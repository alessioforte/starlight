use super::{Status, Task, Wiring, Worker};
use crate::ext::duration::DurationExt;
use crate::task;
use async_trait::async_trait;
use futures::StreamExt;
use jb::JsonBuilder;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
};
use tokio::time::Duration;

const NAME: &str = "CsvReader";

#[derive(Debug, Clone, Deserialize)]
pub struct CsvReader {
    filename: String,
    base_path: Option<String>,
    delimiter: Option<String>,
    interval: Option<String>,
    // start_at_line: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct State {
    line_index: AtomicUsize,
}

impl Default for State {
    fn default() -> Self {
        Self {
            line_index: AtomicUsize::new(0),
        }
    }
}

task! {
    CsvReader,
    State,
    async fn execute(&self, _id: Option<&str>) {
        let wiring = self.wiring();
        let filename = self.params.filename.clone();
        let delimiter = self.params.delimiter.clone().unwrap_or(",".to_string());
        let duration = Duration::parse(&self.params.interval.clone().unwrap_or("1000ms".to_string()))
            .unwrap_or(Duration::from_millis(1000));
        let base_path = self.params.base_path.clone().unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .to_string_lossy()
                .to_string()
        });
        let path = format!("{}/{}", base_path, filename);
        let running = self.running();

        // let file = tokio::fs::File::open(&path).await;
        let file = tokio::fs::read_to_string(&path).await;
        if file.is_err() {
            let err = format!("Failed to read file: {}", path);
            log::error!("{}", err);
            self.set_status(Status::Error(err)).await;
            return;
        }

        let file = file.unwrap();
        let mut rdr = csv_async::AsyncReaderBuilder::new()
            .delimiter(delimiter.as_bytes()[0])
            .create_reader(file.as_bytes());


        let headers = match rdr.headers().await {
            Ok(h) => h.clone(),
            Err(e) => {
                let err = format!("Failed to read CSV headers: {}", e);
                log::error!("{}", err);
                self.set_status(Status::Error(err)).await;
                return;
            }
        };

        let mut records = rdr.records();

        // Skip already-processed lines
        let start_index = self.state.line_index.load(Relaxed);
        for _ in 0..start_index {
            if records.next().await.is_none() {
                return;
            }
        }

        log::debug!("Starting CSV reader at line {}", start_index);
        while let Some(record) = records.next().await {
            if !running.load(Relaxed) {
                log::debug!("Execution paused at line {}", self.state.line_index.load(Relaxed));
                break;
            }

            let record = match record {
                Ok(r) => r,
                Err(e) => {
                    let err = format!("Error reading record: {}", e);
                    log::error!("{}", err);
                    self.set_status(Status::Error(err)).await;
                    continue;
                }
            };
            let data = Value::Object(Map::new());
            let mut jb = JsonBuilder::new(data);
            for (i, header) in headers.iter().enumerate() {
                let value = record.get(i).unwrap();
                let value = serde_json::json!(value);
                let _ = jb.set_value(header, value);
            }

            let data = jb.data();

            // let v = data.get("timestamp").unwrap().as_str().unwrap();
            // let number = v.parse::<f64>().unwrap();
            // let ts = number as i64;
            // println!("{} {}", ts, format_time(ts));

            let out = wiring.out_txs.get("out").unwrap();
            out.iter().for_each(|handle| {
                let _ = handle.tx.send(data.clone());
            });
            // for handle in out {
            //     let _ = handle.tx.send(data.clone());
            // }

            // Update line index
            self.state.line_index.fetch_add(1, Relaxed);
            tokio::time::sleep(duration).await;
        }

        // Sleep to avoid busy-waiting
        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}

// use chrono::{DateTime, Local, TimeZone, Utc};
// pub fn format_time(timestamp: i64) -> String {
//     let dt = Utc.timestamp_millis_opt(timestamp).unwrap();
//     let local_dt: DateTime<Local> = dt.with_timezone(&Local);
//     local_dt.format("%M:%S%.3f").to_string()
// }
