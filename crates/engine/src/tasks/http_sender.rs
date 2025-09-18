use super::{HandleTarget, Task, Wiring, Worker};
use crate::task;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::time::{self, Duration};

const NAME: &str = "HttpSender";

#[derive(Deserialize, Debug, Clone)]
pub struct HttpSender {
    endpoint: String,
    method: String,
    interval: u64,
    // headers: serde_json::Value,
}

task! {
    HttpSender,
    async fn execute(&self, channel: Option<&HandleTarget>) {

        let wiring = self.wiring.clone();
        for channel in wiring.in_rxs.iter() {
            // tokio::task::spawn({
            //     let mut rx = channel.1.rx.lock().await;
            //     while let Some(payload) = rx.recv().await {

            //     }
            // });
        }

        // let mut buffer = Vec::new();
        // let mut interval = time::interval(Duration::from_millis(self.params.interval));
        // interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        // let task_id = self.id.clone();
        // let endpoint = self.params.endpoint.clone();
        // let interval = self.params.interval;
        // // let headers = self.params.headers.clone();
        // let client = Client::new();

        // let mut sum = 0;
        // loop {
        //     let batch_duration = Duration::from_millis(interval);
        //     let timeout = time::sleep(batch_duration);
        //     tokio::pin!(timeout);

        //     loop {
        //         tokio::select! {
        //             Some(value) = rx.recv() => {
        //                 buffer.push(value);
        //             }
        //             _ = &mut timeout => {
        //                 break;
        //             }
        //         }
        //     }

        //     if !buffer.is_empty() {
        //         let method: reqwest::Method = self.params.method.parse().unwrap();
        //         let res = client
        //             .request(method, endpoint.as_str())
        //             .json(&buffer)
        //             .send()
        //             .await;

        //         if res.is_err() {
        //             let err = res.unwrap_err();
        //             log::error!(
        //                 "[{NAME}]-{task_id} channel-{channel_id} Error sending data: {err}"
        //             );
        //         }
        //         sum += buffer.len();
        //         println!("count: {} - tot: {}", buffer.len(), sum);
        //         buffer.clear();

        //         let value = Value::from(sum);
        //         if let Err(e) = self.wiring.state_tx.send(value) {
        //             log::error!("[{NAME}]-{task_id} channel-{channel_id} Error sending state: {e}");
        //         };
        //     }
        // }
    }
}
