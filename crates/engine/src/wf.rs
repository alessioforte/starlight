use super::tasks::{Command, HandleSource, HandleTarget, Status, Tasks, Wiring};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use super::cfg::{Component, Config};

/// Workflow
/// A workflow is a collection of tasks that are executed in a specific order
/// and with specific dependencies.
/// The workflow is responsible for creating the tasks and managing their execution.
/// The workflow is also responsible for managing the state of the tasks and the
/// communication between them.
#[derive(Debug)]
pub struct Workflow {
    pub id: String,                  // Workflow ID
    pub name: Option<String>,        // Workflow name
    pub description: Option<String>, // Workflow description
    pub state: Arc<Mutex<Value>>,    // Workflow state
    cmd_tx: watch::Sender<Command>,  // Shared command channel
    tasks: Vec<Component>,           // List of tasks
    handles: Vec<JoinHandle<()>>,    // Handles for running tasks
    pub status: Status,              // Workflow status
}

#[derive(Debug)]
pub struct Info {
    pub id: String,
    pub name: String,
    pub description: String,
    pub state: Value,
}

impl Workflow {
    pub fn new(
        id: String,
        name: Option<String>,
        description: Option<String>,
        tasks: Vec<Component>,
    ) -> Self {
        let (cmd_tx, _) = watch::channel(Command::None);
        let state = Value::Object(serde_json::Map::new());
        Self {
            id,
            name,
            description,
            tasks,
            cmd_tx,
            state: Arc::new(Mutex::new(state)),
            handles: Vec::new(),
            status: Status::Idle,
        }
    }

    pub fn load(&mut self) {
        let (state_tx, state_rx) = mpsc::unbounded_channel::<Value>();

        let mut channels: HashMap<
            String,
            (
                mpsc::UnboundedSender<Value>,
                Arc<Mutex<mpsc::UnboundedReceiver<Value>>>,
            ),
        > = HashMap::new();

        // Create channels for each task dependencies
        for task in &self.tasks {
            for dep in &task.dependencies {
                if !channels.contains_key(dep) {
                    let (tx, rx) = mpsc::unbounded_channel::<Value>();
                    channels.insert(dep.clone(), (tx, Arc::new(Mutex::new(rx))));
                }
            }
        }

        for task in &self.tasks {
            // Create output transmitters
            let out_txs: HashMap<String, Vec<HandleSource>> = task
                .handles
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.iter()
                            .map(|id| HandleSource {
                                id: id.clone(),
                                tx: channels.get(id).unwrap().0.clone(),
                            })
                            .collect(),
                    )
                })
                .collect();

            // Create input receivers
            let in_rxs: HashMap<String, HandleTarget> = task
                .dependencies
                .iter()
                .map(|id| {
                    (
                        id.clone(),
                        HandleTarget {
                            id: id.clone(),
                            rx: channels.get(id).unwrap().1.clone(),
                        },
                    )
                })
                .collect();

            let wiring = Wiring {
                cmd_rx: self.cmd_tx.subscribe(),
                state_tx: state_tx.clone(),
                out_txs,
                in_rxs,
            };

            let handle = Tasks::spawner(task, wiring);
            self.handles.push(handle);
        }

        let state_manager = self.state_manager(state_rx);
        self.handles.push(state_manager);
        self.status = Status::Ready;

        log::info!(
            "Workflow {} loaded with {} tasks",
            self.id,
            self.tasks.len()
        );
    }

    fn state_manager(
        &self,
        mut state_rx: mpsc::UnboundedReceiver<Value>,
    ) -> tokio::task::JoinHandle<()> {
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            while let Some(msg) = state_rx.recv().await {
                let mut state = state.lock().await;
            }
        })
    }

    pub async fn info(&self) -> Info {
        Info {
            id: self.id.clone(),
            name: self.name.clone().unwrap_or_else(|| "".to_string()),
            description: self.description.clone().unwrap_or_else(|| "".to_string()),
            state: self.state.lock().await.clone(),
        }
    }

    pub async fn get_state(&self) -> Value {
        self.state.lock().await.clone()
    }

    pub async fn display(&self) {
        let info = self.info().await;
        println!("Workflow:    {}", info.id);
        println!("Name:        {}", info.name);
        println!("Description: {}", info.description);
        println!("State:       {}", info.state);
    }

    pub fn get_config(&self) -> Config {
        let config = Config {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            tasks: self.tasks.clone(),
        };
        config
    }

    pub fn execute(&mut self) {
        if matches!(self.status, Status::Ready | Status::Paused) {
            let _ = self.cmd_tx.send(Command::Execute);
            self.status = Status::Running;
        }
    }

    pub fn pause(&mut self) {
        let _ = self.cmd_tx.send(Command::Pause);
        self.status = Status::Paused;
    }

    pub fn stop(&mut self) {
        let _ = self.cmd_tx.send(Command::Stop);
        self.status = Status::Stopped;
    }
}
