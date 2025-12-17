use super::cfg::Component;
use super::tasks;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, atomic::Ordering::Relaxed};
use tokio::sync::{RwLock, broadcast, watch};

mod csv_reader;
mod csv_writer;
mod json_remapper;
// mod kafka_consumer;
mod kafka_producer;
mod math_exp_eval;
mod random_numbers_generator;
mod simulator;
mod splitter;
mod stdout_logger;
mod type_converter;

/// Represents the different types of tasks that can be executed in the workflow.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Tasks {
    RandomNumbersGenerator,
    JsonRemapper,
    StdoutLogger,
    CsvReader,
    TypeConverter,
    CsvWriter,
    Splitter,
    MathExpEval,
    Simulator,
    KafkaProducer,
    // KafkaConsumer,
    // HttpSender,
    // Scheduler,
}

impl Tasks {
    /// Spawns a task based on the component type and wiring provided.
    ///
    /// This allows for dynamic task creation based on the configuration defined in the `Component`.
    pub fn spawner(component: &Component, wiring: Wiring) -> tokio::task::JoinHandle<()> {
        match component.kind {
            Tasks::StdoutLogger => {
                let task = tasks::stdout_logger::create(component, wiring);
                task.spawn()
            }
            Tasks::JsonRemapper => {
                let task = tasks::json_remapper::create(component, wiring);
                task.spawn()
            }
            Tasks::Simulator => {
                let task = tasks::simulator::create(component, wiring);
                task.spawn()
            }
            Tasks::RandomNumbersGenerator => {
                let task = tasks::random_numbers_generator::create(component, wiring);
                task.spawn()
            }
            Tasks::CsvReader => {
                let task = tasks::csv_reader::create(component, wiring);
                task.spawn()
            }
            Tasks::TypeConverter => {
                let task = tasks::type_converter::create(component, wiring);
                task.spawn()
            }
            Tasks::CsvWriter => {
                let task = tasks::csv_writer::create(component, wiring);
                task.spawn()
            }
            Tasks::Splitter => {
                let task = tasks::splitter::create(component, wiring);
                task.spawn()
            }
            Tasks::MathExpEval => {
                let task = tasks::math_exp_eval::create(component, wiring);
                task.spawn()
            }
            Tasks::KafkaProducer => {
                let task = tasks::kafka_producer::create(component, wiring);
                task.spawn()
            } // Tasks::KafkaConsumer => {
              //     let task = tasks::kafka_consumer::create(component, wiring);
              //     task.spawn()
              // }
        }
    }
}

/// Represents the commands that can be sent to control the task execution.
#[derive(Debug, Clone)]
pub enum Command {
    None,
    Execute,
    Pause,
    Stop,
    // PauseGracefully,
    // StopGracefully,
}

/// Represents the status of a task in the workflow.
/// It can be one of the following:
/// - `Idle`: The task is not running and is ready to be executed.
/// - `Ready`: The task is ready to be executed but has not started yet.
/// - `Running`: The task is currently executing.
/// - `Paused`: The task is paused and can be resumed.
/// - `Stopped`: The task has been stopped and will not execute further.
/// - `Error(String)`: The task encountered an error, with the error message provided as a string.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Idle,
    Ready,
    Running,
    Paused,
    Stopped,
    Error(String),
}

/// Represents a source handle for sending data to other tasks.
///
/// It contains:
/// - `id`: The identifier of the task that this handle belongs to
/// - `tx`: A sender for outgoing data, wrapped in an `mpsc::UnboundedSender<Value>` to allow sending values without blocking.
#[derive(Debug)]
pub struct HandleSource {
    pub id: String,
    pub tx: broadcast::Sender<Value>,
}

/// Represents a target handle for receiving data from other tasks.
///
/// It contains:
/// - `id`: The identifier of the task that this handle belongs to
/// - `tx`: A sender for incoming data, wrapped in a `broadcast::Sender<Value>` to allow broadcasting values to multiple receivers.
#[derive(Debug)]
pub struct HandleTarget {
    pub id: String,
    pub tx: broadcast::Sender<Value>,
}

/// Wiring structure that holds the communication channels and handles for a task.
/// It contains:
/// - `cmd_rx`: A receiver for commands to control the task (e.g., start, stop, pause)
/// - `state_tx`: A sender for task state updates
/// - `out_txs`: A map of output channels for sending data to other tasks, keyed by label
/// - `in_rxs`: A map of input channels for receiving data from other tasks, keyed by task ID
/// - The `ctx` field is commented out, but it could be used to hold additional context information if needed.
#[derive(Debug)]
pub struct Wiring {
    pub cmd_rx: watch::Receiver<Command>,
    // pub state_tx: mpsc::UnboundedSender<Value>,
    pub out_txs: DashMap<String, Vec<HandleSource>>,
    pub in_rxs: DashMap<String, HandleTarget>,
    // pub out_txs: HashMap<String, Vec<HandleSource>>,
    // pub in_rxs: HashMap<String, HandleTarget>,
    // pub ctx: Value, TODO
}

/// Represents a task with its parameters and wiring.
/// It contains:
/// - `id`: A unique identifier for the task
/// - `params`: The parameters for the task
/// - `wiring`: The wiring structure that holds the communication channels and handles for the task
/// - `leading`: A boolean indicating whether this task is the leading task in the workflow
/// - `running`: An atomic boolean that indicates whether the task is currently running
/// - `state`: An optional field for the task state, which can be used to store additional information about the task (currently commented out)
/// - `status`: An optional field for the task status, which can be used to track the execution state of the task (currently commented out)
pub struct Task<T, S> {
    id: String,
    params: T,
    state: S,
    leading: bool,
    wiring: Arc<Wiring>,
    running: Arc<AtomicBool>,
    status: RwLock<Status>,
}

impl<T, S> Task<T, S> {
    /// Creates a new task with the given ID, parameters, and wiring.
    ///
    /// The `params` argument is expected to be a JSON value that can be deserialized into the type `T`.
    ///
    /// The `wiring` argument contains the communication channels and handles for the task.
    pub fn new(id: String, params: Value, wiring: Wiring) -> Self
    where
        T: DeserializeOwned,
        S: Default,
    {
        let params: T = serde_json::from_value(params).unwrap();
        let leading = wiring.in_rxs.is_empty();
        Task {
            id,
            params,
            wiring: Arc::new(wiring),
            leading,
            state: S::default(),
            status: RwLock::new(Status::Ready),
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
pub trait Worker<T>: Send + Sync {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    fn wiring(&self) -> Arc<Wiring>;
    fn running(&self) -> Arc<AtomicBool>;
    fn leading(&self) -> bool;
    fn subscribe(&self, id: &str) -> broadcast::Receiver<Value> {
        let wiring = self.wiring();
        let handle = wiring.in_rxs.get(id).expect("Channel not found");
        handle.tx.subscribe()
    }

    async fn set_status(&self, status: Status);

    /// Spawns the task and returns a `JoinHandle` to manage its execution.
    fn spawn(self) -> tokio::task::JoinHandle<()>;

    /// Executes the task logic.
    ///
    /// This method should be implemented by the task to define its behavior.
    // async fn execute(&self, channel: Option<&HandleTarget>);
    async fn execute(&self, id: Option<&str>);

    /// This method is responsible for valuating if the task is leading or not.
    /// If the task is leading, it can execute immediately without waiting for input channels.
    async fn run(&self) {
        // if the task is leading it means it has no input channels
        // so it can execute immediately
        if self.leading() {
            self.execute(None).await;
            return;
        }

        // If the task is not leading, it will wait for input channels to be ready
        // In order to do that, it will create a futures stream that will listen to all input channels
        // and execute the task when any of them receives a message
        let wiring = self.wiring();
        let mut futures = FuturesUnordered::new();

        // for (_, channel) in wiring.in_rxs.iter() {
        //     futures.push(self.execute(Some(channel)));
        // }

        wiring.in_rxs.iter().for_each(|channel| {
            let task_ref = self;
            futures.push(async move {
                task_ref.execute(Some(channel.key())).await;
            });
        });

        loop {
            tokio::select! {
                _ = futures.next() => {} // REVIEW: Is there a better way to handle this?
            }
        }
    }

    /// Loads the task and starts listening for commands to control its execution.
    /// This method will run in a loop, waiting for commands to execute, pause, or stop the task.
    async fn load(self)
    where
        Self: Sized,
    {
        let id = self.id();
        let name = self.name();
        let mut cmd_rx = self.wiring().cmd_rx.clone();

        loop {
            tokio::select! {
                _ = self.run() => {}
                _ = cmd_rx.changed() => {
                    let cmd = cmd_rx.borrow_and_update().clone();
                    match cmd {
                        Command::Stop => {
                            let running = self.running();
                            if running.load(Relaxed) {
                                running.store(false, Relaxed);
                                log::info!("STOP    [{name}]-{id}");
                            }
                            break;
                        }
                        Command::Execute => {
                            let running = self.running();
                            if !running.load(Relaxed) {
                                running.store(true, Relaxed);
                                log::info!("EXECUTE [{name}]-{id}");
                            }
                        }
                        Command::Pause => {
                            let running = self.running();
                            running.store(false, Relaxed);
                            log::info!("PAUSE   [{name}]-{id}");
                        }
                        _ => break,
                    }
                }
            }
        }
    }
}

/// Macro that provides a default implementation for tasks.
/// The macro expects the following parameters:
/// - The Params type of the task
/// - Custom method `execute` that the task should implement
#[macro_export]
macro_rules! task {
    ($type:ty, $state:ty, $($custom_method:item)*) => {
        pub fn create<>(component: &crate::cfg::Component, wiring: Wiring) -> impl Worker<$type>
        where
            $type: serde::de::DeserializeOwned,
            $state: Default + Send + Sync + 'static,
        {
            Task::<$type, $state>::new(component.id.clone(), component.params.clone(), wiring)
        }

        #[async_trait]
        impl Worker<$type> for Task<$type, $state> {
            fn id(&self) -> &str {
                &self.id
            }
            fn name(&self) -> &str {
                NAME
            }
            fn wiring(&self) -> Arc<Wiring> {
                Arc::clone(&self.wiring)
            }

            fn running(&self) -> Arc<AtomicBool> {
                Arc::clone(&self.running)
            }

            fn leading(&self) -> bool {
                self.leading
            }

            fn spawn(self) -> tokio::task::JoinHandle<()> {
                tokio::spawn(self.load())
            }

            async fn set_status(&self, status: Status) {
                let mut lock = self.status.write().await;
                *lock = status;
            }

            $($custom_method)*
        }
    };
}
