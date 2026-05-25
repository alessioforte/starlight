//! Task context and channel abstractions
//!
//! This module provides simple, safe abstractions for task communication.
//! Task developers work with `Input` and `Output` types instead of raw channels.

use crate::err::{ChannelError, EngineError, Result};
use crate::metrics::{CoarseClock, MetricsSnapshot, TaskMetrics};
use crate::msg::Msg;
use crate::resource::{ResourceMap, ResourceValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Notify, mpsc};
use tokio::task::AbortHandle;

// ---------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------

/// Input channel abstraction for receiving data
///
/// Wraps an `mpsc::Receiver` so that tasks never deal with raw channels.
/// Each `Input` can only be owned by one consumer — there is no cloning.
///
/// When metrics are configured (via [`TaskContext`]), `recv()` automatically
/// increments `messages_in` and updates `last_message_at` — zero syscalls.
pub struct Input {
    id: String,
    rx: mpsc::Receiver<Msg>,
    metrics: Option<Arc<TaskMetrics>>,
    clock: Option<CoarseClock>,
}

impl Input {
    /// Create a new input (without metrics — used in tests)
    #[cfg(test)]
    pub(crate) fn new(id: String, rx: mpsc::Receiver<Msg>) -> Self {
        Self {
            id,
            rx,
            metrics: None,
            clock: None,
        }
    }

    /// Create a new input with metrics tracking
    pub(crate) fn with_metrics(
        id: String,
        rx: mpsc::Receiver<Msg>,
        metrics: Arc<TaskMetrics>,
        clock: Option<CoarseClock>,
    ) -> Self {
        Self {
            id,
            rx,
            metrics: Some(metrics),
            clock,
        }
    }

    /// Receive the next message from this input channel
    ///
    /// Blocks until a message is available or the channel is closed.
    /// Automatically updates metrics counters when configured.
    pub async fn recv(&mut self) -> Result<Msg> {
        let msg = self
            .rx
            .recv()
            .await
            .ok_or_else(|| EngineError::Channel(ChannelError::Closed(self.id.clone())))?;

        if let Some(m) = &self.metrics {
            m.messages_in.fetch_add(1, Ordering::Relaxed);
            if let Some(c) = &self.clock {
                m.last_message_at.store(c.now_millis(), Ordering::Relaxed);
            }
        }

        Ok(msg)
    }

    /// Try to receive a message without blocking
    ///
    /// Returns `Ok(Some(msg))` if a message is available,
    /// `Ok(None)` if no message is available,
    /// or an error if the channel is closed.
    pub fn try_recv(&mut self) -> Result<Option<Msg>> {
        match self.rx.try_recv() {
            Ok(msg) => {
                if let Some(m) = &self.metrics {
                    m.messages_in.fetch_add(1, Ordering::Relaxed);
                    if let Some(c) = &self.clock {
                        m.last_message_at.store(c.now_millis(), Ordering::Relaxed);
                    }
                }
                Ok(Some(msg))
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                Err(EngineError::Channel(ChannelError::Closed(self.id.clone())))
            }
        }
    }

    /// Get the input channel ID
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Input").field("id", &self.id).finish()
    }
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// Output channel abstraction for sending data
///
/// Holds one `mpsc::Sender` per downstream consumer.
/// `send()` is async and provides **backpressure**: if any downstream
/// buffer is full the call will suspend until space is available.
///
/// When metrics are configured, `send()` automatically increments
/// `messages_out` on success.
pub struct Output {
    label: String,
    handles: Vec<mpsc::Sender<Msg>>,
    metrics: Option<Arc<TaskMetrics>>,
}

impl Output {
    /// Create a new output (without metrics — used in tests)
    #[cfg(test)]
    pub(crate) fn new(label: String, handles: Vec<mpsc::Sender<Msg>>) -> Self {
        Self {
            label,
            handles,
            metrics: None,
        }
    }

    /// Create a new output with metrics tracking
    pub(crate) fn with_metrics(
        label: String,
        handles: Vec<mpsc::Sender<Msg>>,
        metrics: Arc<TaskMetrics>,
    ) -> Self {
        Self {
            label,
            handles,
            metrics: Some(metrics),
        }
    }

    /// Send a message to all connected downstream consumers
    ///
    /// Optimised for the common case:
    /// - Fan-out = 0: no-op
    /// - Fan-out = 1: zero-copy (owned `Msg` forwarded directly)
    /// - Fan-out > 1: one `Arc::new` + N cheap `Arc::clone`s
    pub async fn send(&self, msg: Msg) -> Result<()> {
        let err = |this: &Self| {
            if let Some(m) = &this.metrics {
                m.errors.fetch_add(1, Ordering::Relaxed);
            }
            EngineError::Channel(ChannelError::NoReceivers(this.label.clone()))
        };

        match self.handles.len() {
            0 => {
                tracing::trace!("Output '{}' has no receivers", self.label);
                return Ok(());
            }
            1 => {
                self.handles[0].send(msg).await.map_err(|_| err(self))?;
            }
            _ => {
                let shared = msg.to_shared();
                for handle in &self.handles {
                    handle.send(shared.clone()).await.map_err(|_| err(self))?;
                }
            }
        }

        if let Some(m) = &self.metrics {
            m.messages_out.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Get the output label
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Number of downstream consumers
    pub fn consumer_count(&self) -> usize {
        self.handles.len()
    }
}

impl std::fmt::Debug for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Output")
            .field("label", &self.label)
            .field("consumer_count", &self.consumer_count())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// TaskContext
// ---------------------------------------------------------------------------

/// Task execution context
///
/// Provides everything a task needs to interact with the workflow:
/// - Input channels via named ports (mpsc receivers — consumed on first access)
/// - Output channels (mpsc senders with backpressure)
/// - Running / paused / stopped state
///
/// This is the main interface tasks use to communicate.
#[derive(Clone)]
pub struct TaskContext {
    /// Unique task identifier
    pub id: String,

    /// Names of the input ports (for metadata queries — always available)
    input_port_names: Arc<Vec<String>>,

    /// Names of the output labels (for metadata queries — always available)
    output_label_names: Arc<Vec<String>>,

    /// Input receivers, keyed by port name.
    /// Each port can have multiple channels (channel_id, Receiver).
    /// Behind a Mutex because `mpsc::Receiver` is not Clone and must be *taken*.
    inputs: Arc<tokio::sync::Mutex<HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>>>>,

    /// Output senders, keyed by label
    outputs: Arc<std::sync::Mutex<HashMap<String, Vec<mpsc::Sender<Msg>>>>>,

    /// Read-only resources visible to this task
    resources: Arc<ResourceMap>,

    /// Channel capacity (used when creating the merged‐input channel)
    channel_capacity: usize,

    /// Whether the task should continue running
    running: Arc<AtomicBool>,

    /// Whether the task has been permanently stopped
    stopped: Arc<AtomicBool>,

    /// Notification for waking up paused tasks
    resume_notify: Arc<Notify>,

    /// Abort handles for merged-input forwarder tasks
    forwarders: Arc<std::sync::Mutex<Vec<AbortHandle>>>,

    /// Per-task metrics (always present)
    metrics: Arc<TaskMetrics>,

    /// Shared coarse clock (optional — set by WorkflowBuilder)
    clock: Option<CoarseClock>,
}

impl TaskContext {
    /// Create a new task context (no clock — useful for tests)
    ///
    /// `inputs` is keyed by port name; each port maps to a list of
    /// `(channel_id, Receiver)` pairs.
    pub fn new(
        id: String,
        inputs: HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Msg>>>,
    ) -> Self {
        Self::build(
            id,
            inputs,
            outputs,
            Arc::new(ResourceMap::new()),
            1000,
            None,
        )
    }

    /// Create a new task context with explicit capacity and optional clock
    pub fn with_capacity(
        id: String,
        inputs: HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Msg>>>,
        channel_capacity: usize,
        clock: Option<CoarseClock>,
    ) -> Self {
        Self::build(
            id,
            inputs,
            outputs,
            Arc::new(ResourceMap::new()),
            channel_capacity,
            clock,
        )
    }

    /// Create a new task context with explicit resources, capacity, and clock.
    pub(crate) fn with_capacity_and_resources(
        id: String,
        inputs: HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Msg>>>,
        resources: Arc<ResourceMap>,
        channel_capacity: usize,
        clock: Option<CoarseClock>,
    ) -> Self {
        Self::build(id, inputs, outputs, resources, channel_capacity, clock)
    }

    fn build(
        id: String,
        inputs: HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Msg>>>,
        resources: Arc<ResourceMap>,
        channel_capacity: usize,
        clock: Option<CoarseClock>,
    ) -> Self {
        let port_names: Vec<String> = inputs.keys().cloned().collect();
        let output_labels: Vec<String> = outputs.keys().cloned().collect();
        Self {
            id,
            input_port_names: Arc::new(port_names),
            output_label_names: Arc::new(output_labels),
            inputs: Arc::new(tokio::sync::Mutex::new(inputs)),
            outputs: Arc::new(std::sync::Mutex::new(outputs)),
            resources,
            channel_capacity,
            running: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            resume_notify: Arc::new(Notify::new()),
            forwarders: Arc::new(std::sync::Mutex::new(Vec::new())),
            metrics: Arc::new(TaskMetrics::new()),
            clock,
        }
    }

    // ------------------------------------------------------------------
    // Resource access
    // ------------------------------------------------------------------

    /// Get a read-only resource visible to this task.
    pub fn resource(&self, id: &str) -> Result<ResourceValue> {
        self.resources
            .get(id)
            .cloned()
            .ok_or_else(|| EngineError::config(format!("resource '{}' not found", id)))
    }

    /// Get all resource IDs visible to this task.
    pub fn resource_ids(&self) -> Vec<&str> {
        self.resources.keys().map(|s| s.as_str()).collect()
    }

    // ------------------------------------------------------------------
    // Input access  (each method *takes* receivers — one-shot)
    // ------------------------------------------------------------------

    /// Get an input by port name
    ///
    /// Takes all receivers for the given port and merges them into a single
    /// `Input`. If the port has exactly one channel, no merging overhead.
    ///
    /// The receivers are **moved** out: calling this twice with the same
    /// port name returns `InputNotFound` on the second call.
    pub async fn input(&self, port: &str) -> Result<Input> {
        let mut inputs = self.inputs.lock().await;
        let receivers = inputs
            .remove(port)
            .ok_or_else(|| EngineError::Channel(ChannelError::InputNotFound(port.to_string())))?;

        self.merge_receivers(port.to_string(), receivers)
    }

    /// Get a merged input that receives from ALL input ports
    ///
    /// If there is a single receiver across all ports it is returned
    /// directly (zero overhead). For multiple receivers a small set of
    /// forwarding tasks is spawned; they terminate automatically when
    /// the upstream senders drop.
    pub async fn merged_input(&self) -> Result<Input> {
        let mut inputs = self.inputs.lock().await;

        // Flatten all ports into a single Vec of receivers
        let all_receivers: Vec<(String, mpsc::Receiver<Msg>)> =
            inputs.drain().flat_map(|(_, rxs)| rxs).collect();

        if all_receivers.is_empty() {
            return Err(EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            )));
        }

        self.merge_receivers("merged".to_string(), all_receivers)
    }

    /// Get one merged `Input` per port name
    ///
    /// Useful for tasks that need to process each port independently
    /// (e.g., a Join with `"left"` and `"right"` ports).
    pub async fn named_inputs(&self) -> Result<HashMap<String, Input>> {
        let mut inputs = self.inputs.lock().await;

        if inputs.is_empty() {
            return Err(EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            )));
        }

        let mut result = HashMap::new();
        for (port, receivers) in inputs.drain() {
            let input = self.merge_receivers(port.clone(), receivers)?;
            result.insert(port, input);
        }
        Ok(result)
    }

    /// Get all input channels as a flat list
    ///
    /// Drains every receiver from every port.
    pub async fn inputs(&self) -> Result<Vec<Input>> {
        let mut inputs = self.inputs.lock().await;
        let result = inputs
            .drain()
            .flat_map(|(_, rxs)| rxs)
            .map(|(id, rx)| {
                Input::with_metrics(id, rx, Arc::clone(&self.metrics), self.clock.clone())
            })
            .collect();
        Ok(result)
    }

    /// Get the first available input channel
    ///
    /// Convenience for tasks with a single input port.
    pub async fn first_input(&self) -> Result<Input> {
        let mut inputs = self.inputs.lock().await;
        let key = inputs.keys().next().cloned().ok_or_else(|| {
            EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            ))
        })?;
        let receivers = inputs.remove(&key).unwrap();
        self.merge_receivers(key, receivers)
    }

    /// Internal helper: merge a list of (channel_id, Receiver) into a single Input.
    ///
    /// Fast path for single receiver (zero overhead).
    fn merge_receivers(
        &self,
        name: String,
        mut receivers: Vec<(String, mpsc::Receiver<Msg>)>,
    ) -> Result<Input> {
        if receivers.is_empty() {
            return Err(EngineError::Channel(ChannelError::InputNotFound(name)));
        }

        // Fast path: single receiver — no forwarding needed
        if receivers.len() == 1 {
            let (channel_id, rx) = receivers.pop().unwrap();
            let label = if name == channel_id { name } else { name };
            return Ok(Input::with_metrics(
                label,
                rx,
                Arc::clone(&self.metrics),
                self.clock.clone(),
            ));
        }

        // Multiple receivers: merge into a single mpsc channel
        let (merged_tx, merged_rx) = mpsc::channel(self.channel_capacity);
        let mut abort_handles = Vec::new();

        for (channel_id, mut rx) in receivers {
            let tx = merged_tx.clone();
            let handle = tokio::spawn(async move {
                while let Some(value) = rx.recv().await {
                    if tx.send(value).await.is_err() {
                        break; // merged receiver dropped
                    }
                }
                tracing::debug!("Merged forwarder for '{}' terminated", channel_id);
            });
            abort_handles.push(handle.abort_handle());
        }
        // Drop the original sender so merged_rx closes when all forwarders finish
        drop(merged_tx);

        // Store abort handles so stop() can cancel them
        self.forwarders
            .lock()
            .expect("forwarders mutex poisoned")
            .extend(abort_handles);

        Ok(Input::with_metrics(
            name,
            merged_rx,
            Arc::clone(&self.metrics),
            self.clock.clone(),
        ))
    }

    // ------------------------------------------------------------------
    // Output access
    // ------------------------------------------------------------------

    /// Get an output channel by label
    pub fn output(&self, label: &str) -> Result<Output> {
        let handles: Vec<mpsc::Sender<Msg>> = self
            .outputs
            .lock()
            .expect("outputs mutex poisoned")
            .get(label)
            .ok_or_else(|| EngineError::Channel(ChannelError::OutputNotFound(label.to_string())))?
            .clone();

        Ok(Output::with_metrics(
            label.to_string(),
            handles,
            Arc::clone(&self.metrics),
        ))
    }

    // ------------------------------------------------------------------
    // Execution control
    // ------------------------------------------------------------------

    /// Cooperative check: returns `true` if the task should continue, `false` if stopped.
    ///
    /// **Blocks while paused**, automatically resuming when the workflow is unpaused.
    ///
    /// ```ignore
    /// while ctx.running().await {
    ///     // Process data... suspends here if paused
    /// }
    /// ```
    pub async fn running(&self) -> bool {
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return false;
            }
            if self.running.load(Ordering::Acquire) {
                return true;
            }
            self.resume_notify.notified().await;
        }
    }

    /// Non-blocking check if the task is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Resume the task (internal use)
    pub(crate) fn resume(&self) {
        self.running.store(true, Ordering::Release);
        self.resume_notify.notify_waiters();
    }

    /// Pause the task (internal use)
    pub(crate) fn pause(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Permanently stop the task (internal use)
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.running.store(false, Ordering::Release);
        self.resume_notify.notify_waiters();
        self.close_outputs();

        // Cancel any merged-input forwarder tasks
        let handles = self.forwarders.lock().expect("forwarders mutex poisoned");
        for h in handles.iter() {
            h.abort();
        }
    }

    /// Drop all output senders held by this context.
    ///
    /// The workflow keeps task contexts around for state/metrics inspection after
    /// a task exits. Without explicitly clearing outputs here, downstream sinks
    /// that wait for channel close would never observe producer completion.
    pub(crate) fn close_outputs(&self) {
        self.outputs.lock().expect("outputs mutex poisoned").clear();
    }

    // ------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------

    /// Take a point-in-time snapshot of this task's metrics.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    // ------------------------------------------------------------------
    // Metadata (always synchronous, never consumes receivers)
    // ------------------------------------------------------------------

    /// Get list of input port names configured at build time
    pub fn input_ports(&self) -> Vec<&str> {
        self.input_port_names.iter().map(|s| s.as_str()).collect()
    }

    /// Number of input ports configured at build time
    pub fn input_count(&self) -> usize {
        self.input_port_names.len()
    }

    /// Get list of available output labels
    pub fn output_labels(&self) -> Vec<&str> {
        self.output_label_names.iter().map(|s| s.as_str()).collect()
    }

    /// Check if this is a source task (no inputs)
    pub fn is_source(&self) -> bool {
        self.input_port_names.is_empty()
    }

    /// Check if this is a sink task (no outputs)
    pub fn is_sink(&self) -> bool {
        self.output_label_names.is_empty()
    }
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("id", &self.id)
            .field("input_ports", &self.input_ports())
            .field("outputs", &self.output_labels())
            .field("resources", &self.resource_ids())
            .field("is_running", &self.is_running())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    /// Helper: create a port-keyed input map from a list of (port, channel_id, Receiver).
    fn make_inputs(
        entries: Vec<(&str, &str, mpsc::Receiver<Msg>)>,
    ) -> HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>> {
        let mut map: HashMap<String, Vec<(String, mpsc::Receiver<Msg>)>> = HashMap::new();
        for (port, ch_id, rx) in entries {
            map.entry(port.to_string())
                .or_default()
                .push((ch_id.to_string(), rx));
        }
        map
    }

    #[tokio::test]
    async fn test_input_output() {
        // Create an mpsc channel: sender goes to Output, receiver to Input
        let (tx, rx) = mpsc::channel(10);

        let mut input = Input::new("test".to_string(), rx);
        let output = Output::new("out".to_string(), vec![tx]);

        // Send and receive
        output.send(json!({"test": "value"}).into()).await.unwrap();
        let received = input.recv().await.unwrap();

        assert_eq!(received, json!({"test": "value"}));
    }

    #[test]
    fn test_context_creation() {
        let inputs = HashMap::new();
        let outputs = HashMap::new();

        let ctx = TaskContext::new("task1".to_string(), inputs, outputs);

        assert_eq!(ctx.id, "task1");
        assert!(ctx.is_source());
        assert!(ctx.is_sink());
        assert!(!ctx.is_running());
    }

    #[test]
    fn test_context_resource_access() {
        let mut resources = ResourceMap::new();
        resources.insert(
            "aliases".to_string(),
            ResourceValue::Json(Arc::new(json!({"IT": "Italy"}))),
        );

        let ctx = TaskContext::with_capacity_and_resources(
            "task1".to_string(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(resources),
            1000,
            None,
        );

        match ctx.resource("aliases").unwrap() {
            ResourceValue::Json(value) => assert_eq!(value["IT"], "Italy"),
            _ => panic!("expected json resource"),
        }
        assert!(ctx.resource("missing").is_err());
    }

    #[tokio::test]
    async fn test_context_with_channels() {
        let (_tx1, rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        let inputs = make_inputs(vec![("in", "input1", rx1)]);
        let mut outputs: HashMap<String, Vec<mpsc::Sender<Msg>>> = HashMap::new();
        outputs.insert("out1".to_string(), vec![tx2]);

        let ctx = TaskContext::new("task1".to_string(), inputs, outputs);

        assert!(!ctx.is_source());
        assert!(!ctx.is_sink());
        assert_eq!(ctx.input_ports(), vec!["in"]);
        assert_eq!(ctx.output_labels(), vec!["out1"]);
    }

    #[tokio::test]
    async fn test_merged_input() {
        let (tx1, rx1) = mpsc::channel(10);
        let (tx2, rx2) = mpsc::channel(10);

        let inputs = make_inputs(vec![("in", "input1", rx1), ("in", "input2", rx2)]);

        let ctx = Arc::new(TaskContext::new(
            "task1".to_string(),
            inputs,
            HashMap::new(),
        ));

        // Get merged input (takes ownership of receivers)
        let mut merged = ctx.merged_input().await.unwrap();

        // Small yield to let forwarding tasks start
        tokio::task::yield_now().await;

        // Send messages to both input channels
        tx1.send(json!({"source": "input1", "value": 1}).into())
            .await
            .unwrap();
        tx2.send(json!({"source": "input2", "value": 2}).into())
            .await
            .unwrap();

        // Should receive from both channels
        let msg1 = merged.recv().await.unwrap();
        let msg2 = merged.recv().await.unwrap();

        // Both messages should be received (order may vary)
        let received_values: Vec<i64> = vec![
            msg1["value"].as_i64().unwrap(),
            msg2["value"].as_i64().unwrap(),
        ];

        assert!(received_values.contains(&1));
        assert!(received_values.contains(&2));
    }

    #[tokio::test]
    async fn test_merged_input_across_ports() {
        let (tx1, rx1) = mpsc::channel(10);
        let (tx2, rx2) = mpsc::channel(10);

        // Two different ports — merged_input should merge both
        let inputs = make_inputs(vec![("left", "ch_left", rx1), ("right", "ch_right", rx2)]);

        let ctx = Arc::new(TaskContext::new(
            "task1".to_string(),
            inputs,
            HashMap::new(),
        ));

        let mut merged = ctx.merged_input().await.unwrap();
        tokio::task::yield_now().await;

        tx1.send(json!({"side": "left"}).into()).await.unwrap();
        tx2.send(json!({"side": "right"}).into()).await.unwrap();

        let msg1 = merged.recv().await.unwrap();
        let msg2 = merged.recv().await.unwrap();

        let sides: Vec<&str> = vec![
            msg1["side"].as_str().unwrap(),
            msg2["side"].as_str().unwrap(),
        ];
        assert!(sides.contains(&"left"));
        assert!(sides.contains(&"right"));
    }

    #[tokio::test]
    async fn test_input_by_port_name() {
        let (tx1, rx1) = mpsc::channel(10);
        let (tx2, rx2) = mpsc::channel(10);

        let inputs = make_inputs(vec![("left", "ch_csv", rx1), ("right", "ch_api", rx2)]);

        let ctx = Arc::new(TaskContext::new(
            "join1".to_string(),
            inputs,
            HashMap::new(),
        ));

        let mut left = ctx.input("left").await.unwrap();
        let mut right = ctx.input("right").await.unwrap();

        tx1.send(json!({"from": "csv"}).into()).await.unwrap();
        tx2.send(json!({"from": "api"}).into()).await.unwrap();

        let l = left.recv().await.unwrap();
        let r = right.recv().await.unwrap();

        assert_eq!(l["from"], "csv");
        assert_eq!(r["from"], "api");
    }

    #[tokio::test]
    async fn test_named_inputs() {
        let (tx1, rx1) = mpsc::channel(10);
        let (tx2, rx2) = mpsc::channel(10);

        let inputs = make_inputs(vec![("left", "ch1", rx1), ("right", "ch2", rx2)]);

        let ctx = Arc::new(TaskContext::new(
            "join1".to_string(),
            inputs,
            HashMap::new(),
        ));

        let mut named = ctx.named_inputs().await.unwrap();
        assert_eq!(named.len(), 2);
        assert!(named.contains_key("left"));
        assert!(named.contains_key("right"));

        tx1.send(json!("L").into()).await.unwrap();
        tx2.send(json!("R").into()).await.unwrap();

        let l = named.get_mut("left").unwrap().recv().await.unwrap();
        let r = named.get_mut("right").unwrap().recv().await.unwrap();
        assert_eq!(l, json!("L"));
        assert_eq!(r, json!("R"));
    }

    #[tokio::test]
    async fn test_input_port_not_found() {
        let ctx = TaskContext::new("t".to_string(), HashMap::new(), HashMap::new());
        let result = ctx.input("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_forwarders_aborted_on_stop() {
        let (tx1, rx1) = mpsc::channel(10);
        let (_tx2, rx2) = mpsc::channel(10);

        let inputs = make_inputs(vec![("in", "a", rx1), ("in", "b", rx2)]);

        let ctx = Arc::new(TaskContext::new(
            "task1".to_string(),
            inputs,
            HashMap::new(),
        ));

        let mut merged = ctx.merged_input().await.unwrap();
        tokio::task::yield_now().await;

        // Verify forwarders are registered
        assert_eq!(
            ctx.forwarders.lock().unwrap().len(),
            2,
            "two forwarder abort handles expected"
        );

        // Messages flow normally
        tx1.send(json!(1).into()).await.unwrap();
        let msg = merged.recv().await.unwrap();
        assert_eq!(msg, json!(1));

        // Stop the context — forwarders should be aborted
        ctx.stop();
        tokio::task::yield_now().await;

        // After abort, trying to send still succeeds on the mpsc sender
        // but the forwarder won't forward it. merged.recv() should return
        // Err (channel closed) since the forwarder is dead.
        let _ = tx1.send(json!(999).into()).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // The merged channel should be closed (or at least not deliver new msgs)
        let result = merged.try_recv();
        // Either Err (closed) or Ok(None) — but NOT Ok(Some(999))
        match result {
            Ok(Some(val)) => panic!("forwarder should be dead, but got: {val:?}"),
            _ => {} // Err(Closed) or Ok(None) — both correct
        }
    }

    #[tokio::test]
    async fn test_backpressure() {
        // Create a channel with capacity 2
        let (tx, rx) = mpsc::channel(2);
        let output = Output::new("out".to_string(), vec![tx]);

        // Fill the buffer
        output.send(json!(1).into()).await.unwrap();
        output.send(json!(2).into()).await.unwrap();

        // The next send should block until we drain one message
        let mut input = Input::new("test".to_string(), rx);
        let send_handle = tokio::spawn(async move {
            output.send(json!(3).into()).await.unwrap();
        });

        // Give the send a moment to attempt (it should be blocked)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(
            !send_handle.is_finished(),
            "send should be blocked by backpressure"
        );

        // Drain one message — this should unblock the send
        let _ = input.recv().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(
            send_handle.is_finished(),
            "send should have completed after drain"
        );
    }
}
