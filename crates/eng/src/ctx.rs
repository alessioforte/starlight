//! Task context and channel abstractions
//!
//! This module provides simple, safe abstractions for task communication.
//! Task developers work with `Input` and `Output` types instead of raw channels.

use crate::err::{ChannelError, EngineError, Result};
use crate::metrics::{CoarseClock, MetricsSnapshot, TaskMetrics};
use serde_json::Value;
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
    rx: mpsc::Receiver<Value>,
    metrics: Option<Arc<TaskMetrics>>,
    clock: Option<CoarseClock>,
}

impl Input {
    /// Create a new input (without metrics — used in tests)
    #[cfg(test)]
    pub(crate) fn new(id: String, rx: mpsc::Receiver<Value>) -> Self {
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
        rx: mpsc::Receiver<Value>,
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
    pub async fn recv(&mut self) -> Result<Value> {
        let val = self.rx.recv().await.ok_or_else(|| {
            EngineError::Channel(ChannelError::Closed(self.id.clone()))
        })?;

        if let Some(m) = &self.metrics {
            m.messages_in.fetch_add(1, Ordering::Relaxed);
            if let Some(c) = &self.clock {
                m.last_message_at
                    .store(c.now_millis(), Ordering::Relaxed);
            }
        }

        Ok(val)
    }

    /// Try to receive a message without blocking
    ///
    /// Returns `Ok(Some(value))` if a message is available,
    /// `Ok(None)` if no message is available,
    /// or an error if the channel is closed.
    pub fn try_recv(&mut self) -> Result<Option<Value>> {
        match self.rx.try_recv() {
            Ok(val) => {
                if let Some(m) = &self.metrics {
                    m.messages_in.fetch_add(1, Ordering::Relaxed);
                    if let Some(c) = &self.clock {
                        m.last_message_at
                            .store(c.now_millis(), Ordering::Relaxed);
                    }
                }
                Ok(Some(val))
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
    handles: Vec<mpsc::Sender<Value>>,
    metrics: Option<Arc<TaskMetrics>>,
}

impl Output {
    /// Create a new output (without metrics — used in tests)
    #[cfg(test)]
    pub(crate) fn new(label: String, handles: Vec<mpsc::Sender<Value>>) -> Self {
        Self {
            label,
            handles,
            metrics: None,
        }
    }

    /// Create a new output with metrics tracking
    pub(crate) fn with_metrics(
        label: String,
        handles: Vec<mpsc::Sender<Value>>,
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
    /// The value is cloned for each consumer.  The call awaits until
    /// every consumer has room in its buffer (backpressure).
    pub async fn send(&self, value: Value) -> Result<()> {
        if self.handles.is_empty() {
            tracing::trace!("Output '{}' has no receivers", self.label);
            return Ok(());
        }

        for handle in &self.handles {
            handle.send(value.clone()).await.map_err(|_| {
                if let Some(m) = &self.metrics {
                    m.errors.fetch_add(1, Ordering::Relaxed);
                }
                EngineError::Channel(ChannelError::NoReceivers(self.label.clone()))
            })?;
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
/// - Input channels (mpsc receivers — consumed on first access)
/// - Output channels (mpsc senders with backpressure)
/// - Running / paused / stopped state
///
/// This is the main interface tasks use to communicate.
#[derive(Clone)]
pub struct TaskContext {
    /// Unique task identifier
    pub id: String,

    /// Names of the input channels (for metadata queries — always available)
    input_names: Arc<Vec<String>>,

    /// Input receivers, keyed by channel ID.
    /// Behind a Mutex because `mpsc::Receiver` is not Clone and must be *taken*.
    inputs: Arc<tokio::sync::Mutex<HashMap<String, mpsc::Receiver<Value>>>>,

    /// Output senders, keyed by label
    outputs: Arc<HashMap<String, Vec<mpsc::Sender<Value>>>>,

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
    pub fn new(
        id: String,
        inputs: HashMap<String, mpsc::Receiver<Value>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Value>>>,
    ) -> Self {
        Self::build(id, inputs, outputs, 1000, None)
    }

    /// Create a new task context with explicit capacity and optional clock
    pub fn with_capacity(
        id: String,
        inputs: HashMap<String, mpsc::Receiver<Value>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Value>>>,
        channel_capacity: usize,
        clock: Option<CoarseClock>,
    ) -> Self {
        Self::build(id, inputs, outputs, channel_capacity, clock)
    }

    fn build(
        id: String,
        inputs: HashMap<String, mpsc::Receiver<Value>>,
        outputs: HashMap<String, Vec<mpsc::Sender<Value>>>,
        channel_capacity: usize,
        clock: Option<CoarseClock>,
    ) -> Self {
        let input_names: Vec<String> = inputs.keys().cloned().collect();
        Self {
            id,
            input_names: Arc::new(input_names),
            inputs: Arc::new(tokio::sync::Mutex::new(inputs)),
            outputs: Arc::new(outputs),
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
    // Input access  (each method *takes* the receiver — one-shot)
    // ------------------------------------------------------------------

    /// Get an input channel by channel ID
    ///
    /// The receiver is **moved** out of the context: calling this twice
    /// with the same `channel_id` returns `InputNotFound` on the second call.
    pub async fn input(&self, channel_id: &str) -> Result<Input> {
        let mut inputs = self.inputs.lock().await;
        let rx = inputs.remove(channel_id).ok_or_else(|| {
            EngineError::Channel(ChannelError::InputNotFound(channel_id.to_string()))
        })?;
        Ok(Input::with_metrics(
            channel_id.to_string(),
            rx,
            Arc::clone(&self.metrics),
            self.clock.clone(),
        ))
    }

    /// Get all input channels
    ///
    /// Drains every receiver from the context.
    pub async fn inputs(&self) -> Result<Vec<Input>> {
        let mut inputs = self.inputs.lock().await;
        let result = inputs
            .drain()
            .map(|(id, rx)| {
                Input::with_metrics(id, rx, Arc::clone(&self.metrics), self.clock.clone())
            })
            .collect();
        Ok(result)
    }

    /// Get a merged input that receives from all input channels
    ///
    /// If there is a single input it is returned directly (zero overhead).
    /// For multiple inputs a small set of forwarding tasks is spawned;
    /// they terminate automatically when the upstream senders drop.
    pub async fn merged_input(&self) -> Result<Input> {
        let mut inputs = self.inputs.lock().await;

        if inputs.is_empty() {
            return Err(EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            )));
        }

        // Fast path: single input — no forwarding needed
        if inputs.len() == 1 {
            let (id, rx) = inputs.drain().next().unwrap();
            return Ok(Input::with_metrics(
                id,
                rx,
                Arc::clone(&self.metrics),
                self.clock.clone(),
            ));
        }

        // Multiple inputs: merge into a single mpsc channel
        let (merged_tx, merged_rx) = mpsc::channel(self.channel_capacity);
        let mut abort_handles = Vec::new();

        for (channel_id, mut rx) in inputs.drain() {
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
            "merged".to_string(),
            merged_rx,
            Arc::clone(&self.metrics),
            self.clock.clone(),
        ))
    }

    /// Get the first available input channel
    ///
    /// Convenience for tasks with a single input.
    pub async fn first_input(&self) -> Result<Input> {
        let mut inputs = self.inputs.lock().await;
        let key = inputs.keys().next().cloned().ok_or_else(|| {
            EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            ))
        })?;
        let rx = inputs.remove(&key).unwrap();
        Ok(Input::with_metrics(
            key,
            rx,
            Arc::clone(&self.metrics),
            self.clock.clone(),
        ))
    }

    // ------------------------------------------------------------------
    // Output access
    // ------------------------------------------------------------------

    /// Get an output channel by label
    pub fn output(&self, label: &str) -> Result<Output> {
        let handles = self
            .outputs
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

        // Cancel any merged-input forwarder tasks
        let handles = self
            .forwarders
            .lock()
            .expect("forwarders mutex poisoned");
        for h in handles.iter() {
            h.abort();
        }
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

    /// Get list of input channel IDs configured at build time
    pub fn input_ids(&self) -> Vec<&str> {
        self.input_names.iter().map(|s| s.as_str()).collect()
    }

    /// Number of input channels configured at build time
    pub fn input_count(&self) -> usize {
        self.input_names.len()
    }

    /// Get list of available output labels
    pub fn output_labels(&self) -> Vec<&str> {
        self.outputs.keys().map(|s| s.as_str()).collect()
    }

    /// Check if this is a source task (no inputs)
    pub fn is_source(&self) -> bool {
        self.input_names.is_empty()
    }

    /// Check if this is a sink task (no outputs)
    pub fn is_sink(&self) -> bool {
        self.outputs.is_empty()
    }
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("id", &self.id)
            .field("inputs", &self.input_ids())
            .field("outputs", &self.output_labels())
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

    #[tokio::test]
    async fn test_input_output() {
        // Create an mpsc channel: sender goes to Output, receiver to Input
        let (tx, rx) = mpsc::channel(10);

        let mut input = Input::new("test".to_string(), rx);
        let output = Output::new("out".to_string(), vec![tx]);

        // Send and receive
        output.send(json!({"test": "value"})).await.unwrap();
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

    #[tokio::test]
    async fn test_context_with_channels() {
        let mut inputs = HashMap::new();
        let mut outputs: HashMap<String, Vec<mpsc::Sender<Value>>> = HashMap::new();

        let (_tx1, rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        inputs.insert("input1".to_string(), rx1);
        outputs.insert("out1".to_string(), vec![tx2]);

        let ctx = TaskContext::new("task1".to_string(), inputs, outputs);

        assert!(!ctx.is_source());
        assert!(!ctx.is_sink());
        assert_eq!(ctx.input_ids(), vec!["input1"]);
        assert_eq!(ctx.output_labels(), vec!["out1"]);
    }

    #[tokio::test]
    async fn test_merged_input() {
        let mut inputs = HashMap::new();

        let (tx1, rx1) = mpsc::channel(10);
        let (tx2, rx2) = mpsc::channel(10);

        inputs.insert("input1".to_string(), rx1);
        inputs.insert("input2".to_string(), rx2);

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
        tx1.send(json!({"source": "input1", "value": 1})).await.unwrap();
        tx2.send(json!({"source": "input2", "value": 2})).await.unwrap();

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
    async fn test_forwarders_aborted_on_stop() {
        let mut inputs = HashMap::new();

        // Two inputs → merged_input will spawn 2 forwarder tasks
        let (tx1, rx1) = mpsc::channel(10);
        let (_tx2, rx2) = mpsc::channel(10);

        inputs.insert("a".to_string(), rx1);
        inputs.insert("b".to_string(), rx2);

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
        tx1.send(json!(1)).await.unwrap();
        let msg = merged.recv().await.unwrap();
        assert_eq!(msg, json!(1));

        // Stop the context — forwarders should be aborted
        ctx.stop();
        tokio::task::yield_now().await;

        // After abort, trying to send still succeeds on the mpsc sender
        // but the forwarder won't forward it. merged.recv() should return
        // Err (channel closed) since the forwarder is dead.
        let _ = tx1.send(json!(999)).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // The merged channel should be closed (or at least not deliver new msgs)
        let result = merged.try_recv();
        // Either Err (closed) or Ok(None) — but NOT Ok(Some(999))
        match result {
            Ok(Some(val)) => panic!("forwarder should be dead, but got: {val}"),
            _ => {} // Err(Closed) or Ok(None) — both correct
        }
    }

    #[tokio::test]
    async fn test_backpressure() {
        // Create a channel with capacity 2
        let (tx, rx) = mpsc::channel(2);
        let output = Output::new("out".to_string(), vec![tx]);

        // Fill the buffer
        output.send(json!(1)).await.unwrap();
        output.send(json!(2)).await.unwrap();

        // The next send should block until we drain one message
        let mut input = Input::new("test".to_string(), rx);
        let send_handle = tokio::spawn(async move {
            output.send(json!(3)).await.unwrap();
        });

        // Give the send a moment to attempt (it should be blocked)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(!send_handle.is_finished(), "send should be blocked by backpressure");

        // Drain one message — this should unblock the send
        let _ = input.recv().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(send_handle.is_finished(), "send should have completed after drain");
    }
}
