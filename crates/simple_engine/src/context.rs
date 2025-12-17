//! Task context and channel abstractions
//!
//! This module provides simple, safe abstractions for task communication.
//! Task developers work with `Input` and `Output` types instead of raw channels.

use crate::error::{ChannelError, EngineError, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast;

/// Input channel abstraction for receiving data
///
/// Provides a simple interface for tasks to receive messages without
/// dealing with the underlying channel implementation.
pub struct Input {
    id: String,
    rx: broadcast::Receiver<Value>,
}

impl Input {
    /// Create a new input from a broadcast receiver
    pub(crate) fn new(id: String, rx: broadcast::Receiver<Value>) -> Self {
        Self { id, rx }
    }

    /// Receive the next message from this input channel
    ///
    /// This method will block until a message is available or the channel is closed.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel is closed or if receiving fails.
    pub async fn recv(&mut self) -> Result<Value> {
        self.rx.recv().await.map_err(|e| {
            EngineError::Channel(ChannelError::RecvFailed(self.id.clone(), e.to_string()))
        })
    }

    /// Try to receive a message without blocking
    ///
    /// Returns `Ok(Some(value))` if a message is available,
    /// `Ok(None)` if no message is available,
    /// or an error if the channel is closed.
    pub fn try_recv(&mut self) -> Result<Option<Value>> {
        match self.rx.try_recv() {
            Ok(val) => Ok(Some(val)),
            Err(broadcast::error::TryRecvError::Empty) => Ok(None),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                log::warn!("Input '{}' lagged by {} messages", self.id, n);
                Ok(None)
            }
            Err(e) => Err(EngineError::Channel(ChannelError::RecvFailed(
                self.id.clone(),
                e.to_string(),
            ))),
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

/// Output channel abstraction for sending data
///
/// Handles broadcasting to multiple downstream tasks transparently.
pub struct Output {
    label: String,
    handles: Vec<broadcast::Sender<Value>>,
}

impl Output {
    /// Create a new output with multiple target channels
    pub(crate) fn new(label: String, handles: Vec<broadcast::Sender<Value>>) -> Self {
        Self { label, handles }
    }

    /// Send a message to all connected outputs
    ///
    /// The message is cloned for each receiver. If no receivers are connected,
    /// this is a no-op and does not return an error.
    ///
    /// # Errors
    ///
    /// Returns an error only if sending fundamentally fails (rare).
    pub fn send(&self, value: Value) -> Result<()> {
        if self.handles.is_empty() {
            log::trace!("Output '{}' has no receivers", self.label);
            return Ok(());
        }

        for handle in &self.handles {
            // Ignore if no receivers - they might have disconnected
            let _ = handle.send(value.clone());
        }
        Ok(())
    }

    /// Send a message without cloning for the last receiver (optimization)
    ///
    /// This is more efficient when you don't need the value after sending.
    pub fn send_owned(self, value: Value) -> Result<()> {
        if self.handles.is_empty() {
            return Ok(());
        }

        let mut iter = self.handles.into_iter();
        let last = iter.next_back();

        // Clone for all but the last
        for handle in iter {
            let _ = handle.send(value.clone());
        }

        // Move into the last one
        if let Some(handle) = last {
            let _ = handle.send(value);
        }

        Ok(())
    }

    /// Get the total number of active receivers across all channels
    pub fn receiver_count(&self) -> usize {
        self.handles.iter().map(|h| h.receiver_count()).sum()
    }

    /// Get the output label
    pub fn label(&self) -> &str {
        &self.label
    }
}

impl std::fmt::Debug for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Output")
            .field("label", &self.label)
            .field("receiver_count", &self.receiver_count())
            .finish()
    }
}

/// Task execution context
///
/// Provides everything a task needs to interact with the workflow:
/// - Input channels
/// - Output channels
/// - Running state
/// - Task metadata
///
/// This is the main interface tasks use to communicate.
#[derive(Clone)]
pub struct TaskContext {
    /// Unique task identifier
    pub id: String,

    /// Input channels by dependency ID
    inputs: Arc<HashMap<String, broadcast::Sender<Value>>>,

    /// Output channels by label
    outputs: Arc<HashMap<String, Vec<broadcast::Sender<Value>>>>,

    /// Whether the task should continue running
    running: Arc<AtomicBool>,
}

impl TaskContext {
    /// Create a new task context
    pub fn new(
        id: String,
        inputs: HashMap<String, broadcast::Sender<Value>>,
        outputs: HashMap<String, Vec<broadcast::Sender<Value>>>,
    ) -> Self {
        Self {
            id,
            inputs: Arc::new(inputs),
            outputs: Arc::new(outputs),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get an input channel by channel ID (from dependencies)
    ///
    /// # Errors
    ///
    /// Returns an error if the input channel doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use simple_engine::prelude::*;
    /// # async fn example(ctx: Arc<TaskContext>) -> Result<()> {
    /// let mut input = ctx.input("upstream_channel_id")?;
    /// let data = input.recv().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn input(&self, channel_id: &str) -> Result<Input> {
        let tx = self.inputs.get(channel_id).ok_or_else(|| {
            EngineError::Channel(ChannelError::InputNotFound(channel_id.to_string()))
        })?;

        Ok(Input::new(channel_id.to_string(), tx.subscribe()))
    }

    /// Get all input channels
    ///
    /// Returns a vector of all available input channels.
    /// Useful for tasks that need to process from multiple input sources.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use simple_engine::prelude::*;
    /// # async fn example(ctx: Arc<TaskContext>) -> Result<()> {
    /// let inputs = ctx.inputs()?;
    /// for mut input in inputs {
    ///     let data = input.recv().await?;
    ///     // Process data...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn inputs(&self) -> Result<Vec<Input>> {
        let mut result = Vec::new();
        for (channel_id, tx) in self.inputs.iter() {
            result.push(Input::new(channel_id.clone(), tx.subscribe()));
        }
        Ok(result)
    }

    /// Get a merged input that receives from all input channels
    ///
    /// This method combines all input channels into a single receiver,
    /// allowing tasks to process messages from multiple upstream sources
    /// as a unified stream.
    ///
    /// # Errors
    ///
    /// Returns an error if there are no input channels.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use simple_engine::prelude::*;
    /// # async fn example(ctx: Arc<TaskContext>) -> Result<()> {
    /// // Receive from all input channels as a single stream
    /// let mut input = ctx.merged_input()?;
    /// while ctx.is_running() {
    ///     let data = input.recv().await?;
    ///     // Process data from any upstream channel
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn merged_input(&self) -> Result<Input> {
        if self.inputs.is_empty() {
            return Err(EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            )));
        }

        // If there's only one input, just return it directly
        if self.inputs.len() == 1 {
            let (channel_id, tx) = self.inputs.iter().next().unwrap();
            return Ok(Input::new(channel_id.clone(), tx.subscribe()));
        }

        // For multiple inputs, create a merged channel
        let (merged_tx, merged_rx) = broadcast::channel(1000);

        // Spawn tasks to forward from each input to the merged channel
        for (channel_id, tx) in self.inputs.iter() {
            let mut rx = tx.subscribe();
            let merged_tx_clone = merged_tx.clone();
            let channel_id = channel_id.clone();

            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(value) => {
                            // Forward to merged channel, ignore if no receivers
                            let _ = merged_tx_clone.send(value);
                        }
                        Err(_) => {
                            // Channel closed or error, exit
                            log::debug!("Input channel '{}' closed", channel_id);
                            break;
                        }
                    }
                }
            });
        }

        Ok(Input::new("merged".to_string(), merged_rx))
    }

    /// Get the first available input channel
    ///
    /// Convenience method for tasks with a single input.
    /// For tasks with multiple inputs, prefer `merged_input()`.
    ///
    /// # Errors
    ///
    /// Returns an error if there are no input channels.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use simple_engine::prelude::*;
    /// # async fn example(ctx: Arc<TaskContext>) -> Result<()> {
    /// let mut input = ctx.first_input()?;
    /// let data = input.recv().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn first_input(&self) -> Result<Input> {
        let (channel_id, tx) = self.inputs.iter().next().ok_or_else(|| {
            EngineError::Channel(ChannelError::InputNotFound(
                "(no inputs available)".to_string(),
            ))
        })?;

        Ok(Input::new(channel_id.clone(), tx.subscribe()))
    }

    /// Get an output channel by label
    ///
    /// # Errors
    ///
    /// Returns an error if the output channel doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use simple_engine::prelude::*;
    /// # use serde_json::json;
    /// # async fn example(ctx: Arc<TaskContext>) -> Result<()> {
    /// let output = ctx.output("out")?;
    /// output.send(json!({"key": "value"}))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn output(&self, label: &str) -> Result<Output> {
        let handles = self
            .outputs
            .get(label)
            .ok_or_else(|| EngineError::Channel(ChannelError::OutputNotFound(label.to_string())))?
            .clone();

        Ok(Output::new(label.to_string(), handles))
    }

    /// Check if the task should continue running
    ///
    /// Tasks should periodically check this and stop processing when it returns false.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use simple_engine::prelude::*;
    /// # async fn example(ctx: Arc<TaskContext>) -> Result<()> {
    /// while ctx.is_running() {
    ///     // Process data...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Set the running state (internal use)
    pub(crate) fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::Relaxed);
    }

    /// Get list of available input channel IDs
    pub fn input_ids(&self) -> Vec<&str> {
        self.inputs.keys().map(|s| s.as_str()).collect()
    }

    /// Get the number of input channels
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Get list of available output labels
    pub fn output_labels(&self) -> Vec<&str> {
        self.outputs.keys().map(|s| s.as_str()).collect()
    }

    /// Check if this is a source task (no inputs)
    pub fn is_source(&self) -> bool {
        self.inputs.is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_input_output() {
        let (tx, _) = broadcast::channel(10);
        let tx_clone = tx.clone();

        let mut input = Input::new("test".to_string(), tx.subscribe());
        let output = Output::new("out".to_string(), vec![tx_clone]);

        // Send and receive
        output.send(json!({"test": "value"})).unwrap();
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
    fn test_context_with_channels() {
        let mut inputs = HashMap::new();
        let mut outputs = HashMap::new();

        let (tx1, _) = broadcast::channel(10);
        let (tx2, _) = broadcast::channel(10);

        inputs.insert("input1".to_string(), tx1);
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

        let (tx1, _) = broadcast::channel(10);
        let (tx2, _) = broadcast::channel(10);

        inputs.insert("input1".to_string(), tx1.clone());
        inputs.insert("input2".to_string(), tx2.clone());

        let ctx = Arc::new(TaskContext::new(
            "task1".to_string(),
            inputs,
            HashMap::new(),
        ));

        // Get merged input BEFORE sending (must subscribe first)
        let mut merged = ctx.merged_input().unwrap();

        // Give the forwarder tasks time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Send messages to both input channels
        tx1.send(json!({"source": "input1", "value": 1})).unwrap();
        tx2.send(json!({"source": "input2", "value": 2})).unwrap();

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
}
