mod csv_reader;
mod dummy;
mod json_mapper;
mod logger;
mod number_generator;

use crate::err::Result;
use crate::task::Task;
use serde::{Deserialize, Serialize};

/// Type alias for task factory functions
///
/// This is used when registering tasks with the workflow builder.
pub type TaskFactory =
    Box<dyn Fn(String, serde_json::Value) -> Result<Box<dyn Task>> + Send + Sync>;

/// Represents the different types of tasks that can be executed in the workflow.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Tasks {
    CsvReader,
    JsonMapper,
    NumberGenerator,
    Logger,
    Dummy,
    // TypeConverter,
    // CsvWriter,
    // Splitter,
    // MathExpEval,
    // Simulator,
    // KafkaProducer,
    // RandomNumbersGenerator,
    // KafkaConsumer,
    // HttpSender,
    // Scheduler,
}

impl Tasks {
    pub fn to_factory(&self) -> TaskFactory {
        match self {
            Tasks::CsvReader => Box::new(csv_reader::CsvReader::create),
            Tasks::JsonMapper => Box::new(json_mapper::JsonMapper::create),
            Tasks::NumberGenerator => Box::new(number_generator::NumberGenerator::create),
            Tasks::Logger => Box::new(logger::Logger::create),
            Tasks::Dummy => Box::new(dummy::Dummy::create),
        }
    }
}
