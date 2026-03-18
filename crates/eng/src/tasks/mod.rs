mod aggregator;
mod csv_reader;
mod csv_writer;
mod dummy;
mod filter;
mod http_sender;
mod json_mapper;
mod logger;
mod number_generator;
mod splitter;
mod timer;
mod type_converter;

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
    Aggregator,
    CsvReader,
    CsvWriter,
    JsonMapper,
    NumberGenerator,
    Logger,
    Filter,
    HttpSender,
    Splitter,
    Timer,
    TypeConverter,
    Dummy,
    // MathExpEval,
    // Simulator,
}

impl Tasks {
    pub fn to_factory(&self) -> TaskFactory {
        match self {
            Tasks::Aggregator => Box::new(aggregator::Aggregator::create),
            Tasks::CsvReader => Box::new(csv_reader::CsvReader::create),
            Tasks::CsvWriter => Box::new(csv_writer::CsvWriter::create),
            Tasks::JsonMapper => Box::new(json_mapper::JsonMapper::create),
            Tasks::NumberGenerator => Box::new(number_generator::NumberGenerator::create),
            Tasks::Logger => Box::new(logger::Logger::create),
            Tasks::Filter => Box::new(filter::Filter::create),
            Tasks::HttpSender => Box::new(http_sender::HttpSender::create),
            Tasks::Splitter => Box::new(splitter::Splitter::create),
            Tasks::Timer => Box::new(timer::Timer::create),
            Tasks::TypeConverter => Box::new(type_converter::TypeConverter::create),
            Tasks::Dummy => Box::new(dummy::Dummy::create),
        }
    }
}
