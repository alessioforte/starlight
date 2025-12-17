//! Built-in task implementations
//!
//! This module provides example task implementations demonstrating
//! how to build tasks with the simple_engine framework.

pub mod csv_reader;
pub mod csv_writer;
pub mod json_mapper;
pub mod logger;
pub mod number_generator;

// Re-export for convenience
pub use csv_reader::CsvReader;
pub use csv_writer::CsvWriter;
pub use json_mapper::JsonMapper;
pub use logger::Logger;
pub use number_generator::NumberGenerator;
