# Workflow Configuration for Engine

## Configuration Guide

### Configuration File Forma

The application supports both YAML and JSON configuration formats. Choose the format that best suits your needs:

- config.yaml (recommended)
- config.json

### Example Configuration

```yaml
id: csv_to_kafka_example
name: CSV to Kafka Example
description: >-
  An example workflow that reads data from a CSV file and sends it to a Kafka topic.
tasks:
  - id: csv_reader
    type: csv_reader
    dependencies: []
    params:
      filename: m_34.csv
      base_path: ./.starlight/data/
      interval: 8ms
    handles:
      out:
        - csv_reader_out
```

### Fields Description

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | - | Unique identifier for the workflow |
| `name` | string | Yes | - | Application name used in logs and monitoring |
| `description` | string | No | "" | Brief description of the workflow |
| `tasks` | list of task objects | Yes | - | List of tasks that make


Task Object Structure
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | Yes | - | Unique identifier for the task |
| `type` | string | Yes | - | Type of the task (e.g., csv_reader, kafka_producer) |
| `dependencies` | list of strings | No | [] | List of link IDs that this task depends on |
| `params` | object | Yes | - | Parameters specific to the task type |
| `handles` | object | No | {} | Output handles for the task |
