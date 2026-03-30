## Schema

A workflow config is a JSON object with the following structure:

```json
{
  "id": "<string>",
  "name": "<string>",
  "description": "<string>",
  "tasks": [
    {
      "id": "<string>",
      "type": "<task_type>",
      "dependencies": [],
      "params": {},
      "outputs": {
        "out": ["<channel_id>"]
      }
    }
  ]
}
```

- `id`: unique workflow identifier (snake_case)
- `name`: human-readable name
- `description`: optional description
- `tasks`: list of task configurations
  - `id`: unique task identifier within the workflow
  - `type`: one of the available task types
  - `dependencies`: list of upstream channel IDs (empty array for source tasks)
  - `params`: task-specific parameters object
  - `outputs`: map of output labels to arrays of channel IDs. Sink tasks use empty object `{}`

## Channels

- Each task output sends messages to named channels (e.g., `"gen_out"`)
- Downstream tasks list those channel names in their `dependencies` array
- One output can feed multiple tasks; one task can receive from multiple channels

## Available task types

- number_generator: min(int), max(int), interval_ms(int,default:1000), count(int,opt), seed(int,opt)
- timer: mode.interval(int ms) OR mode.cron(string), count(int,opt), immediate(bool,opt), payload.static(object,opt)
- csv_reader: filename(string), delimiter(char,default:','), interval_ms(int,default:0), start_line(int,opt)
- logger: level(info|debug|warn|error), prefix(string,opt), pretty(bool,opt)
- csv_writer: filename(string), delimiter(char,default:','), write_mode(overwrite|append), flush_every(int,default:1)
- filter: conditions([{field,operator(eq|ne|gt|gte|lt|lte|contains|exists),value}]), mode(and|or)
- splitter: field(string), mode(value|map|ranges), default_output(string,opt)
- json_mapper: mappings({output_field: "input.path"}), pass_through(bool,opt)
- type_converter: conversions([{field,to(string|int|float|bool|json|timestamp|array|null)}])
- math_exp_eval: vars({name: "field.path"}), consts({name: value}), expressions({name: "expr"}), mapping({name: "output.path"})
- aggregator: columns([{field,fn(count|sum|avg|min|max|collect),alias(opt)}]), window_count(int,opt), window_ms(int,opt), group_by(string,opt)
- http_sender: url(string), method(GET|POST|PUT|PATCH|DELETE), headers(map,opt), timeout_ms(int,default:30000)
- dummy: {} (no params)

## Examples

### Simple pipeline: generate numbers and log them

```json
{
  "id": "rand",
  "name": "Random number generator and logger",
  "description": "Generates random numbers and logs them",
  "tasks": [
    {
      "id": "number_generator",
      "type": "number_generator",
      "dependencies": [],
      "params": {
        "min": 1,
        "max": 100
      },
      "outputs": {
        "out": ["number_generator_out"]
      }
    },
    {
      "id": "logger",
      "type": "logger",
      "dependencies": ["number_generator_out"],
      "params": {},
      "outputs": {}
    }
  ]
}
```

### Two sources merged into one sink

```json
{
  "id": "rand_2_1",
  "name": "2 Random number generators and logger",
  "description": "Two generators feeding one logger",
  "tasks": [
    {
      "id": "gen_a",
      "type": "number_generator",
      "dependencies": [],
      "params": {
        "min": 1,
        "max": 10,
        "interval_ms": 200
      },
      "outputs": {
        "out": ["small_numbers"]
      }
    },
    {
      "id": "gen_b",
      "type": "number_generator",
      "dependencies": [],
      "params": {
        "min": 100,
        "max": 200
      },
      "outputs": {
        "out": ["large_numbers"]
      }
    },
    {
      "id": "logger",
      "type": "logger",
      "dependencies": ["small_numbers", "large_numbers"],
      "params": {
        "prefix": "[MERGED]"
      },
      "outputs": {}
    }
  ]
}
```
