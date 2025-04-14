# Declarative Beam Pipeline Framework

A declarative framework for creating Apache Beam pipelines using YAML configuration files. This framework allows you to define complex data processing pipelines without writing extensive code.

## Features

- Define Apache Beam pipelines using YAML configuration
- Support for complex DAG structures with multiple inputs/outputs
- Side input handling for data enrichment
- Branching pipelines with conditional processing
- Environment variable substitution
- Extensible plugin system for custom transforms
- Comprehensive transform library for common operations
- Integration with various runners (Direct, Dataflow, Spark, Flink)

## Installation

```bash
# Using uv (recommended)
uv pip install -e .

# For development
uv pip install -e ".[dev]"

# For Kubernetes support
uv pip install -e ".[kubernetes]"

# For visualization support
uv pip install -e ".[visualization]"
```

## Quick Start

1. Define your pipeline in YAML:

```yaml
# pipeline.yaml
pipeline_options:
  runner: "DirectRunner"
  job_name: "my-pipeline-job"

transforms:
  - name: "ReadData"
    type: "ReadFromText"
    outputs: ["raw_data"]
    file_pattern: "gs://my-bucket/input/*.csv"
  
  - name: "ParseData"
    type: "ParDo"
    inputs: ["raw_data"]
    outputs: ["parsed_data"]
    fn_module: "my_transforms.parsers"
    fn_name: "parse_csv_line"
    params:
      delimiter: ","
      fields: ["id", "name", "value"]
```

2. Create your transform functions:

```python
# my_transforms/parsers.py
def parse_csv_line(delimiter=",", fields=None):
    """Parse a CSV line into a dictionary."""
    if fields is None:
        fields = []
    
    def _parse_line(line):
        values = line.strip().split(delimiter)
        result = {}
        
        for i, field in enumerate(fields):
            if i < len(values):
                result[field] = values[i]
            else:
                result[field] = ""
        
        return result
    
    return _parse_line
```

3. Run your pipeline:

```bash
beam-pipeline run --config pipeline.yaml
```

## Documentation

For detailed documentation, see the [docs](docs/) directory.

## Examples

Check out the [examples](examples/) directory for sample pipelines.

## Development

This project follows test-driven development practices. To run tests:

```bash
pytest
```

## License

MIT
