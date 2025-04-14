# Creating Your First Pipeline

This guide walks you through creating your first pipeline using the Declarative Beam Pipeline framework.

## Basic Pipeline Structure

A declarative beam pipeline consists of:

1. A YAML configuration file defining the pipeline structure
2. Input data sources
3. A series of transforms to process the data
4. Output sinks to store the results

## Example: Simple CSV Processing Pipeline

Let's create a simple pipeline that reads a CSV file, filters records, and writes the results to a text file.

### Step 1: Create the YAML Configuration

Create a file named `simple_pipeline.yaml` with the following content:

```yaml
name: Simple CSV Processing Pipeline
description: Reads a CSV file, filters records, and writes results to a text file

transforms:
  - name: ReadCSV
    type: ReadFromText
    config:
      file_pattern: input.csv
    outputs:
      - raw_lines

  - name: ParseCSV
    type: Map
    config:
      fn_module: csv_utils
      fn_name: parse_csv_line
    inputs:
      - raw_lines
    outputs:
      - parsed_records

  - name: FilterRecords
    type: Filter
    config:
      fn_module: filter_utils
      fn_name: filter_by_value
      params:
        field: value
        min_value: 50
    inputs:
      - parsed_records
    outputs:
      - filtered_records

  - name: FormatResults
    type: Map
    config:
      fn_module: format_utils
      fn_name: format_result
    inputs:
      - filtered_records
    outputs:
      - formatted_results

  - name: WriteResults
    type: WriteToText
    config:
      file_path_prefix: output/results
    inputs:
      - formatted_results
```

### Step 2: Create the Utility Functions

Create a file named `csv_utils.py`:

```python
def parse_csv_line(line):
    """Parse a CSV line into a dictionary."""
    values = line.strip().split(',')
    if len(values) >= 3:
        return {
            'id': values[0],
            'name': values[1],
            'value': values[2]
        }
    return None
```

Create a file named `filter_utils.py`:

```python
def filter_by_value(record, field, min_value):
    """Filter records based on a minimum value."""
    if record and field in record:
        try:
            return int(record[field]) >= min_value
        except (ValueError, TypeError):
            return False
    return False
```

Create a file named `format_utils.py`:

```python
def format_result(record):
    """Format a record as a string."""
    return str(record)
```

### Step 3: Run the Pipeline

Create a Python script named `run_pipeline.py`:

```python
from declarative_beam.core.yaml_processor import YAMLProcessor
from declarative_beam.core.pipeline_builder import PipelineBuilder

def main():
    # Load the YAML configuration
    yaml_processor = YAMLProcessor()
    pipeline_config = yaml_processor.load_yaml('simple_pipeline.yaml')
    
    # Build and run the pipeline
    builder = PipelineBuilder(pipeline_config)
    pipeline = builder.build_pipeline()
    pipeline.run()

if __name__ == '__main__':
    main()
```

Run the pipeline:

```bash
python run_pipeline.py
```

### Step 4: Check the Results

After the pipeline completes, you should see the filtered results in the `output/results` directory.

## Next Steps

- Try modifying the YAML configuration to add more transforms
- Experiment with different filter conditions
- Add error handling for invalid records
- Try using side inputs for data enrichment

For more complex examples, see the [Examples](../examples/README.md) section.

## Tips for Success

- Start with simple pipelines and gradually add complexity
- Test each transform individually before combining them
- Use meaningful names for transforms and outputs
- Add descriptions to your transforms for better documentation
- Use the test framework to verify your pipeline's behavior
