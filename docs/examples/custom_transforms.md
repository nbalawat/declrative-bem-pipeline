# Custom Transforms Example

This example demonstrates how to use custom transforms in a declarative beam pipeline. It shows both the `CustomMultiplyTransform` and `CustomCategorizeTransform` in action.

## Overview

The example pipeline:

1. Reads data from a CSV file
2. Parses the CSV data into records
3. Multiplies the 'value' field by 2 using `CustomMultiplyTransform`
4. Categorizes the records based on the multiplied value using `CustomCategorizeTransform`
5. Writes the categorized records to separate output files

## Sample Data

The sample data is a CSV file with product information:

```csv
id,name,value,category
1,Product A,15,Electronics
2,Product B,45,Clothing
3,Product C,85,Home
4,Product D,5,Food
5,Product E,65,Electronics
6,Product F,25,Clothing
7,Product G,95,Home
8,Product H,invalid,Food
```

## Pipeline Configuration

The pipeline is defined in a YAML configuration file:

```yaml
name: Custom Transforms Example Pipeline
description: Demonstrates the use of CustomMultiplyTransform and CustomCategorizeTransform

transforms:
  - name: ReadCSV
    type: ReadFromText
    config:
      file_pattern: examples/custom_transforms/sample_data.csv
    outputs:
      - raw_lines

  - name: ParseCSV
    type: Map
    config:
      fn_module: examples.custom_transforms.csv_utils
      fn_name: parse_csv_line
    inputs:
      - raw_lines
    outputs:
      - parsed_records

  - name: FilterNulls
    type: Filter
    config:
      fn_module: builtins
      fn_name: bool
    inputs:
      - parsed_records
    outputs:
      - valid_records

  - name: MultiplyValues
    type: CustomMultiplyTransform
    config:
      field: value
      factor: 2
    inputs:
      - valid_records
    outputs:
      - multiplied_records

  - name: CategorizeValues
    type: CustomCategorizeTransform
    config:
      field: value
      categories:
        low:
          min: 0
          max: 30
        medium:
          min: 30
          max: 70
        high:
          min: 70
          max: 200
    inputs:
      - multiplied_records
    outputs:
      - low
      - medium
      - high
      - other
      - invalid

  - name: WriteLowValues
    type: WriteToText
    config:
      file_path_prefix: examples/custom_transforms/output/low_values
    inputs:
      - low

  - name: WriteMediumValues
    type: WriteToText
    config:
      file_path_prefix: examples/custom_transforms/output/medium_values
    inputs:
      - medium

  - name: WriteHighValues
    type: WriteToText
    config:
      file_path_prefix: examples/custom_transforms/output/high_values
    inputs:
      - high

  - name: WriteInvalidValues
    type: WriteToText
    config:
      file_path_prefix: examples/custom_transforms/output/invalid_values
    inputs:
      - invalid
```

## Running the Example

To run the example:

```bash
# Make sure you're in the project root directory
cd /path/to/declarative-beam-pipeline

# Run the example script
python examples/custom_transforms/run_pipeline.py
```

## Expected Output

The pipeline will categorize the products based on their multiplied values:

### Low Values (0-30)
- Product D (original value: 5, multiplied value: 10)

### Medium Values (30-70)
- Product A (original value: 15, multiplied value: 30)
- Product F (original value: 25, multiplied value: 50)
- Product B (original value: 45, multiplied value: 90)

### High Values (70-200)
- Product E (original value: 65, multiplied value: 130)
- Product C (original value: 85, multiplied value: 170)
- Product G (original value: 95, multiplied value: 190)

### Invalid Values
- Product H (original value: "invalid")

## How It Works

### CSV Parsing

The CSV parsing is handled by a simple utility function:

```python
def parse_csv_line(line: str) -> Optional[Dict[str, str]]:
    """Parse a CSV line into a dictionary."""
    try:
        # Skip the header line
        if line.startswith("id,name,value,category"):
            return None
            
        # Split the line into values
        values = line.strip().split(',')
        if len(values) >= 4:
            return {
                'id': values[0],
                'name': values[1],
                'value': values[2],
                'original_category': values[3]
            }
        return None
    except Exception:
        return None
```

### CustomMultiplyTransform

The `CustomMultiplyTransform` multiplies the value in the specified field by a factor:

```yaml
- name: MultiplyValues
  type: CustomMultiplyTransform
  config:
    field: value    # Field to multiply
    factor: 2       # Multiplication factor
```

For example, if a record has `value: "15"`, after the transform it will have `value: "30"` and `original_value: "15"`.

### CustomCategorizeTransform

The `CustomCategorizeTransform` categorizes records based on the value in a specified field:

```yaml
- name: CategorizeValues
  type: CustomCategorizeTransform
  config:
    field: value    # Field to categorize by
    categories:     # Category definitions
      low:
        min: 0
        max: 30
      medium:
        min: 30
        max: 70
      high:
        min: 70
        max: 200
```

Each record is sent to the output corresponding to its category. Records with invalid values are sent to the `invalid` output.

## Key Takeaways

1. **Custom transforms** can be easily integrated into declarative beam pipelines using the YAML configuration.
2. **Multiple outputs** from transforms like `CustomCategorizeTransform` can be connected to different downstream transforms.
3. **Type conversion and validation** is handled by the custom transforms, making the pipeline more robust.
4. **Composition of transforms** allows for complex data processing pipelines to be built declaratively.

For more information on creating custom transforms, see [Creating Custom Transforms](../advanced/custom_transforms.md).
