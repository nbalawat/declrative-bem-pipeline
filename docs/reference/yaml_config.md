# YAML Configuration Reference

This document provides a comprehensive reference for the YAML configuration format used in the Declarative Beam Pipeline framework.

## Configuration Structure

A pipeline configuration YAML file must include a top-level `runner` section and use top-level parameters for all transforms. Example:

```yaml
runner:
  type: DirectRunner
  options: {}

description: Pipeline Description

transforms:
  - name: TransformName1
    type: TransformType1
    param1: value1
    param2: value2
    inputs:
      - input_pcollection_name
    outputs:
      - output_pcollection_name

  - name: TransformName2
    type: TransformType2
    paramA: valueA
    paramB: valueB
    inputs:
      - output_pcollection_name
    outputs:
      - output_pcollection_name2
```

## Top-Level Properties

| Property | Type | Description | Required |
|----------|------|-------------|----------|
| `runner` | Object | Beam runner configuration | Yes |
| `name` | String | Name of the pipeline | Yes |
| `description` | String | Description of the pipeline | No |
| `transforms` | Array | List of transforms in the pipeline | Yes |

## Transform Properties

Each transform in the `transforms` array has the following properties:

| Property | Type | Description | Required |
|----------|------|-------------|----------|
| `name` | String | Unique name for the transform instance | Yes |
| `type` | String | Type of transform (must be registered in TransformRegistry) | Yes |
| `config` | Object | Configuration parameters specific to the transform type | No |
| `inputs` | Array | List of input PCollection names | No* |
| `outputs` | Array | List of output PCollection names | Yes |
| `side_inputs` | Object | Mapping of side input names to PCollection names | No |

\* Not required for source transforms that don't have inputs

## Data Types

The following data types are supported in the configuration:

| Type | Description | Example |
|------|-------------|---------|
| `string` | Text value | `"hello"` |
| `number` | Numeric value (integer or float) | `42`, `3.14` |
| `boolean` | Boolean value | `true`, `false` |
| `object` | Key-value mapping | `{"key": "value"}` |
| `array` | List of values | `[1, 2, 3]` |
| `null` | Null value | `null` |

## Example Configuration

Here's a complete example of a pipeline configuration:

```yaml
name: Data Processing Pipeline
description: Processes data from CSV files and outputs results

transforms:
  # Read input data
  - name: ReadCSV
    type: ReadFromText
    config:
      file_pattern: data/input.csv
    outputs:
      - raw_lines

  # Parse CSV lines
  - name: ParseCSV
    type: Map
    config:
      fn_module: csv_utils
      fn_name: parse_csv_line
    inputs:
      - raw_lines
    outputs:
      - parsed_records

  # Filter records
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

  # Extract key-value pairs
  - name: ExtractKeyValue
    type: Map
    config:
      fn_module: kv_utils
      fn_name: extract_key_value
      params:
        key_field: id
    inputs:
      - filtered_records
    outputs:
      - key_value_pairs

  # Group by key
  - name: GroupByKey
    type: GroupByKey
    inputs:
      - key_value_pairs
    outputs:
      - grouped_records

  # Format results
  - name: FormatResults
    type: Map
    config:
      fn_module: format_utils
      fn_name: format_result
      params:
        include_key: true
    inputs:
      - grouped_records
    outputs:
      - formatted_results

  # Write results
  - name: WriteResults
    type: WriteToText
    config:
      file_path_prefix: output/results
    inputs:
      - formatted_results
```

## Transform Dependencies

The framework automatically determines the dependencies between transforms based on the input and output PCollection names. Transforms are executed in an order that respects these dependencies.

For example, in the configuration above:
- `ParseCSV` depends on `ReadCSV` because it uses the `raw_lines` output
- `FilterRecords` depends on `ParseCSV` because it uses the `parsed_records` output
- And so on...

## Side Inputs

Side inputs are additional inputs to a transform that are not processed as the main input. They are typically used for lookups, enrichment, or configuration.

Example:

```yaml
- name: EnrichRecords
  type: EnrichRecords
  config:
    join_field: id
    target_field: additional_info
  inputs:
    - main_records
  outputs:
    - enriched_records
  side_inputs:
    reference_data: reference_dict
```

In this example, `reference_dict` is a side input PCollection that is used to enrich the main records.

## Multiple Outputs

Some transforms can produce multiple output PCollections. These are specified in the `outputs` array.

Example:

```yaml
- name: SplitByValue
  type: SplitByValue
  config:
    value_field: amount
    threshold: 100
    numeric: true
  inputs:
    - parsed_records
  outputs:
    - high_value_records
    - low_value_records
    - invalid_records
```

In this example, the `SplitByValue` transform produces three output PCollections: `high_value_records`, `low_value_records`, and `invalid_records`.

## Dynamic Parameters

Some transform parameters can reference environment variables or other dynamic values:

```yaml
- name: ReadCSV
  type: ReadFromText
  config:
    file_pattern: ${INPUT_PATH}/data.csv
  outputs:
    - raw_lines
```

In this example, `${INPUT_PATH}` will be replaced with the value of the `INPUT_PATH` environment variable.

## Validation

The framework validates the YAML configuration before building the pipeline, checking for:

1. Required properties
2. Valid transform types
3. Consistent input and output PCollection names
4. Cycles in the transform dependency graph
5. Transform-specific parameter validation

If validation fails, the framework raises an error with details about the issue.

## Best Practices

1. **Use meaningful names**: Choose clear, descriptive names for transforms and PCollections
2. **Document your configuration**: Add comments and descriptions to explain the pipeline
3. **Organize transforms logically**: Group related transforms together
4. **Keep configurations DRY**: Avoid duplicating configuration parameters
5. **Validate early**: Test your configuration with small datasets before processing large ones
6. **Use version control**: Store your configurations in version control to track changes

## Common Patterns

### Branching Pipelines

```yaml
- name: ParseRecords
  type: Map
  config:
    fn_module: parse_utils
    fn_name: parse_record
  inputs:
    - raw_records
  outputs:
    - parsed_records

# Branch 1
- name: ProcessBranch1
  type: Map
  config:
    fn_module: process_utils
    fn_name: process_branch1
  inputs:
    - parsed_records
  outputs:
    - branch1_results

# Branch 2
- name: ProcessBranch2
  type: Map
  config:
    fn_module: process_utils
    fn_name: process_branch2
  inputs:
    - parsed_records
  outputs:
    - branch2_results
```

### Joining Data

```yaml
# Create a dictionary from reference data
- name: CreateReferenceDict
  type: AsDict
  inputs:
    - reference_key_value_pairs
  outputs:
    - reference_dict

# Enrich main data with reference data
- name: EnrichRecords
  type: EnrichRecords
  config:
    join_field: id
    target_field: additional_info
  inputs:
    - main_records
  outputs:
    - enriched_records
  side_inputs:
    reference_data: reference_dict
```

### Error Handling

```yaml
- name: SplitByValidity
  type: SplitByValue
  config:
    fn_module: validation_utils
    fn_name: is_valid_record
  inputs:
    - parsed_records
  outputs:
    - valid_records
    - invalid_records

# Process valid records
- name: ProcessValidRecords
  type: Map
  config:
    fn_module: process_utils
    fn_name: process_record
  inputs:
    - valid_records
  outputs:
    - processed_records

# Log invalid records
- name: LogInvalidRecords
  type: WriteToText
  config:
    file_path_prefix: output/invalid_records
  inputs:
    - invalid_records
```

## Conclusion

The YAML configuration format provides a flexible, declarative way to define Apache Beam pipelines. By following the structure and guidelines in this reference, you can create complex data processing pipelines without writing extensive code.
