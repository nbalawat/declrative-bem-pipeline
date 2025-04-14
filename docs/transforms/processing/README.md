# Processing Transforms

This section documents the processing transforms available in the Declarative Beam Pipeline framework.

## Basic Processing Transforms

- [Map](map.md): Apply a function to each element in a PCollection
- [FlatMap](flatmap.md): Apply a function that returns multiple elements for each input element
- [Filter](filter.md): Filter elements based on a predicate function
- [ParDo](pardo.md): Apply a DoFn to each element in a PCollection

## Advanced Processing Transforms

- [ParDoWithMultipleOutputs](pardo_multiple_outputs.md): Apply a DoFn that produces multiple output PCollections
- [SplitByValue](split_by_value.md): Split a PCollection into multiple PCollections based on value criteria
- [EnrichRecords](enrich_records.md): Enrich records with data from a side input
- [AsDict](as_dict.md): Convert key-value pairs into a dictionary for use as a side input

## Example: Using Map Transform

```yaml
- name: ParseCSV
  type: Map
  config:
    fn_module: csv_utils
    fn_name: parse_csv_line
  inputs:
    - raw_lines
  outputs:
    - parsed_records
```

## Example: Using SplitByValue Transform

```yaml
- name: SplitByValue
  type: SplitByValue
  config:
    value_field: value
    threshold: 50
    numeric: true
  inputs:
    - parsed_records
  outputs:
    - above_threshold
    - below_threshold
    - invalid
```

## Example: Using EnrichRecords with Side Input

```yaml
- name: EnrichHighValueRecords
  type: EnrichRecords
  config:
    join_field: id
    target_field: department
  inputs:
    - above_threshold
  outputs:
    - enriched_records
  side_inputs:
    reference_dict: reference_dict
```

For detailed information about each transform, click on the transform name to view its documentation.
