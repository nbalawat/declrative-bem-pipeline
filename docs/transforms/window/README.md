# Window Transforms

This section documents the window transforms available in the Declarative Beam Pipeline framework for working with streaming data.

## Window Types

- [Window](window.md): Apply a windowing strategy to a PCollection
- [WithTimestamps](with_timestamps.md): Add timestamps to elements in a PCollection
- [GroupByWindow](group_by_window.md): Group elements in a PCollection by window

## Window Strategies

The framework supports the following windowing strategies:

### Fixed Windows

Fixed windows divide the data into consistent, non-overlapping time intervals.

```yaml
- name: Window
  type: Window
  config:
    window_type: fixed
    window_size: 60  # 60-second windows
  inputs:
    - timestamped_records
  outputs:
    - windowed_records
```

### Sliding Windows

Sliding windows define overlapping time intervals, useful for moving averages and other sliding computations.

```yaml
- name: Window
  type: Window
  config:
    window_type: sliding
    window_size: 60  # 60-second windows
    window_period: 30  # 30-second period (50% overlap)
  inputs:
    - timestamped_records
  outputs:
    - windowed_records
```

### Session Windows

Session windows group elements within a certain gap duration of inactivity.

```yaml
- name: Window
  type: Window
  config:
    window_type: session
    gap_size: 300  # 5-minute gap
  inputs:
    - timestamped_records
  outputs:
    - windowed_records
```

### Global Windows

Global windows place all elements into a single window, useful for batch processing.

```yaml
- name: Window
  type: Window
  config:
    window_type: global
  inputs:
    - timestamped_records
  outputs:
    - windowed_records
```

## Adding Timestamps

Before applying windowing, elements must have timestamps. The `WithTimestamps` transform adds timestamps to elements:

```yaml
- name: AddTimestamps
  type: WithTimestamps
  config:
    fn_module: timestamp_utils
    fn_name: extract_timestamp
    params:
      time_field: timestamp
  inputs:
    - parsed_records
  outputs:
    - timestamped_records
```

The timestamp function should extract a Unix timestamp (seconds since epoch) from each element:

```python
def extract_timestamp(element, time_field='timestamp'):
    """Extract timestamp from an element."""
    if isinstance(element, dict) and time_field in element:
        try:
            return float(element[time_field])
        except (ValueError, TypeError):
            return 0.0
    return 0.0
```

## Grouping by Window

After windowing, you can group elements within each window:

```yaml
- name: GroupByWindow
  type: GroupByWindow
  inputs:
    - key_value_pairs
  outputs:
    - grouped_records
```

This is typically used after a `GroupByKey` operation to ensure that grouping respects window boundaries.

## Example: Complete Windowing Pipeline

```yaml
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

  - name: AddTimestamps
    type: WithTimestamps
    config:
      fn_module: timestamp_utils
      fn_name: extract_timestamp
      params:
        time_field: timestamp
    inputs:
      - parsed_records
    outputs:
      - timestamped_records

  - name: Window
    type: Window
    config:
      window_type: fixed
      window_size: 60
    inputs:
      - timestamped_records
    outputs:
      - windowed_records

  - name: ExtractKeyValue
    type: Map
    config:
      fn_module: kv_utils
      fn_name: extract_key_value
      params:
        key_field: category
    inputs:
      - windowed_records
    outputs:
      - key_value_pairs

  - name: GroupByWindow
    type: GroupByWindow
    inputs:
      - key_value_pairs
    outputs:
      - grouped_records

  - name: FormatResults
    type: Map
    config:
      fn_module: format_utils
      fn_name: format_result
    inputs:
      - grouped_records
    outputs:
      - formatted_results

  - name: WriteResults
    type: WriteToText
    config:
      file_path_prefix: output/windowed_results
    inputs:
      - formatted_results
```

For more advanced windowing scenarios, including custom windowing functions and triggers, see the [Advanced Windowing](../../advanced/advanced_windowing.md) section.
