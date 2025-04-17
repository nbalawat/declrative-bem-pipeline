# Example Pipelines

This section provides complete examples of pipelines built with the Declarative Beam Pipeline framework.

## YAML Pipeline Schema (Updated)

All example YAMLs must include a top-level `runner` section and place transform parameters at the top level. Example:

```yaml
runner:
  type: DirectRunner
  options: {}

transforms:
  - name: ReadCSV
    type: ReadFromText
    file_pattern: examples/data/input.csv
    outputs: [csv_lines]
```

## Basic Examples

- [Simple Pipeline](simple_pipeline.md): A basic pipeline that reads, processes, and writes data
- [Multiple Outputs](multiple_outputs.md): A pipeline demonstrating multiple output PCollections
- [Side Inputs](side_inputs.md): A pipeline using side inputs for data enrichment

## Advanced Examples

- [Windowing Pipeline](windowing_pipeline.md): A pipeline using windowing for time-based processing
- [Error Handling](error_handling.md): A pipeline with robust error handling
- [Composite Transforms](composite_transforms.md): A pipeline using composite transforms
- [Custom Transforms](custom_transforms.md): A pipeline demonstrating custom transforms for specialized operations

## Real-World Examples

- [Log Analysis](log_analysis.md): Analyzing log files to extract insights
- [Data Enrichment](data_enrichment.md): Enriching data with external references
- [Streaming Analytics](streaming_analytics.md): Real-time analytics on streaming data
