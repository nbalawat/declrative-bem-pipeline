# Transform Reference

This section provides detailed documentation for all the transforms available in the Declarative Beam Pipeline framework.

## Transform Categories

- [Processing Transforms](processing/README.md): Transforms for processing data
- [Aggregation Transforms](aggregation/README.md): Transforms for aggregating data
- [I/O Transforms](io/README.md): Transforms for reading and writing data
- [Window Transforms](window/README.md): Transforms for windowing operations

## Transform Structure

Each transform in the framework follows a common structure in the YAML configuration:

```yaml
- name: TransformName
  type: TransformType
  config:
    # Transform-specific configuration
  inputs:
    - input_pcollection_name
  outputs:
    - output_pcollection_name
  side_inputs:
    - side_input_name
```

### Common Properties

- **name**: A unique name for the transform instance
- **type**: The type of transform to use (must be registered in the TransformRegistry)
- **config**: Configuration parameters specific to the transform type
- **inputs**: List of input PCollection names
- **outputs**: List of output PCollection names
- **side_inputs**: Optional list of side input PCollection names

## Custom Transforms

You can create custom transforms by implementing the `BaseTransform` class and registering them with the `TransformRegistry`. See [Creating Custom Transforms](../advanced/custom_transforms.md) for details.
