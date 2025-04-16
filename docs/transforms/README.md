# Transform Reference

This section provides detailed documentation for all the transforms available in the Declarative Beam Pipeline framework.

## Transform Categories

- [Processing Transforms](processing/README.md): Transforms for processing data
- [Aggregation Transforms](aggregation/README.md): Transforms for aggregating data
- [I/O Transforms](io/README.md): Transforms for reading and writing data
- [Window Transforms](window/README.md): Transforms for windowing operations
- [Custom Transforms](custom/README.md): Custom transforms for specialized operations

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

### Available Custom Transforms

The framework includes the following custom transforms:

#### CustomMultiplyTransform

Multiplies values in a specified field by a given factor.

```yaml
- name: MultiplyValues
  type: CustomMultiplyTransform
  config:
    field: value     # Field containing the value to multiply
    factor: 5        # Multiplication factor
  inputs:
    - input_records
  outputs:
    - multiplied_records
```

#### CustomCategorizeTransform

Categorizes elements based on a field value and defined ranges. This transform produces multiple outputs, one for each category plus 'other' and 'invalid' outputs.

```yaml
- name: CategorizeValues
  type: CustomCategorizeTransform
  config:
    field: value     # Field to use for categorization
    categories:      # Dictionary mapping category names to value ranges
      low:
        min: 0
        max: 25
      medium:
        min: 25
        max: 75
      high:
        min: 75
        max: 100
  inputs:
    - input_records
  outputs:
    - low            # Elements with field value between 0 and 25
    - medium         # Elements with field value between 25 and 75
    - high           # Elements with field value between 75 and 100
    - other          # Elements that don't match any category
    - invalid        # Elements with invalid or missing field values
```

For more details on implementing your own custom transforms, see [Creating Custom Transforms](../advanced/custom_transforms.md).
