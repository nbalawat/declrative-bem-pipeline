# Custom Transforms

This section documents the custom transforms available in the Declarative Beam Pipeline framework.

## Overview

Custom transforms extend the framework's capabilities by providing specialized functionality for specific use cases. These transforms follow the same interface as the built-in transforms but implement custom logic.

## Available Custom Transforms

### CustomMultiplyTransform

Multiplies values in a specified field by a given factor.

#### Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| field | string | Field containing the value to multiply | Yes |
| factor | number | Multiplication factor | Yes |

#### Example

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

#### Behavior

The transform:
1. Takes each element from the input PCollection
2. Extracts the value from the specified field
3. Multiplies it by the factor
4. Stores the original value in a new field called `original_value`
5. Updates the original field with the multiplied value
6. Outputs the modified element

### CustomCategorizeTransform

Categorizes elements based on a field value and defined ranges. This transform produces multiple outputs, one for each category plus 'other' and 'invalid' outputs.

#### Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| field | string | Field to use for categorization | Yes |
| categories | object | Dictionary mapping category names to value ranges | Yes |

Each category in the `categories` object should have:
- `min`: The minimum value for the category (inclusive)
- `max`: The maximum value for the category (exclusive)

#### Example

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

#### Behavior

The transform:
1. Takes each element from the input PCollection
2. Extracts the value from the specified field
3. Determines which category the value falls into based on the defined ranges
4. Adds a `category` field to the element with the name of the matching category
5. Outputs the element to the corresponding output PCollection
6. If the value doesn't match any category, outputs to the 'other' PCollection
7. If the field is missing or has an invalid value, outputs to the 'invalid' PCollection

## Creating Your Own Custom Transforms

For information on creating your own custom transforms, see [Creating Custom Transforms](../../advanced/custom_transforms.md).
