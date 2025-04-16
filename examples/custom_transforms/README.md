# Custom Transforms Example

This directory contains an example of using custom transforms in the declarative beam pipeline framework.

## Files

- `pipeline.yaml`: YAML configuration for the pipeline
- `run_pipeline.py`: Script to run the pipeline
- `utils.py`: Utility functions for the pipeline
- `sample_data.csv`: Sample data for the pipeline

## Custom Transforms

This example demonstrates two custom transforms:

1. **CustomMultiplyTransform**: Multiplies values in a specified field by a given factor
2. **CustomCategorizeTransform**: Categorizes elements based on a field value and defined ranges

## Running the Example

To run the example, execute the following command:

```bash
python examples/custom_transforms/run_pipeline.py
```

## Pipeline Flow

1. Read data from a CSV file
2. Parse the CSV lines into dictionaries
3. Apply the CustomMultiplyTransform to multiply the 'value' field by 2
4. Apply the CustomCategorizeTransform to categorize records based on their value
5. Write the categorized records to separate files

## Output

The pipeline produces the following output files in the `examples/custom_transforms/output` directory:

- `yaml_low_values-*`: Records with values between 0 and 30
- `yaml_medium_values-*`: Records with values between 30 and 70
- `yaml_high_values-*`: Records with values between 70 and 200
- `yaml_invalid_values-*`: Records with invalid values

## Implementation

The custom transforms are implemented in `declarative_beam/transforms/custom/basic.py`.
