# Data Enrichment Example

This example demonstrates how to use the Declarative Beam Pipeline framework to enrich data with information from a reference dataset.

## Use Case

A common data processing task is to enrich records with additional information from a reference dataset. For example, you might have:

- A primary dataset with user transactions
- A reference dataset with user profile information

You want to combine these datasets to create enriched records with both transaction details and user profile information.

## Pipeline Configuration

Here's the YAML configuration for a data enrichment pipeline:

```yaml
name: Data Enrichment Pipeline
description: Enriches transaction data with user profile information

transforms:
  # Read the main dataset
  - name: ReadTransactions
    type: ReadFromText
    config:
      file_pattern: data/transactions.csv
    outputs:
      - raw_transactions

  # Parse the transactions
  - name: ParseTransactions
    type: Map
    config:
      fn_module: csv_utils
      fn_name: parse_csv_line
      params:
        headers: ["transaction_id", "user_id", "amount", "timestamp"]
    inputs:
      - raw_transactions
    outputs:
      - parsed_transactions

  # Read the reference dataset
  - name: ReadUserProfiles
    type: ReadFromText
    config:
      file_pattern: data/user_profiles.csv
    outputs:
      - raw_profiles

  # Parse the user profiles
  - name: ParseUserProfiles
    type: Map
    config:
      fn_module: csv_utils
      fn_name: parse_csv_line
      params:
        headers: ["user_id", "name", "email", "age", "location"]
    inputs:
      - raw_profiles
    outputs:
      - parsed_profiles

  # Extract key-value pairs from the profiles
  - name: ExtractProfileKeyValue
    type: Map
    config:
      fn_module: kv_utils
      fn_name: extract_key_value
      params:
        key_field: user_id
    inputs:
      - parsed_profiles
    outputs:
      - profile_key_value_pairs

  # Create a dictionary from the profiles for use as a side input
  - name: CreateProfileDict
    type: AsDict
    inputs:
      - profile_key_value_pairs
    outputs:
      - profile_dict

  # Split transactions by amount
  - name: SplitTransactions
    type: SplitByValue
    config:
      value_field: amount
      threshold: 100
      numeric: true
    inputs:
      - parsed_transactions
    outputs:
      - high_value_transactions
      - low_value_transactions
      - invalid_transactions

  # Enrich high-value transactions with user profile information
  - name: EnrichHighValueTransactions
    type: EnrichRecords
    config:
      join_field: user_id
      target_field: user_profile
    inputs:
      - high_value_transactions
    outputs:
      - enriched_transactions
    side_inputs:
      reference_dict: profile_dict

  # Format the enriched transactions
  - name: FormatEnrichedTransactions
    type: Map
    config:
      fn_module: format_utils
      fn_name: format_enriched_transaction
    inputs:
      - enriched_transactions
    outputs:
      - formatted_enriched_transactions

  # Write the enriched transactions to a file
  - name: WriteEnrichedTransactions
    type: WriteToText
    config:
      file_path_prefix: output/enriched_transactions
    inputs:
      - formatted_enriched_transactions

  # Format the low-value transactions
  - name: FormatLowValueTransactions
    type: Map
    config:
      fn_module: format_utils
      fn_name: format_transaction
    inputs:
      - low_value_transactions
    outputs:
      - formatted_low_value_transactions

  # Write the low-value transactions to a file
  - name: WriteLowValueTransactions
    type: WriteToText
    config:
      file_path_prefix: output/low_value_transactions
    inputs:
      - formatted_low_value_transactions

  # Format the invalid transactions
  - name: FormatInvalidTransactions
    type: Map
    config:
      fn_module: format_utils
      fn_name: format_transaction
    inputs:
      - invalid_transactions
    outputs:
      - formatted_invalid_transactions

  # Write the invalid transactions to a file
  - name: WriteInvalidTransactions
    type: WriteToText
    config:
      file_path_prefix: output/invalid_transactions
    inputs:
      - formatted_invalid_transactions
```

## Utility Functions

Here are the utility functions used in this pipeline:

### csv_utils.py

```python
def parse_csv_line(line, headers=None):
    """Parse a CSV line into a dictionary."""
    values = line.strip().split(',')
    
    if headers and len(values) == len(headers):
        return dict(zip(headers, values))
    elif not headers and len(values) >= 3:
        # Default parsing if headers not provided
        return {
            'id': values[0],
            'name': values[1],
            'value': values[2]
        }
    return None
```

### kv_utils.py

```python
def extract_key_value(element, key_field):
    """Extract a key-value pair from an element."""
    if isinstance(element, dict) and key_field in element:
        key = element[key_field]
        return (key, element)
    return None
```

### format_utils.py

```python
def format_transaction(transaction):
    """Format a transaction record as a string."""
    return str(transaction)

def format_enriched_transaction(transaction):
    """Format an enriched transaction with user profile information."""
    result = dict(transaction)
    
    # Extract user profile information
    if 'user_profile' in transaction and isinstance(transaction['user_profile'], dict):
        profile = transaction['user_profile']
        # Flatten the user profile into the result
        for key, value in profile.items():
            if key != 'user_id':  # Avoid duplicate user_id
                result[f'user_{key}'] = value
        
        # Remove the nested user_profile
        del result['user_profile']
    
    return str(result)
```

## Running the Pipeline

Create a Python script to run the pipeline:

```python
from declarative_beam.core.yaml_processor import YAMLProcessor
from declarative_beam.core.pipeline_builder import PipelineBuilder

def main():
    # Load the YAML configuration
    yaml_processor = YAMLProcessor()
    pipeline_config = yaml_processor.load_yaml('data_enrichment_pipeline.yaml')
    
    # Build and run the pipeline
    builder = PipelineBuilder(pipeline_config)
    pipeline = builder.build_pipeline()
    pipeline.run()

if __name__ == '__main__':
    main()
```

## Expected Output

The pipeline will produce three output files:

1. `output/enriched_transactions-00000-of-00001.txt`: High-value transactions enriched with user profile information
2. `output/low_value_transactions-00000-of-00001.txt`: Low-value transactions
3. `output/invalid_transactions-00000-of-00001.txt`: Transactions with invalid amounts

## Key Concepts Demonstrated

This example demonstrates several key concepts:

1. **Multiple Inputs**: Reading from multiple data sources
2. **Side Inputs**: Using a reference dataset as a side input
3. **Multiple Outputs**: Splitting data into different streams based on criteria
4. **Data Enrichment**: Joining data from different sources
5. **Error Handling**: Separating invalid records for later analysis

## Extensions

You could extend this pipeline in several ways:

- Add windowing to process streaming transaction data
- Implement more sophisticated joining logic
- Add aggregations to compute statistics by user or location
- Implement data quality checks and validation
- Add logging and monitoring transforms

For more advanced enrichment scenarios, see the [Advanced Data Processing](../advanced/advanced_data_processing.md) section.
