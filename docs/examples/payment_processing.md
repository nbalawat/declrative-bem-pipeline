# Payment Processing Pipeline Example

This example demonstrates how to build a payment processing pipeline using the declarative beam framework and custom payment processing transforms.

## Overview

The payment processing pipeline performs the following operations:

1. Read payment data from a CSV file
2. Parse and validate payment records
3. Categorize payments by amount (small, medium, large)
4. Enrich payments with additional metadata
5. Detect potentially fraudulent transactions
6. Convert payment amounts to a common currency (USD)
7. Write results to output files

## Pipeline Structure

The pipeline is structured in stages, following a modular approach that allows for easy testing and extension of individual components.

### Data Flow

```
CSV Input → Parse → Validate → Enrich → Fraud Detection → Currency Conversion → Output
```

### Key Components

- **Input**: CSV payment data with transaction details
- **Validation**: Checks for required fields and valid amount ranges
- **Enrichment**: Adds timestamps, date components, and processing metadata
- **Fraud Detection**: Identifies suspicious transactions based on multiple indicators
- **Currency Conversion**: Standardizes amounts to a single currency for reporting

## Running the Example

### Prerequisites

- Python 3.8 or higher
- Apache Beam installed
- Declarative Beam Pipeline framework
- Sample payment data (generated using the provided script)

### Environment Setup

If you're using a virtual environment with `uv` (as per user preferences):

```bash
# Create and activate a virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv sync
```

### Generate Sample Data

First, generate the sample payment data:

```bash
python examples/payment_processing/generate_test_data.py
```

This will create sample payment data files in the `examples/payment_processing/data` directory:
- `payments.csv`: Main payment data file with 100 records
- `test_payments.csv`: Additional test data with 20 records

### Run the Pipeline Step by Step

You can run different versions of the pipeline to understand each component:

1. **Simple Validation Pipeline**:
   ```bash
   python -m declarative_beam.cli.main run --config examples/payment_processing/simple_pipeline.yaml
   ```
   This runs basic validation and categorization of payments.

2. **Enrichment Pipeline**:
   ```bash
   python -m declarative_beam.cli.main run --config examples/payment_processing/enrichment_pipeline.yaml
   ```
   This adds payment enrichment with additional metadata.

3. **Fraud Detection Pipeline**:
   ```bash
   python -m declarative_beam.cli.main run --config examples/payment_processing/fraud_pipeline.yaml
   ```
   This adds fraud detection and currency conversion.

4. **Complete Pipeline**:
   ```bash
   python -m declarative_beam.cli.main run --config examples/payment_processing/final_pipeline.yaml
   ```
   This runs the full payment processing pipeline.

### Examining the Results

After running the pipeline, you can examine the output files:

```bash
# List output files
ls -la examples/payment_processing/output/

# View sample of low risk payments
head -n 3 examples/payment_processing/output/low_risk_payments-00000-of-00001

# View sample of medium risk payments
head -n 3 examples/payment_processing/output/medium_risk_payments-00000-of-00001

# View sample of high risk payments
head -n 3 examples/payment_processing/output/high_risk_payments-00000-of-00001
```

## Pipeline Configuration

The pipeline is defined in YAML configuration files. Multiple versions are provided to demonstrate different aspects of payment processing:

- `simple_pipeline.yaml`: Basic validation and categorization
- `enrichment_pipeline.yaml`: Adds payment enrichment
- `fraud_pipeline.yaml`: Adds fraud detection and currency conversion
- `final_pipeline.yaml`: Complete payment processing pipeline

### Example Configuration

Here's a simplified version of the payment processing pipeline:

```yaml
runner:
  type: DirectRunner
  options: {}

transforms:
  # Read input payment data
  - name: ReadPaymentData
    type: ReadFromText
    file_pattern: examples/payment_processing/data/payments.csv
    outputs:
      - raw_payments

  # Parse CSV records into payment dictionaries
  - name: ParsePayments
    type: Map
    fn_module: examples.payment_processing.utils
    fn_name: parse_csv_line_wrapper
    params:
      field_names: "transaction_id,user_id,amount,currency,timestamp,payment_method,ip_address,card_number"
    inputs:
      - raw_payments
    outputs:
      - parsed_payments

  # Validate payments and route them accordingly
  - name: ValidatePayments
    type: PaymentValidationTransform
    required_fields:
      - transaction_id
      - user_id
      - amount
      - currency
      - timestamp
    amount_field: amount
    min_amount: 0.01
    max_amount: 10000.00
    inputs:
      - parsed_payments
    outputs:
      - small_payments
      - medium_payments
      - large_payments
      - invalid_payments

  # Write results to output files
  - name: WriteSmallPayments
    type: WriteToText
    file_path_prefix: examples/payment_processing/output/small_payments
    inputs:
      - small_payments
```

## Extending the Example

The payment processing pipeline can be extended in several ways:

1. **Additional Validation Rules**: Add more sophisticated validation logic
2. **Machine Learning Fraud Detection**: Replace rule-based fraud detection with ML models
3. **Real-time Processing**: Adapt for streaming data sources
4. **Payment Analytics**: Add aggregation transforms for payment analytics
5. **Integration with Payment Gateways**: Add transforms for interacting with payment APIs

## Reference

For detailed information about the payment processing transforms, see the [Payment Processing Transforms](../transforms/custom/payment_processing.md) documentation.
