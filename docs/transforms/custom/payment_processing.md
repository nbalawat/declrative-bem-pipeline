# Payment Processing Transforms

This document describes the custom transforms for payment processing in the declarative beam pipeline framework.

## Overview

The payment processing transforms provide functionality for building payment processing pipelines, including:

- Payment validation and categorization
- Payment data enrichment
- Fraud detection
- Currency conversion
- Payment aggregation

These transforms can be used together to build comprehensive payment processing pipelines or individually for specific payment processing tasks.

## Available Transforms

### PaymentValidationTransform

Validates payment transactions and routes them to appropriate outputs based on amount ranges.

#### Parameters

| Parameter | Type | Description | Required | Default |
|-----------|------|-------------|----------|---------|
| `required_fields` | array | List of fields that must be present in a valid payment | Yes | - |
| `amount_field` | string | Field containing the payment amount | Yes | - |
| `min_amount` | number | Minimum valid payment amount | No | 0.01 |
| `max_amount` | number | Maximum valid payment amount | No | 1000000.00 |

#### Outputs

- `small_payments`: Payments with amount < 100
- `medium_payments`: Payments with amount between 100 and 1000
- `large_payments`: Payments with amount >= 1000
- `invalid_payments`: Payments that fail validation

#### Example

```yaml
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
```

### PaymentEnrichmentTransform

Enriches payment data with additional information such as processing timestamps, date components, and custom fields.

#### Parameters

| Parameter | Type | Description | Required | Default |
|-----------|------|-------------|----------|---------|
| `timestamp_field` | string | Field containing the payment timestamp | Yes | - |
| `timezone` | string | Timezone to use for date calculations | No | UTC |
| `add_fields` | object | Additional fields to add to each payment | No | {} |

#### Example

```yaml
- name: EnrichPayments
  type: PaymentEnrichmentTransform
  timestamp_field: timestamp
  timezone: UTC
  add_fields:
    processing_type: standard_payment
    priority: medium
  inputs:
    - payments
  outputs:
    - enriched_payments
```

### FraudDetectionTransform

Detects potentially fraudulent payments based on various rules and assigns fraud scores.

#### Parameters

| Parameter | Type | Description | Required | Default |
|-----------|------|-------------|----------|---------|
| `amount_field` | string | Field containing the payment amount | Yes | - |
| `timestamp_field` | string | Field containing the payment timestamp | Yes | - |
| `ip_address_field` | string | Field containing the IP address | No | - |
| `card_number_field` | string | Field containing the masked card number | No | - |
| `velocity_threshold` | integer | Maximum number of transactions allowed in the time window | No | 5 |
| `velocity_window_minutes` | integer | Time window for velocity check in minutes | No | 10 |

#### Outputs

- `low_risk_payments`: Payments with no fraud indicators
- `medium_risk_payments`: Payments with one fraud indicator
- `high_risk_payments`: Payments with multiple fraud indicators
- `error_payments`: Payments that couldn't be processed

#### Example

```yaml
- name: DetectFraudulentPayments
  type: FraudDetectionTransform
  amount_field: amount
  timestamp_field: timestamp
  ip_address_field: ip_address
  card_number_field: card_number
  velocity_threshold: 3
  velocity_window_minutes: 5
  inputs:
    - enriched_payments
  outputs:
    - low_risk_payments
    - medium_risk_payments
    - high_risk_payments
    - error_payments
```

### CurrencyConversionTransform

Converts payment amounts between currencies using configurable exchange rates.

#### Parameters

| Parameter | Type | Description | Required | Default |
|-----------|------|-------------|----------|---------|
| `amount_field` | string | Field containing the payment amount | Yes | - |
| `source_currency_field` | string | Field containing the source currency code | Yes | - |
| `target_currency` | string | Target currency to convert to | Yes | - |
| `exchange_rates` | object | Dictionary of exchange rates relative to a base currency | Yes | - |
| `base_currency` | string | Base currency for the exchange rates | Yes | - |

#### Example

```yaml
- name: ConvertPayments
  type: CurrencyConversionTransform
  amount_field: amount
  source_currency_field: currency
  target_currency: USD
  base_currency: USD
  exchange_rates:
    EUR: 1.1
    GBP: 1.3
    JPY: 0.0091
    CAD: 0.75
    AUD: 0.68
  inputs:
    - payments
  outputs:
    - converted_payments
```

## Testing and Running the Transforms

### Unit Testing

The payment processing transforms include unit tests that verify their functionality. You can run these tests using pytest:

```bash
# Run all payment processing tests
python -m pytest tests/transforms/custom/test_payment_processing.py -v

# Run specific test class
python -m pytest tests/transforms/custom/test_payment_processing.py::TestPaymentValidationTransform -v

# Run a specific test method
python -m pytest tests/transforms/custom/test_payment_processing.py::TestPaymentValidationTransform::test_valid_payments -v
```

### Using in Custom Pipelines

To use these transforms in your own pipelines:

1. Import the transforms in your custom module:
   ```python
   from declarative_beam.transforms.custom.payment_processing import (
       PaymentValidationTransform,
       PaymentEnrichmentTransform,
       FraudDetectionTransform,
       CurrencyConversionTransform
   )
   ```

2. Create a YAML configuration file that uses these transforms (see examples above)

3. Run your pipeline using the declarative beam CLI:
   ```bash
   python -m declarative_beam.cli.main run --config path/to/your/pipeline.yaml
   ```

### Docker Support

You can also run the payment processing pipeline in Docker for isolated testing:

```bash
# Build the Docker image
docker build -t declarative-beam-pipeline -f docker/Dockerfile .

# Run the container with Jupyter support
docker run -p 8888:8888 -v $(pwd):/app declarative-beam-pipeline

# Run a specific pipeline in the container
docker run -v $(pwd):/app declarative-beam-pipeline python -m declarative_beam.cli.main run --config examples/payment_processing/final_pipeline.yaml
```

## Complete Pipeline Example

For a complete example of a payment processing pipeline, see the [Payment Processing Example](../../examples/payment_processing.md).
