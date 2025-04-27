"""
Custom transforms for payment processing in the declarative beam pipeline framework.

This module provides custom transforms for payment processing operations.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import datetime
import re
import uuid

import apache_beam as beam

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("PaymentValidationTransform")
class PaymentValidationTransform(BaseTransform):
    """Validate payment transactions and route them to appropriate outputs.
    
    Parameters:
        required_fields: List of fields that must be present in a valid payment
        amount_field: Field containing the payment amount
        min_amount: Minimum valid payment amount
        max_amount: Maximum valid payment amount
    """
    
    PARAMETERS = {
        'required_fields': {
            'type': 'array',
            'description': 'List of fields that must be present in a valid payment',
            'required': True
        },
        'amount_field': {
            'type': 'string',
            'description': 'Field containing the payment amount',
            'required': True
        },
        'min_amount': {
            'type': 'number',
            'description': 'Minimum valid payment amount',
            'required': False,
            'default': 0.01
        },
        'max_amount': {
            'type': 'number',
            'description': 'Maximum valid payment amount',
            'required': False,
            'default': 1000000.00
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        required_fields = self.config.get('required_fields', [])
        amount_field = self.config.get('amount_field')
        min_amount = float(self.config.get('min_amount', 0.01))
        max_amount = float(self.config.get('max_amount', 1000000.00))
        
        if not required_fields or not amount_field:
            raise ValueError(f"Transform '{self.name}' requires 'required_fields' and 'amount_field'")
        
        # Get the output names from the transform configuration
        output_names = self.get_output_names()
        
        class PaymentValidationDoFn(beam.DoFn):
            def process(self, element):
                if not isinstance(element, dict):
                    yield beam.pvalue.TaggedOutput('invalid_payments', {
                        'original_data': element,
                        'error': 'Payment is not a dictionary'
                    })
                    return
                
                # Check for required fields
                missing_fields = [field for field in required_fields if field not in element]
                if missing_fields:
                    element['validation_error'] = f"Missing required fields: {', '.join(missing_fields)}"
                    yield beam.pvalue.TaggedOutput('invalid_payments', element)
                    return
                
                # Check amount field
                try:
                    amount = float(element.get(amount_field, 0))
                    if amount < min_amount:
                        element['validation_error'] = f"Amount {amount} is below minimum {min_amount}"
                        yield beam.pvalue.TaggedOutput('invalid_payments', element)
                        return
                    
                    if amount > max_amount:
                        element['validation_error'] = f"Amount {amount} exceeds maximum {max_amount}"
                        yield beam.pvalue.TaggedOutput('invalid_payments', element)
                        return
                    
                    # Valid payment, route based on amount
                    if amount < 100:
                        yield beam.pvalue.TaggedOutput('small_payments', element)
                    elif amount < 1000:
                        yield beam.pvalue.TaggedOutput('medium_payments', element)
                    else:
                        yield beam.pvalue.TaggedOutput('large_payments', element)
                        
                except (ValueError, TypeError):
                    element['validation_error'] = f"Invalid amount format: {element.get(amount_field)}"
                    yield beam.pvalue.TaggedOutput('invalid_payments', element)
        
        return beam.ParDo(PaymentValidationDoFn()).with_outputs(*output_names)


@TransformRegistry.register("PaymentEnrichmentTransform")
class PaymentEnrichmentTransform(BaseTransform):
    """Enrich payment data with additional information.
    
    Parameters:
        timestamp_field: Field containing the payment timestamp
        timezone: Timezone to use for date calculations
        add_fields: Additional fields to add to each payment
    """
    
    PARAMETERS = {
        'timestamp_field': {
            'type': 'string',
            'description': 'Field containing the payment timestamp',
            'required': True
        },
        'timezone': {
            'type': 'string',
            'description': 'Timezone to use for date calculations',
            'required': False,
            'default': 'UTC'
        },
        'add_fields': {
            'type': 'object',
            'description': 'Additional fields to add to each payment',
            'required': False,
            'default': {}
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        timestamp_field = self.config.get('timestamp_field')
        timezone = self.config.get('timezone', 'UTC')
        add_fields = self.config.get('add_fields', {})
        
        if not timestamp_field:
            raise ValueError(f"Transform '{self.name}' requires 'timestamp_field'")
        
        return beam.Map(lambda x: self._enrich_payment(
            x, timestamp_field, timezone, add_fields))
    
    def _enrich_payment(self, element: Dict[str, Any], timestamp_field: str, 
                       timezone: str, add_fields: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich payment with additional information.
        
        Args:
            element: The input payment (dictionary)
            timestamp_field: Field containing the payment timestamp
            timezone: Timezone to use for date calculations
            add_fields: Additional fields to add to each payment
            
        Returns:
            The enriched payment
        """
        if not isinstance(element, dict):
            return element
        
        result = dict(element)
        
        # Add a unique transaction ID if not present
        if 'transaction_id' not in result:
            result['transaction_id'] = str(uuid.uuid4())
        
        # Add processing timestamp
        result['processing_timestamp'] = datetime.datetime.now().isoformat()
        
        # Extract date components from timestamp if present
        if timestamp_field in result:
            try:
                timestamp = result[timestamp_field]
                # Simple parsing for ISO format timestamps
                dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                
                result['payment_year'] = dt.year
                result['payment_month'] = dt.month
                result['payment_day'] = dt.day
                result['payment_hour'] = dt.hour
                result['payment_weekday'] = dt.weekday()
            except (ValueError, TypeError, AttributeError):
                # If timestamp parsing fails, don't add the date components
                pass
        
        # Add all additional fields
        for key, value in add_fields.items():
            result[key] = value
            
        return result


@TransformRegistry.register("FraudDetectionTransform")
class FraudDetectionTransform(BaseTransform):
    """Detect potentially fraudulent payments based on various rules.
    
    Parameters:
        amount_field: Field containing the payment amount
        timestamp_field: Field containing the payment timestamp
        ip_address_field: Field containing the IP address
        card_number_field: Field containing the masked card number
        velocity_threshold: Maximum number of transactions allowed in the time window
        velocity_window_minutes: Time window for velocity check in minutes
    """
    
    PARAMETERS = {
        'amount_field': {
            'type': 'string',
            'description': 'Field containing the payment amount',
            'required': True
        },
        'timestamp_field': {
            'type': 'string',
            'description': 'Field containing the payment timestamp',
            'required': True
        },
        'ip_address_field': {
            'type': 'string',
            'description': 'Field containing the IP address',
            'required': False
        },
        'card_number_field': {
            'type': 'string',
            'description': 'Field containing the masked card number',
            'required': False
        },
        'velocity_threshold': {
            'type': 'integer',
            'description': 'Maximum number of transactions allowed in the time window',
            'required': False,
            'default': 5
        },
        'velocity_window_minutes': {
            'type': 'integer',
            'description': 'Time window for velocity check in minutes',
            'required': False,
            'default': 10
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        amount_field = self.config.get('amount_field')
        timestamp_field = self.config.get('timestamp_field')
        ip_address_field = self.config.get('ip_address_field')
        card_number_field = self.config.get('card_number_field')
        velocity_threshold = int(self.config.get('velocity_threshold', 5))
        velocity_window_minutes = int(self.config.get('velocity_window_minutes', 10))
        
        if not amount_field or not timestamp_field:
            raise ValueError(f"Transform '{self.name}' requires 'amount_field' and 'timestamp_field'")
        
        # Get the output names from the transform configuration
        output_names = self.get_output_names()
        
        class FraudDetectionDoFn(beam.DoFn):
            def __init__(self):
                self.recent_transactions = {}
                
            def process(self, element):
                if not isinstance(element, dict):
                    yield beam.pvalue.TaggedOutput('error_payments', element)
                    return
                
                fraud_indicators = []
                
                # Check 1: Unusual amount patterns
                try:
                    amount = float(element.get(amount_field, 0))
                    # Round numbers often indicate fraud
                    if amount > 0 and amount == round(amount) and amount >= 100:
                        fraud_indicators.append('round_amount')
                        
                    # Repeated digits can indicate fraud
                    amount_str = str(amount)
                    if re.search(r'(\d)\1{3,}', amount_str.replace('.', '')):
                        fraud_indicators.append('repeated_digits')
                except (ValueError, TypeError):
                    pass
                
                # Check 2: IP address checks
                if ip_address_field and ip_address_field in element:
                    ip = element[ip_address_field]
                    # Check for known high-risk IP patterns
                    if ip.startswith('192.168.') or ip.startswith('10.0.'):
                        fraud_indicators.append('internal_ip')
                
                # Check 3: Card number validation
                if card_number_field and card_number_field in element:
                    card_num = element[card_number_field]
                    # Simple Luhn algorithm check would go here
                    if card_num.startswith('0000'):
                        fraud_indicators.append('invalid_card')
                
                # Check 4: Velocity checks
                if 'user_id' in element:
                    user_id = element['user_id']
                    current_time = datetime.datetime.now()
                    
                    # Clean up old transactions
                    cutoff_time = current_time - datetime.timedelta(minutes=velocity_window_minutes)
                    if user_id in self.recent_transactions:
                        self.recent_transactions[user_id] = [
                            t for t in self.recent_transactions[user_id] if t > cutoff_time
                        ]
                    
                    # Add current transaction
                    if user_id not in self.recent_transactions:
                        self.recent_transactions[user_id] = []
                    self.recent_transactions[user_id].append(current_time)
                    
                    # Check velocity
                    if len(self.recent_transactions[user_id]) > velocity_threshold:
                        fraud_indicators.append('velocity_check')
                
                # Add fraud indicators to the element
                element['fraud_indicators'] = fraud_indicators
                element['fraud_score'] = len(fraud_indicators) * 25  # Simple scoring
                
                # Route based on fraud score
                if len(fraud_indicators) > 0:
                    if len(fraud_indicators) >= 2:
                        yield beam.pvalue.TaggedOutput('high_risk_payments', element)
                    else:
                        yield beam.pvalue.TaggedOutput('medium_risk_payments', element)
                else:
                    yield beam.pvalue.TaggedOutput('low_risk_payments', element)
        
        return beam.ParDo(FraudDetectionDoFn()).with_outputs(*output_names)


@TransformRegistry.register("CurrencyConversionTransform")
class CurrencyConversionTransform(BaseTransform):
    """Convert payment amounts between currencies.
    
    Parameters:
        amount_field: Field containing the payment amount
        source_currency_field: Field containing the source currency code
        target_currency: Target currency to convert to
        exchange_rates: Dictionary of exchange rates relative to a base currency
        base_currency: Base currency for the exchange rates
    """
    
    PARAMETERS = {
        'amount_field': {
            'type': 'string',
            'description': 'Field containing the payment amount',
            'required': True
        },
        'source_currency_field': {
            'type': 'string',
            'description': 'Field containing the source currency code',
            'required': True
        },
        'target_currency': {
            'type': 'string',
            'description': 'Target currency to convert to',
            'required': True
        },
        'exchange_rates': {
            'type': 'object',
            'description': 'Dictionary of exchange rates relative to a base currency',
            'required': True
        },
        'base_currency': {
            'type': 'string',
            'description': 'Base currency for the exchange rates',
            'required': True
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        amount_field = self.config.get('amount_field')
        source_currency_field = self.config.get('source_currency_field')
        target_currency = self.config.get('target_currency')
        exchange_rates = self.config.get('exchange_rates', {})
        base_currency = self.config.get('base_currency')
        
        if not all([amount_field, source_currency_field, target_currency, exchange_rates, base_currency]):
            raise ValueError(f"Transform '{self.name}' requires all currency conversion parameters")
        
        return beam.Map(lambda x: self._convert_currency(
            x, amount_field, source_currency_field, target_currency, exchange_rates, base_currency))
    
    def _convert_currency(self, element: Dict[str, Any], amount_field: str, 
                         source_currency_field: str, target_currency: str,
                         exchange_rates: Dict[str, float], base_currency: str) -> Dict[str, Any]:
        """Convert payment amount between currencies.
        
        Args:
            element: The input payment (dictionary)
            amount_field: Field containing the payment amount
            source_currency_field: Field containing the source currency code
            target_currency: Target currency to convert to
            exchange_rates: Dictionary of exchange rates relative to a base currency
            base_currency: Base currency for the exchange rates
            
        Returns:
            The payment with converted amount
        """
        if not isinstance(element, dict):
            return element
        
        if amount_field not in element or source_currency_field not in element:
            return element
        
        try:
            amount = float(element[amount_field])
            source_currency = element[source_currency_field]
            
            # Skip if already in target currency
            if source_currency == target_currency:
                element['converted_amount'] = amount
                element['converted_currency'] = target_currency
                return element
            
            # Convert to base currency first
            if source_currency == base_currency:
                base_amount = amount
            elif source_currency in exchange_rates:
                base_amount = amount / exchange_rates[source_currency]
            else:
                # Can't convert this currency
                element['conversion_error'] = f"Unknown source currency: {source_currency}"
                return element
            
            # Convert from base currency to target
            if target_currency == base_currency:
                target_amount = base_amount
            elif target_currency in exchange_rates:
                target_amount = base_amount * exchange_rates[target_currency]
            else:
                # Can't convert to this currency
                element['conversion_error'] = f"Unknown target currency: {target_currency}"
                return element
            
            # Store original and converted amounts
            element['original_amount'] = amount
            element['original_currency'] = source_currency
            element['converted_amount'] = round(target_amount, 2)
            element['converted_currency'] = target_currency
            
            return element
        except (ValueError, TypeError):
            element['conversion_error'] = f"Invalid amount format: {element.get(amount_field)}"
            return element


@TransformRegistry.register("PaymentAggregationTransform")
class PaymentAggregationTransform(BaseTransform):
    """Aggregate payment data by specified grouping fields.
    
    Parameters:
        group_by_fields: List of fields to group by
        amount_field: Field containing the payment amount
        count_field: Output field name for the count
        sum_field: Output field name for the sum
        avg_field: Output field name for the average
        min_field: Output field name for the minimum
        max_field: Output field name for the maximum
    """
    
    PARAMETERS = {
        'group_by_fields': {
            'type': 'array',
            'description': 'List of fields to group by',
            'required': True
        },
        'amount_field': {
            'type': 'string',
            'description': 'Field containing the payment amount',
            'required': True
        },
        'count_field': {
            'type': 'string',
            'description': 'Output field name for the count',
            'required': False,
            'default': 'count'
        },
        'sum_field': {
            'type': 'string',
            'description': 'Output field name for the sum',
            'required': False,
            'default': 'total_amount'
        },
        'avg_field': {
            'type': 'string',
            'description': 'Output field name for the average',
            'required': False,
            'default': 'avg_amount'
        },
        'min_field': {
            'type': 'string',
            'description': 'Output field name for the minimum',
            'required': False,
            'default': 'min_amount'
        },
        'max_field': {
            'type': 'string',
            'description': 'Output field name for the maximum',
            'required': False,
            'default': 'max_amount'
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        group_by_fields = self.config.get('group_by_fields', [])
        amount_field = self.config.get('amount_field')
        count_field = self.config.get('count_field', 'count')
        sum_field = self.config.get('sum_field', 'total_amount')
        avg_field = self.config.get('avg_field', 'avg_amount')
        min_field = self.config.get('min_field', 'min_amount')
        max_field = self.config.get('max_field', 'max_amount')
        
        if not group_by_fields or not amount_field:
            raise ValueError(f"Transform '{self.name}' requires 'group_by_fields' and 'amount_field'")
        
        class ExtractKeyValue(beam.DoFn):
            def process(self, element):
                if not isinstance(element, dict):
                    return
                
                # Extract the key (tuple of group by field values)
                key_values = []
                for field in group_by_fields:
                    if field in element:
                        key_values.append(element[field])
                    else:
                        key_values.append(None)
                
                # Extract the amount
                try:
                    amount = float(element.get(amount_field, 0))
                    yield (tuple(key_values), amount)
                except (ValueError, TypeError):
                    # Skip elements with invalid amounts
                    pass
        
        class PaymentStatsCombineFn(beam.CombineFn):
            def create_accumulator(self):
                return {
                    'count': 0,
                    'sum': 0.0,
                    'min': float('inf'),
                    'max': float('-inf')
                }
            
            def add_input(self, accumulator, input_value):
                accumulator['count'] += 1
                accumulator['sum'] += input_value
                accumulator['min'] = min(accumulator['min'], input_value)
                accumulator['max'] = max(accumulator['max'], input_value)
                return accumulator
            
            def merge_accumulators(self, accumulators):
                merged = self.create_accumulator()
                for acc in accumulators:
                    merged['count'] += acc['count']
                    merged['sum'] += acc['sum']
                    merged['min'] = min(merged['min'], acc['min']) if acc['count'] > 0 else merged['min']
                    merged['max'] = max(merged['max'], acc['max']) if acc['count'] > 0 else merged['max']
                return merged
            
            def extract_output(self, accumulator):
                if accumulator['count'] == 0:
                    return {
                        'count': 0,
                        'sum': 0.0,
                        'avg': 0.0,
                        'min': 0.0,
                        'max': 0.0
                    }
                
                return {
                    'count': accumulator['count'],
                    'sum': accumulator['sum'],
                    'avg': accumulator['sum'] / accumulator['count'],
                    'min': accumulator['min'],
                    'max': accumulator['max']
                }
        
        class FormatResults(beam.DoFn):
            def process(self, element):
                key, stats = element
                
                # Create result dictionary with group by fields
                result = {}
                for i, field in enumerate(group_by_fields):
                    result[field] = key[i]
                
                # Add statistics
                result[count_field] = stats['count']
                result[sum_field] = stats['sum']
                result[avg_field] = stats['avg']
                result[min_field] = stats['min']
                result[max_field] = stats['max']
                
                yield result
        
        return beam.PTransform(
            lambda pcoll: (
                pcoll
                | "ExtractKeyValue" >> beam.ParDo(ExtractKeyValue())
                | "CombineStats" >> beam.CombinePerKey(PaymentStatsCombineFn())
                | "FormatResults" >> beam.ParDo(FormatResults())
            )
        )
