"""
Tests for payment processing custom transforms.
"""

from typing import Dict, Any, List
import datetime
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from declarative_beam.transforms.custom.payment_processing import (
    PaymentValidationTransform,
    PaymentEnrichmentTransform,
    FraudDetectionTransform,
    CurrencyConversionTransform,
    PaymentAggregationTransform
)


class TestPaymentValidationTransform(unittest.TestCase):
    """Tests for PaymentValidationTransform."""

    def test_valid_payments(self) -> None:
        """Test that valid payments are routed correctly."""
        transform = PaymentValidationTransform(
            name="test_validation",
            config={
                "required_fields": ["id", "amount", "currency"],
                "amount_field": "amount",
                "min_amount": 1.0,
                "max_amount": 1000.0
            }
        )
        
        # Create test data
        test_data = [
            {"id": "1", "amount": "5.0", "currency": "USD"},     # Small payment
            {"id": "2", "amount": "150.0", "currency": "EUR"},   # Medium payment
            {"id": "3", "amount": "950.0", "currency": "GBP"},   # Large payment
        ]
        
        # Run the transform
        with TestPipeline() as p:
            inputs = p | beam.Create(test_data)
            outputs = inputs | transform.build_transform()
            
            # Check small payments
            assert_that(
                outputs.small_payments,
                equal_to([{"id": "1", "amount": "5.0", "currency": "USD"}]),
                label="CheckSmallPayments"
            )
            
            # Check medium payments
            assert_that(
                outputs.medium_payments,
                equal_to([{"id": "2", "amount": "150.0", "currency": "EUR"}]),
                label="CheckMediumPayments"
            )
            
            # Check large payments
            assert_that(
                outputs.large_payments,
                equal_to([{"id": "3", "amount": "950.0", "currency": "GBP"}]),
                label="CheckLargePayments"
            )
    
    def test_invalid_payments(self) -> None:
        """Test that invalid payments are detected."""
        transform = PaymentValidationTransform(
            name="test_validation",
            config={
                "required_fields": ["id", "amount", "currency"],
                "amount_field": "amount",
                "min_amount": 1.0,
                "max_amount": 1000.0
            }
        )
        
        # Create test data with various validation issues
        test_data = [
            {"id": "4", "currency": "USD"},                      # Missing amount
            {"id": "5", "amount": "0.5", "currency": "EUR"},     # Below min amount
            {"id": "6", "amount": "1500.0", "currency": "GBP"},  # Above max amount
            {"id": "7", "amount": "invalid", "currency": "USD"}, # Invalid amount format
            "not_a_dict"                                         # Not a dictionary
        ]
        
        # Run the transform
        with TestPipeline() as p:
            inputs = p | beam.Create(test_data)
            outputs = inputs | transform.build_transform()
            
            # All test data should go to invalid_payments
            assert_that(
                outputs.invalid_payments,
                equal_to([
                    {"id": "4", "currency": "USD", "validation_error": "Missing required fields: amount"},
                    {"id": "5", "amount": "0.5", "currency": "EUR", "validation_error": "Amount 0.5 is below minimum 1.0"},
                    {"id": "6", "amount": "1500.0", "currency": "GBP", "validation_error": "Amount 1500.0 exceeds maximum 1000.0"},
                    {"id": "7", "amount": "invalid", "currency": "USD", "validation_error": "Invalid amount format: invalid"},
                    {"original_data": "not_a_dict", "error": "Payment is not a dictionary"}
                ]),
                label="CheckInvalidPayments"
            )


class TestPaymentEnrichmentTransform(unittest.TestCase):
    """Tests for PaymentEnrichmentTransform."""
    
    def test_payment_enrichment(self) -> None:
        """Test that payments are enriched correctly."""
        transform = PaymentEnrichmentTransform(
            name="test_enrichment",
            config={
                "timestamp_field": "timestamp",
                "timezone": "UTC",
                "add_fields": {
                    "processor": "test_processor",
                    "environment": "testing"
                }
            }
        )
        
        # Create test data
        test_data = [
            {
                "id": "1", 
                "amount": "100.0", 
                "currency": "USD",
                "timestamp": "2023-01-15T12:30:45Z"
            }
        ]
        
        # Run the transform
        with TestPipeline() as p:
            inputs = p | beam.Create(test_data)
            outputs = inputs | transform.build_transform()
            
            # Define a custom matcher to handle dynamic fields
            def matcher(output: List[Dict[str, Any]]) -> None:
                self.assertEqual(len(output), 1)
                result = output[0]
                
                # Check original fields are preserved
                self.assertEqual(result["id"], "1")
                self.assertEqual(result["amount"], "100.0")
                self.assertEqual(result["currency"], "USD")
                self.assertEqual(result["timestamp"], "2023-01-15T12:30:45Z")
                
                # Check added fields
                self.assertEqual(result["processor"], "test_processor")
                self.assertEqual(result["environment"], "testing")
                
                # Check extracted date components
                self.assertEqual(result["payment_year"], 2023)
                self.assertEqual(result["payment_month"], 1)
                self.assertEqual(result["payment_day"], 15)
                self.assertEqual(result["payment_hour"], 12)
                
                # Check dynamic fields
                self.assertIn("transaction_id", result)
                self.assertIn("processing_timestamp", result)
            
            assert_that(outputs, matcher)


class TestCurrencyConversionTransform(unittest.TestCase):
    """Tests for CurrencyConversionTransform."""
    
    def test_currency_conversion(self) -> None:
        """Test that currency conversion works correctly."""
        transform = CurrencyConversionTransform(
            name="test_conversion",
            config={
                "amount_field": "amount",
                "source_currency_field": "currency",
                "target_currency": "USD",
                "base_currency": "USD",
                "exchange_rates": {
                    "EUR": 1.1,
                    "GBP": 1.3,
                    "JPY": 0.0091
                }
            }
        )
        
        # Create test data
        test_data = [
            {"id": "1", "amount": "100.0", "currency": "USD"},  # No conversion needed
            {"id": "2", "amount": "100.0", "currency": "EUR"},  # EUR to USD
            {"id": "3", "amount": "100.0", "currency": "GBP"},  # GBP to USD
            {"id": "4", "amount": "10000", "currency": "JPY"},  # JPY to USD
            {"id": "5", "amount": "100.0", "currency": "CAD"},  # Unknown currency
            {"id": "6", "amount": "invalid", "currency": "USD"} # Invalid amount
        ]
        
        # Run the transform
        with TestPipeline() as p:
            inputs = p | beam.Create(test_data)
            outputs = inputs | transform.build_transform()
            
            # Define expected outputs
            expected_outputs = [
                {
                    "id": "1", "amount": "100.0", "currency": "USD",
                    "original_amount": 100.0, "original_currency": "USD",
                    "converted_amount": 100.0, "converted_currency": "USD"
                },
                {
                    "id": "2", "amount": "100.0", "currency": "EUR",
                    "original_amount": 100.0, "original_currency": "EUR",
                    "converted_amount": 90.91, "converted_currency": "USD"
                },
                {
                    "id": "3", "amount": "100.0", "currency": "GBP",
                    "original_amount": 100.0, "original_currency": "GBP",
                    "converted_amount": 76.92, "converted_currency": "USD"
                },
                {
                    "id": "4", "amount": "10000", "currency": "JPY",
                    "original_amount": 10000.0, "original_currency": "JPY",
                    "converted_amount": 1098.9, "converted_currency": "USD"
                },
                {
                    "id": "5", "amount": "100.0", "currency": "CAD",
                    "conversion_error": "Unknown source currency: CAD"
                },
                {
                    "id": "6", "amount": "invalid", "currency": "USD",
                    "conversion_error": "Invalid amount format: invalid"
                }
            ]
            
            assert_that(outputs, equal_to(expected_outputs))


class TestPaymentAggregationTransform(unittest.TestCase):
    """Tests for PaymentAggregationTransform."""
    
    def test_payment_aggregation(self) -> None:
        """Test that payments are aggregated correctly."""
        transform = PaymentAggregationTransform(
            name="test_aggregation",
            config={
                "group_by_fields": ["currency", "payment_method"],
                "amount_field": "amount",
                "count_field": "count",
                "sum_field": "total",
                "avg_field": "average",
                "min_field": "min",
                "max_field": "max"
            }
        )
        
        # Create test data
        test_data = [
            {"id": "1", "amount": "10.0", "currency": "USD", "payment_method": "credit_card"},
            {"id": "2", "amount": "20.0", "currency": "USD", "payment_method": "credit_card"},
            {"id": "3", "amount": "30.0", "currency": "USD", "payment_method": "paypal"},
            {"id": "4", "amount": "40.0", "currency": "EUR", "payment_method": "credit_card"},
            {"id": "5", "amount": "50.0", "currency": "EUR", "payment_method": "credit_card"},
            {"id": "6", "amount": "invalid", "currency": "USD", "payment_method": "credit_card"}
        ]
        
        # Run the transform
        with TestPipeline() as p:
            inputs = p | beam.Create(test_data)
            outputs = inputs | transform.build_transform()
            
            # Define expected outputs
            expected_outputs = [
                {
                    "currency": "USD", 
                    "payment_method": "credit_card",
                    "count": 2,
                    "total": 30.0,
                    "average": 15.0,
                    "min": 10.0,
                    "max": 20.0
                },
                {
                    "currency": "USD", 
                    "payment_method": "paypal",
                    "count": 1,
                    "total": 30.0,
                    "average": 30.0,
                    "min": 30.0,
                    "max": 30.0
                },
                {
                    "currency": "EUR", 
                    "payment_method": "credit_card",
                    "count": 2,
                    "total": 90.0,
                    "average": 45.0,
                    "min": 40.0,
                    "max": 50.0
                }
            ]
            
            # Sort the results for consistent comparison
            def sort_key(item: Dict[str, Any]) -> tuple:
                return (item["currency"], item["payment_method"])
            
            assert_that(
                outputs,
                equal_to(sorted(expected_outputs, key=sort_key)),
                label="CheckAggregation"
            )


class TestFraudDetectionTransform(unittest.TestCase):
    """Tests for FraudDetectionTransform."""
    
    def test_fraud_detection(self) -> None:
        """Test that fraud detection works correctly."""
        transform = FraudDetectionTransform(
            name="test_fraud_detection",
            config={
                "amount_field": "amount",
                "timestamp_field": "timestamp",
                "ip_address_field": "ip_address",
                "card_number_field": "card_number",
                "velocity_threshold": 2,
                "velocity_window_minutes": 10
            }
        )
        
        # Create test data with various fraud indicators
        test_data = [
            # Low risk payment
            {
                "id": "1", 
                "user_id": "user_1",
                "amount": "123.45", 
                "timestamp": datetime.datetime.now().isoformat(),
                "ip_address": "203.0.113.1",
                "card_number": "************1234"
            },
            # Medium risk payment (round amount)
            {
                "id": "2", 
                "user_id": "user_2",
                "amount": "500.00", 
                "timestamp": datetime.datetime.now().isoformat(),
                "ip_address": "203.0.113.2",
                "card_number": "************5678"
            },
            # High risk payment (internal IP and invalid card)
            {
                "id": "3", 
                "user_id": "user_3",
                "amount": "999.99", 
                "timestamp": datetime.datetime.now().isoformat(),
                "ip_address": "192.168.1.1",
                "card_number": "0000********9012"
            },
            # Velocity check (same user as previous)
            {
                "id": "4", 
                "user_id": "user_3",
                "amount": "888.88", 
                "timestamp": datetime.datetime.now().isoformat(),
                "ip_address": "203.0.113.4",
                "card_number": "************3456"
            },
            # Another transaction for the same user (should trigger velocity)
            {
                "id": "5", 
                "user_id": "user_3",
                "amount": "777.77", 
                "timestamp": datetime.datetime.now().isoformat(),
                "ip_address": "203.0.113.5",
                "card_number": "************7890"
            }
        ]
        
        # Run the transform
        with TestPipeline() as p:
            inputs = p | beam.Create(test_data)
            outputs = inputs | transform.build_transform()
            
            # Check low risk payments
            assert_that(
                outputs.low_risk_payments,
                equal_to([
                    {
                        "id": "1", 
                        "user_id": "user_1",
                        "amount": "123.45", 
                        "timestamp": test_data[0]["timestamp"],
                        "ip_address": "203.0.113.1",
                        "card_number": "************1234",
                        "fraud_indicators": [],
                        "fraud_score": 0
                    }
                ]),
                label="CheckLowRiskPayments"
            )
            
            # Check medium risk payments (should include the round amount payment)
            def check_medium_risk(output: List[Dict[str, Any]]) -> None:
                self.assertEqual(len(output), 1)
                payment = output[0]
                self.assertEqual(payment["id"], "2")
                self.assertEqual(payment["user_id"], "user_2")
                self.assertIn("round_amount", payment["fraud_indicators"])
                self.assertEqual(payment["fraud_score"], 25)
            
            assert_that(
                outputs.medium_risk_payments,
                check_medium_risk,
                label="CheckMediumRiskPayments"
            )
            
            # Check high risk payments (should include the internal IP and velocity check payments)
            def check_high_risk(output: List[Dict[str, Any]]) -> None:
                self.assertEqual(len(output), 3)
                
                # Find the first high risk payment
                payment1 = next(p for p in output if p["id"] == "3")
                self.assertEqual(payment1["user_id"], "user_3")
                self.assertIn("internal_ip", payment1["fraud_indicators"])
                self.assertIn("invalid_card", payment1["fraud_indicators"])
                self.assertEqual(payment1["fraud_score"], 50)
                
                # Find the subsequent payments that should trigger velocity check
                payment2 = next(p for p in output if p["id"] == "4")
                self.assertEqual(payment2["user_id"], "user_3")
                
                payment3 = next(p for p in output if p["id"] == "5")
                self.assertEqual(payment3["user_id"], "user_3")
                self.assertIn("velocity_check", payment3["fraud_indicators"])
            
            assert_that(
                outputs.high_risk_payments,
                check_high_risk,
                label="CheckHighRiskPayments"
            )


if __name__ == "__main__":
    unittest.main()
