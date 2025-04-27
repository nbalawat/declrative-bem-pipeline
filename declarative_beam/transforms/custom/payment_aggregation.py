"""
Simplified payment aggregation transform for the declarative beam pipeline framework.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
import datetime

import apache_beam as beam
from apache_beam.transforms import CombinePerKey, Keys, Values

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("SimplePaymentAggregationTransform")
class SimplePaymentAggregationTransform(BaseTransform):
    """Aggregate payment data by user ID.
    
    Parameters:
        amount_field: Field containing the payment amount
    """
    
    PARAMETERS = {
        'amount_field': {
            'type': 'string',
            'description': 'Field containing the payment amount',
            'required': True,
            'default': 'amount'
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        amount_field = self.config.get('amount_field', 'amount')
        
        class ExtractUserAndAmount(beam.DoFn):
            def process(self, element, amount_field=amount_field):
                if not isinstance(element, dict):
                    return
                
                user_id = element.get('user_id', 'unknown')
                
                try:
                    # Try to get amount from converted_amount first, fall back to amount_field
                    if 'converted_amount' in element:
                        amount = float(element['converted_amount'])
                    elif amount_field in element:
                        amount = float(element[amount_field])
                    else:
                        return
                        
                    yield (user_id, amount)
                except (ValueError, TypeError):
                    # Skip elements with invalid amounts
                    return
        
        class FormatResults(beam.DoFn):
            def process(self, element):
                user_id, amounts = element
                
                # Calculate statistics
                total = sum(amounts)
                count = len(amounts)
                avg = total / count if count > 0 else 0
                min_val = min(amounts) if amounts else 0
                max_val = max(amounts) if amounts else 0
                
                # Create result dictionary
                result = {
                    'user_id': user_id,
                    'transaction_count': count,
                    'total_amount': total,
                    'avg_amount': avg,
                    'min_amount': min_val,
                    'max_amount': max_val
                }
                
                yield result
        
        return beam.PTransform(
            lambda pcoll: (
                pcoll
                | "ExtractUserAndAmount" >> beam.ParDo(ExtractUserAndAmount())
                | "GroupByUser" >> beam.GroupByKey()
                | "FormatResults" >> beam.ParDo(FormatResults())
            )
        )
