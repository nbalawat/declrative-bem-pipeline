"""
Custom transforms for the declarative beam pipeline framework.

This module provides custom transforms for testing and demonstration purposes.
"""

from typing import Any, Dict, Optional

import apache_beam as beam

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("CustomMultiplyTransform")
class CustomMultiplyTransform(BaseTransform):
    """Multiply values in a field by a factor.
    
    Parameters:
        field: Field containing the value to multiply
        factor: Multiplication factor
    """
    
    PARAMETERS = {
        'field': {
            'type': 'string',
            'description': 'Field containing the value to multiply',
            'required': True
        },
        'factor': {
            'type': 'number',
            'description': 'Multiplication factor',
            'required': True
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        field = self.config.get('field')
        factor = self.config.get('factor')
        
        if not field or factor is None:
            raise ValueError(f"Transform '{self.name}' requires 'field' and 'factor'")
        
        return beam.Map(lambda x: self._multiply_field(x, field, factor))
    
    def _multiply_field(self, element: Dict[str, Any], field: str, factor: float) -> Dict[str, Any]:
        """Multiply the value in the specified field by the factor.
        
        Args:
            element: The input element (dictionary)
            field: The field to multiply
            factor: The multiplication factor
            
        Returns:
            The element with the multiplied field
        """
        if isinstance(element, dict) and field in element:
            try:
                result = dict(element)
                # Store the original value for testing
                result['original_value'] = result[field]
                result[field] = str(int(float(result[field])) * factor)
                return result
            except (ValueError, TypeError):
                return element
        return element


@TransformRegistry.register("CustomCategorizeTransform")
class CustomCategorizeTransform(BaseTransform):
    """Categorize elements based on a field value.
    
    Parameters:
        field: Field to use for categorization
        categories: Dictionary mapping category names to value ranges
    """
    
    PARAMETERS = {
        'field': {
            'type': 'string',
            'description': 'Field to use for categorization',
            'required': True
        },
        'categories': {
            'type': 'object',
            'description': 'Dictionary mapping category names to value ranges',
            'required': True
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        field = self.config.get('field')
        categories = self.config.get('categories', {})
        
        if not field or not categories:
            raise ValueError(f"Transform '{self.name}' requires 'field' and 'categories'")
        
        class CategorizeDoFn(beam.DoFn):
            def process(self, element, field=field, categories=categories):
                if not isinstance(element, dict) or field not in element:
                    yield beam.pvalue.TaggedOutput('invalid', element)
                    return
                
                try:
                    value = float(element[field])
                    for category, range_info in categories.items():
                        min_val = float(range_info.get('min', float('-inf')))
                        max_val = float(range_info.get('max', float('inf')))
                        
                        if min_val <= value < max_val:
                            element['category'] = category
                            yield beam.pvalue.TaggedOutput(category, element)
                            return
                    
                    # If no category matches
                    yield beam.pvalue.TaggedOutput('other', element)
                except (ValueError, TypeError):
                    yield beam.pvalue.TaggedOutput('invalid', element)
        
        return beam.ParDo(
            CategorizeDoFn(),
            *[category for category in categories.keys()] + ['other', 'invalid']
        )
