"""
Test transform functions for the declarative beam pipeline framework.

This module provides transform functions for testing the pipeline.
"""

from typing import Any, Dict, Iterable, List, Tuple, Union

import apache_beam as beam


def parse_csv_line(delimiter: str = ",", fields: List[str] = None) -> callable:
    """Parse a CSV line into a dictionary.
    
    Args:
        delimiter: The delimiter to use for splitting the line.
        fields: The field names to use for the dictionary keys.
        
    Returns:
        A DoFn class that parses a CSV line into a dictionary.
    """
    if fields is None:
        fields = []
    
    class ParseCSVDoFn(beam.DoFn):
        def process(self, line: str) -> Iterable[Dict[str, str]]:
            values = line.strip().split(delimiter)
            result = {}
            
            for i, field in enumerate(fields):
                if i < len(values):
                    result[field] = values[i]
                else:
                    result[field] = ""
            
            yield result
    
    return ParseCSVDoFn


def filter_by_value(field: str, min_value: float = 0.0) -> callable:
    """Filter elements by a numeric value in a field.
    
    Args:
        field: The field to check.
        min_value: The minimum value to accept.
        
    Returns:
        A function that returns True if the element passes the filter.
    """
    def _filter(element: Dict[str, Any]) -> bool:
        try:
            value = float(element.get(field, 0))
            return value >= min_value
        except (ValueError, TypeError):
            return False
    
    return _filter


def extract_key_value(key_field: str) -> callable:
    """Extract a key-value pair from a dictionary.
    
    Args:
        key_field: The field to use as the key.
        
    Returns:
        A function that extracts a key-value pair from a dictionary.
    """
    def _extract(element: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        key = element.get(key_field, "")
        return key, element
    
    return _extract


def sum_values() -> callable:
    """Create a combiner function that sums values.
    
    Returns:
        A function that combines values by summing them.
    """
    def _sum(values: Iterable[float]) -> float:
        return sum(values)
    
    return _sum


def format_result(include_key: bool = True) -> callable:
    """Format a result for output.
    
    Args:
        include_key: Whether to include the key in the output.
        
    Returns:
        A function that formats a result for output.
    """
    def _format(element: Union[Dict[str, Any], Tuple[str, Any]]) -> str:
        if include_key and isinstance(element, tuple):
            key, values = element
            
            # Handle GroupByKey output which is (key, iterable of values)
            if isinstance(values, Iterable) and not isinstance(values, (str, dict)):
                values_list = list(values)
                return f"{key}: {values_list}"
            else:
                return f"{key}: {values}"
        elif isinstance(element, dict):
            return str(element)
        else:
            return str(element)
    
    return _format


class MultiOutputDoFn(beam.DoFn):
    """A DoFn that produces multiple outputs based on a condition.
    
    This DoFn splits input elements into multiple outputs based on a field value.
    """
    
    def __init__(self, field: str, threshold: float = 0.0):
        """Initialize the DoFn.
        
        Args:
            field: The field to check.
            threshold: The threshold value for splitting.
        """
        self.field = field
        self.threshold = threshold
    
    def process(self, element: Dict[str, Any]) -> Iterable[beam.pvalue.TaggedOutput]:
        """Process an element and emit to multiple outputs.
        
        Args:
            element: The element to process.
            
        Yields:
            Tagged outputs for different output collections.
        """
        try:
            value = float(element.get(self.field, 0))
            if value < self.threshold:
                yield beam.pvalue.TaggedOutput("below_threshold", element)
            else:
                yield beam.pvalue.TaggedOutput("above_threshold", element)
        except (ValueError, TypeError):
            yield beam.pvalue.TaggedOutput("invalid", element)


def create_multi_output_dofn(field: str, threshold: float = 0.0) -> MultiOutputDoFn:
    """Create a MultiOutputDoFn instance.
    
    Args:
        field: The field to check.
        threshold: The threshold value for splitting.
        
    Returns:
        A MultiOutputDoFn instance.
    """
    return MultiOutputDoFn(field, threshold)


class EnrichDoFn(beam.DoFn):
    """A DoFn that enriches elements with data from a side input.
    
    This DoFn adds information from a side input dictionary to each element.
    """
    
    def __init__(self, join_field: str, target_field: str):
        """Initialize the DoFn.
        
        Args:
            join_field: The field to use for joining with the side input.
            target_field: The field to add to the element.
        """
        self.join_field = join_field
        self.target_field = target_field
    
    def process(self, element: Dict[str, Any], reference_dict: Dict[str, Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Process an element with a side input.
        
        Args:
            element: The element to process.
            reference_dict: The side input dictionary (named to match the YAML configuration).
            
        Yields:
            Enriched elements.
        """
        join_value = element.get(self.join_field)
        if join_value and join_value in reference_dict:
            # Add the target field from the side input
            element[self.target_field] = reference_dict[join_value].get(self.target_field, "")
        else:
            # Add an empty value if no match
            element[self.target_field] = ""
        
        yield element


def create_enrich_dofn(join_field: str, target_field: str) -> EnrichDoFn:
    """Create an EnrichDoFn instance.
    
    Args:
        join_field: The field to use for joining with the side input.
        target_field: The field to add to the element.
        
    Returns:
        An EnrichDoFn instance.
    """
    return EnrichDoFn(join_field, target_field)
