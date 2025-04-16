"""
Utility functions for the custom transforms examples.
"""
from typing import Dict, List, Tuple, Any, Optional, Iterable
import statistics
import apache_beam as beam


def parse_csv_line():
    """
    Returns a function that parses a CSV line into a dictionary.
    
    Returns:
        A function that takes a CSV line string and returns a dictionary with the CSV fields
    """
    def _parse_line(line: str) -> Dict[str, Any]:
        # Skip header line
        if line.startswith('id,name,value,category'):
            return {}
        
        # Split the line by comma
        parts = line.split(',')
        
        # Ensure we have the expected number of parts
        if len(parts) < 4:
            return {'error': 'Invalid CSV format'}
        
        # Create a dictionary from the parts
        return {
            'id': parts[0],
            'name': parts[1],
            'value': parts[2],
            'category': parts[3]
        }
    
    return _parse_line


def extract_key_value():
    """
    Returns a function that extracts category and value from an element.
    
    Returns:
        A function that takes an element and returns a (category, value) tuple
    """
    def _extract(element: Dict[str, Any]) -> Tuple[str, float]:
        # Skip empty elements
        if not element:
            return ('unknown', 0)
        
        category = element.get('category', 'unknown')
        try:
            value = float(element.get('value', 0))
            return (category, value)
        except (ValueError, TypeError):
            return (category, 0)
    
    return _extract


def calculate_stats():
    """
    Returns a function that calculates statistics on values.
    
    Returns:
        A callable that takes an iterable of values and returns statistics
    """
    def _combine_stats(values: Iterable[Any]) -> Dict[str, Any]:
        # Filter out non-numeric values and convert to float
        numeric_values = []
        for v in values:
            if isinstance(v, (int, float)):
                numeric_values.append(float(v))
            elif isinstance(v, str) and v.replace('.', '', 1).isdigit():
                numeric_values.append(float(v))
        
        if not numeric_values:
            return {
                'count': 0,
                'sum': 0,
                'mean': 0,
                'min': 0,
                'max': 0
            }
        
        return {
            'count': len(numeric_values),
            'sum': sum(numeric_values),
            'mean': statistics.mean(numeric_values),
            'min': min(numeric_values),
            'max': max(numeric_values)
        }
    
    return _combine_stats


def add_processing_info():
    """
    Returns a function that adds processing information to elements.
    
    Returns:
        A function that takes an element and adds processing info
    """
    def _add_info(element: Dict[str, Any]) -> Dict[str, Any]:
        if not element:
            return element
        
        result = dict(element)
        result['processed'] = True
        result['processing_info'] = f"Processed {result.get('name', 'unknown')} with value {result.get('value', 'unknown')}"
        return result
    
    return _add_info


def enrich_with_stats():
    """
    Returns a DoFn that enriches elements with statistics from a side input.
    
    Returns:
        A DoFn that takes elements and a side input of statistics
    """
    class EnrichDoFn(beam.DoFn):
        def process(self, element, stats_dict):
            if not element:
                yield element
                return
            
            category = element.get('category', 'unknown')
            stats = stats_dict.get(category, {})
            
            result = dict(element)
            result['category_stats'] = stats
            result['category_avg'] = stats.get('mean', 0)
            result['value_vs_avg'] = 'above' if float(result.get('value', 0)) > stats.get('mean', 0) else 'below'
            
            yield result
    
    return EnrichDoFn()
