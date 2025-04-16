"""
Utility functions for the custom transforms examples.
"""
from typing import Dict, Any, Optional


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
