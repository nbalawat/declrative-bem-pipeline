"""
Test window transform functions for the declarative beam pipeline framework.

This module provides transform functions for testing windowing in the pipeline.
"""

import datetime
from typing import Any, Dict, Optional

import apache_beam as beam


def extract_timestamp(element: Dict[str, Any]) -> Optional[float]:
    """Extract a timestamp from an element.
    
    Args:
        element: The element to extract the timestamp from.
        
    Returns:
        The timestamp as a float (seconds since epoch), or None if not available.
    """
    timestamp_str = element.get('timestamp')
    if not timestamp_str:
        return None
    
    try:
        # Parse timestamp in format YYYY-MM-DD HH:MM:SS
        dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        # Convert to seconds since epoch
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def window_timestamp_key() -> callable:
    """Create a key function that extracts the window timestamp.
    
    Returns:
        A function that extracts the window timestamp from an element.
    """
    def _key(element: Any) -> float:
        # Extract the window timestamp from the element
        # This is used for testing window-based operations
        return beam.window.TimestampedValue.get_timestamp(element)
    
    return _key
