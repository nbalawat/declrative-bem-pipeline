"""
Window transforms for the declarative beam pipeline framework.

This module provides window transforms for working with streaming data.
"""

from typing import Any, Dict, List, Optional, Union, Callable, TypeVar, Iterable

import apache_beam as beam
from apache_beam.transforms.core import WindowInto, DoFn, ParDo
from apache_beam.transforms.window import FixedWindows, SlidingWindows, Sessions, GlobalWindows, TimestampedValue
import importlib

from declarative_beam.core.base_transform import BaseTransform, import_function
from declarative_beam.core.transform_registry import TransformRegistry

# Define a custom WithTimestamps transform since it's not directly available
T = TypeVar('T')

class WithTimestamps(beam.PTransform):
    """A PTransform that adds timestamps to elements in a PCollection.
    
    Each element in the output PCollection has the same value as the corresponding
    element in the input PCollection, but with a timestamp associated with it.
    """
    
    def __init__(self, timestamp_fn: Callable[[T], float]):
        """Initialize a WithTimestamps transform.
        
        Args:
            timestamp_fn: A function that returns the timestamp (in seconds since epoch)
                for a given element.
        """
        self._timestamp_fn = timestamp_fn
    
    def expand(self, pcoll):
        class AddTimestampDoFn(DoFn):
            def __init__(self, timestamp_fn):
                self._timestamp_fn = timestamp_fn
            
            def process(self, element):
                timestamp = self._timestamp_fn(element)
                yield TimestampedValue(element, timestamp)
        
        return pcoll | ParDo(AddTimestampDoFn(self._timestamp_fn))


@TransformRegistry.register("Window")
class WindowTransform(BaseTransform):
    """
    Apply a windowing strategy to a PCollection.
    
    Parameters:
        window_type: Type of window (fixed, sliding, session, global)
        window_size: Size of the window in seconds (for fixed and sliding windows)
        window_period: Period of the window in seconds (for sliding windows)
        gap_size: Gap duration in seconds (for session windows)
    """
    
    PARAMETERS = {
        'window_type': {
            'type': 'string',
            'description': 'Type of window (fixed, sliding, session, global)',
            'required': True
        },
        'window_size': {
            'type': 'number',
            'description': 'Size of the window in seconds (for fixed and sliding windows)',
            'required': False
        },
        'window_period': {
            'type': 'number',
            'description': 'Period of the window in seconds (for sliding windows)',
            'required': False
        },
        'gap_size': {
            'type': 'number',
            'description': 'Gap duration in seconds (for session windows)',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        window_type = self.config.get('window_type')
        
        if not window_type:
            raise ValueError(f"Transform '{self.name}' requires 'window_type'")
        
        window_fn = None
        
        if window_type.lower() == 'fixed':
            window_size = self.config.get('window_size')
            if window_size is None:
                raise ValueError(f"Transform '{self.name}' with window_type 'fixed' requires 'window_size'")
            window_fn = FixedWindows(window_size)
        
        elif window_type.lower() == 'sliding':
            window_size = self.config.get('window_size')
            window_period = self.config.get('window_period')
            if window_size is None or window_period is None:
                raise ValueError(
                    f"Transform '{self.name}' with window_type 'sliding' requires "
                    f"'window_size' and 'window_period'"
                )
            window_fn = SlidingWindows(window_size, window_period)
        
        elif window_type.lower() == 'session':
            gap_size = self.config.get('gap_size')
            if gap_size is None:
                raise ValueError(f"Transform '{self.name}' with window_type 'session' requires 'gap_size'")
            window_fn = Sessions(gap_size)
        
        elif window_type.lower() == 'global':
            window_fn = GlobalWindows()
        
        else:
            raise ValueError(f"Unknown window_type '{window_type}'")
        
        return WindowInto(window_fn)


@TransformRegistry.register("WithTimestamps")
class WithTimestampsTransform(BaseTransform):
    """
    Add timestamps to elements in a PCollection.
    
    Parameters:
        fn_module: Module containing the timestamp function
        fn_name: Name of the timestamp function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the timestamp function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the timestamp function',
            'required': True
        },
        'params': {
            'type': 'object',
            'description': 'Parameters to pass to the function',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        fn_module = self.config.get('fn_module')
        fn_name = self.config.get('fn_name')
        
        if not fn_module or not fn_name:
            raise ValueError(f"Transform '{self.name}' requires 'fn_module' and 'fn_name'")
        
        # Dynamically import the function
        timestamp_fn = import_function(fn_module, fn_name)
        
        # Handle any parameters to be passed to the function
        params = self.config.get('params', {})
        
        # Create a wrapper function that applies the parameters to the timestamp function
        def timestamp_wrapper(element):
            return timestamp_fn(element, **params)
        
        # Return a WithTimestamps transform with the wrapper function
        return WithTimestamps(timestamp_wrapper)


@TransformRegistry.register("GroupByWindow")
class GroupByWindowTransform(BaseTransform):
    """
    Group elements in a PCollection by window.
    
    This is typically used after a GroupByKey transform to group elements by window.
    """
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        return beam.GroupByKey()


@TransformRegistry.register("Rewindow")
class RewindowTransform(BaseTransform):
    """
    Rewindow elements in a PCollection.
    
    This is useful for changing the windowing strategy of a PCollection.
    
    Parameters:
        window_type: Type of window (fixed, sliding, session, global)
        window_size: Size of the window in seconds (for fixed and sliding windows)
        window_period: Period of the window in seconds (for sliding windows)
        gap_size: Gap duration in seconds (for session windows)
    """
    
    PARAMETERS = {
        'window_type': {
            'type': 'string',
            'description': 'Type of window (fixed, sliding, session, global)',
            'required': True
        },
        'window_size': {
            'type': 'number',
            'description': 'Size of the window in seconds (for fixed and sliding windows)',
            'required': False
        },
        'window_period': {
            'type': 'number',
            'description': 'Period of the window in seconds (for sliding windows)',
            'required': False
        },
        'gap_size': {
            'type': 'number',
            'description': 'Gap duration in seconds (for session windows)',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        # Reuse the WindowTransform implementation
        window_transform = WindowTransform(
            name=self.name,
            config=self.config,
            inputs=self.inputs,
            outputs=self.outputs,
            side_inputs=self.side_inputs,
        )
        return window_transform.build_transform(side_inputs)
