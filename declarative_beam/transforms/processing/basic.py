"""
Basic processing transforms for the declarative beam pipeline framework.

This module provides basic processing transforms like Map, FlatMap, Filter, etc.
"""

import importlib
from typing import Any, Dict, List, Optional, Union

import apache_beam as beam

from declarative_beam.core.base_transform import BaseTransform, import_function
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("Map")
class MapTransform(BaseTransform):
    """
    Apply a function to each element in the PCollection.
    
    Parameters:
        fn_module: Module containing the mapping function
        fn_name: Name of the mapping function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the mapping function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the mapping function',
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
        fn = import_function(fn_module, fn_name)
        
        # Handle any parameters to be passed to the function
        params = self.config.get('params', {})
        
        # Return a Map transform with the function
        return beam.Map(fn(**params))


@TransformRegistry.register("FlatMap")
class FlatMapTransform(BaseTransform):
    """
    Apply a function to each element in the PCollection and flatten the results.
    
    Parameters:
        fn_module: Module containing the mapping function
        fn_name: Name of the mapping function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the mapping function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the mapping function',
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
        fn = import_function(fn_module, fn_name)
        
        # Handle any parameters to be passed to the function
        params = self.config.get('params', {})
        
        # Return a FlatMap transform with the function
        return beam.FlatMap(fn(**params))


@TransformRegistry.register("Filter")
class FilterTransform(BaseTransform):
    """
    Filter elements from a PCollection based on a predicate function.
    
    Parameters:
        fn_module: Module containing the filter function
        fn_name: Name of the filter function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the filter function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the filter function',
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
        fn = import_function(fn_module, fn_name)
        
        # Handle any parameters to be passed to the function
        params = self.config.get('params', {})
        
        # Return a Filter transform with the function
        return beam.Filter(fn(**params))


@TransformRegistry.register("ParDo")
class ParDoTransform(BaseTransform):
    """
    Apply a DoFn to each element in the PCollection.
    
    Parameters:
        fn_module: Module containing the DoFn class or factory function
        fn_name: Name of the DoFn class or factory function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the DoFn class or factory function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the DoFn class or factory function',
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
        fn = import_function(fn_module, fn_name)
        
        # Handle any parameters to be passed to the function
        params = self.config.get('params', {})
        
        # Get the DoFn class or factory function
        dofn_class_or_factory = fn(**params)
        
        # Check if the result is a class (needs instantiation) or an instance
        if isinstance(dofn_class_or_factory, type):
            # It's a class, instantiate it
            dofn_instance = dofn_class_or_factory()
        else:
            # It's already an instance or a function
            dofn_instance = dofn_class_or_factory
        
        # Create the ParDo transform
        if side_inputs and self.side_inputs:
            side_input_pcols = [side_inputs.get(name) for name in self.side_inputs]
            return beam.ParDo(dofn_instance, *side_input_pcols)
        else:
            return beam.ParDo(dofn_instance)
