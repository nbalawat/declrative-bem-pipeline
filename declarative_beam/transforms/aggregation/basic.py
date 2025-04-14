"""
Basic aggregation transforms for the declarative beam pipeline framework.

This module provides basic aggregation transforms like GroupByKey, Combine, etc.
"""

from typing import Any, Dict, List, Optional, Union

import apache_beam as beam

from declarative_beam.core.base_transform import BaseTransform, import_function
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("GroupByKey")
class GroupByKeyTransform(BaseTransform):
    """
    Group elements by key.
    
    This transform takes a PCollection of key-value pairs and returns a PCollection
    where each element consists of a key and an iterable of values.
    """
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        return beam.GroupByKey()


@TransformRegistry.register("Combine")
class CombineTransform(BaseTransform):
    """
    Combine elements using a combiner function.
    
    Parameters:
        fn_module: Module containing the combine function
        fn_name: Name of the combine function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the combine function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the combine function',
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
        
        # Return a Combine transform with the function
        return beam.Combine(fn(**params))


@TransformRegistry.register("CombinePerKey")
class CombinePerKeyTransform(BaseTransform):
    """
    Combine elements per key using a combiner function.
    
    Parameters:
        fn_module: Module containing the combine function
        fn_name: Name of the combine function
        params: Parameters to pass to the function
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the combine function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the combine function',
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
        
        # Return a CombinePerKey transform with the function
        return beam.CombinePerKey(fn(**params))


@TransformRegistry.register("Count")
class CountTransform(BaseTransform):
    """
    Count the number of elements in a PCollection.
    """
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        return beam.combiners.Count.Globally()


@TransformRegistry.register("CountPerKey")
class CountPerKeyTransform(BaseTransform):
    """
    Count the number of elements per key in a PCollection of key-value pairs.
    """
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        return beam.combiners.Count.PerKey()


@TransformRegistry.register("Top")
class TopTransform(BaseTransform):
    """
    Get the top N elements from a PCollection.
    
    Parameters:
        n: Number of top elements to return
        key: Optional key function to use for comparison
    """
    
    PARAMETERS = {
        'n': {
            'type': 'integer',
            'description': 'Number of top elements to return',
            'required': True
        },
        'key_fn_module': {
            'type': 'string',
            'description': 'Module containing the key function',
            'required': False
        },
        'key_fn_name': {
            'type': 'string',
            'description': 'Name of the key function',
            'required': False
        },
        'key_params': {
            'type': 'object',
            'description': 'Parameters to pass to the key function',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        n = self.config.get('n')
        
        if n is None:
            raise ValueError(f"Transform '{self.name}' requires 'n'")
        
        key_fn_module = self.config.get('key_fn_module')
        key_fn_name = self.config.get('key_fn_name')
        
        if key_fn_module and key_fn_name:
            # Dynamically import the key function
            key_fn = import_function(key_fn_module, key_fn_name)
            
            # Handle any parameters to be passed to the function
            key_params = self.config.get('key_params', {})
            
            # Return a Top transform with the key function
            return beam.combiners.Top.Of(n, key=key_fn(**key_params))
        else:
            # Return a Top transform without a key function
            return beam.combiners.Top.Of(n)


@TransformRegistry.register("TopPerKey")
class TopPerKeyTransform(BaseTransform):
    """
    Get the top N elements per key from a PCollection of key-value pairs.
    
    Parameters:
        n: Number of top elements to return per key
        key: Optional key function to use for comparison
    """
    
    PARAMETERS = {
        'n': {
            'type': 'integer',
            'description': 'Number of top elements to return per key',
            'required': True
        },
        'key_fn_module': {
            'type': 'string',
            'description': 'Module containing the key function',
            'required': False
        },
        'key_fn_name': {
            'type': 'string',
            'description': 'Name of the key function',
            'required': False
        },
        'key_params': {
            'type': 'object',
            'description': 'Parameters to pass to the key function',
            'required': False
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        n = self.config.get('n')
        
        if n is None:
            raise ValueError(f"Transform '{self.name}' requires 'n'")
        
        key_fn_module = self.config.get('key_fn_module')
        key_fn_name = self.config.get('key_fn_name')
        
        if key_fn_module and key_fn_name:
            # Dynamically import the key function
            key_fn = import_function(key_fn_module, key_fn_name)
            
            # Handle any parameters to be passed to the function
            key_params = self.config.get('key_params', {})
            
            # Return a TopPerKey transform with the key function
            return beam.combiners.Top.PerKey(n, key=key_fn(**key_params))
        else:
            # Return a TopPerKey transform without a key function
            return beam.combiners.Top.PerKey(n)
