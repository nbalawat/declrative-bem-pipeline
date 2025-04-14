"""
Advanced processing transforms for the declarative beam pipeline framework.

This module provides advanced processing transforms like ParDoWithMultipleOutputs,
ParDoWithSideInputs, etc.
"""

import importlib
from typing import Any, Dict, List, Optional, Union, Tuple, Iterable

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.util import Keys, Values, KvSwap
from apache_beam.transforms.core import DoFn, PTransform

from declarative_beam.core.base_transform import BaseTransform, import_function
from declarative_beam.core.transform_registry import TransformRegistry


@TransformRegistry.register("ParDoWithMultipleOutputs")
class ParDoWithMultipleOutputsTransform(BaseTransform):
    """
    Apply a DoFn to each element in the PCollection with multiple outputs.
    
    Parameters:
        fn_module: Module containing the DoFn class or factory function
        fn_name: Name of the DoFn class or factory function
        params: Parameters to pass to the function
        output_tags: Mapping of output names to tags
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
        },
        'output_tags': {
            'type': 'object',
            'description': 'Mapping of output names to tags',
            'required': True
        }
    }
    
    def get_output_tag_mapping(self) -> Dict[str, str]:
        """Get the mapping of output names to tags.
        
        Returns:
            A dictionary mapping output names to tags.
        """
        output_tags = self.config.get('output_tags', {})
        return output_tags
    
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
        
        # Get output tags
        output_tags = self.config.get('output_tags', {})
        
        # Create the ParDo transform with multiple outputs
        if side_inputs and self.side_inputs:
            side_input_pcols = [side_inputs.get(name) for name in self.side_inputs]
            return beam.ParDo(dofn_instance, *side_input_pcols).with_outputs(*output_tags.values())
        else:
            return beam.ParDo(dofn_instance).with_outputs(*output_tags.values())


@TransformRegistry.register("ParDoWithSideInputs")
class ParDoWithSideInputsTransform(BaseTransform):
    """
    Apply a DoFn to each element in the PCollection with side inputs.
    
    Parameters:
        fn_module: Module containing the DoFn class or factory function
        fn_name: Name of the DoFn class or factory function
        params: Parameters to pass to the function
        side_inputs: List of side input names
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
        
        # Validate side inputs
        if not side_inputs or not self.side_inputs:
            raise ValueError(f"Transform '{self.name}' requires side inputs")
            
        # Create the ParDo transform with side inputs
        side_input_dict = {}
        for name in self.side_inputs:
            if name not in side_inputs:
                raise ValueError(
                    f"Transform '{self.name}' requires side input '{name}' "
                    f"which is not available"
                )
            # The side input is already a view transform (AsDict) from the pipeline builder
            side_input_dict[name] = side_inputs[name]
        
        # Use the side inputs directly without converting them again
        return beam.ParDo(dofn_instance, **side_input_dict)


@TransformRegistry.register("Flatten")
class FlattenTransform(BaseTransform):
    """
    Flatten multiple PCollections into a single PCollection.
    """
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        return beam.Flatten()


@TransformRegistry.register("CoGroupByKey")
class CoGroupByKeyTransform(BaseTransform):
    """
    Group by key across multiple PCollections.
    """
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        return beam.CoGroupByKey()


@TransformRegistry.register("Partition")
class PartitionTransform(BaseTransform):
    """
    Partition a PCollection into multiple PCollections.
    
    Parameters:
        fn_module: Module containing the partition function
        fn_name: Name of the partition function
        params: Parameters to pass to the function
        num_partitions: Number of partitions
    """
    
    PARAMETERS = {
        'fn_module': {
            'type': 'string',
            'description': 'Module containing the partition function',
            'required': True
        },
        'fn_name': {
            'type': 'string',
            'description': 'Name of the partition function',
            'required': True
        },
        'params': {
            'type': 'object',
            'description': 'Parameters to pass to the function',
            'required': False
        },
        'num_partitions': {
            'type': 'integer',
            'description': 'Number of partitions',
            'required': True
        }
    }
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        fn_module = self.config.get('fn_module')
        fn_name = self.config.get('fn_name')
        num_partitions = self.config.get('num_partitions')
        
        if not fn_module or not fn_name:
            raise ValueError(f"Transform '{self.name}' requires 'fn_module' and 'fn_name'")
        
        if num_partitions is None:
            raise ValueError(f"Transform '{self.name}' requires 'num_partitions'")
        
        # Dynamically import the function
        fn = import_function(fn_module, fn_name)
        
        # Handle any parameters to be passed to the function
        params = self.config.get('params', {})
        
        # Return a Partition transform with the function
        return beam.Partition(fn(**params), num_partitions)


@TransformRegistry.register("AsDict")
class AsDictTransform(BaseTransform):
    """
    Convert a PCollection of key-value pairs to a side input dictionary.
    
    Parameters:
        key_fn_module: Optional module containing the key function
        key_fn_name: Optional name of the key function
        key_params: Optional parameters to pass to the key function
    """
    
    PARAMETERS = {
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
        key_fn_module = self.config.get('key_fn_module')
        key_fn_name = self.config.get('key_fn_name')
        
        if key_fn_module and key_fn_name:
            # Dynamically import the key function
            key_fn = import_function(key_fn_module, key_fn_name)
            
            # Handle any parameters to be passed to the function
            key_params = self.config.get('key_params', {})
            
            # Create a Map transform with the key function
            key_function = key_fn(**key_params)
            return beam.Map(lambda kv: (key_function(kv[0]), kv[1]))
        else:
            # For side inputs, we need to return a Map transform that creates key-value pairs
            # The pipeline builder will handle creating the side input view
            return beam.Map(lambda kv: kv)
