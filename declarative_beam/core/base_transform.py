"""
Base transform class for the declarative beam pipeline framework.

This module provides the base class for all transforms in the pipeline.
"""

import importlib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union

import apache_beam as beam
from apache_beam.pvalue import PCollection, PBegin, PDone

from declarative_beam.core.transform_registry import TransformRegistry


class BaseTransform(ABC):
    """Base class for all transforms in the pipeline.

    All transforms must inherit from this class and implement the build_transform method.
    """

    # Define parameters that this transform accepts
    PARAMETERS: Dict[str, Dict[str, Any]] = {}

    def __init__(
        self,
        name: str,
        config: Dict[str, Any],
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        side_inputs: Optional[List[str]] = None,
    ) -> None:
        """Initialize the transform.

        Args:
            name: The name of the transform.
            config: The configuration for the transform.
            inputs: The names of the inputs to the transform.
            outputs: The names of the outputs from the transform.
            side_inputs: The names of the side inputs to the transform.
        """
        self.name = name
        self.config = config
        self.inputs = inputs or []
        self.outputs = outputs or []
        self.side_inputs = side_inputs or []

    @abstractmethod
    def build_transform(
        self, side_inputs: Optional[Dict[str, Any]] = None
    ) -> beam.PTransform:
        """Build the Apache Beam PTransform for this transform.

        Args:
            side_inputs: A dictionary of side inputs, where the keys are the names
                of the side inputs and the values are the side input PCollections.

        Returns:
            An Apache Beam PTransform.
        """
        pass

    def get_input_dependencies(self) -> List[str]:
        """Get the input dependencies for this transform.

        Returns:
            A list of input names that this transform depends on.
        """
        return self.inputs

    def get_side_input_dependencies(self) -> List[str]:
        """Get the side input dependencies for this transform.

        Returns:
            A list of side input names that this transform depends on.
        """
        return self.side_inputs

    def get_output_names(self) -> List[str]:
        """Get the output names for this transform.

        Returns:
            A list of output names that this transform produces.
        """
        return self.outputs

    def get_output_tag_mapping(self) -> Dict[str, str]:
        """Get the mapping of output names to tags.

        This is used for transforms that produce multiple outputs.

        Returns:
            A dictionary mapping output names to tags.
        """
        return {output: output for output in self.outputs}

    def validate_config(self) -> None:
        """Validate the transform configuration.

        Raises:
            ValueError: If the configuration is invalid.
        """
        # Check required parameters
        for param_name, param_info in self.PARAMETERS.items():
            if param_info.get("required", False) and param_name not in self.config:
                raise ValueError(
                    f"Required parameter '{param_name}' missing for transform '{self.name}'"
                )


def import_function(module_name: str, function_name: str) -> Any:
    """Import a function from a module.

    Args:
        module_name: The name of the module.
        function_name: The name of the function.

    Returns:
        The imported function.

    Raises:
        ImportError: If the module or function cannot be imported.
    """
    try:
        module = importlib.import_module(module_name)
        return getattr(module, function_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(
            f"Could not import function '{function_name}' from module '{module_name}': {e}"
        )
