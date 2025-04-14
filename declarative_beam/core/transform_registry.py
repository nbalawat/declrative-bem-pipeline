"""
Transform registry for the declarative beam pipeline framework.

This module provides a registry for transforms that can be used in the pipeline.
"""

import importlib
import importlib.util
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Type

logger = logging.getLogger(__name__)


class TransformRegistry:
    """Registry for all transforms available to the pipeline builder."""

    _registry: Dict[str, Type] = {}
    _plugin_directories: List[str] = []

    @classmethod
    def register(cls, name: str) -> Callable:
        """Decorator to register a transform implementation.

        Args:
            name: The name of the transform to register.

        Returns:
            A decorator function that registers the transform class.
        """

        def decorator(transform_class: Type) -> Type:
            cls._registry[name] = transform_class
            return transform_class

        return decorator

    @classmethod
    def get_transform(cls, name: str) -> Type:
        """Get a transform class by name.

        Args:
            name: The name of the transform to get.

        Returns:
            The transform class.

        Raises:
            ValueError: If the transform is not found in the registry.
        """
        if name not in cls._registry:
            raise ValueError(f"Transform '{name}' not found in registry")
        return cls._registry[name]

    @classmethod
    def register_plugin_directory(cls, directory: str) -> None:
        """Register a directory containing custom transform plugins.

        All Python modules in this directory will be imported.

        Args:
            directory: The directory containing the plugins.
        """
        cls._plugin_directories.append(directory)

        # Import all Python modules in the directory
        for filename in os.listdir(directory):
            if filename.endswith(".py") and not filename.startswith("_"):
                module_name = filename[:-3]
                module_path = os.path.join(directory, filename)

                # Import the module
                try:
                    spec = importlib.util.spec_from_file_location(module_name, module_path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        logger.info(f"Loaded plugin module: {module_name}")
                except Exception as e:
                    logger.error(f"Error loading plugin module {module_name}: {e}")

    @classmethod
    def list_transforms(cls) -> List[str]:
        """List all registered transforms.

        Returns:
            A sorted list of transform names.
        """
        return sorted(cls._registry.keys())

    @classmethod
    def get_transform_info(cls, name: str) -> Dict[str, Any]:
        """Get information about a transform.

        Args:
            name: The name of the transform.

        Returns:
            A dictionary containing information about the transform.

        Raises:
            ValueError: If the transform is not found in the registry.
        """
        if name not in cls._registry:
            raise ValueError(f"Transform '{name}' not found in registry")

        transform_class = cls._registry[name]
        return {
            "name": name,
            "description": transform_class.__doc__ or "No description available",
            "module": transform_class.__module__,
            "parameters": getattr(transform_class, "PARAMETERS", {}),
        }
