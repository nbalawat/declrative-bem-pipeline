"""
YAML processor for the declarative beam pipeline framework.

This module provides functionality for processing YAML pipeline configurations.
"""

import os
import re
from typing import Any, Dict, Optional, Union

import yaml


def substitute_env_vars(value: Any) -> Any:
    """Substitute environment variables in a value.

    Supports the format ${VAR} or ${VAR:-default}.

    Args:
        value: The value to substitute environment variables in.

    Returns:
        The value with environment variables substituted.
    """
    if isinstance(value, str):
        # Match ${VAR} or ${VAR:-default}
        pattern = r"\${([^}^:]+)(?::-([^}]+))?}"

        def replace_env_var(match: re.Match) -> str:
            env_var = match.group(1)
            default = match.group(2)
            return os.environ.get(env_var, default if default is not None else "")

        return re.sub(pattern, replace_env_var, value)
    elif isinstance(value, dict):
        return {k: substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [substitute_env_vars(item) for item in value]
    else:
        return value


def load_yaml_config(config_path: str, substitute_env: bool = True) -> Dict[str, Any]:
    """Load a YAML configuration file.

    Args:
        config_path: The path to the YAML configuration file.
        substitute_env: Whether to substitute environment variables in the configuration.

    Returns:
        The loaded configuration as a dictionary.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        yaml.YAMLError: If the YAML file is invalid.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if substitute_env:
        config = substitute_env_vars(config)

    return config


def validate_transform_config(
    transform_config: Dict[str, Any], transform_name: str
) -> None:
    """Validate a transform configuration.

    Args:
        transform_config: The transform configuration to validate.
        transform_name: The name of the transform.

    Raises:
        ValueError: If the transform configuration is invalid.
    """
    # Check that the transform has a type
    if "type" not in transform_config:
        raise ValueError(f"Transform '{transform_name}' is missing a type")

    # Check that inputs and outputs are lists if present
    for field in ["inputs", "outputs", "side_inputs"]:
        if field in transform_config and not isinstance(transform_config[field], list):
            raise ValueError(
                f"Transform '{transform_name}' has an invalid {field} field: "
                f"expected a list, got {type(transform_config[field]).__name__}"
            )


def validate_pipeline_config(config: Dict[str, Any]) -> None:
    """Validate a pipeline configuration.

    Args:
        config: The pipeline configuration to validate.

    Raises:
        ValueError: If the pipeline configuration is invalid.
    """
    # Check that the configuration has a transforms section
    if "transforms" not in config:
        raise ValueError("Pipeline configuration is missing a 'transforms' section")

    # Check that transforms is a list
    if not isinstance(config["transforms"], list):
        raise ValueError(
            f"Pipeline configuration has an invalid 'transforms' section: "
            f"expected a list, got {type(config['transforms']).__name__}"
        )

    # Validate each transform
    for transform in config["transforms"]:
        if not isinstance(transform, dict):
            raise ValueError(
                f"Invalid transform in pipeline configuration: "
                f"expected a dictionary, got {type(transform).__name__}"
            )

        if "name" not in transform:
            raise ValueError("Transform is missing a name")

        validate_transform_config(transform, transform["name"])
