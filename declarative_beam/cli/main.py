"""
Command-line interface for the declarative beam pipeline framework.

This module provides a command-line interface for running pipelines defined in YAML files.
"""

import argparse
import logging
import os
import sys
import time
from typing import Dict, List, Optional

# Explicitly import all transform modules to ensure they get registered
from declarative_beam.transforms.io import text
from declarative_beam.transforms.processing import basic as processing_basic
from declarative_beam.transforms.processing import advanced as processing_advanced
from declarative_beam.transforms.aggregation import basic as aggregation_basic
from declarative_beam.transforms.window import basic as window_basic

from declarative_beam.core.pipeline_builder import PipelineBuilder
from declarative_beam.core.transform_registry import TransformRegistry


def setup_logging(level: str = "INFO") -> None:
    """Set up logging configuration.
    
    Args:
        level: The logging level to use.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_pipeline(config_path: str, plugin_dir: Optional[str] = None) -> None:
    """Run a pipeline from a YAML configuration file.
    
    Args:
        config_path: Path to the YAML configuration file.
        plugin_dir: Optional directory containing custom transform plugins.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Register plugin directory if provided
    if plugin_dir:
        if not os.path.exists(plugin_dir):
            raise FileNotFoundError(f"Plugin directory not found: {plugin_dir}")
        TransformRegistry.register_plugin_directory(plugin_dir)
    
    # Build and run the pipeline
    start_time = time.time()
    logging.info(f"Building pipeline from {config_path}")
    
    builder = PipelineBuilder(config_path)
    pipeline = builder.build_pipeline()
    
    logging.info("Running pipeline")
    result = pipeline.run()
    result.wait_until_finish()
    
    end_time = time.time()
    runtime_seconds = end_time - start_time
    
    logging.info(f"Pipeline completed in {runtime_seconds:.2f} seconds")


def list_transforms() -> None:
    """List all available transforms."""
    transforms = TransformRegistry.list_transforms()
    
    print("Available transforms:")
    for transform in transforms:
        info = TransformRegistry.get_transform_info(transform)
        print(f"  - {transform}: {info['description'].split('.')[0]}")


def main() -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Declarative Beam Pipeline Framework")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("--config", required=True, help="Path to pipeline configuration")
    run_parser.add_argument("--plugin-dir", help="Directory with custom transform plugins")
    run_parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List available transforms")
    list_parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    if args.command == "run":
        setup_logging(args.log_level)
        try:
            run_pipeline(args.config, args.plugin_dir)
        except Exception as e:
            logging.error(f"Error running pipeline: {e}")
            sys.exit(1)
    
    elif args.command == "list":
        setup_logging(args.log_level)
        try:
            list_transforms()
        except Exception as e:
            logging.error(f"Error listing transforms: {e}")
            sys.exit(1)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
