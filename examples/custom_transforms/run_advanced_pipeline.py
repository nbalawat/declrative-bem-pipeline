#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run the advanced pipeline example demonstrating branching, fan-out/fan-in, and side inputs.

This script loads the advanced_pipeline.yaml configuration and executes the pipeline,
providing detailed console output about the processing steps and results.
"""

import os
import sys
import logging
import json
from typing import Dict, Any, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Add the project root to the path so we can import the declarative_beam package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Explicitly import all transform modules to ensure they get registered
from declarative_beam.transforms.io import text
from declarative_beam.transforms.processing import basic as processing_basic
from declarative_beam.transforms.processing import advanced as processing_advanced
from declarative_beam.transforms.aggregation import basic as aggregation_basic
from declarative_beam.transforms.window import basic as window_basic
from declarative_beam.transforms.custom import basic as custom_basic

from declarative_beam.core.pipeline_builder import PipelineBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_file_contents(file_path: str, description: str) -> None:
    """
    Print the contents of a file with a description.
    
    Args:
        file_path: Path to the file to print
        description: Description of the file contents
    """
    try:
        if os.path.exists(file_path):
            logger.info(f"\n{'-'*80}\n{description}:\n{'-'*80}")
            with open(file_path, 'r') as f:
                for line in f:
                    # Try to parse as JSON for pretty printing
                    try:
                        data = json.loads(line.strip())
                        logger.info(json.dumps(data, indent=2))
                    except json.JSONDecodeError:
                        logger.info(line.strip())
        else:
            logger.warning(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")


def find_latest_output_files(output_dir: str) -> Dict[str, str]:
    """
    Find the latest output files in the beam-temp directories.
    
    Args:
        output_dir: Directory containing the output files
        
    Returns:
        Dictionary mapping output types to file paths
    """
    result = {}
    try:
        # Find all beam-temp directories
        for item in os.listdir(output_dir):
            item_path = os.path.join(output_dir, item)
            if os.path.isdir(item_path) and item.startswith('beam-temp-'):
                # Extract the output type from the directory name
                output_type = item.split('-')[2].split('-')[0]
                
                # Find the latest file in the directory
                latest_file = None
                latest_time = 0
                for subitem in os.listdir(item_path):
                    subitem_path = os.path.join(item_path, subitem)
                    if os.path.isfile(subitem_path):
                        file_time = os.path.getmtime(subitem_path)
                        if file_time > latest_time:
                            latest_time = file_time
                            latest_file = subitem_path
                
                if latest_file:
                    result[output_type] = latest_file
    except Exception as e:
        logger.error(f"Error finding output files: {str(e)}")
    
    return result


def run_advanced_pipeline() -> None:
    """
    Run the advanced pipeline example.
    """
    try:
        # Set up the pipeline options
        options = PipelineOptions()
        
        # Create the pipeline builder
        builder = PipelineBuilder(
            config_path='examples/custom_transforms/advanced_pipeline.yaml'
        )
        
        logger.info("Building the advanced pipeline...")
        pipeline = builder.build_pipeline()
        
        logger.info("Running the advanced pipeline...")
        result = pipeline.run()
        
        # Wait for the pipeline to finish
        logger.info("Waiting for the pipeline to finish...")
        result.wait_until_finish()
        
        logger.info("Pipeline execution completed successfully!")
        
        # Find and print the output files
        output_dir = 'examples/custom_transforms/output'
        output_files = find_latest_output_files(output_dir)
        
        # Print the contents of each output file
        for output_type, file_path in output_files.items():
            print_file_contents(file_path, f"{output_type.replace('_', ' ').title()} Output")
            
        logger.info("Advanced pipeline execution completed with detailed output.")
        
    except Exception as e:
        logger.error(f"Error running advanced pipeline: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        logger.info("Advanced pipeline execution completed with errors.")


if __name__ == "__main__":
    run_advanced_pipeline()
