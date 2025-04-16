#!/usr/bin/env python
"""
Script to run the complete YAML pipeline with both custom transforms.
"""

import os
import sys
import logging
from typing import Dict, List, Any

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import required modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Import all transform modules to ensure they get registered
from declarative_beam.transforms.io import text
from declarative_beam.transforms.processing import basic as processing_basic
from declarative_beam.transforms.processing import advanced as processing_advanced
from declarative_beam.transforms.aggregation import basic as aggregation_basic
from declarative_beam.transforms.window import basic as window_basic
from declarative_beam.transforms.custom import basic as custom_basic

# Import local utils
import examples.custom_transforms.utils

# Import the pipeline builder
from declarative_beam.core.pipeline_builder import PipelineBuilder
from declarative_beam.core.transform_registry import TransformRegistry

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_yaml_pipeline():
    """Run the pipeline using the YAML configuration."""
    logger.info("Running YAML pipeline...")
    
    # Display registered transforms
    logger.info(f"Registered transforms: {TransformRegistry.list_transforms()}")
    
    # Ensure output directory exists
    os.makedirs('examples/custom_transforms/output', exist_ok=True)
    
    # Build and run the pipeline
    try:
        from declarative_beam.core.pipeline_builder import PipelineBuilder
        builder = PipelineBuilder('examples/custom_transforms/complete_pipeline.yaml')
        pipeline = builder.build_pipeline()
        result = pipeline.run()
        result.wait_until_finish()
        logger.info("YAML pipeline execution completed successfully.")
    except Exception as e:
        logger.error(f"Error running YAML pipeline: {e}")
        import traceback
        traceback.print_exc()

def print_results():
    """Print the results of the pipeline execution."""
    output_dir = 'examples/custom_transforms/output'
    
    print("\n=== YAML Pipeline Results ===\n")
    
    # Print categorized results
    categories = ['low', 'medium', 'high', 'invalid']
    for category in categories:
        print(f"\n{category.upper()} Values:")
        print("-" * 40)
        try:
            output_files = [f for f in os.listdir(output_dir) if f.startswith(f'yaml_{category}_values')]
            if output_files:
                for output_file in output_files:
                    file_path = os.path.join(output_dir, output_file)
                    if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                        with open(file_path, 'r') as f:
                            for line in f:
                                print(line.strip())
            else:
                print(f"No {category} values found.")
        except Exception as e:
            print(f"Error reading {category} values: {e}")
    
    print("\n=== End of Results ===\n")

def main():
    """Main function."""
    logger.info("Starting YAML pipeline execution...")
    try:
        run_yaml_pipeline()
        logger.info("YAML pipeline execution completed.")
    except Exception as e:
        logger.error(f"Error running YAML pipeline: {e}")
        import traceback
        traceback.print_exc()
    
    # Print results
    print_results()

if __name__ == "__main__":
    main()
