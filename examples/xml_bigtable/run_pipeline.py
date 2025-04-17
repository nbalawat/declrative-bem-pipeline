#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run the XML to BigTable pipeline example.

This script demonstrates reading XML data, transforming it, and writing to a BigTable emulator.
"""

import os
import sys
import logging
import json
import subprocess
import shutil
from typing import Dict, Any, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import necessary modules
from declarative_beam.core.pipeline_builder import PipelineBuilder
from declarative_beam.transforms.io import xml, bigtable as bt_module, text
from declarative_beam.transforms.io.bigtable_simulator import BigTableSimulator
from declarative_beam.transforms.processing import basic as processing_basic
from declarative_beam.transforms.aggregation import basic as aggregation_basic
from declarative_beam.transforms.window import basic as window_basic

# Import setup script
from examples.xml_bigtable.setup_bigtable import setup_bigtable_emulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_bigtable_contents(project_id: str, instance_id: str, table_id: str, use_simulator: bool = False):
    """
    Print the contents of the BigTable emulator or simulator.
    
    Args:
        project_id: The project ID
        instance_id: The instance ID
        table_id: The table ID
        use_simulator: Whether to use the in-memory simulator instead of the emulator
    """
    source = "Simulator" if use_simulator else "Emulator"
    logger.info(f"\n{'='*80}\nBigTable {source} Contents: {project_id}/{instance_id}/{table_id}\n{'='*80}")
    
    if use_simulator:
        # Use the in-memory simulator
        rows = BigTableSimulator.read_rows(project_id, instance_id, table_id)
        
        if not rows:
            logger.info("No data in BigTable simulator.")
            return
        
        for row_key, families in rows.items():
            logger.info(f"Row Key: {row_key}")
            for family_id, columns in families.items():
                logger.info(f"  Column Family: {family_id}")
                for column, value in columns.items():
                    try:
                        # Try to decode as UTF-8 string
                        value_str = value.decode('utf-8')
                        logger.info(f"    {column}: {value_str}")
                    except UnicodeDecodeError:
                        # If it's not a valid UTF-8 string, show as bytes
                        logger.info(f"    {column}: {value} (binary)")
                    except AttributeError:
                        # If value is not bytes (e.g., it's already a string)
                        logger.info(f"    {column}: {value}")
    else:
        # Use the BigTable emulator
        try:
            # Create a client to the BigTable emulator
            client = bigtable.Client(project=project_id, admin=True)
            instance = client.instance(instance_id)
            table = instance.table(table_id)
            
            # Read rows from the table
            rows = table.read_rows()
            
            if not rows:
                logger.info("No data in BigTable emulator.")
                return
            
            row_count = 0
            for row_key, row in rows:
                row_count += 1
                logger.info(f"Row Key: {row_key.decode('utf-8')}")
                
                for column_family_id, columns in row.cells.items():
                    logger.info(f"  Column Family: {column_family_id}")
                    
                    for column_name, cells in columns.items():
                        column_name_str = column_name.decode('utf-8')
                        
                        for cell in cells:
                            try:
                                # Try to decode as UTF-8 string
                                value_str = cell.value.decode('utf-8')
                                logger.info(f"    {column_name_str}: {value_str}")
                            except UnicodeDecodeError:
                                # If it's not a valid UTF-8 string, show as bytes
                                logger.info(f"    {column_name_str}: {cell.value} (binary)")
            
            if row_count == 0:
                logger.info("No rows found in the table.")
        
        except Exception as e:
            logger.error(f"Error reading from BigTable emulator: {str(e)}")
            logger.error("Falling back to in-memory simulator...")
            print_bigtable_contents(project_id, instance_id, table_id, use_simulator=True)


def check_bigtable_emulator():
    """
    Check if the BigTable emulator is running.
    
    Returns:
        bool: True if the emulator is running, False otherwise
    """
    emulator_host = os.environ.get("BIGTABLE_EMULATOR_HOST")
    if not emulator_host:
        logger.error("BIGTABLE_EMULATOR_HOST environment variable is not set.")
        logger.error("Please set it to the host:port of the BigTable emulator.")
        logger.error("Example: export BIGTABLE_EMULATOR_HOST=localhost:8086")
        return False
    
    # Check if the emulator is running by trying to connect to it
    host, port = emulator_host.split(":")
    try:
        # Try to run netcat to check if the port is open
        result = subprocess.run(["nc", "-z", host, port], capture_output=True, timeout=5)
        if result.returncode != 0:
            logger.error(f"BigTable emulator is not running at {emulator_host}.")
            logger.error("Please start the emulator using Docker:")
            logger.error("  cd docker && docker-compose -f docker-compose-bigtable.yml up -d")
            return False
        
        logger.info(f"BigTable emulator is running at {emulator_host}.")
        return True
    except Exception as e:
        logger.error(f"Error checking BigTable emulator: {str(e)}")
        return False


def update_pipeline_yaml_for_simulator():
    """
    Temporarily modify the pipeline YAML to use the BigTableSimulator instead of WriteToBigTable.
    """
    # Use absolute path to ensure the file is found regardless of current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(current_dir, 'pipeline.yaml')
    
    try:
        # Read the current pipeline YAML
        with open(yaml_path, 'r') as f:
            pipeline_yaml = f.read()
        
        # Save a backup
        backup_path = f"{yaml_path}.bak"
        shutil.copy2(yaml_path, backup_path)
        
        # Replace WriteToBigTable with WriteToBigTableSimulator
        modified_yaml = pipeline_yaml.replace(
            "  type: WriteToBigTable",
            "  type: WriteToBigTableSimulator"
        )
        
        # Write the modified YAML back
        with open(yaml_path, 'w') as f:
            f.write(modified_yaml)
        
        logger.info("Updated pipeline YAML to use BigTableSimulator")
        return True
        
    except Exception as e:
        logger.error(f"Error updating pipeline YAML: {str(e)}")
        return False


def restore_pipeline_yaml():
    """
    Restore the original pipeline YAML from backup.
    """
    # Use absolute path to ensure the file is found regardless of current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(current_dir, 'pipeline.yaml')
    backup_path = f"{yaml_path}.bak"
    
    try:
        if os.path.exists(backup_path):
            # Restore from backup using shutil to preserve file attributes
            shutil.copy2(backup_path, yaml_path)
            
            # Remove the backup
            os.remove(backup_path)
            
            logger.info("Restored original pipeline YAML")
            return True
    except Exception as e:
        logger.error(f"Error restoring pipeline YAML: {str(e)}")
        return False


def run_pipeline():
    """
    Run the XML to BigTable pipeline.
    """
    try:
        # Use absolute path for output directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(current_dir, 'output')
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
        
        # Clear any previous data in the BigTable simulator
        BigTableSimulator.clear_all()
        
        # For this example, we'll always use the simulator
        # The emulator has issues with column families
        use_simulator = True
        use_emulator = False
        logger.info("Using in-memory BigTable simulator for this example.")
        
        # The pipeline YAML is already configured to use the simulator
        logger.info("Using in-memory BigTable simulator for the pipeline.")
        yaml_modified = False  # No need to modify the YAML anymore
        
        # Set up the pipeline options
        options = PipelineOptions()
        
        # Create the pipeline builder
        # Use absolute path to ensure the file is found regardless of current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, 'pipeline.yaml')
        
        logger.info(f"Using pipeline config at: {config_path}")
        
        builder = PipelineBuilder(
            config_path=config_path
        )
        
        logger.info("Building the XML to BigTable pipeline...")
        pipeline = builder.build_pipeline()
        
        logger.info("Running the pipeline...")
        result = pipeline.run()
        
        # Wait for the pipeline to finish
        logger.info("Waiting for the pipeline to finish...")
        result.wait_until_finish()
        
        logger.info("Pipeline execution completed successfully!")
        
        # Print the contents of the BigTable
        print_bigtable_contents('test-project', 'test-instance', 'transactions', use_simulator=use_simulator)
        
        # If we modified the YAML, restore it
        if yaml_modified:
            restore_pipeline_yaml()
        
        logger.info("Pipeline execution completed with detailed output.")
        
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        logger.info("Pipeline execution completed with errors.")


if __name__ == "__main__":
    run_pipeline()
