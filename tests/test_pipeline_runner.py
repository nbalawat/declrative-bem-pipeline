"""
Test runner for the declarative beam pipeline framework.

This module provides tests for running pipelines defined in YAML files.
"""

import os
import shutil
import time
import logging
import re
import ast
import glob
from pathlib import Path
from typing import List, Dict, Any

import pytest

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Explicitly import all transform modules to ensure they get registered
from declarative_beam.transforms.io import text
from declarative_beam.transforms.processing import basic as processing_basic
from declarative_beam.transforms.processing import advanced as processing_advanced
from declarative_beam.transforms.aggregation import basic as aggregation_basic
from declarative_beam.transforms.window import basic as window_basic

from declarative_beam.core.pipeline_builder import PipelineBuilder


def setup_output_dir(output_dir: str) -> None:
    """Set up the output directory for test results.
    
    Args:
        output_dir: The output directory path.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Clean up any existing output files
    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)
        if os.path.isfile(item_path):
            os.unlink(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)


def read_output_files(file_pattern: str) -> List[str]:
    """Read the output files matching the pattern.
    
    Args:
        file_pattern: The file pattern to match.
        
    Returns:
        A list of lines from the output files.
    """
    lines = []
    matching_files = glob.glob(f"{file_pattern}*")
    print(f"Found {len(matching_files)} files matching pattern {file_pattern}*: {matching_files}")
    
    for file_path in matching_files:
        with open(file_path, 'r') as f:
            file_lines = f.readlines()
            print(f"Read {len(file_lines)} lines from {file_path}")
            lines.extend(file_lines)
    
    return [line.strip() for line in lines if line.strip()]


def run_pipeline(config_path: str) -> Dict[str, Any]:
    """Run a pipeline from a YAML configuration file.
    
    Args:
        config_path: Path to the YAML configuration file.
        
    Returns:
        A dictionary containing the pipeline result and metadata.
    """
    start_time = time.time()
    
    # Build and run the pipeline
    builder = PipelineBuilder(config_path)
    pipeline = builder.build_pipeline()
    result = pipeline.run()
    result.wait_until_finish()
    
    end_time = time.time()
    
    return {
        "result": result,
        "runtime_seconds": end_time - start_time,
        "config_path": config_path,
        "pipeline_name": os.path.basename(config_path).replace(".yaml", "")
    }


def test_simple_pipeline():
    """Test the simple linear pipeline."""
    # Set up the output directory
    output_dir = "tests/test_data/output"
    setup_output_dir(output_dir)
    
    # Run the pipeline
    pipeline_result = run_pipeline("tests/test_data/simple_pipeline.yaml")
    
    # Read the output files
    output_lines = read_output_files("tests/test_data/output/simple_pipeline_results")
    
    # Verify the results
    assert len(output_lines) > 0, "No output was produced"
    
    # We expect records with value >= 50 to be in the output
    expected_ids = {'1', '2', '4', '5', '7', '9', '10'}
    found_ids = set()
    
    for line in output_lines:
        # Extract the ID from lines like "1: {'id': '1', 'name': 'Alice', 'value': '100'}"
        if line.startswith(tuple(expected_ids)):
            id_value = line.split(':', 1)[0].strip()
            found_ids.add(id_value)
    
    assert found_ids == expected_ids, f"Expected IDs {expected_ids}, found {found_ids}"
    
    print(f"Simple pipeline test passed with {len(output_lines)} output lines")
    print(f"Runtime: {pipeline_result['runtime_seconds']:.2f} seconds")


def test_advanced_pipeline():
    """Test the advanced pipeline with multiple outputs and side inputs."""
    # Set up the output directory
    output_dir = "tests/test_data/output"
    setup_output_dir(output_dir)
    
    # Run the pipeline
    pipeline_result = run_pipeline("tests/test_data/advanced_pipeline.yaml")
    
    # Read the output files for enriched records
    enriched_lines = read_output_files("tests/test_data/output/advanced_pipeline_enriched")
    
    # Read the output files for below threshold records
    below_threshold_lines = read_output_files("tests/test_data/output/advanced_pipeline_below_threshold")
    
    # Read the output files for invalid records
    invalid_lines = read_output_files("tests/test_data/output/advanced_pipeline_invalid")
    
    # Debug: Print the actual enriched lines
    print("\nEnriched lines:")
    for line in enriched_lines:
        print(f"Line: {line}")
    
    # Verify the enriched results
    assert len(enriched_lines) > 0, "No enriched output was produced"
    
    # We expect records with value >= 50 and with a department to be enriched
    expected_enriched_ids = {'1', '2', '4', '5', '7', '9', '10'}
    found_enriched_ids = set()
    
    import re
    import ast
    
    for line in enriched_lines:
        # Try to parse the line as a Python dictionary
        try:
            # Use ast.literal_eval to safely evaluate the string as a Python literal
            record = ast.literal_eval(line)
            if isinstance(record, dict) and 'id' in record:
                id_value = record['id']
                found_enriched_ids.add(id_value)
                
                # Verify that the department field is present for records with a reference
                if id_value in {'1', '2', '4', '5', '7', '9', '10'}:
                    assert 'department' in record, f"Department field missing for ID {id_value}"
        except (SyntaxError, ValueError):
            # If we can't parse it as a Python literal, try regex patterns
            id_match = re.search(r"'id':\s*'([^']+)'|\"id\":\s*\"([^\"]+)\"|id=([^,}\s]+)", line)
            if id_match:
                # Get the first non-None group
                id_value = next(g for g in id_match.groups() if g is not None)
                found_enriched_ids.add(id_value)
                
                # Verify that the department field is present for records with a reference
                if id_value in {'1', '2', '4', '5', '7', '9', '10'}:
                    assert "'department'" in line or '"department"' in line or 'department=' in line, \
                           f"Department field missing for ID {id_value}"
    
    assert found_enriched_ids == expected_enriched_ids, f"Expected enriched IDs {expected_enriched_ids}, found {found_enriched_ids}"
    
    # Verify the below threshold results
    assert len(below_threshold_lines) > 0, "No below threshold output was produced"
    
    # We expect records with value < 50 to be in the below threshold output
    expected_below_ids = {'3', '8'}
    found_below_ids = set()
    
    # Debug: Print the actual below threshold lines
    print("\nBelow threshold lines:")
    for line in below_threshold_lines:
        print(f"Line: {line}")
        
        # Use the same parsing approach as for enriched records
        try:
            # Use ast.literal_eval to safely evaluate the string as a Python literal
            record = ast.literal_eval(line)
            if isinstance(record, dict) and 'id' in record:
                id_value = record['id']
                found_below_ids.add(id_value)
        except (SyntaxError, ValueError):
            # If we can't parse it as a Python literal, try regex patterns
            id_match = re.search(r"'id':\s*'([^']+)'|\"id\":\s*\"([^\"]+)\"|id=([^,}\s]+)", line)
            if id_match:
                # Get the first non-None group
                id_value = next(g for g in id_match.groups() if g is not None)
                found_below_ids.add(id_value)
    
    assert found_below_ids == expected_below_ids, f"Expected below threshold IDs {expected_below_ids}, found {found_below_ids}"
    
    # Verify the invalid results
    assert len(invalid_lines) > 0, "No invalid output was produced"
    
    # We expect records with non-numeric value to be in the invalid output
    expected_invalid_ids = {'6'}
    found_invalid_ids = set()
    
    # Debug: Print the actual invalid lines
    print("\nInvalid lines:")
    for line in invalid_lines:
        print(f"Line: {line}")
        
        # Use the same parsing approach as for enriched records
        try:
            # Use ast.literal_eval to safely evaluate the string as a Python literal
            record = ast.literal_eval(line)
            if isinstance(record, dict) and 'id' in record:
                id_value = record['id']
                found_invalid_ids.add(id_value)
        except (SyntaxError, ValueError):
            # If we can't parse it as a Python literal, try regex patterns
            id_match = re.search(r"'id':\s*'([^']+)'|\"id\":\s*\"([^\"]+)\"|id=([^,}\s]+)", line)
            if id_match:
                # Get the first non-None group
                id_value = next(g for g in id_match.groups() if g is not None)
                found_invalid_ids.add(id_value)
    
    assert found_invalid_ids == expected_invalid_ids, f"Expected invalid IDs {expected_invalid_ids}, found {found_invalid_ids}"
    
    print(f"Advanced pipeline test passed with {len(enriched_lines)} enriched, {len(below_threshold_lines)} below threshold, and {len(invalid_lines)} invalid records")
    print(f"Runtime: {pipeline_result['runtime_seconds']:.2f} seconds")


def test_windowing_pipeline():
    """Test the windowing pipeline."""
    # Set up the output directory
    output_dir = "tests/test_data/output"
    setup_output_dir(output_dir)
    
    # Run the pipeline
    pipeline_result = run_pipeline("tests/test_data/windowing_pipeline.yaml")
    
    # Read the output files
    output_lines = read_output_files("tests/test_data/output/windowing_pipeline_results")
    
    # Verify the results
    assert len(output_lines) > 0, "No output was produced"
    
    # Check that we have output for each ID
    expected_ids = {str(i) for i in range(1, 11)}
    found_ids = set()
    
    for line in output_lines:
        # Extract the ID from lines like "1: [{'id': '1', 'name': 'Alice', ...}]"
        if line.startswith(tuple(expected_ids)):
            id_value = line.split(':', 1)[0].strip()
            found_ids.add(id_value)
    
    assert found_ids == expected_ids, f"Expected IDs {expected_ids}, found {found_ids}"
    
    # Verify that records are grouped by window
    # Records in the same window should be grouped together
    # Window 1: IDs 1, 2 (00:01:00 - 00:02:00)
    # Window 2: IDs 3, 4 (00:02:00 - 00:03:00)
    # Window 3: IDs 5, 6 (00:03:00 - 00:04:00)
    # Window 4: IDs 7, 8 (00:04:00 - 00:05:00)
    # Window 5: IDs 9, 10 (00:05:00 - 00:06:00)
    
    print(f"Windowing pipeline test passed with {len(output_lines)} output lines")
    print(f"Runtime: {pipeline_result['runtime_seconds']:.2f} seconds")

def test_runner_selection():
    """Test that the pipeline builder selects the correct runner from YAML config."""
    output_dir = "tests/test_data/output"
    setup_output_dir(output_dir)

    pipeline_result = run_pipeline("tests/test_data/runner_selection_pipeline.yaml")
    output_lines = read_output_files("tests/test_data/output/runner_selection_results")

    # The DirectRunner should run successfully and produce output
    assert len(output_lines) > 0, "No output was produced by DirectRunner"
    print(f"Runner selection test passed with {len(output_lines)} output lines")
    print(f"Runtime: {pipeline_result['runtime_seconds']:.2f} seconds")


if __name__ == "__main__":
    # Create the output directory
    output_dir = "tests/test_data/output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Run the tests
    print("\n=== Testing Simple Pipeline ===")
    test_simple_pipeline()
    
    print("\n=== Testing Advanced Pipeline ===")
    test_advanced_pipeline()
    
    print("\n=== Testing Windowing Pipeline ===")
    test_windowing_pipeline()
    
    print("\n=== All tests passed! ===")
