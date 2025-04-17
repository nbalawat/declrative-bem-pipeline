"""
BigTable Simulator for the declarative beam pipeline framework.

This module provides a simple in-memory simulator for BigTable to facilitate testing
without requiring a real BigTable instance or emulator.
"""

import logging
import json
from typing import Any, Dict, List, Optional

import apache_beam as beam

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry

logger = logging.getLogger(__name__)


# In-memory BigTable simulator for testing
class BigTableSimulator:
    """A simple in-memory simulator for Google Cloud BigTable."""
    
    _tables = {}
    
    @classmethod
    def clear_all(cls):
        """Clear all data from the simulator."""
        cls._tables = {}
    
    @classmethod
    def write_row(cls, project_id, instance_id, table_id, row_key, column_family_id, data):
        """Write a row to the simulator."""
        table_key = f"{project_id}/{instance_id}/{table_id}"
        if table_key not in cls._tables:
            cls._tables[table_key] = {}
        
        if row_key not in cls._tables[table_key]:
            cls._tables[table_key][row_key] = {}
            
        if column_family_id not in cls._tables[table_key][row_key]:
            cls._tables[table_key][row_key][column_family_id] = {}
            
        for column, value in data.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value).encode('utf-8')
            elif not isinstance(value, bytes):
                value = str(value).encode('utf-8')
            cls._tables[table_key][row_key][column_family_id][column] = value
    
    @classmethod
    def read_rows(cls, project_id, instance_id, table_id):
        """Read all rows from the simulator."""
        table_key = f"{project_id}/{instance_id}/{table_id}"
        return cls._tables.get(table_key, {})


class _BigTableSimulatorWriteDoFn(beam.DoFn):
    """
    A DoFn that writes elements to the in-memory BigTable simulator.
    """
    def __init__(self, project_id, instance_id, table_id, column_family_id, row_key_field, skip_invalid_records=False):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family_id = column_family_id
        self.row_key_field = row_key_field
        self.skip_invalid_records = skip_invalid_records
    
    def process(self, element: Dict[str, Any]):
        try:
            # Get the row key from the element
            if self.row_key_field not in element:
                if self.skip_invalid_records:
                    logger.warning(f"Skipping record without row key field: {self.row_key_field}")
                    return
                else:
                    raise ValueError(f"Row key field '{self.row_key_field}' not found in element")
            
            row_key = str(element[self.row_key_field])
            
            # Prepare data for the simulator
            data = {}
            for key, value in element.items():
                if key == self.row_key_field:
                    continue  # Skip the row key field
                
                data[key] = value
            
            # Write to the simulator
            BigTableSimulator.write_row(
                self.project_id,
                self.instance_id,
                self.table_id,
                row_key,
                self.column_family_id,
                data
            )
            
            # Return the element for downstream processing
            yield element
        
        except Exception as e:
            if self.skip_invalid_records:
                logger.warning(f"Error processing record: {str(e)}")
            else:
                raise


@TransformRegistry.register("WriteToBigTableSimulator")
class WriteToBigTableSimulator(BaseTransform):
    """
    Write data to the in-memory BigTable simulator for testing.
    
    Args:
        project_id: The project ID
        instance_id: The instance ID
        table_id: The table ID
        column_family_id: The column family ID
        row_key_field: The field to use as the row key
        skip_invalid_records: Whether to skip invalid records
    """
    PARAMETERS = {
        "project_id": {
            "type": "string",
            "description": "The project ID",
            "required": True,
        },
        "instance_id": {
            "type": "string",
            "description": "The instance ID",
            "required": True,
        },
        "table_id": {
            "type": "string",
            "description": "The table ID",
            "required": True,
        },
        "column_family_id": {
            "type": "string",
            "description": "The column family ID",
            "required": True,
        },
        "row_key_field": {
            "type": "string",
            "description": "The field to use as the row key",
            "required": True,
        },
        "batch_size": {
            "type": "integer",
            "description": "The batch size for writing to BigTable",
            "required": False,
            "default": 1000,
        },
        "max_retries": {
            "type": "integer",
            "description": "The maximum number of retries for writing to BigTable",
            "required": False,
            "default": 3,
        },
        "skip_invalid_records": {
            "type": "boolean",
            "description": "Whether to skip invalid records",
            "required": False,
            "default": False,
        },
    }
    
    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)
        # These will be set in build_transform from self.config
    
    def build_transform(self, side_inputs: Optional[Dict[str, Any]] = None) -> beam.PTransform:
        """
        Build the Apache Beam PTransform for writing to the BigTable simulator.
        
        Args:
            side_inputs: Optional side inputs for the transform
            
        Returns:
            A PTransform that writes to the BigTable simulator
        """
        project_id = self.config.get("project_id")
        instance_id = self.config.get("instance_id")
        table_id = self.config.get("table_id")
        column_family_id = self.config.get("column_family_id")
        row_key_field = self.config.get("row_key_field")
        skip_invalid_records = self.config.get("skip_invalid_records", False)
        
        # Create a custom PTransform that uses the DoFn
        class BigTableSimulatorWriteTransform(beam.PTransform):
            def expand(self, pcoll):
                return pcoll | beam.ParDo(_BigTableSimulatorWriteDoFn(
                    project_id=project_id,
                    instance_id=instance_id,
                    table_id=table_id,
                    column_family_id=column_family_id,
                    row_key_field=row_key_field,
                    skip_invalid_records=skip_invalid_records
                ))
        
        return BigTableSimulatorWriteTransform()
    
    def expand(self, pcoll):
        return pcoll | beam.ParDo(_BigTableSimulatorWriteDoFn(
            project_id=self.project_id,
            instance_id=self.instance_id,
            table_id=self.table_id,
            column_family_id=self.column_family_id,
            row_key_field=self.row_key_field,
            skip_invalid_records=self.skip_invalid_records
        ))
