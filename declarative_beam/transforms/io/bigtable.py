"""
BigTable I/O transforms for the declarative beam pipeline framework.

This module provides transforms for writing to Google Cloud BigTable.
"""

import logging
import uuid
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import apache_beam as beam
from google.cloud import bigtable
from google.cloud.bigtable import row
from google.cloud.bigtable.row import DirectRow
from google.api_core.exceptions import GoogleAPIError, DeadlineExceeded, ServiceUnavailable

from declarative_beam.core.base_transform import BaseTransform
from declarative_beam.core.transform_registry import TransformRegistry

logger = logging.getLogger(__name__)


@TransformRegistry.register("WriteToBigTable")
class WriteToBigTableTransform(BaseTransform):
    """
    Write to Google Cloud BigTable.

    Parameters:
        project_id: Google Cloud project ID.
        instance_id: BigTable instance ID.
        table_id: BigTable table ID.
        column_family_id: The column family to write data into.
        row_key_field: The field in the input dictionary to use as the row key.
        batch_size: Maximum number of mutations per batch.
        max_retries: Maximum number of retry attempts for transient errors.
        skip_invalid_records: If True, invalid records will be skipped instead of failing.
    """

    PARAMETERS = {
        "project_id": {
            "type": "string",
            "description": "Google Cloud project ID",
            "required": True,
        },
        "instance_id": {
            "type": "string",
            "description": "BigTable instance ID",
            "required": True,
        },
        "table_id": {
            "type": "string",
            "description": "BigTable table ID",
            "required": True,
        },
        "column_family_id": {
            "type": "string",
            "description": "The column family to write data into",
            "required": True,
        },
        "row_key_field": {
            "type": "string",
            "description": "The field in the input dictionary to use as the row key",
            "required": True,
        },
        "batch_size": {
            "type": "integer",
            "description": "Maximum number of mutations per batch",
            "required": False,
        },
        "max_retries": {
            "type": "integer",
            "description": "Maximum number of retry attempts for transient errors",
            "required": False,
        },
        "skip_invalid_records": {
            "type": "boolean",
            "description": "If True, invalid records will be skipped instead of failing",
            "required": False,
        },
    }

    def build_transform(
        self, side_inputs: Optional[Dict[str, Any]] = None
    ) -> beam.PTransform:
        """Build the Apache Beam PTransform for this transform.

        Args:
            side_inputs: A dictionary of side inputs (not used for this transform).

        Returns:
            An Apache Beam PTransform.
        """
        project_id = self.config.get("project_id")
        instance_id = self.config.get("instance_id")
        table_id = self.config.get("table_id")
        column_family_id = self.config.get("column_family_id")
        row_key_field = self.config.get("row_key_field")
        batch_size = self.config.get("batch_size", 1000)
        max_retries = self.config.get("max_retries", 3)
        skip_invalid_records = self.config.get("skip_invalid_records", False)

        return _BigTableWriteTransform(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            column_family_id=column_family_id,
            row_key_field=row_key_field,
            batch_size=batch_size,
            max_retries=max_retries,
            skip_invalid_records=skip_invalid_records,
        )


class _BigTableWriteTransform(beam.PTransform):
    """A PTransform for writing to BigTable."""

    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family_id: str,
        row_key_field: str,
        batch_size: int = 1000,
        max_retries: int = 3,
        skip_invalid_records: bool = False,
    ):
        super().__init__()
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family_id = column_family_id
        self.row_key_field = row_key_field
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.skip_invalid_records = skip_invalid_records

    def expand(self, pcoll):
        return (
            pcoll
            | "ConvertToBigTableRow" >> beam.Map(self._dict_to_direct_row)
            | "WriteToBigTable" >> _WriteToBigTableFn(
                project_id=self.project_id,
                instance_id=self.instance_id,
                table_id=self.table_id,
                batch_size=self.batch_size,
                max_retries=self.max_retries,
                skip_invalid_records=self.skip_invalid_records,
            )
        )

    def _dict_to_direct_row(self, element: Dict[str, Any]) -> DirectRow:
        """Convert a dictionary to a BigTable DirectRow."""
        row_key = element.get(self.row_key_field)

        if row_key is None:
            logger.warning(f"Skipping row due to missing key '{self.row_key_field}': {element}")
            beam.metrics.Metrics.counter("BigTableWrite", "missing_row_key").inc()
            raise ValueError(f"Missing row key '{self.row_key_field}' in element: {element}")

        # Convert row key to bytes
        try:
            if isinstance(row_key, str):
                row_key_bytes = row_key.encode("utf-8")
            elif isinstance(row_key, bytes):
                row_key_bytes = row_key
            else:
                row_key_bytes = str(row_key).encode("utf-8")
                logger.debug(f"Converted non-string/bytes row key '{row_key}' to bytes.")
        except Exception as e:
            logger.error(
                f"Failed to convert row key '{row_key}' to bytes: {e}. Element: {element}"
            )
            beam.metrics.Metrics.counter("BigTableWrite", "row_key_conversion_error").inc()
            raise TypeError(f"Cannot convert row key {row_key} to bytes") from e

        direct_row = DirectRow(row_key=row_key_bytes)

        for col, value in element.items():
            if col == self.row_key_field:
                continue  # Don't write the key field as a separate column

            # Convert value to bytes
            try:
                if isinstance(value, bytes):
                    value_bytes = value
                elif value is None:
                    value_bytes = b""  # Use empty bytes for None
                else:
                    value_bytes = str(value).encode("utf-8")
            except Exception as e:
                logger.warning(
                    f"Failed to encode value for column '{col}' in row '{row_key}': {e}. Skipping column."
                )
                beam.metrics.Metrics.counter("BigTableWrite", "value_encoding_error").inc()
                continue  # Skip this cell

            # Convert column name to bytes
            try:
                col_bytes = col.encode("utf-8")
            except Exception as e:
                logger.warning(
                    f"Failed to encode column name '{col}' in row '{row_key}': {e}. Skipping column."
                )
                beam.metrics.Metrics.counter("BigTableWrite", "column_encoding_error").inc()
                continue  # Skip this cell

            direct_row.set_cell(
                column_family_id=self.column_family_id,
                column=col_bytes,
                value=value_bytes
                # Timestamp handled by BigTable
            )

        return direct_row


class _WriteToBigTableFn(beam.PTransform):
    """A PTransform that writes DirectRow objects to BigTable."""

    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        batch_size: int = 1000,
        max_retries: int = 3,
        skip_invalid_records: bool = False,
    ):
        super().__init__()
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.skip_invalid_records = skip_invalid_records

    def expand(self, pcoll):
        return (
            pcoll
            | "WriteToTable" >> beam.ParDo(
                _BigTableWriteDoFn(
                    project_id=self.project_id,
                    instance_id=self.instance_id,
                    table_id=self.table_id,
                    batch_size=self.batch_size,
                    max_retries=self.max_retries,
                    skip_invalid_records=self.skip_invalid_records,
                )
            )
        )


class _BigTableWriteDoFn(beam.DoFn):
    """DoFn for writing to BigTable."""

    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        batch_size: int = 1000,
        max_retries: int = 3,
        skip_invalid_records: bool = False,
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.skip_invalid_records = skip_invalid_records
        self.client = None
        self.instance = None
        self.table = None
        self.batch = None
        self.batch_count = 0

    def setup(self):
        """Initialize the BigTable client."""
        self.client = bigtable.Client(project=self.project_id, admin=False)
        self.instance = self.client.instance(self.instance_id)
        self.table = self.instance.table(self.table_id)

    def start_bundle(self):
        """Start a new batch for this bundle."""
        self.batch = []
        self.batch_count = 0

    def process(self, element: DirectRow):
        """Process a single DirectRow."""
        try:
            self.batch.append(element)
            self.batch_count += 1

            if self.batch_count >= self.batch_size:
                self._flush_batch()
        except Exception as e:
            logger.error(f"Error processing row: {e}")
            if not self.skip_invalid_records:
                raise

    def finish_bundle(self):
        """Flush any remaining rows in the batch."""
        if self.batch_count > 0:
            self._flush_batch()

    def _flush_batch(self):
        """Flush the current batch to BigTable."""
        if not self.batch:
            return

        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                status = self.table.mutate_rows(self.batch)
                for i, status_entry in enumerate(status):
                    if not status_entry.code:
                        beam.metrics.Metrics.counter("BigTableWrite", "rows_written").inc()
                    else:
                        logger.warning(
                            f"Failed to write row {i} in batch: {status_entry.code} - {status_entry.message}"
                        )
                        beam.metrics.Metrics.counter("BigTableWrite", "rows_failed").inc()
                break
            except (DeadlineExceeded, ServiceUnavailable) as e:
                retry_count += 1
                if retry_count <= self.max_retries:
                    logger.warning(
                        f"Transient error writing to BigTable, retrying {retry_count}/{self.max_retries}: {e}"
                    )
                else:
                    logger.error(f"Failed to write to BigTable after {self.max_retries} retries: {e}")
                    if not self.skip_invalid_records:
                        raise
            except Exception as e:
                logger.error(f"Error writing to BigTable: {e}")
                if not self.skip_invalid_records:
                    raise
                break

        # Reset the batch
        self.batch = []
        self.batch_count = 0


# BigTable Simulator for local testing
class BigTableSimulator:
    """A simple in-memory simulator for BigTable."""

    _instances = {}

    @classmethod
    def get_instance(cls, project_id: str, instance_id: str):
        """Get or create a BigTable instance."""
        key = f"{project_id}:{instance_id}"
        if key not in cls._instances:
            cls._instances[key] = {"tables": {}}
        return cls._instances[key]

    @classmethod
    def get_table(cls, project_id: str, instance_id: str, table_id: str):
        """Get or create a table in a BigTable instance."""
        instance = cls.get_instance(project_id, instance_id)
        if table_id not in instance["tables"]:
            instance["tables"][table_id] = {"rows": {}, "column_families": set()}
        return instance["tables"][table_id]

    @classmethod
    def add_column_family(cls, project_id: str, instance_id: str, table_id: str, column_family_id: str):
        """Add a column family to a table."""
        table = cls.get_table(project_id, instance_id, table_id)
        table["column_families"].add(column_family_id)

    @classmethod
    def write_row(cls, project_id: str, instance_id: str, table_id: str, row_key: bytes, cells: Dict[str, Dict[str, bytes]]):
        """Write a row to a table."""
        table = cls.get_table(project_id, instance_id, table_id)
        row_key_str = row_key.decode("utf-8")
        if row_key_str not in table["rows"]:
            table["rows"][row_key_str] = {}
        
        for column_family_id, columns in cells.items():
            if column_family_id not in table["column_families"]:
                table["column_families"].add(column_family_id)
            
            if column_family_id not in table["rows"][row_key_str]:
                table["rows"][row_key_str][column_family_id] = {}
            
            for column, value in columns.items():
                column_str = column.decode("utf-8")
                table["rows"][row_key_str][column_family_id][column_str] = value

    @classmethod
    def read_row(cls, project_id: str, instance_id: str, table_id: str, row_key: bytes):
        """Read a row from a table."""
        table = cls.get_table(project_id, instance_id, table_id)
        row_key_str = row_key.decode("utf-8")
        if row_key_str not in table["rows"]:
            return None
        return table["rows"][row_key_str]

    @classmethod
    def read_rows(cls, project_id: str, instance_id: str, table_id: str):
        """Read all rows from a table."""
        table = cls.get_table(project_id, instance_id, table_id)
        return table["rows"]

    @classmethod
    def clear_table(cls, project_id: str, instance_id: str, table_id: str):
        """Clear all rows from a table."""
        table = cls.get_table(project_id, instance_id, table_id)
        table["rows"] = {}

    @classmethod
    def delete_table(cls, project_id: str, instance_id: str, table_id: str):
        """Delete a table."""
        instance = cls.get_instance(project_id, instance_id)
        if table_id in instance["tables"]:
            del instance["tables"][table_id]

    @classmethod
    def delete_instance(cls, project_id: str, instance_id: str):
        """Delete an instance."""
        key = f"{project_id}:{instance_id}"
        if key in cls._instances:
            del cls._instances[key]

    @classmethod
    def clear_all(cls):
        """Clear all data."""
        cls._instances = {}


@TransformRegistry.register("WriteToBigTableSimulator")
class WriteToBigTableSimulatorTransform(BaseTransform):
    """
    Write to a BigTable simulator for local testing.

    Parameters:
        project_id: Google Cloud project ID.
        instance_id: BigTable instance ID.
        table_id: BigTable table ID.
        column_family_id: The column family to write data into.
        row_key_field: The field in the input dictionary to use as the row key.
    """

    PARAMETERS = {
        "project_id": {
            "type": "string",
            "description": "Google Cloud project ID",
            "required": True,
        },
        "instance_id": {
            "type": "string",
            "description": "BigTable instance ID",
            "required": True,
        },
        "table_id": {
            "type": "string",
            "description": "BigTable table ID",
            "required": True,
        },
        "column_family_id": {
            "type": "string",
            "description": "The column family to write data into",
            "required": True,
        },
        "row_key_field": {
            "type": "string",
            "description": "The field in the input dictionary to use as the row key",
            "required": True,
        },
    }

    def build_transform(
        self, side_inputs: Optional[Dict[str, Any]] = None
    ) -> beam.PTransform:
        """Build the Apache Beam PTransform for this transform.

        Args:
            side_inputs: A dictionary of side inputs (not used for this transform).

        Returns:
            An Apache Beam PTransform.
        """
        project_id = self.config.get("project_id")
        instance_id = self.config.get("instance_id")
        table_id = self.config.get("table_id")
        column_family_id = self.config.get("column_family_id")
        row_key_field = self.config.get("row_key_field")

        # Ensure the column family exists in the simulator
        BigTableSimulator.add_column_family(project_id, instance_id, table_id, column_family_id)

        return beam.ParDo(
            _BigTableSimulatorDoFn(
                project_id=project_id,
                instance_id=instance_id,
                table_id=table_id,
                column_family_id=column_family_id,
                row_key_field=row_key_field,
            )
        )


class _BigTableSimulatorDoFn(beam.DoFn):
    """DoFn for writing to the BigTable simulator."""

    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family_id: str,
        row_key_field: str,
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.column_family_id = column_family_id
        self.row_key_field = row_key_field

    def process(self, element: Dict[str, Any]):
        """Process a single element."""
        try:
            row_key = element.get(self.row_key_field)
            if row_key is None:
                logger.warning(f"Skipping row due to missing key '{self.row_key_field}': {element}")
                return

            # Convert row key to bytes
            if isinstance(row_key, str):
                row_key_bytes = row_key.encode("utf-8")
            elif isinstance(row_key, bytes):
                row_key_bytes = row_key
            else:
                row_key_bytes = str(row_key).encode("utf-8")

            # Prepare cells
            cells = {self.column_family_id: {}}
            for col, value in element.items():
                if col == self.row_key_field:
                    continue  # Don't write the key field as a separate column

                # Convert value to bytes
                if isinstance(value, bytes):
                    value_bytes = value
                elif value is None:
                    value_bytes = b""  # Use empty bytes for None
                else:
                    value_bytes = str(value).encode("utf-8")

                # Convert column name to bytes
                col_bytes = col.encode("utf-8")
                cells[self.column_family_id][col_bytes] = value_bytes

            # Write to simulator
            BigTableSimulator.write_row(
                self.project_id, self.instance_id, self.table_id, row_key_bytes, cells
            )
            
            # Return the element for potential further processing
            yield element
        except Exception as e:
            logger.error(f"Error writing to BigTable simulator: {e}")
            raise
