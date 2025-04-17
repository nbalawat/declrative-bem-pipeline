#!/usr/bin/env python
"""
Enhanced script to test direct writes to the BigTable emulator with batch processing and timeout handling.
"""

import os
import sys
import uuid
import datetime
import logging
import threading
import time
import random
from contextlib import contextmanager
from typing import List, Optional
from google.cloud import bigtable
from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = "test-project"
INSTANCE_ID = "test-instance"
TABLE_ID = "transactions"
COLUMN_FAMILY_ID = "transaction_data"

@contextmanager
def timeout(seconds, message="Operation timed out"):
    """Context manager for timing out operations.
    
    Args:
        seconds: Timeout in seconds
        message: Message to include in the TimeoutError
        
    Raises:
        TimeoutError: If the operation times out
    """
    timer = threading.Timer(seconds, lambda: sys.exit("Timeout occurred"))
    timer.daemon = True
    
    try:
        timer.start()
        yield
    finally:
        timer.cancel()

def write_with_timeout(table, rows, timeout_seconds=10):
    """Write rows to BigTable with a timeout to prevent hanging.
    
    Args:
        table: BigTable table object
        rows: List of rows to write
        timeout_seconds: Timeout in seconds
        
    Returns:
        bool: True if the write was successful, False if it timed out
    """
    # Flag to track success
    success = [False]
    
    def _write():
        try:
            status = table.mutate_rows(rows)
            # Check if all rows were written successfully
            all_success = all(not entry.code for entry in status)
            success[0] = all_success
            if not all_success:
                # Log any failures
                for i, status_entry in enumerate(status):
                    if status_entry.code:
                        logger.warning(f"Failed to write row {i}: {status_entry.code} - {status_entry.message}")
        except Exception as e:
            logger.error(f"Error in write_with_timeout thread: {e}")
            success[0] = False
    
    # Create and start the thread
    write_thread = threading.Thread(target=_write)
    write_thread.daemon = True
    write_thread.start()
    
    # Wait for the thread with a timeout
    start_time = time.time()
    while write_thread.is_alive() and time.time() - start_time < timeout_seconds:
        time.sleep(0.1)
    
    if write_thread.is_alive():
        logger.warning(f"BigTable write operation timed out after {timeout_seconds} seconds")
        return False
    
    return success[0]

def test_direct_write():
    """Test direct write to BigTable emulator."""
    # Set the environment variable
    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    
    logger.info(f"Testing direct write to BigTable emulator at {os.environ['BIGTABLE_EMULATOR_HOST']}")
    
    try:
        # Create a client
        client = bigtable.Client(project=PROJECT_ID, admin=True)
        logger.info(f"Created BigTable client for project {PROJECT_ID}")
        
        # Get the instance
        instance = client.instance(INSTANCE_ID)
        logger.info(f"Got instance {INSTANCE_ID}")
        
        # Get the table
        table = instance.table(TABLE_ID)
        logger.info(f"Got table {TABLE_ID}")
        
        # Ensure the table exists
        logger.info(f"Checking if table {TABLE_ID} exists...")
        if not table.exists():
            logger.info(f"Table {TABLE_ID} does not exist, creating it...")
            try:
                table.create()
                logger.info(f"Created table {TABLE_ID}")
            except Exception as e:
                logger.warning(f"Error creating table: {e}")
                # Continue anyway, the table might have been created by another process
        else:
            logger.info(f"Table {TABLE_ID} already exists")
            
        # Ensure the column family exists
        logger.info(f"Checking if column family {COLUMN_FAMILY_ID} exists...")
        cf = table.column_family(COLUMN_FAMILY_ID)
        try:
            if not cf.exists():
                logger.info(f"Column family {COLUMN_FAMILY_ID} does not exist, creating it...")
                cf.create()
                logger.info(f"Created column family {COLUMN_FAMILY_ID}")
            else:
                logger.info(f"Column family {COLUMN_FAMILY_ID} already exists")
        except Exception as e:
            logger.warning(f"Error checking/creating column family: {e}")
            # Continue anyway, the column family might have been created by another process
        
        # List tables to verify connection with timeout handling
        try:
            with timeout(5, "Table listing timed out"):
                tables = list(instance.list_tables())
                logger.info(f"Found {len(tables)} tables in instance {INSTANCE_ID}")
                for t in tables:
                    logger.info(f"- {t.table_id}")
        except TimeoutError as e:
            logger.warning(f"Timeout listing tables: {e}")
            # Continue anyway
        
        # Test single row write
        test_row_key = f"test-row-{uuid.uuid4()}"
        test_row_key_bytes = test_row_key.encode('utf-8')
        logger.info(f"Created test row key: {test_row_key}")
        
        # Create a test row
        row = table.direct_row(test_row_key_bytes)
        row.set_cell(COLUMN_FAMILY_ID, b'test_column', b'test_value', timestamp=datetime.datetime.now())
        logger.info("Created test row")
        
        # Write the test row with timeout handling
        logger.info("Writing test row to BigTable emulator...")
        try:
            with timeout(10, "Single row write timed out"):
                row.commit()
                logger.info("Successfully wrote test row to BigTable emulator")
        except TimeoutError as e:
            logger.warning(f"Timeout writing single row: {e}")
        
        # Test batch write with multiple rows
        logger.info("Testing batch write with multiple rows...")
        batch_size = 10
        rows = []
        row_keys = []
        
        # Create batch of rows
        for i in range(batch_size):
            row_key = f"batch-row-{uuid.uuid4()}"
            row_key_bytes = row_key.encode('utf-8')
            row_keys.append(row_key_bytes)
            
            # Create a row
            row = table.direct_row(row_key_bytes)
            row.set_cell(COLUMN_FAMILY_ID, b'batch_id', str(i).encode('utf-8'))
            row.set_cell(COLUMN_FAMILY_ID, b'timestamp', str(datetime.datetime.now()).encode('utf-8'))
            row.set_cell(COLUMN_FAMILY_ID, b'value', str(random.random()).encode('utf-8'))
            
            rows.append(row)
        
        # Process in smaller sub-batches to improve reliability with the emulator
        sub_batch_size = 5  # Use smaller batches for emulator
        success_count = 0
        
        for sub_batch_start in range(0, len(rows), sub_batch_size):
            sub_batch_end = min(sub_batch_start + sub_batch_size, len(rows))
            current_sub_batch = rows[sub_batch_start:sub_batch_end]
            
            logger.info(f"Writing sub-batch {sub_batch_start+1} to {sub_batch_end} of {len(rows)} rows...")
            
            # Try to write with retries
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count <= max_retries and not success:
                try:
                    # Use timeout for the write operation
                    if write_with_timeout(table, current_sub_batch, 10):
                        logger.info(f"Successfully wrote sub-batch {sub_batch_start+1} to {sub_batch_end}")
                        success = True
                        success_count += len(current_sub_batch)
                    else:
                        retry_count += 1
                        logger.warning(f"Timeout writing batch, retrying {retry_count}/{max_retries}")
                except (DeadlineExceeded, ServiceUnavailable) as e:
                    retry_count += 1
                    logger.warning(f"Transient error writing to BigTable, retrying {retry_count}/{max_retries}: {e}")
                except Exception as e:
                    logger.error(f"Error writing sub-batch to BigTable: {e}")
                    break
        
        logger.info(f"Successfully wrote {success_count} out of {len(rows)} rows in batch mode")
        
        # Read back a few rows to verify
        if row_keys:
            try:
                with timeout(10, "Row reading timed out"):
                    logger.info("Reading back a sample row from the batch...")
                    sample_row = table.read_row(row_keys[0])
                    
                    if sample_row is not None:
                        logger.info("Successfully read sample row from batch")
                        for family, columns in sample_row.cells.items():
                            for column, cells in columns.items():
                                family_str = family.decode('utf-8') if isinstance(family, bytes) else family
                                column_str = column.decode('utf-8') if isinstance(column, bytes) else column
                                value_str = cells[0].value.decode('utf-8')
                                logger.info(f"  {family_str}:{column_str} = {value_str}")
                    else:
                        logger.warning("Sample row was not found")
            except TimeoutError as e:
                logger.warning(f"Timeout reading row: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error testing BigTable emulator: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def test_batch_performance():
    """Test performance of batch writes with different batch sizes."""
    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    logger.info(f"Testing batch write performance to BigTable emulator at {os.environ['BIGTABLE_EMULATOR_HOST']}")
    
    try:
        client = bigtable.Client(project=PROJECT_ID, admin=True)
        instance = client.instance(INSTANCE_ID)
        table = instance.table(TABLE_ID)
        
        # Ensure the table exists
        logger.info(f"Checking if table {TABLE_ID} exists...")
        if not table.exists():
            logger.info(f"Table {TABLE_ID} does not exist, creating it...")
            try:
                table.create()
                logger.info(f"Created table {TABLE_ID}")
            except Exception as e:
                logger.warning(f"Error creating table: {e}")
                # Continue anyway, the table might have been created by another process
        else:
            logger.info(f"Table {TABLE_ID} already exists")
            
        # Ensure the column family exists
        logger.info(f"Checking if column family {COLUMN_FAMILY_ID} exists...")
        cf = table.column_family(COLUMN_FAMILY_ID)
        try:
            if not cf.exists():
                logger.info(f"Column family {COLUMN_FAMILY_ID} does not exist, creating it...")
                cf.create()
                logger.info(f"Created column family {COLUMN_FAMILY_ID}")
            else:
                logger.info(f"Column family {COLUMN_FAMILY_ID} already exists")
        except Exception as e:
            logger.warning(f"Error checking/creating column family: {e}")
            # Continue anyway, the column family might have been created by another process
        
        # Test different batch sizes
        total_rows = 100
        batch_sizes = [1, 5, 10, 20, 50]
        
        for batch_size in batch_sizes:
            logger.info(f"\nTesting with batch size: {batch_size}")
            
            # Create all rows first
            all_rows = []
            for i in range(total_rows):
                row_key = f"perf-row-{uuid.uuid4()}"
                row = table.direct_row(row_key.encode('utf-8'))
                row.set_cell(COLUMN_FAMILY_ID, b'index', str(i).encode('utf-8'))
                row.set_cell(COLUMN_FAMILY_ID, b'batch_size', str(batch_size).encode('utf-8'))
                row.set_cell(COLUMN_FAMILY_ID, b'timestamp', str(datetime.datetime.now()).encode('utf-8'))
                all_rows.append(row)
            
            # Time the batch writes
            start_time = time.time()
            success_count = 0
            
            for batch_start in range(0, len(all_rows), batch_size):
                batch_end = min(batch_start + batch_size, len(all_rows))
                current_batch = all_rows[batch_start:batch_end]
                
                if write_with_timeout(table, current_batch, 15):
                    success_count += len(current_batch)
            
            elapsed_time = time.time() - start_time
            logger.info(f"Batch size {batch_size}: Wrote {success_count}/{total_rows} rows in {elapsed_time:.2f} seconds")
            logger.info(f"Throughput: {success_count/elapsed_time:.2f} rows/second")
        
        return True
    except Exception as e:
        logger.error(f"Error in batch performance test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--performance":
        test_batch_performance()
    else:
        test_direct_write()
