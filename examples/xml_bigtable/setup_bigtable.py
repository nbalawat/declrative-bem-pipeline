#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Set up the BigTable emulator for testing.

This script creates the necessary instance, table, and column families in the BigTable emulator.
"""

import os
import sys
import logging
from google.cloud import bigtable
from google.cloud.bigtable import column_family

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# BigTable emulator configuration
PROJECT_ID = "test-project"
INSTANCE_ID = "test-instance"
TABLE_ID = "transactions"
COLUMN_FAMILY_ID = "transaction_data"

def setup_bigtable_emulator():
    """
    Set up the BigTable emulator with the necessary table and column families.
    
    Note: The BigTable emulator has limitations and doesn't fully implement all admin operations.
    This simplified setup focuses on the minimal operations needed to run the pipeline.
    """
    # Check if the BIGTABLE_EMULATOR_HOST environment variable is set
    emulator_host = os.environ.get("BIGTABLE_EMULATOR_HOST")
    if not emulator_host:
        logger.error("BIGTABLE_EMULATOR_HOST environment variable is not set.")
        logger.error("Please set it to the host:port of the BigTable emulator.")
        logger.error("Example: export BIGTABLE_EMULATOR_HOST=localhost:8086")
        return False

    logger.info(f"Using BigTable emulator at {emulator_host}")

    try:
        # Import datetime here to avoid circular import
        import datetime
        
        # Create a client
        client = bigtable.Client(project=PROJECT_ID, admin=True)
        logger.info(f"Created BigTable client for project {PROJECT_ID}")

        # Get the instance (assume it exists in the emulator)
        logger.info(f"Using instance {INSTANCE_ID}")
        instance = client.instance(INSTANCE_ID)
        
        # Create a table
        logger.info(f"Creating table {TABLE_ID}...")
        table = instance.table(TABLE_ID)
        
        # Try to create the table - this might fail if the table already exists
        # or if the emulator doesn't support this operation
        try:
            table.create()
            logger.info(f"Created table {TABLE_ID}")
        except Exception as e:
            # Log the error but continue - the table might already exist
            # or we might be able to write to it anyway
            logger.warning(f"Could not create table {TABLE_ID}: {str(e)}")
            logger.warning("Continuing anyway - the pipeline may still work if the table exists")

        # Try to create the column family
        logger.info(f"Creating column family {COLUMN_FAMILY_ID}...")
        try:
            max_age_rule = column_family.MaxAgeGCRule(datetime.timedelta(days=7))
            table.create_column_family(COLUMN_FAMILY_ID, max_age_rule)
            logger.info(f"Created column family {COLUMN_FAMILY_ID}")
        except Exception as e:
            # Log the error but continue
            logger.warning(f"Could not create column family {COLUMN_FAMILY_ID}: {str(e)}")
            logger.warning("Continuing anyway - the pipeline may still work if the column family exists")

        # Try a simple operation to verify the emulator is working
        try:
            # Try to read rows - this should work even if we couldn't create the table
            table.read_rows(limit=1)
            logger.info("Successfully connected to BigTable emulator")
            return True
        except Exception as e:
            logger.error(f"Could not read from table: {str(e)}")
            logger.error("The emulator might not be fully functional")
            return False

    except Exception as e:
        logger.error(f"Error setting up BigTable emulator: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    # Add missing import
    import datetime
    
    # Run the setup
    setup_bigtable_emulator()
