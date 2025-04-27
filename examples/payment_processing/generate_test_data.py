"""
Generate sample payment data for testing the payment processing pipeline.

This script creates sample payment data files in the data directory.
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from examples.payment_processing.utils import create_sample_payment_file


def main():
    """Generate sample payment data files."""
    # Create data directory if it doesn't exist
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Create output directory if it doesn't exist
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Generate sample payment data
    payments_file = data_dir / "payments.csv"
    print(f"Generating sample payment data: {payments_file}")
    create_sample_payment_file(str(payments_file), num_records=100)
    
    # Generate additional test data with specific patterns
    test_payments_file = data_dir / "test_payments.csv"
    print(f"Generating test payment data: {test_payments_file}")
    create_sample_payment_file(str(test_payments_file), num_records=20)
    
    print("Sample data generation complete.")


if __name__ == "__main__":
    main()
