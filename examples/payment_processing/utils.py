"""
Utility functions for payment processing pipeline examples.
"""

from typing import Dict, Any, List, Optional
import csv
import datetime
import io
import random
import uuid
from typing import Dict, List, Optional, Tuple, Union


def parse_csv_line(line, field_names=None):
    """
    Parse a CSV line into a dictionary.
    
    Args:
        line: CSV line to parse (str or dict with element already parsed)
        field_names: Comma-separated list of field names
        
    Returns:
        Dictionary with field names as keys and CSV values
    """
    # If line is already a dictionary (happens in some Beam contexts), return it
    if isinstance(line, dict):
        return line
        
    # Handle string input
    if not isinstance(line, str) or not line:
        return {}
    
    # Parse field names
    if field_names:
        fields = [f.strip() for f in field_names.split(',')]
    else:
        return {}
    
    # Parse CSV line
    reader = csv.reader(io.StringIO(line))
    values = next(reader, None)
    
    if not values or len(values) != len(fields):
        return {}
    
    # Create dictionary
    result = {}
    for i, field in enumerate(fields):
        if i < len(values):
            result[field] = values[i]
    
    return result


def parse_csv_line_wrapper(field_names=None):
    """
    Wrapper for parse_csv_line that returns a function compatible with Beam's Map transform.
    
    Args:
        field_names: Comma-separated list of field names
        
    Returns:
        A function that takes a line and returns a dictionary
    """
    def wrapper(line):
        return parse_csv_line(line, field_names)
    return wrapper


def generate_sample_payment_data(num_records: int = 100) -> List[str]:
    """
    Generate sample payment data for testing.
    
    Args:
        num_records: Number of payment records to generate
        
    Returns:
        List of CSV strings representing payment records
    """
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto']
    user_ids = [f'user_{i}' for i in range(1, 21)]  # 20 different users
    
    # Header
    records = [
        "transaction_id,user_id,amount,currency,timestamp,payment_method,ip_address,card_number"
    ]
    
    # Generate random payment records
    for _ in range(num_records):
        transaction_id = str(uuid.uuid4())
        user_id = random.choice(user_ids)
        amount = round(random.uniform(1, 5000), 2)
        currency = random.choice(currencies)
        
        # Generate timestamp within last 7 days
        days_ago = random.randint(0, 7)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        timestamp = (datetime.datetime.now() - 
                    datetime.timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago))
        timestamp_str = timestamp.isoformat()
        
        payment_method = random.choice(payment_methods)
        
        # Generate random IP address
        ip_parts = [str(random.randint(1, 255)) for _ in range(4)]
        ip_address = '.'.join(ip_parts)
        
        # Generate masked card number
        card_number = f"{'*' * 12}{random.randint(1000, 9999)}"
        
        # Create CSV record
        record = f"{transaction_id},{user_id},{amount},{currency},{timestamp_str}," \
                f"{payment_method},{ip_address},{card_number}"
        records.append(record)
    
    return records


def create_sample_payment_file(file_path: str, num_records: int = 100) -> None:
    """
    Create a sample payment data file for testing.
    
    Args:
        file_path: Path to write the sample data file
        num_records: Number of payment records to generate
    """
    records = generate_sample_payment_data(num_records)
    
    with open(file_path, 'w') as f:
        for record in records:
            f.write(f"{record}\n")


def extract_payment_date(payment: Dict[str, Any], timestamp_field: str = 'timestamp') -> Optional[str]:
    """
    Extract date from payment timestamp in YYYY-MM-DD format.
    
    Args:
        payment: Payment dictionary
        timestamp_field: Field containing the timestamp
        
    Returns:
        Date string in YYYY-MM-DD format or None if parsing fails
    """
    if not isinstance(payment, dict) or timestamp_field not in payment:
        return None
    
    try:
        timestamp = payment[timestamp_field]
        # Simple parsing for ISO format timestamps
        dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError, AttributeError):
        return None


def calculate_payment_metrics(payments: List[Dict[str, Any]], 
                             amount_field: str = 'amount') -> Dict[str, float]:
    """
    Calculate basic metrics for a list of payments.
    
    Args:
        payments: List of payment dictionaries
        amount_field: Field containing the payment amount
        
    Returns:
        Dictionary with calculated metrics
    """
    if not payments:
        return {
            'count': 0,
            'total': 0,
            'average': 0,
            'min': 0,
            'max': 0
        }
    
    amounts = []
    for payment in payments:
        if isinstance(payment, dict) and amount_field in payment:
            try:
                amount = float(payment[amount_field])
                amounts.append(amount)
            except (ValueError, TypeError):
                pass
    
    if not amounts:
        return {
            'count': 0,
            'total': 0,
            'average': 0,
            'min': 0,
            'max': 0
        }
    
    return {
        'count': len(amounts),
        'total': sum(amounts),
        'average': sum(amounts) / len(amounts),
        'min': min(amounts),
        'max': max(amounts)
    }


def validate_payment(payment: Dict[str, Any], 
                    required_fields: List[str], 
                    amount_field: str = 'amount',
                    min_amount: float = 0.01,
                    max_amount: float = 10000.0) -> Tuple[bool, Optional[str]]:
    """
    Validate a payment transaction.
    
    Args:
        payment: Payment dictionary to validate
        required_fields: List of required fields
        amount_field: Field containing the payment amount
        min_amount: Minimum valid payment amount
        max_amount: Maximum valid payment amount
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not isinstance(payment, dict):
        return False, "Payment is not a dictionary"
    
    # Check required fields
    missing_fields = [field for field in required_fields if field not in payment]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    # Validate amount
    if amount_field in payment:
        try:
            amount = float(payment[amount_field])
            if amount < min_amount:
                return False, f"Amount {amount} is below minimum {min_amount}"
            if amount > max_amount:
                return False, f"Amount {amount} exceeds maximum {max_amount}"
        except (ValueError, TypeError):
            return False, f"Invalid amount format: {payment.get(amount_field)}"
    
    return True, None
