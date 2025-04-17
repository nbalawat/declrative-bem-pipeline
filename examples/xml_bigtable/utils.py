"""
Utility functions for the XML to BigTable pipeline example.
"""

from typing import Dict, Any, Optional
import datetime


def enrich_transaction_data():
    """
    Enriches transaction data with additional information.
    
    Returns:
        A function that takes a transaction dictionary and returns an enriched transaction
    """
    def _enrich(transaction: Dict[str, Any]) -> Dict[str, Any]:
        # Create a new dictionary to avoid modifying the input
        enriched = dict(transaction)
        
        # Extract transaction ID from attributes
        transaction_id = transaction.get("@id", "unknown")
        enriched["id"] = transaction_id
        
        try:
            enriched["amount"] = float(enriched.get("amount", 0))
        except (ValueError, TypeError):
            enriched["amount"] = 0.0
        
        # Add processing timestamp
        enriched["processing_timestamp"] = datetime.datetime.now().isoformat()
        
        # Calculate fee based on transaction type and amount
        transaction_type = enriched.get("type", "").lower()
        amount = enriched["amount"]
        
        if transaction_type == "wire":
            # Wire transfers have a 0.25% fee with a minimum of $25
            fee = max(25.0, amount * 0.0025)
        elif transaction_type == "ach":
            # ACH transfers have a flat fee of $5
            fee = 5.0
        elif transaction_type == "check":
            # Check payments have a $2 fee
            fee = 2.0
        else:
            # Default fee
            fee = 0.0
        
        enriched["fee"] = fee
        enriched["net_amount"] = amount - fee
        
        # Add risk score (simple example based on amount)
        if amount > 10000.0:
            enriched["risk_score"] = "high"
        elif amount > 1000.0:
            enriched["risk_score"] = "medium"
        else:
            enriched["risk_score"] = "low"
        
        # Flatten nested structures for BigTable storage
        if "sender" in enriched:
            for key, value in enriched["sender"].items():
                enriched[f"sender_{key}"] = value
        
        if "receiver" in enriched:
            for key, value in enriched["receiver"].items():
                enriched[f"receiver_{key}"] = value
        
        if "metadata" in enriched:
            for key, value in enriched["metadata"].items():
                enriched[f"metadata_{key}"] = value
        
        return enriched
    
    return _enrich


def is_high_value_transaction():
    """
    Filters transactions with an amount greater than $1000.
    
    Returns:
        A function that takes a transaction dictionary and returns a boolean
    """
    def _is_high_value(transaction: Dict[str, Any]) -> bool:
        try:
            amount = float(transaction.get("amount", 0))
            return amount > 1000.0
        except (ValueError, TypeError):
            return False
    
    return _is_high_value
