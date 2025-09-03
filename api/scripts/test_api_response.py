#!/usr/bin/env python3
"""
Test script to examine actual SearchCarriers API responses for different carriers.
This will help us understand what fields are actually returned.
"""

import sys
import os
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.searchcarriers_client import SearchCarriersClient

def test_carriers_api():
    """Test multiple carriers to see API response structure."""
    
    # Test carriers from JB Hunt list with varying data
    test_carriers = [
        (3330908, "Enlightened Truckers of America LLC"),  # 763 inspections
        (2440672, "Keen Cargo INC"),                      # 834 inspections
        (1341816, "Orozoco Trucking INC"),                # 1,199 inspections
        (2124287, "Universe Carrier INC"),                # 695 inspections
    ]
    
    print("Testing SearchCarriers API Response Structure")
    print("=" * 60)
    
    try:
        client = SearchCarriersClient()
        
        for dot_number, carrier_name in test_carriers:
            print(f"\n\nTesting DOT {dot_number} ({carrier_name})")
            print("-" * 50)
            
            # Get inspections with just 1 record to examine structure
            result = client.get_inspections(dot_number, since_months=24, per_page=1)
            
            if "error" in result:
                print(f"  ERROR: {result['error']}")
                continue
            
            if not result.get("data"):
                print("  No data returned")
                continue
            
            inspection = result["data"][0]
            
            print(f"  Found {len(result.get('data', []))} inspection(s)")
            print("\n  Fields in first inspection:")
            
            # Print all fields and their values (truncate long values)
            for key, value in sorted(inspection.items()):
                value_str = str(value)
                if len(value_str) > 50:
                    value_str = value_str[:47] + "..."
                print(f"    {key:30s}: {value_str}")
            
            # Check for date-related fields specifically
            print("\n  Date-related fields found:")
            date_fields = [k for k in inspection.keys() if 'date' in k.lower() or 'time' in k.lower()]
            if date_fields:
                for field in date_fields:
                    print(f"    - {field}: {inspection[field]}")
            else:
                print("    - No fields containing 'date' or 'time'")
            
            # Check for violation/OOS fields
            print("\n  Violation/OOS-related fields found:")
            violation_fields = [k for k in inspection.keys() if any(x in k.lower() for x in ['viol', 'oos', 'out_of_service'])]
            if violation_fields:
                for field in violation_fields:
                    print(f"    - {field}: {inspection[field]}")
            else:
                print("    - No violation or OOS fields found")
                
    except Exception as e:
        print(f"\nError initializing client: {e}")
        print("Make sure SEARCH_CARRIERS_API_TOKEN is set in environment")

if __name__ == "__main__":
    test_carriers_api()