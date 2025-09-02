"""
CSV Parser Utility Module for RICO Data Ingestion.

This module provides pure functions for parsing carrier data from CSV files,
handling various data formats and edge cases commonly found in trucking industry data.
"""

import csv
import io
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path


def parse_insurance_amount(amount_str: str) -> Optional[float]:
    """
    Parse insurance amount string to float value.
    
    Handles various formats:
    - "$1 million" -> 1000000.0
    - "$750k" -> 750000.0
    - "1000000" -> 1000000.0
    - "n/a", "", "-" -> None
    
    Args:
        amount_str: String representation of insurance amount
        
    Returns:
        Float value in dollars or None if unparseable
    """
    if not amount_str or amount_str.lower() in ['n/a', 'na', '-', '']:
        return None
    
    amount_str = amount_str.strip()
    
    # Handle common formats
    if '$1 million' in amount_str.lower():
        return 1000000.0
    elif '$750k' in amount_str.lower():
        return 750000.0
    elif 'million' in amount_str.lower():
        # Extract number before 'million'
        try:
            num = float(amount_str.lower().replace('$', '').replace('million', '').replace(',', '').strip())
            return num * 1000000
        except (ValueError, TypeError):
            return None
    elif 'k' in amount_str.lower():
        # Extract number before 'k'
        try:
            num = float(amount_str.lower().replace('$', '').replace('k', '').replace(',', '').strip())
            return num * 1000
        except (ValueError, TypeError):
            return None
    
    # Try to parse as regular number
    try:
        return float(amount_str.replace('$', '').replace(',', ''))
    except (ValueError, TypeError):
        return None


def parse_number(value: str) -> Optional[int]:
    """
    Parse number string with commas to integer.
    
    Handles:
    - "1,234" -> 1234
    - "  156  " -> 156
    - "-", "", None -> None
    
    Args:
        value: String representation of number
        
    Returns:
        Integer value or None if unparseable
    """
    if not value or str(value).strip() in ['-', '', 'n/a', 'N/A']:
        return None
    
    try:
        # Remove commas, spaces, and quotes, then convert to int
        cleaned = str(value).replace(',', '').replace(' ', '').replace('"', '').strip()
        return int(cleaned)
    except (ValueError, TypeError):
        return None


def parse_percentage(value: str) -> Optional[float]:
    """
    Parse percentage string to float value.
    
    Handles:
    - "2.5%" -> 2.5
    - "35.40%" -> 35.4
    - "-", "" -> None
    
    Args:
        value: String representation of percentage
        
    Returns:
        Float value (percentage as number) or None if unparseable
    """
    if not value or str(value).strip() in ['-', '', 'n/a', 'N/A']:
        return None
    
    try:
        # Remove % sign and convert to float
        return float(str(value).replace('%', '').strip())
    except (ValueError, TypeError):
        return None


def parse_boolean(value: str) -> bool:
    """
    Parse boolean string value.
    
    Args:
        value: String representation of boolean
        
    Returns:
        Boolean value (default False for unparseable)
    """
    if not value:
        return False
    return str(value).strip().lower() in ['yes', 'true', '1', 'y']


def parse_carriers_csv(
    csv_content: Union[str, Path, io.StringIO],
    data_source: str = "CSV_IMPORT"
) -> Tuple[List[Dict], List[str]]:
    """
    Parse carriers from CSV file or content.
    
    Args:
        csv_content: CSV file path, string content, or StringIO object
        data_source: Source identifier for tracking data origin
        
    Returns:
        Tuple of (carriers list, unique insurance providers list)
        
    Raises:
        FileNotFoundError: If file path provided but file doesn't exist
        ValueError: If CSV format is invalid
    """
    carriers = []
    insurance_providers = set()
    
    # Handle different input types
    if isinstance(csv_content, Path) or isinstance(csv_content, str):
        # Check if it's a file path
        if isinstance(csv_content, str) and '\n' not in csv_content and len(csv_content) < 260:
            # Likely a file path
            path = Path(csv_content)
            if not path.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_content}")
            with open(path, 'r', encoding='utf-8') as f:
                csv_string = f.read()
        else:
            # It's CSV content as string
            csv_string = str(csv_content)
        
        # Skip empty lines at the beginning
        lines = [line for line in csv_string.split('\n') if line.strip()]
        csv_content = io.StringIO('\n'.join(lines))
    
    # Parse CSV
    reader = csv.DictReader(csv_content)
    
    if not reader.fieldnames:
        raise ValueError("CSV file appears to be empty or invalid")
    
    for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is line 1)
        # Skip completely empty rows
        if all(not v.strip() for v in row.values()):
            continue
        
        dot_number = row.get('dot_number', '').strip()
        
        # Parse the carrier data - handle column names with spaces
        try:
            carrier = {
                'usdot': parse_number(dot_number),
                'jb_carrier': parse_boolean(row.get('JB Carrier', '')),
                'carrier_name': row.get('Carrier', '').strip(),
                'primary_officer': row.get('Primary Officer', '').strip(),
                'insurance_provider': None,
                'insurance_amount': None,
                'trucks': parse_number(row.get(' Trucks ', row.get('Trucks', ''))),
                'inspections': parse_number(row.get(' Inspections ', row.get('Inspections', ''))),
                'violations': parse_number(row.get(' Violations ', row.get('Violations', ''))),
                'oos': parse_number(row.get(' OOS ', row.get('OOS', ''))),
                'crashes': parse_number(row.get(' Crashes ', row.get('Crashes', ''))) or 0,
                'driver_oos_rate': parse_percentage(row.get('Driver OOS Rate', '')),
                'vehicle_oos_rate': parse_percentage(row.get('Vehicle OOS Rate', '')),
                'mcs150_drivers': parse_number(row.get(' MCS150 Drivers ', row.get('MCS150 Drivers', ''))),
                'mcs150_miles': parse_number(row.get(' MCS150 Miles ', row.get('MCS150 Miles', ''))),
                'ampd': parse_number(row.get(' AMPD ', row.get('AMPD', ''))),
                'data_source': data_source,
                'row_number': row_num  # Track source row for error reporting
            }
            
            # Parse insurance fields (handle column name with leading space)
            insurance_name = row.get(' Insurance', row.get('Insurance', '')).strip()
            if insurance_name and insurance_name.lower() not in ['n/a', 'na', '']:
                carrier['insurance_provider'] = insurance_name
                insurance_providers.add(insurance_name)
            
            amount = parse_insurance_amount(row.get('Amount', ''))
            if amount:
                carrier['insurance_amount'] = amount
            
            # Don't skip invalid data here - let validation handle it
            # This ensures we can count validation errors properly
            
            carriers.append(carrier)
            
        except Exception as e:
            # Include row number in error for debugging
            carrier_name = row.get('Carrier', 'Unknown')
            raise ValueError(f"Error parsing row {row_num} (carrier: {carrier_name}): {str(e)}")
    
    return carriers, sorted(list(insurance_providers))


def validate_carrier_data(carrier: Dict) -> Tuple[bool, List[str]]:
    """
    Validate carrier data for required fields and data integrity.
    
    Args:
        carrier: Dictionary containing carrier data
        
    Returns:
        Tuple of (is_valid, list of validation errors)
    """
    errors = []
    
    # Required fields
    if not carrier.get('usdot'):
        errors.append("Missing required field: usdot")
    elif not isinstance(carrier['usdot'], int) or carrier['usdot'] <= 0:
        errors.append(f"Invalid USDOT number: {carrier.get('usdot')}")
    
    if not carrier.get('carrier_name'):
        errors.append("Missing required field: carrier_name")
    
    # Validate numeric fields if present
    numeric_fields = [
        'trucks', 'inspections', 'violations', 'oos', 'crashes',
        'mcs150_drivers', 'mcs150_miles', 'ampd'
    ]
    
    for field in numeric_fields:
        if field in carrier and carrier[field] is not None:
            if not isinstance(carrier[field], (int, float)) or carrier[field] < 0:
                errors.append(f"Invalid {field} value: {carrier.get(field)}")
    
    # Validate percentage fields
    percentage_fields = ['driver_oos_rate', 'vehicle_oos_rate']
    for field in percentage_fields:
        if field in carrier and carrier[field] is not None:
            if not isinstance(carrier[field], (int, float)) or carrier[field] < 0 or carrier[field] > 100:
                errors.append(f"Invalid {field} value: {carrier.get(field)} (must be 0-100)")
    
    # Validate insurance amount if present
    if 'insurance_amount' in carrier and carrier['insurance_amount'] is not None:
        if not isinstance(carrier['insurance_amount'], (int, float)) or carrier['insurance_amount'] < 0:
            errors.append(f"Invalid insurance_amount: {carrier.get('insurance_amount')}")
    
    return len(errors) == 0, errors


def extract_unique_values(carriers: List[Dict]) -> Dict[str, List]:
    """
    Extract unique values from carrier data for entity creation.
    
    Args:
        carriers: List of carrier dictionaries
        
    Returns:
        Dictionary containing unique insurance providers and officers
    """
    insurance_providers = set()
    officers = set()
    
    for carrier in carriers:
        # Extract insurance provider
        if carrier.get('insurance_provider'):
            insurance_providers.add(carrier['insurance_provider'])
        
        # Extract officer
        if carrier.get('primary_officer') and carrier['primary_officer'].lower() not in ['n/a', 'na', '']:
            officers.add(carrier['primary_officer'])
    
    return {
        'insurance_providers': sorted(list(insurance_providers)),
        'officers': sorted(list(officers))
    }