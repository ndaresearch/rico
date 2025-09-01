#!/usr/bin/env python3
"""
Import JB Hunt carriers from CSV file into the graph database.
Creates Carriers, InsuranceProviders, and relationships.
"""

import csv
import json
import requests
import sys
import os
from typing import List, Dict, Optional
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

load_dotenv()


def parse_insurance_amount(amount_str: str) -> Optional[float]:
    """Parse insurance amount string to float"""
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
            num = float(amount_str.lower().replace('$', '').replace('million', '').strip())
            return num * 1000000
        except:
            return None
    elif 'k' in amount_str.lower():
        # Extract number before 'k'
        try:
            num = float(amount_str.lower().replace('$', '').replace('k', '').strip())
            return num * 1000
        except:
            return None
    
    # Try to parse as regular number
    try:
        return float(amount_str.replace('$', '').replace(',', ''))
    except:
        return None


def parse_number(value: str) -> Optional[int]:
    """Parse number with commas to int"""
    if not value or value.strip() in ['-', '']:
        return None
    
    try:
        # Remove commas and spaces, then convert to int
        return int(value.replace(',', '').replace(' ', ''))
    except:
        return None


def parse_percentage(value: str) -> Optional[float]:
    """Parse percentage string to float"""
    if not value or value.strip() in ['-', '']:
        return None
    
    try:
        # Remove % sign and convert to float
        return float(value.replace('%', '').strip())
    except:
        return None


def load_jb_hunt_carriers(filename: str) -> List[Dict]:
    """Load carriers from JB Hunt CSV file"""
    carriers = []
    insurance_providers = set()
    
    with open(filename, 'r', encoding='utf-8') as csvfile:
        # Skip empty lines at the beginning
        lines = csvfile.readlines()
        # Find the first non-empty line (header)
        csv_lines = [line for line in lines if line.strip()]
        
        # Create CSV reader from cleaned lines
        import io
        csv_content = ''.join(csv_lines)
        reader = csv.DictReader(io.StringIO(csv_content))
        
        for row in reader:
            # Skip empty rows
            if not row.get('dot_number') or row['dot_number'].strip() == '':
                continue
            
            # Parse the carrier data - handle column names with spaces
            carrier = {
                'usdot': parse_number(row.get('dot_number', '')),
                'jb_carrier': row.get('JB Carrier', '').strip().lower() == 'yes',
                'carrier_name': row.get('Carrier', '').strip(),
                'primary_officer': row.get('Primary Officer', '').strip(),
                'insurance_provider': row.get(' Insurance', row.get('Insurance', '')).strip() if row.get(' Insurance', row.get('Insurance', '')).strip() not in ['n/a', 'N/A', ''] else None,
                'insurance_amount': parse_insurance_amount(row.get('Amount', '')),
                'trucks': parse_number(row.get(' Trucks ', row.get('Trucks', ''))),
                'inspections': parse_number(row.get(' Inspections ', row.get('Inspections', ''))),
                'violations': parse_number(row.get(' Violations ', row.get('Violations', ''))),
                'oos': parse_number(row.get(' OOS ', row.get('OOS', ''))),
                'crashes': parse_number(row.get(' Crashes ', row.get('Crashes', ''))) or 0,  # Default 0 for "-"
                'driver_oos_rate': parse_percentage(row.get('Driver OOS Rate', '')),
                'vehicle_oos_rate': parse_percentage(row.get('Vehicle OOS Rate', '')),
                'mcs150_drivers': parse_number(row.get(' MCS150 Drivers ', row.get('MCS150 Drivers', ''))),
                'mcs150_miles': parse_number(row.get(' MCS150 Miles ', row.get('MCS150 Miles', ''))),
                'ampd': parse_number(row.get(' AMPD ', row.get('AMPD', ''))),
                'data_source': 'JB_HUNT_CSV'
            }
            
            # Skip if USDOT is invalid
            if not carrier['usdot']:
                continue
            
            # Track insurance providers
            if carrier['insurance_provider']:
                insurance_providers.add(carrier['insurance_provider'])
            
            carriers.append(carrier)
    
    return carriers, list(insurance_providers)


def create_jb_hunt_target_company(api_url: str, api_key: str) -> bool:
    """Create JB Hunt as a target company if it doesn't exist"""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    
    # JB Hunt's actual DOT number
    jb_hunt_dot = 39874  # JB Hunt Transport Services Inc.
    
    # Check if already exists
    check_url = f"{api_url}/target-companies/{jb_hunt_dot}"
    response = requests.get(check_url, headers=headers)
    
    if response.status_code == 200:
        print(f"✓ JB Hunt target company already exists (DOT: {jb_hunt_dot})")
        return True
    
    # Create JB Hunt
    jb_hunt_data = {
        "dot_number": jb_hunt_dot,
        "legal_name": "J.B. Hunt Transport Services, Inc.",
        "entity_type": "BROKER",
        "authority_status": "ACTIVE",
        "data_source": "JB_HUNT_IMPORT"
    }
    
    create_url = f"{api_url}/target-companies/"
    response = requests.post(create_url, json=jb_hunt_data, headers=headers)
    
    if response.status_code == 201:
        print(f"✓ Created JB Hunt target company (DOT: {jb_hunt_dot})")
        return True
    else:
        print(f"✗ Failed to create JB Hunt: {response.status_code} - {response.text}")
        return False


def create_insurance_providers(api_url: str, api_key: str, providers: List[str]) -> Dict[str, str]:
    """Create insurance providers and return mapping of name to ID"""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    provider_map = {}
    
    for provider_name in providers:
        # Create provider
        provider_data = {
            "name": provider_name,
            "data_source": "JB_HUNT_CSV"
        }
        
        url = f"{api_url}/insurance-providers/"
        response = requests.post(url, json=provider_data, headers=headers)
        
        if response.status_code == 201:
            provider_id = response.json()['provider_id']
            provider_map[provider_name] = provider_id
            print(f"  ✓ Created insurance provider: {provider_name}")
        elif response.status_code == 409:
            # Already exists, fetch it
            print(f"  - Insurance provider already exists: {provider_name}")
        else:
            print(f"  ✗ Failed to create provider {provider_name}: {response.status_code}")
    
    return provider_map


def import_carriers(api_url: str, api_key: str, carriers: List[Dict], batch_size: int = 50):
    """Import carriers using bulk endpoint"""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    
    total = len(carriers)
    successful = 0
    failed = 0
    
    print(f"\nImporting {total} carriers in batches of {batch_size}...")
    
    for i in range(0, total, batch_size):
        batch = carriers[i:i+batch_size]
        
        # Use bulk endpoint
        url = f"{api_url}/carriers/bulk"
        response = requests.post(url, json=batch, headers=headers)
        
        if response.status_code == 201:
            created = response.json().get('created', len(batch))
            successful += created
            print(f"  ✓ Batch {i//batch_size + 1}: Created {created} carriers")
        else:
            failed += len(batch)
            print(f"  ✗ Batch {i//batch_size + 1} failed: {response.status_code} - {response.text}")
    
    print(f"\nImport complete: {successful} successful, {failed} failed")
    return successful, failed


def create_carrier_relationships(api_url: str, api_key: str, carriers: List[Dict]):
    """Create relationships between carriers and JB Hunt"""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    
    jb_hunt_dot = 39874
    successful = 0
    failed = 0
    
    print(f"\nCreating contracts between carriers and JB Hunt...")
    
    for carrier in carriers:
        if not carrier['usdot']:
            continue
        
        # Create contract with JB Hunt
        contract_data = {
            "target_dot_number": jb_hunt_dot,
            "active": True
        }
        
        url = f"{api_url}/carriers/{carrier['usdot']}/contract"
        response = requests.post(url, json=contract_data, headers=headers)
        
        if response.status_code == 201:
            successful += 1
            if successful % 10 == 0:
                print(f"  ✓ Created {successful} contracts...")
        else:
            failed += 1
            if failed <= 5:  # Only show first 5 failures
                print(f"  ✗ Failed to create contract for USDOT {carrier['usdot']}: {response.status_code}")
    
    print(f"Relationships complete: {successful} successful, {failed} failed")
    return successful, failed


def create_insurance_relationships(api_url: str, api_key: str, carriers: List[Dict]):
    """Create INSURED_BY relationships between carriers and insurance providers"""
    headers = {"X-API-Key": api_key}
    
    successful = 0
    failed = 0
    skipped = 0
    
    print(f"\nCreating insurance relationships...")
    
    for carrier in carriers:
        if not carrier['usdot']:
            continue
        
        # Skip if no insurance provider or it's n/a
        if not carrier.get('insurance_provider') or carrier['insurance_provider'].lower() in ['n/a', 'na']:
            skipped += 1
            continue
        
        # Create insurance relationship
        url = f"{api_url}/carriers/{carrier['usdot']}/insurance"
        params = {
            "provider_name": carrier['insurance_provider']
        }
        if carrier.get('insurance_amount'):
            params["amount"] = carrier['insurance_amount']
        
        response = requests.post(url, headers=headers, params=params)
        
        if response.status_code in [200, 201]:
            successful += 1
            if successful % 10 == 0:
                print(f"  ✓ Created {successful} insurance relationships...")
        else:
            failed += 1
            if failed <= 5:  # Only show first 5 failures
                print(f"  ✗ Failed to link carrier {carrier['usdot']} to {carrier['insurance_provider']}: {response.status_code}")
    
    print(f"Insurance relationships complete: {successful} created, {failed} failed, {skipped} skipped (no insurance)")
    return successful, failed


def main():
    """Main import function"""
    # Configuration
    API_URL = os.getenv("API_URL", "http://localhost:8000")
    API_KEY = os.getenv("API_KEY", "test-api-key")
    CSV_FILE = "csv/real_data/jb_hunt_carriers.csv"
    
    # Check if file exists
    if not os.path.exists(CSV_FILE):
        print(f"Error: CSV file not found: {CSV_FILE}")
        sys.exit(1)
    
    print("=" * 60)
    print("JB Hunt Carriers Import Script")
    print("=" * 60)
    
    # Step 1: Load CSV data
    print(f"\n1. Loading carriers from {CSV_FILE}...")
    carriers, insurance_providers = load_jb_hunt_carriers(CSV_FILE)
    print(f"   ✓ Loaded {len(carriers)} carriers")
    print(f"   ✓ Found {len(insurance_providers)} unique insurance providers")
    
    # Step 2: Create JB Hunt as target company
    print("\n2. Creating JB Hunt target company...")
    if not create_jb_hunt_target_company(API_URL, API_KEY):
        print("   Warning: Could not create JB Hunt, continuing anyway...")
    
    # Step 3: Create insurance providers
    print(f"\n3. Creating {len(insurance_providers)} insurance providers...")
    provider_map = create_insurance_providers(API_URL, API_KEY, insurance_providers)
    
    # Step 4: Import carriers
    print("\n4. Importing carriers...")
    successful, failed = import_carriers(API_URL, API_KEY, carriers)
    
    # Step 5: Create carrier-to-JB Hunt relationships
    print("\n5. Creating carrier-to-JB Hunt relationships...")
    rel_success, rel_failed = create_carrier_relationships(API_URL, API_KEY, carriers)
    
    # Step 6: Create insurance relationships
    print("\n6. Creating insurance relationships...")
    ins_success, ins_failed = create_insurance_relationships(API_URL, API_KEY, carriers)
    
    # Summary
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)
    print(f"Carriers imported: {successful}/{len(carriers)}")
    print(f"Carrier-JB Hunt relationships: {rel_success}/{len(carriers)}")
    print(f"Insurance relationships: {ins_success}/{len(carriers) - 2}")  # -2 for carriers with n/a insurance
    print(f"Insurance providers: {len(insurance_providers)}")
    
    if failed > 0 or rel_failed > 0 or ins_failed > 0:
        print(f"\n⚠ Some operations failed. Check logs for details.")
        sys.exit(1)
    else:
        print(f"\n✅ Import completed successfully!")


if __name__ == "__main__":
    main()