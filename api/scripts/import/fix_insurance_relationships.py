#!/usr/bin/env python3
"""
Fix missing insurance relationships for existing carriers.
This script creates INSURED_BY relationships between carriers and insurance providers
that were imported but not linked in the original import.
"""

import os
import sys
import requests
from pathlib import Path
from typing import Dict, List, Tuple
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

load_dotenv()


def get_all_carriers(api_url: str, api_key: str, limit: int = 1000) -> List[Dict]:
    """Fetch all carriers from the API"""
    headers = {"X-API-Key": api_key}
    url = f"{api_url}/carriers"
    params = {"limit": limit}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch carriers: {response.status_code} - {response.text}")
        return []


def create_insurance_relationship(
    api_url: str, 
    api_key: str, 
    usdot: int, 
    provider_name: str, 
    amount: float = None
) -> bool:
    """Create INSURED_BY relationship between carrier and insurance provider"""
    headers = {"X-API-Key": api_key}
    url = f"{api_url}/carriers/{usdot}/insurance"
    params = {
        "provider_name": provider_name
    }
    if amount is not None:
        params["amount"] = amount
    
    response = requests.post(url, headers=headers, params=params)
    
    if response.status_code in [200, 201]:
        return True
    elif response.status_code == 409:
        # Relationship already exists
        return True
    else:
        print(f"  ✗ Failed to link carrier {usdot} to {provider_name}: {response.status_code}")
        return False


def fix_insurance_relationships(api_url: str, api_key: str) -> Tuple[int, int, int]:
    """
    Create missing insurance relationships for all carriers.
    Returns: (relationships_created, relationships_failed, carriers_skipped)
    """
    print("Fetching all carriers...")
    carriers = get_all_carriers(api_url, api_key)
    
    if not carriers:
        print("No carriers found!")
        return 0, 0, 0
    
    print(f"Found {len(carriers)} carriers")
    
    relationships_created = 0
    relationships_failed = 0
    carriers_skipped = 0
    
    print("\nCreating insurance relationships...")
    
    for carrier in carriers:
        usdot = carrier.get('usdot')
        provider_name = carrier.get('insurance_provider')
        amount = carrier.get('insurance_amount')
        
        # Skip if no insurance provider or it's n/a
        if not provider_name or provider_name.lower() in ['n/a', 'na']:
            carriers_skipped += 1
            continue
        
        # Create the relationship
        success = create_insurance_relationship(
            api_url, api_key, usdot, provider_name, amount
        )
        
        if success:
            relationships_created += 1
            if relationships_created % 10 == 0:
                print(f"  ✓ Created {relationships_created} relationships...")
        else:
            relationships_failed += 1
    
    return relationships_created, relationships_failed, carriers_skipped


def main():
    """Main function to fix insurance relationships"""
    # Configuration
    API_URL = os.getenv("API_URL", "http://localhost:8000")
    API_KEY = os.getenv("API_KEY", "test-api-key")
    
    print("=" * 60)
    print("Insurance Relationship Fix Script")
    print("=" * 60)
    print(f"API URL: {API_URL}")
    print()
    
    # Run the fix
    created, failed, skipped = fix_insurance_relationships(API_URL, API_KEY)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"✓ Relationships created: {created}")
    print(f"✗ Relationships failed: {failed}")
    print(f"- Carriers skipped (no insurance): {skipped}")
    print(f"Total carriers processed: {created + failed + skipped}")
    
    if created > 0:
        print(f"\n✅ Successfully created {created} insurance relationships!")
    
    if failed > 0:
        print(f"\n⚠️  {failed} relationships failed to create. Check logs for details.")
        sys.exit(1)
    elif created == 0 and skipped == 0:
        print("\n⚠️  No relationships created. This might mean they already exist.")
    else:
        print("\n✅ Script completed successfully!")


if __name__ == "__main__":
    main()