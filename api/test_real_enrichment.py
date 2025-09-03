#!/usr/bin/env python3
"""
Test enrichment with real USDOT numbers that should exist in SearchCarriers.
These are actual trucking companies that should have insurance history.
"""

import base64
import json
import requests
import time

# API configuration
API_BASE_URL = "http://localhost:8000"
API_KEY = "ca8b3dc6d5600e1d756c684d85e3ffb4de284aaca9eb8609f9502aabd842a72f"

# Real carriers that should have SearchCarriers data
# These are well-known carriers that likely have insurance history
REAL_CARRIERS_CSV = """dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks
2218830,Yes,KNIGHT TRANSPORTATION INC,Dave Jackson,Unknown,$1 Million,5000
233574,Yes,SWIFT TRANSPORTATION CO OF ARIZONA LLC,Richard Stocking,Unknown,$1 Million,7000
190979,Yes,SCHNEIDER NATIONAL CARRIERS INC,Mark Rourke,Unknown,$1 Million,9000"""


def test_real_enrichment():
    """Test with real carriers that should have SearchCarriers data"""
    print("=" * 60)
    print("Testing with REAL Carrier DOT Numbers")
    print("=" * 60)
    
    # Encode CSV
    encoded_csv = base64.b64encode(REAL_CARRIERS_CSV.encode('utf-8')).decode('utf-8')
    
    request_data = {
        "csv_content": encoded_csv,
        "target_company": "JB_HUNT",
        "enable_enrichment": True,
        "skip_invalid": True
    }
    
    print("\n📤 Testing with real carriers:")
    print("  - KNIGHT TRANSPORTATION (DOT: 2218830)")
    print("  - SWIFT TRANSPORTATION (DOT: 233574)")
    print("  - SCHNEIDER NATIONAL (DOT: 190979)")
    
    headers = {"X-API-Key": API_KEY}
    
    response = requests.post(
        f"{API_BASE_URL}/ingest/",
        json=request_data,
        headers=headers
    )
    
    print(f"\n📥 Response status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"\nResponse:")
        print(json.dumps(result, indent=2))
        
        if result.get("status") == "processing":
            job_id = result.get('job_id')
            print(f"\n✅ Enrichment triggered for real carriers!")
            print(f"   Job ID: {job_id}")
            
            # Wait longer for real API calls
            print(f"\n⏳ Waiting 5 seconds for enrichment to complete...")
            time.sleep(5)
            
            # Check Docker logs to see results
            print("\n📊 To see enrichment results, run:")
            print(f"   docker logs rico-api --tail 100 | grep -E '(Found.*insurance|policies created|gaps detected)'")
            
    else:
        print(f"❌ Request failed: {response.text}")


def check_enriched_carriers():
    """Check carriers after enrichment"""
    print("\n📊 Checking enriched carriers...")
    
    headers = {"X-API-Key": API_KEY}
    
    # Check specific carriers
    for usdot in [2218830, 233574, 190979]:
        response = requests.get(
            f"{API_BASE_URL}/carriers/{usdot}",
            headers=headers
        )
        
        if response.status_code == 200:
            carrier = response.json()
            print(f"\n✓ {carrier['carrier_name']} (DOT: {carrier['usdot']})")
            
            # Check for insurance relationships (would need a specific endpoint)
            # For now just show the carrier exists
        elif response.status_code == 404:
            print(f"\n✗ Carrier {usdot} not found")


def main():
    print("\n🚀 Testing SearchCarriers Enrichment with REAL Carriers\n")
    
    # Run enrichment with real carriers
    test_real_enrichment()
    
    # Give it time to process
    time.sleep(2)
    
    # Check the results
    check_enriched_carriers()
    
    print("\n" + "=" * 60)
    print("💡 Check Docker logs to see SearchCarriers API results:")
    print("   docker logs rico-api --tail 50 | grep -i insurance")
    print("=" * 60)


if __name__ == "__main__":
    main()