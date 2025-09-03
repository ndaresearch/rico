#!/usr/bin/env python3
"""
Test the JSON ingestion endpoint with enrichment enabled.
This will trigger the SearchCarriers API enrichment in the background.
"""

import base64
import json
import requests
import time

# API configuration - using Docker container on port 8000
API_BASE_URL = "http://localhost:8000"

# Small test CSV with real USDOT numbers that should exist in SearchCarriers
TEST_CSV = """dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks
1234567,Yes,Test Carrier One LLC,John Smith,Unknown Insurance,$1 Million,25
2345678,Yes,Test Carrier Two Inc,Jane Doe,Unknown Insurance,$750k,15
3456789,Yes,Test Carrier Three Corp,Bob Johnson,Unknown Insurance,$2 Million,50"""


def test_ingestion_with_enrichment():
    """Test ingestion with enrichment enabled"""
    print("=" * 60)
    print("Testing Ingestion with SearchCarriers Enrichment")
    print("=" * 60)
    
    # Encode CSV to base64
    encoded_csv = base64.b64encode(TEST_CSV.encode('utf-8')).decode('utf-8')
    
    # Create request with enrichment enabled
    request_data = {
        "csv_content": encoded_csv,
        "target_company": "JB_HUNT",
        "enable_enrichment": True,  # This will trigger background enrichment
        "skip_invalid": True
    }
    
    print("\n📤 Sending ingestion request with enrichment=true...")
    print(f"Request body (truncated):")
    print(json.dumps({
        **request_data,
        "csv_content": request_data["csv_content"][:50] + "..."
    }, indent=2))
    
    # Send request with API key
    headers = {
        "X-API-Key": "ca8b3dc6d5600e1d756c684d85e3ffb4de284aaca9eb8609f9502aabd842a72f"
    }
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
        
        # Check if enrichment was triggered
        if result.get("status") == "processing":
            print("\n✅ SUCCESS: Enrichment triggered!")
            print(f"   Job ID: {result.get('job_id')}")
            print(f"   Status: {result.get('status')}")
            print(f"   Enrichment enabled: {result.get('enrichment', {}).get('enabled')}")
            print(f"   Enrichment status: {result.get('enrichment', {}).get('status')}")
            
            # Try to check job status
            job_id = result.get('job_id')
            if job_id:
                print(f"\n⏳ Waiting 2 seconds before checking job status...")
                time.sleep(2)
                
                status_response = requests.get(
                    f"{API_BASE_URL}/ingest/status/{job_id}",
                    headers=headers
                )
                if status_response.status_code == 200:
                    status_result = status_response.json()
                    print(f"\nJob status check:")
                    print(json.dumps(status_result, indent=2))
                    
        elif result.get("status") in ["completed", "completed_with_errors"]:
            print("\n⚠️  WARNING: Request completed synchronously")
            print("   Enrichment may not have been triggered properly")
            print(f"   Summary: {result.get('summary')}")
            
        else:
            print(f"\n❌ Unexpected status: {result.get('status')}")
            
    else:
        print(f"\n❌ Request failed!")
        print(f"Response: {response.text}")
        
    print("\n" + "=" * 60)


def check_existing_carriers():
    """Check what carriers are currently in the database"""
    print("\n📊 Checking existing carriers in database...")
    
    headers = {
        "X-API-Key": "ca8b3dc6d5600e1d756c684d85e3ffb4de284aaca9eb8609f9502aabd842a72f"
    }
    response = requests.get(f"{API_BASE_URL}/carriers?limit=10", headers=headers)
    
    if response.status_code == 200:
        carriers = response.json()
        print(f"Found {len(carriers)} carriers:")
        for carrier in carriers[:5]:  # Show first 5
            print(f"  - {carrier.get('carrier_name')} (USDOT: {carrier.get('usdot')})")
        if len(carriers) > 5:
            print(f"  ... and {len(carriers) - 5} more")
    else:
        print(f"Could not fetch carriers: {response.status_code}")


def main():
    """Run the enrichment test"""
    print("\n🚀 Testing JSON Ingestion with SearchCarriers Enrichment\n")
    
    # Check existing data
    check_existing_carriers()
    
    # Run ingestion with enrichment
    print("")
    test_ingestion_with_enrichment()
    
    print("\n💡 Note: The enrichment runs in the background.")
    print("   Check the API logs to see the SearchCarriers API calls.")
    print("   Run 'docker logs rico-api -f' to follow the logs.")


if __name__ == "__main__":
    main()