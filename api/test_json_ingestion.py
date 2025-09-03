#!/usr/bin/env python3
"""
Test script to demonstrate JSON ingestion endpoint with proper boolean handling.
This script shows that the enrichment feature now works correctly with boolean values.
"""

import base64
import json
import requests
import sys
from pathlib import Path

# API configuration
API_BASE_URL = "http://localhost:8000"
API_KEY = "test-api-key"  # Replace with your actual API key

# Sample CSV data
SAMPLE_CSV = """dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks
1234567,Yes,Test Carrier LLC,John Doe,State National,$1 Million,25
7654321,Yes,Another Carrier Inc,Jane Smith,Test Insurance,$750k,15
9999999,Yes,Third Carrier Corp,Bob Johnson,Another Insurance,$2 Million,50"""


def test_json_ingestion_without_enrichment():
    """Test JSON ingestion with enrichment=false (synchronous)"""
    print("\n=== Test 1: JSON ingestion WITHOUT enrichment (synchronous) ===")
    
    # Encode CSV to base64
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    # Create JSON request with boolean false (not string "false")
    request_data = {
        "csv_content": encoded_csv,
        "target_company": "JB_HUNT",
        "enable_enrichment": False,  # Boolean false
        "skip_invalid": True
    }
    
    print(f"Request data: {json.dumps(request_data, indent=2)}")
    print(f"Note: enable_enrichment is type {type(request_data['enable_enrichment'])}")
    
    # Send request
    response = requests.post(
        f"{API_BASE_URL}/ingest/",
        json=request_data,
        headers={"X-API-Key": API_KEY}
    )
    
    print(f"\nResponse status: {response.status_code}")
    result = response.json()
    print(f"Response: {json.dumps(result, indent=2)}")
    
    # Verify synchronous completion
    assert result["status"] in ["completed", "completed_with_errors"], \
        f"Expected synchronous completion, got status: {result['status']}"
    assert "summary" in result, "Should have summary for synchronous execution"
    
    print("✅ Test 1 PASSED: Synchronous execution completed successfully")
    return result


def test_json_ingestion_with_enrichment():
    """Test JSON ingestion with enrichment=true (asynchronous)"""
    print("\n=== Test 2: JSON ingestion WITH enrichment (asynchronous) ===")
    
    # Encode CSV to base64
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    # Create JSON request with boolean true (not string "true")
    request_data = {
        "csv_content": encoded_csv,
        "target_company": "JB_HUNT",
        "enable_enrichment": True,  # Boolean true
        "skip_invalid": True
    }
    
    print(f"Request data: {json.dumps(request_data, indent=2)}")
    print(f"Note: enable_enrichment is type {type(request_data['enable_enrichment'])}")
    
    # Send request
    response = requests.post(
        f"{API_BASE_URL}/ingest/",
        json=request_data,
        headers={"X-API-Key": API_KEY}
    )
    
    print(f"\nResponse status: {response.status_code}")
    result = response.json()
    print(f"Response: {json.dumps(result, indent=2)}")
    
    # Verify asynchronous processing
    assert result["status"] == "processing", \
        f"Expected 'processing' status for async execution, got: {result['status']}"
    assert "job_id" in result, "Should have job_id for async execution"
    assert result.get("enrichment", {}).get("enabled") == True, \
        "Enrichment should be enabled"
    assert result.get("enrichment", {}).get("status") == "queued", \
        "Enrichment should be queued"
    
    print("✅ Test 2 PASSED: Asynchronous execution triggered successfully")
    return result


def test_file_path_with_enrichment():
    """Test JSON ingestion with file path and enrichment"""
    print("\n=== Test 3: File path WITH enrichment ===")
    
    # Check if sample file exists
    file_path = "csv/real_data/jb_hunt_carriers.csv"
    if not Path(file_path).exists():
        print(f"⚠️  Skipping test - sample file not found: {file_path}")
        return None
    
    # Create JSON request with file path
    request_data = {
        "file_path": file_path,
        "target_company": "JB_HUNT",
        "enable_enrichment": True,  # Boolean true
        "skip_invalid": True
    }
    
    print(f"Request data: {json.dumps(request_data, indent=2)}")
    
    # Send request
    response = requests.post(
        f"{API_BASE_URL}/ingest/",
        json=request_data,
        headers={"X-API-Key": API_KEY}
    )
    
    print(f"\nResponse status: {response.status_code}")
    result = response.json()
    print(f"Response: {json.dumps(result, indent=2)}")
    
    # Verify asynchronous processing
    assert result["status"] == "processing", \
        f"Expected 'processing' status, got: {result['status']}"
    
    print("✅ Test 3 PASSED: File path with enrichment works correctly")
    return result


def test_sample_endpoint():
    """Test the /ingest/sample endpoint"""
    print("\n=== Test 4: Sample endpoint ===")
    
    response = requests.get(
        f"{API_BASE_URL}/ingest/sample",
        headers={"X-API-Key": API_KEY}
    )
    
    print(f"Response status: {response.status_code}")
    result = response.json()
    
    assert "examples" in result, "Should have examples"
    assert "curl_examples" in result, "Should have curl examples"
    
    print(f"Found {len(result['examples'])} examples")
    print(f"Found {len(result['curl_examples'])} curl examples")
    
    print("✅ Test 4 PASSED: Sample endpoint returns expected format")
    return result


def main():
    """Run all tests"""
    print("=" * 60)
    print("JSON Ingestion Endpoint Test Suite")
    print("Testing boolean parameter handling and enrichment triggering")
    print("=" * 60)
    
    try:
        # Test without enrichment (synchronous)
        test_json_ingestion_without_enrichment()
        
        # Test with enrichment (asynchronous)
        test_json_ingestion_with_enrichment()
        
        # Test file path with enrichment
        test_file_path_with_enrichment()
        
        # Test sample endpoint
        test_sample_endpoint()
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED!")
        print("The JSON ingestion endpoint now correctly handles boolean parameters.")
        print("When enable_enrichment=true, it triggers background processing.")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        print("\n❌ Could not connect to API. Is the server running?")
        print("   Start the API with: cd api && ./start_dev.sh")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()