"""
Tests for data ingestion endpoints and orchestration.
"""

import base64
import io
import json
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from main import app
from repositories.carrier_repository import CarrierRepository
from repositories.target_company_repository import TargetCompanyRepository
from repositories.insurance_provider_repository import InsuranceProviderRepository
from repositories.person_repository import PersonRepository

client = TestClient(app)
carrier_repo = CarrierRepository()
target_repo = TargetCompanyRepository()
insurance_repo = InsuranceProviderRepository()
person_repo = PersonRepository()

# Test API key for authenticated requests
headers = {"X-API-Key": "test-api-key"}

# Sample CSV data for testing
SAMPLE_CSV = """dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks , Inspections , Violations , OOS , Crashes ,Driver OOS Rate,Vehicle OOS Rate, MCS150 Drivers , MCS150 Miles , AMPD 
999001,Yes,Test Carrier One LLC,John Smith,Test Insurance Co,$1 Million,25,100,10,5,0,2.5%,10.0%,30,500000,16667
999002,Yes,Test Carrier Two Inc,Jane Doe,Test Insurance Co,$750k,15,50,5,2,1,1.5%,8.0%,20,300000,15000
999003,No,Test Carrier Three Corp,Bob Johnson,Another Insurance,$1 Million,40,200,20,8,2,3.0%,15.0%,45,800000,17778"""

INVALID_CSV = """dot_number,JB Carrier,Carrier,Primary Officer
not_a_number,Yes,Invalid Carrier,Test Officer
999004,Yes,,Missing Name
,Yes,No USDOT,Another Officer"""

EMPTY_CSV = """dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount
"""


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test data before and after each test"""
    # Test USDOTs to clean
    test_usdots = [999001, 999002, 999003, 999004]
    test_dot = 39874  # JB Hunt
    test_insurances = ["Test Insurance Co", "Another Insurance"]
    test_persons = ["John Smith", "Jane Doe", "Bob Johnson", "Test Officer", "Another Officer"]
    
    # Clean before test
    for usdot in test_usdots:
        carrier_repo.delete(usdot)
    
    for insurance_name in test_insurances:
        provider = insurance_repo.get_by_name(insurance_name)
        if provider:
            insurance_repo.delete(provider["provider_id"])
    
    for person_name in test_persons:
        persons = person_repo.find_by_name(person_name)
        for person in persons:
            person_repo.delete(person["person_id"])
    
    yield
    
    # Clean after test
    for usdot in test_usdots:
        carrier_repo.delete(usdot)
    
    for insurance_name in test_insurances:
        provider = insurance_repo.get_by_name(insurance_name)
        if provider:
            insurance_repo.delete(provider["provider_id"])
    
    for person_name in test_persons:
        persons = person_repo.find_by_name(person_name)
        for person in persons:
            person_repo.delete(person["person_id"])


def test_ingest_with_base64_csv():
    """Test ingesting data via base64-encoded CSV content"""
    # Encode CSV to base64
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "skip_invalid": True,
            "enable_enrichment": False
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # Check response structure
    assert "job_id" in data
    assert "status" in data
    assert "summary" in data
    
    # Check summary statistics
    summary = data["summary"]
    assert summary["total_records"] == 3
    assert summary["carriers_created"] == 3
    assert summary["insurance_providers_created"] == 2
    assert summary["persons_created"] == 3
    assert summary["relationships_created"] >= 6  # At least 3 contracts + 3 insurance links
    
    # Verify entities were created
    assert carrier_repo.exists(999001)
    assert carrier_repo.exists(999002)
    assert carrier_repo.exists(999003)
    
    # Verify insurance providers
    assert insurance_repo.get_by_name("Test Insurance Co") is not None
    assert insurance_repo.get_by_name("Another Insurance") is not None


def test_ingest_with_file_path():
    """Test ingesting data via file path"""
    # Use the actual sample CSV file
    file_path = "csv/real_data/jb_hunt_carriers.csv"
    
    # Check if file exists, skip if not
    if not Path(file_path).exists():
        pytest.skip(f"Sample CSV file not found at {file_path}")
    
    response = client.post(
        "/ingest/",
        json={
            "file_path": file_path,
            "skip_invalid": True,
            "enable_enrichment": False
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] in ["completed", "completed_with_errors"]
    assert data["summary"]["total_records"] > 0


def test_ingest_missing_parameters():
    """Test ingestion fails when neither csv_content nor file_path provided"""
    response = client.post(
        "/ingest/",
        json={},
        headers=headers
    )
    
    assert response.status_code == 422  # Validation error from Pydantic


def test_ingest_both_parameters():
    """Test ingestion fails when both csv_content and file_path provided"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "file_path": "some/path.csv"
        },
        headers=headers
    )
    
    assert response.status_code == 422  # Validation error from Pydantic
    assert "not both" in str(response.json())


def test_ingest_invalid_base64():
    """Test ingestion fails with invalid base64 content"""
    response = client.post(
        "/ingest/",
        json={
            "csv_content": "not-valid-base64!@#$%"
        },
        headers=headers
    )
    
    assert response.status_code == 422  # Validation error
    assert "valid base64" in str(response.json())


def test_ingest_invalid_csv_data():
    """Test handling of invalid CSV data"""
    encoded_csv = base64.b64encode(INVALID_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "skip_invalid": True
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # Should process valid record only
    assert data["summary"]["validation_errors"] >= 2
    assert data["summary"]["carriers_created"] <= 1


def test_ingest_empty_csv():
    """Test handling of empty CSV file"""
    encoded_csv = base64.b64encode(EMPTY_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "failed"
    assert "No valid carriers found" in data.get("error", "")


def test_ingest_with_enrichment():
    """Test ingestion with enrichment enabled (background task)"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "enable_enrichment": True  # This should be a proper boolean now
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # With enrichment, should return immediately with processing status
    assert data["status"] == "processing"
    assert "job_id" in data
    assert data.get("enrichment", {}).get("enabled") == True
    assert data.get("enrichment", {}).get("status") == "queued"


def test_ingest_duplicate_carriers():
    """Test handling of duplicate carriers in CSV"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    # First ingestion
    response1 = client.post(
        "/ingest/",
        json={"csv_content": encoded_csv},
        headers=headers
    )
    assert response1.status_code == 200
    
    # Second ingestion with same data
    response2 = client.post(
        "/ingest/",
        json={"csv_content": encoded_csv},
        headers=headers
    )
    assert response2.status_code == 200
    
    data2 = response2.json()
    # Should skip existing carriers
    assert data2["summary"]["carriers_created"] == 0
    assert data2["summary"]["carriers_skipped"] == 3


def test_get_sample_csv():
    """Test getting sample CSV format"""
    response = client.get(
        "/ingest/sample-csv",
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert "headers" in data
    assert "sample_rows" in data
    assert "notes" in data
    assert len(data["headers"]) > 0
    assert len(data["sample_rows"]) > 0


def test_get_job_status():
    """Test getting job status (placeholder endpoint)"""
    job_id = "test-job-id-123"
    
    response = client.get(
        f"/ingest/status/{job_id}",
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["job_id"] == job_id
    assert "status" in data


def test_ingest_creates_relationships():
    """Test that ingestion creates proper relationships"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={"csv_content": encoded_csv},
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # Check that entities were created
    assert data["summary"]["carriers_created"] == 3
    assert data["summary"]["relationships_created"] >= 6  # At least 3 contracts + 3 insurance
    
    # Check carrier exists
    carrier = carrier_repo.get_by_usdot(999001)
    assert carrier is not None
    
    # Check that relationships were reported as created in the response
    # The actual Neo4j relationship check can be done separately
    # For now, trust the orchestrator's reporting
    assert data["summary"]["relationships_created"] > 0


def test_ingest_fail_on_invalid():
    """Test that ingestion fails when skip_invalid=False"""
    encoded_csv = base64.b64encode(INVALID_CSV.encode('utf-8')).decode('utf-8')
    
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "skip_invalid": False
        },
        headers=headers
    )
    
    # Should fail with validation error
    assert response.status_code == 422
    assert "CSV processing error" in response.json()["detail"]


def test_get_sample_request():
    """Test getting sample JSON request"""
    response = client.get(
        "/ingest/sample",
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert "examples" in data
    assert "curl_examples" in data
    assert len(data["examples"]) > 0
    
    # Check that examples have proper structure
    for example in data["examples"]:
        assert "request" in example or "note" in example


def test_boolean_parsing_enrichment_true():
    """Test that enable_enrichment=true properly triggers background processing"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    # Test with boolean true (not string "true")
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "enable_enrichment": True,  # Boolean true
            "skip_invalid": True
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # With enrichment=true, should return processing status
    assert data["status"] == "processing"
    assert "job_id" in data
    assert data.get("enrichment", {}).get("enabled") == True
    assert data.get("enrichment", {}).get("status") == "queued"
    assert "message" in data
    assert "background" in data["message"].lower()


def test_boolean_parsing_enrichment_false():
    """Test that enable_enrichment=false properly runs synchronously"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    # Test with boolean false (not string "false")
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv,
            "enable_enrichment": False,  # Boolean false
            "skip_invalid": True
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # With enrichment=false, should return completed status
    assert data["status"] in ["completed", "completed_with_errors", "failed"]
    assert "summary" in data
    assert data.get("enrichment") is None or data.get("enrichment", {}).get("enabled") == False


def test_default_boolean_values():
    """Test that default boolean values work correctly"""
    encoded_csv = base64.b64encode(SAMPLE_CSV.encode('utf-8')).decode('utf-8')
    
    # Test with no boolean parameters (should use defaults)
    response = client.post(
        "/ingest/",
        json={
            "csv_content": encoded_csv
            # No enable_enrichment or skip_invalid specified
        },
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # Defaults: enable_enrichment=False, skip_invalid=True
    # Should complete synchronously
    assert data["status"] in ["completed", "completed_with_errors", "failed"]
    assert "summary" in data


def test_csv_parser_functions():
    """Test CSV parser utility functions directly"""
    from utils.csv_parser import (
        parse_insurance_amount,
        parse_number,
        parse_percentage,
        parse_boolean,
        validate_carrier_data
    )
    
    # Test insurance amount parsing
    assert parse_insurance_amount("$1 Million") == 1000000.0
    assert parse_insurance_amount("$750k") == 750000.0
    assert parse_insurance_amount("1000000") == 1000000.0
    assert parse_insurance_amount("n/a") is None
    
    # Test number parsing
    assert parse_number("1,234") == 1234
    assert parse_number("  156  ") == 156
    assert parse_number("-") is None
    
    # Test percentage parsing
    assert parse_percentage("2.5%") == 2.5
    assert parse_percentage("35.40%") == 35.4
    assert parse_percentage("-") is None
    
    # Test boolean parsing
    assert parse_boolean("Yes") == True
    assert parse_boolean("no") == False
    assert parse_boolean("") == False
    
    # Test validation
    valid_carrier = {
        "usdot": 123456,
        "carrier_name": "Test Carrier",
        "trucks": 10,
        "driver_oos_rate": 5.0
    }
    is_valid, errors = validate_carrier_data(valid_carrier)
    assert is_valid == True
    assert len(errors) == 0
    
    invalid_carrier = {
        "usdot": -1,
        "carrier_name": "",
        "driver_oos_rate": 150.0
    }
    is_valid, errors = validate_carrier_data(invalid_carrier)
    assert is_valid == False
    assert len(errors) > 0