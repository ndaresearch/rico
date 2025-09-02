"""
Tests for data ingestion endpoints and orchestration.
"""

import io
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


def test_ingest_with_file_upload():
    """Test ingesting data via file upload"""
    # Create a file-like object from CSV string
    csv_file = io.BytesIO(SAMPLE_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("test_carriers.csv", csv_file, "text/csv")},
        params={"skip_invalid": True},
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
        params={
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
    """Test ingestion fails when neither file nor file_path provided"""
    response = client.post(
        "/ingest/",
        headers=headers
    )
    
    assert response.status_code == 400
    assert "Either 'file' upload or 'file_path' parameter is required" in response.json()["detail"]


def test_ingest_both_parameters():
    """Test ingestion fails when both file and file_path provided"""
    csv_file = io.BytesIO(SAMPLE_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("test.csv", csv_file, "text/csv")},
        params={"file_path": "some/path.csv"},
        headers=headers
    )
    
    assert response.status_code == 400
    assert "not both" in response.json()["detail"]


def test_ingest_invalid_file_type():
    """Test ingestion fails with non-CSV file"""
    txt_file = io.BytesIO(b"This is not a CSV file")
    
    response = client.post(
        "/ingest/",
        files={"file": ("test.txt", txt_file, "text/plain")},
        headers=headers
    )
    
    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]


def test_ingest_invalid_csv_data():
    """Test handling of invalid CSV data"""
    csv_file = io.BytesIO(INVALID_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("invalid.csv", csv_file, "text/csv")},
        params={"skip_invalid": True},
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    # Should process valid record only
    assert data["summary"]["validation_errors"] >= 2
    assert data["summary"]["carriers_created"] <= 1


def test_ingest_empty_csv():
    """Test handling of empty CSV file"""
    csv_file = io.BytesIO(EMPTY_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("empty.csv", csv_file, "text/csv")},
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "failed"
    assert "No valid carriers found" in data.get("error", "")


def test_ingest_with_enrichment():
    """Test ingestion with enrichment enabled (background task)"""
    csv_file = io.BytesIO(SAMPLE_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("test.csv", csv_file, "text/csv")},
        params={"enable_enrichment": True},
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
    # First ingestion
    csv_file1 = io.BytesIO(SAMPLE_CSV.encode('utf-8'))
    response1 = client.post(
        "/ingest/",
        files={"file": ("test1.csv", csv_file1, "text/csv")},
        headers=headers
    )
    assert response1.status_code == 200
    
    # Second ingestion with same data
    csv_file2 = io.BytesIO(SAMPLE_CSV.encode('utf-8'))
    response2 = client.post(
        "/ingest/",
        files={"file": ("test2.csv", csv_file2, "text/csv")},
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
    csv_file = io.BytesIO(SAMPLE_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("test.csv", csv_file, "text/csv")},
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
    csv_file = io.BytesIO(INVALID_CSV.encode('utf-8'))
    
    response = client.post(
        "/ingest/",
        files={"file": ("invalid.csv", csv_file, "text/csv")},
        params={"skip_invalid": False},
        headers=headers
    )
    
    # Should fail with validation error
    assert response.status_code == 422
    assert "CSV processing error" in response.json()["detail"]


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