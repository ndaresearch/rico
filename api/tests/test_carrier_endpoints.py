import pytest
from fastapi.testclient import TestClient

from main import app
from repositories.carrier_repository import CarrierRepository
from repositories.target_company_repository import TargetCompanyRepository
from models.target_company import TargetCompany

client = TestClient(app)
carrier_repo = CarrierRepository()
target_repo = TargetCompanyRepository()

# Test API key for authenticated requests
headers = {"X-API-Key": "test-api-key"}


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test data before and after each test"""
    # Clean before
    test_usdots = [777001, 777002, 777003]
    test_dot = 888999  # For target company
    
    for usdot in test_usdots:
        carrier_repo.delete(usdot)
    target_repo.delete(test_dot)
    
    yield
    
    # Clean after
    for usdot in test_usdots:
        carrier_repo.delete(usdot)
    target_repo.delete(test_dot)


def test_create_carrier():
    """Test creating a new carrier"""
    carrier_data = {
        "usdot": 777001,
        "carrier_name": "Test Carrier LLC",
        "primary_officer": "John Doe",
        "jb_carrier": True,
        "trucks": 25,
        "violations": 5,
        "crashes": 0
    }
    
    response = client.post("/carriers/", json=carrier_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["usdot"] == 777001
    assert data["carrier_name"] == "Test Carrier LLC"
    assert data["jb_carrier"] == True


def test_create_duplicate_carrier():
    """Test creating a duplicate carrier returns 409"""
    carrier_data = {
        "usdot": 777001,
        "carrier_name": "Test Carrier",
        "primary_officer": "Jane Doe"
    }
    
    # Create first carrier
    response = client.post("/carriers/", json=carrier_data, headers=headers)
    assert response.status_code == 201
    
    # Try to create duplicate
    response = client.post("/carriers/", json=carrier_data, headers=headers)
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_get_carrier():
    """Test getting a carrier by USDOT number"""
    # First create a carrier
    carrier_data = {
        "usdot": 777001,
        "carrier_name": "Test Carrier",
        "primary_officer": "Test Officer",
        "trucks": 15,
        "driver_oos_rate": 2.5
    }
    client.post("/carriers/", json=carrier_data, headers=headers)
    
    # Get the carrier
    response = client.get("/carriers/777001", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["usdot"] == 777001
    assert data["trucks"] == 15
    assert data["driver_oos_rate"] == 2.5


def test_update_carrier():
    """Test updating a carrier"""
    # Create a carrier
    carrier_data = {
        "usdot": 777001,
        "carrier_name": "Test Carrier",
        "primary_officer": "Test Officer",
        "trucks": 10
    }
    client.post("/carriers/", json=carrier_data, headers=headers)
    
    # Update the carrier
    update_data = {
        "trucks": 20,
        "violations": 8,
        "driver_oos_rate": 5.5
    }
    response = client.patch("/carriers/777001", json=update_data, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["trucks"] == 20
    assert data["violations"] == 8
    assert data["driver_oos_rate"] == 5.5


def test_delete_carrier():
    """Test deleting a carrier"""
    # Create a carrier
    carrier_data = {
        "usdot": 777001,
        "carrier_name": "Test Carrier",
        "primary_officer": "Test Officer"
    }
    client.post("/carriers/", json=carrier_data, headers=headers)
    
    # Delete the carrier
    response = client.delete("/carriers/777001", headers=headers)
    assert response.status_code == 204
    
    # Verify it's deleted
    response = client.get("/carriers/777001", headers=headers)
    assert response.status_code == 404


def test_get_carriers_with_filters():
    """Test getting carriers with filters"""
    # Create carriers with different attributes
    carriers = [
        {
            "usdot": 777001,
            "carrier_name": "JB Carrier 1",
            "primary_officer": "Officer 1",
            "jb_carrier": True,
            "violations": 10
        },
        {
            "usdot": 777002,
            "carrier_name": "Non-JB Carrier",
            "primary_officer": "Officer 2",
            "jb_carrier": False,
            "violations": 5
        },
        {
            "usdot": 777003,
            "carrier_name": "JB Carrier 2",
            "primary_officer": "Officer 3",
            "jb_carrier": True,
            "violations": 15
        }
    ]
    
    for carrier in carriers:
        client.post("/carriers/", json=carrier, headers=headers)
    
    # Filter by JB carrier
    response = client.get("/carriers/?jb_carrier=true", headers=headers)
    assert response.status_code == 200
    data = response.json()
    jb_carriers = [c for c in data if c["usdot"] >= 777000]
    assert all(c["jb_carrier"] == True for c in jb_carriers)
    
    # Filter by minimum violations
    response = client.get("/carriers/?min_violations=10", headers=headers)
    assert response.status_code == 200
    data = response.json()
    test_carriers = [c for c in data if c["usdot"] >= 777000]
    if test_carriers:
        assert all(c["violations"] >= 10 for c in test_carriers)


def test_create_carrier_contract():
    """Test creating a contract between carrier and target company"""
    # Create a target company
    target_data = TargetCompany(
        dot_number=888999,
        legal_name="Test Target Company",
        entity_type="BROKER"
    )
    target_repo.create(target_data)
    
    # Create a carrier
    carrier_data = {
        "usdot": 777001,
        "carrier_name": "Test Carrier",
        "primary_officer": "Test Officer"
    }
    client.post("/carriers/", json=carrier_data, headers=headers)
    
    # Create contract
    contract_data = {
        "target_dot_number": 888999,
        "active": True
    }
    response = client.post("/carriers/777001/contract", json=contract_data, headers=headers)
    assert response.status_code == 201
    assert "success" in response.json()["message"]


def test_bulk_create_carriers():
    """Test bulk creating carriers"""
    carriers_data = [
        {
            "usdot": 777001,
            "carrier_name": "Bulk Carrier 1",
            "primary_officer": "Officer 1"
        },
        {
            "usdot": 777002,
            "carrier_name": "Bulk Carrier 2",
            "primary_officer": "Officer 2"
        },
        {
            "usdot": 777003,
            "carrier_name": "Bulk Carrier 3",
            "primary_officer": "Officer 3"
        }
    ]
    
    response = client.post("/carriers/bulk", json=carriers_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["created"] == 3
    
    # Verify all were created
    for carrier in carriers_data:
        response = client.get(f"/carriers/{carrier['usdot']}", headers=headers)
        assert response.status_code == 200