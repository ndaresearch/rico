import pytest
from fastapi.testclient import TestClient

from main import app
from repositories.target_company_repository import TargetCompanyRepository

client = TestClient(app)
repo = TargetCompanyRepository()

# Test API key for authenticated requests
headers = {"X-API-Key": "test-api-key"}


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test data before and after each test"""
    # Clean before
    test_dot_numbers = [888001, 888002, 888003]
    for dot in test_dot_numbers:
        repo.delete(dot)
    
    yield
    
    # Clean after
    for dot in test_dot_numbers:
        repo.delete(dot)


def test_create_target_company():
    """Test creating a new target company"""
    company_data = {
        "dot_number": 888001,
        "legal_name": "JB Hunt Transport Services",
        "entity_type": "BROKER",
        "authority_status": "ACTIVE",
        "total_drivers": 100,
        "total_trucks": 50
    }
    
    response = client.post("/target-companies/", json=company_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["dot_number"] == 888001
    assert data["legal_name"] == "JB Hunt Transport Services"


def test_create_duplicate_target_company():
    """Test creating a duplicate target company returns 409"""
    company_data = {
        "dot_number": 888001,
        "legal_name": "Test Target Company",
        "entity_type": "BROKER"
    }
    
    # Create first company
    response = client.post("/target-companies/", json=company_data, headers=headers)
    assert response.status_code == 201
    
    # Try to create duplicate
    response = client.post("/target-companies/", json=company_data, headers=headers)
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_get_target_company():
    """Test getting a target company by DOT number"""
    # First create a company
    company_data = {
        "dot_number": 888001,
        "legal_name": "Test Target Company",
        "entity_type": "FREIGHT_FORWARDER",
        "total_trucks": 30
    }
    client.post("/target-companies/", json=company_data, headers=headers)
    
    # Get the company
    response = client.get("/target-companies/888001", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["dot_number"] == 888001
    assert data["total_trucks"] == 30


def test_get_nonexistent_target_company():
    """Test getting a non-existent target company returns 404"""
    response = client.get("/target-companies/999999", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_update_target_company():
    """Test updating a target company"""
    # Create a company
    company_data = {
        "dot_number": 888001,
        "legal_name": "Test Target Company",
        "entity_type": "BROKER",
        "total_trucks": 20
    }
    client.post("/target-companies/", json=company_data, headers=headers)
    
    # Update the company
    update_data = {
        "total_trucks": 35,
        "authority_status": "INACTIVE",
        "risk_score": 0.65
    }
    response = client.patch("/target-companies/888001", json=update_data, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["total_trucks"] == 35
    assert data["authority_status"] == "INACTIVE"
    assert data["risk_score"] == 0.65


def test_delete_target_company():
    """Test deleting a target company"""
    # Create a company
    company_data = {
        "dot_number": 888001,
        "legal_name": "Test Target Company",
        "entity_type": "BROKER"
    }
    client.post("/target-companies/", json=company_data, headers=headers)
    
    # Delete the company
    response = client.delete("/target-companies/888001", headers=headers)
    assert response.status_code == 204
    
    # Verify it's deleted
    response = client.get("/target-companies/888001", headers=headers)
    assert response.status_code == 404


def test_get_target_companies_with_pagination():
    """Test getting target companies with pagination"""
    # Create multiple companies
    for i in range(1, 4):
        company_data = {
            "dot_number": 888000 + i,
            "legal_name": f"Target Company {i}",
            "entity_type": "BROKER" if i % 2 else "FREIGHT_FORWARDER"
        }
        client.post("/target-companies/", json=company_data, headers=headers)
    
    # Get with pagination
    response = client.get("/target-companies/?skip=0&limit=2", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) <= 2


def test_get_target_company_carriers():
    """Test getting carriers for a target company"""
    # Create a target company
    company_data = {
        "dot_number": 888001,
        "legal_name": "Test Target Company",
        "entity_type": "BROKER"
    }
    client.post("/target-companies/", json=company_data, headers=headers)
    
    # Get carriers (should be empty initially)
    response = client.get("/target-companies/888001/carriers", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data == []