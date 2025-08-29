import pytest
from fastapi.testclient import TestClient
from datetime import date
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set test environment variables
os.environ["NEO4J_URI"] = "bolt://localhost:7687"
os.environ["NEO4J_USER"] = "neo4j"
os.environ["NEO4J_PASSWORD"] = "testpassword123"
os.environ["API_KEY"] = "test-api-key"

from main import app
from repositories.company_repository import CompanyRepository

client = TestClient(app)
repo = CompanyRepository()

# Test API key for authenticated requests
headers = {"X-API-Key": "test-api-key"}


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test data before and after each test"""
    # Clean before
    test_dot_numbers = [999001, 999002, 999003, 999004, 999005]
    for dot in test_dot_numbers:
        repo.delete(dot)
    
    yield
    
    # Clean after
    for dot in test_dot_numbers:
        repo.delete(dot)


def test_health_check():
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "database" in data


def test_create_company():
    """Test creating a new company"""
    company_data = {
        "dot_number": 999001,
        "mc_number": "MC-999001",
        "legal_name": "Test Trucking Inc",
        "entity_type": "CARRIER",
        "authority_status": "ACTIVE",
        "total_drivers": 25,
        "total_trucks": 15,
        "created_date": "2023-01-01"
    }
    
    response = client.post("/companies/", json=company_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["dot_number"] == 999001
    assert data["legal_name"] == "Test Trucking Inc"


def test_create_duplicate_company():
    """Test creating a duplicate company returns 409"""
    company_data = {
        "dot_number": 999001,
        "legal_name": "Test Trucking Inc",
        "entity_type": "CARRIER"
    }
    
    # Create first company
    response = client.post("/companies/", json=company_data, headers=headers)
    assert response.status_code == 201
    
    # Try to create duplicate
    response = client.post("/companies/", json=company_data, headers=headers)
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_get_company():
    """Test getting a company by DOT number"""
    # First create a company
    company_data = {
        "dot_number": 999001,
        "legal_name": "Test Trucking Inc",
        "entity_type": "CARRIER",
        "total_trucks": 20
    }
    client.post("/companies/", json=company_data, headers=headers)
    
    # Get the company
    response = client.get("/companies/999001", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["dot_number"] == 999001
    assert data["total_trucks"] == 20


def test_get_nonexistent_company():
    """Test getting a non-existent company returns 404"""
    response = client.get("/companies/999999", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_update_company():
    """Test updating a company"""
    # Create a company
    company_data = {
        "dot_number": 999001,
        "legal_name": "Test Trucking Inc",
        "entity_type": "CARRIER",
        "total_trucks": 20
    }
    client.post("/companies/", json=company_data, headers=headers)
    
    # Update the company
    update_data = {
        "total_trucks": 25,
        "authority_status": "INACTIVE",
        "safety_rating": "SATISFACTORY"
    }
    response = client.patch("/companies/999001", json=update_data, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["total_trucks"] == 25
    assert data["authority_status"] == "INACTIVE"
    assert data["safety_rating"] == "SATISFACTORY"


def test_delete_company():
    """Test deleting a company"""
    # Create a company
    company_data = {
        "dot_number": 999001,
        "legal_name": "Test Trucking Inc",
        "entity_type": "CARRIER"
    }
    client.post("/companies/", json=company_data, headers=headers)
    
    # Delete the company
    response = client.delete("/companies/999001", headers=headers)
    assert response.status_code == 204
    
    # Verify it's deleted
    response = client.get("/companies/999001", headers=headers)
    assert response.status_code == 404


def test_get_companies_with_pagination():
    """Test getting companies with pagination"""
    # Create multiple companies
    for i in range(1, 6):
        company_data = {
            "dot_number": 999000 + i,
            "legal_name": f"Test Company {i}",
            "entity_type": "CARRIER"
        }
        client.post("/companies/", json=company_data, headers=headers)
    
    # Get first page
    response = client.get("/companies/?skip=0&limit=2", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) <= 2
    
    # Get second page
    response = client.get("/companies/?skip=2&limit=2", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) <= 2


def test_get_companies_with_filters():
    """Test getting companies with filters"""
    # Create companies with different attributes
    companies = [
        {
            "dot_number": 999001,
            "legal_name": "Active Carrier",
            "entity_type": "CARRIER",
            "authority_status": "ACTIVE",
            "total_trucks": 50
        },
        {
            "dot_number": 999002,
            "legal_name": "Inactive Broker",
            "entity_type": "BROKER",
            "authority_status": "INACTIVE",
            "total_trucks": 10
        },
        {
            "dot_number": 999003,
            "legal_name": "Active Broker",
            "entity_type": "BROKER",
            "authority_status": "ACTIVE",
            "total_trucks": 5
        }
    ]
    
    for company in companies:
        client.post("/companies/", json=company, headers=headers)
    
    # Filter by authority status
    response = client.get("/companies/?authority_status=ACTIVE", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert all(c["authority_status"] == "ACTIVE" for c in data if c["dot_number"] >= 999000)
    
    # Filter by entity type
    response = client.get("/companies/?entity_type=BROKER", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert all(c["entity_type"] == "BROKER" for c in data if c["dot_number"] >= 999000)
    
    # Filter by minimum trucks
    response = client.get("/companies/?min_trucks=20", headers=headers)
    assert response.status_code == 200
    data = response.json()
    test_companies = [c for c in data if c["dot_number"] >= 999000]
    if test_companies:
        assert all(c["total_trucks"] >= 20 for c in test_companies)


def test_bulk_create_companies():
    """Test bulk creating companies"""
    companies_data = [
        {
            "dot_number": 999001,
            "legal_name": "Bulk Company 1",
            "entity_type": "CARRIER"
        },
        {
            "dot_number": 999002,
            "legal_name": "Bulk Company 2",
            "entity_type": "BROKER"
        },
        {
            "dot_number": 999003,
            "legal_name": "Bulk Company 3",
            "entity_type": "CARRIER"
        }
    ]
    
    response = client.post("/companies/bulk", json=companies_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["created"] == 3
    
    # Verify all were created
    for company in companies_data:
        response = client.get(f"/companies/{company['dot_number']}", headers=headers)
        assert response.status_code == 200


def test_api_key_authentication():
    """Test that API key authentication works"""
    # Request without API key should fail
    response = client.get("/companies/")
    assert response.status_code == 401
    
    # Request with wrong API key should fail
    wrong_headers = {"X-API-Key": "wrong-key"}
    response = client.get("/companies/", headers=wrong_headers)
    assert response.status_code == 401
    
    # Request with correct API key should succeed
    response = client.get("/companies/", headers=headers)
    assert response.status_code == 200


def test_company_statistics():
    """Test getting company statistics"""
    # Create some test companies
    companies = [
        {
            "dot_number": 999001,
            "legal_name": "Company 1",
            "entity_type": "CARRIER",
            "authority_status": "ACTIVE",
            "total_drivers": 20,
            "total_trucks": 15,
            "chameleon_risk_score": 0.3
        },
        {
            "dot_number": 999002,
            "legal_name": "Company 2",
            "entity_type": "BROKER",
            "authority_status": "ACTIVE",
            "total_drivers": 10,
            "total_trucks": 5,
            "chameleon_risk_score": 0.8
        }
    ]
    
    for company in companies:
        client.post("/companies/", json=company, headers=headers)
    
    response = client.get("/companies/statistics/summary", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "total_companies" in data
    assert "avg_drivers" in data
    assert "active_companies" in data