import pytest
from fastapi.testclient import TestClient
import uuid

from main import app
from repositories.insurance_provider_repository import InsuranceProviderRepository

client = TestClient(app)
repo = InsuranceProviderRepository()

# Test API key for authenticated requests
headers = {"X-API-Key": "test-api-key"}


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test data before and after each test"""
    # Clean before - delete by known test names
    test_names = ["Test Insurance Co", "Another Insurance Co", "Third Insurance Co"]
    for name in test_names:
        provider = repo.get_by_name(name)
        if provider:
            repo.delete(provider["provider_id"])
    
    yield
    
    # Clean after
    for name in test_names:
        provider = repo.get_by_name(name)
        if provider:
            repo.delete(provider["provider_id"])


def test_create_insurance_provider():
    """Test creating a new insurance provider"""
    provider_data = {
        "name": "Test Insurance Co",
        "contact_email": "test@insurance.com",
        "website": "https://testinsurance.com"
    }
    
    response = client.post("/insurance-providers/", json=provider_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Test Insurance Co"
    assert "provider_id" in data


def test_create_duplicate_insurance_provider():
    """Test creating a duplicate insurance provider returns 409"""
    provider_data = {
        "name": "Test Insurance Co"
    }
    
    # Create first provider
    response = client.post("/insurance-providers/", json=provider_data, headers=headers)
    assert response.status_code == 201
    
    # Try to create duplicate
    response = client.post("/insurance-providers/", json=provider_data, headers=headers)
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"]


def test_get_insurance_provider():
    """Test getting an insurance provider by ID"""
    # First create a provider
    provider_data = {
        "name": "Test Insurance Co",
        "contact_phone": "555-0100"
    }
    create_response = client.post("/insurance-providers/", json=provider_data, headers=headers)
    provider_id = create_response.json()["provider_id"]
    
    # Get the provider
    response = client.get(f"/insurance-providers/{provider_id}", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["provider_id"] == provider_id
    assert data["name"] == "Test Insurance Co"


def test_get_nonexistent_insurance_provider():
    """Test getting a non-existent insurance provider returns 404"""
    fake_id = str(uuid.uuid4())
    response = client.get(f"/insurance-providers/{fake_id}", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_get_insurance_providers_with_pagination():
    """Test getting insurance providers with pagination"""
    # Create multiple providers
    providers = ["Test Insurance Co", "Another Insurance Co", "Third Insurance Co"]
    for name in providers:
        provider_data = {"name": name}
        client.post("/insurance-providers/", json=provider_data, headers=headers)
    
    # Get with pagination
    response = client.get("/insurance-providers/?skip=0&limit=2", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) <= 2


def test_get_provider_carriers():
    """Test getting carriers for an insurance provider"""
    # Create a provider
    provider_data = {
        "name": "Test Insurance Co"
    }
    create_response = client.post("/insurance-providers/", json=provider_data, headers=headers)
    provider_id = create_response.json()["provider_id"]
    
    # Get carriers (should be empty initially)
    response = client.get(f"/insurance-providers/{provider_id}/carriers", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data == []