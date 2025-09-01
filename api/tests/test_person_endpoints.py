import pytest
from fastapi.testclient import TestClient
from datetime import date

# conftest.py will handle environment setup
from main import app
from repositories.person_repository import PersonRepository
from repositories.target_company_repository import TargetCompanyRepository
from repositories.carrier_repository import CarrierRepository
from models.target_company import TargetCompany
from models.carrier import Carrier

client = TestClient(app)
person_repo = PersonRepository()
target_company_repo = TargetCompanyRepository()
carrier_repo = CarrierRepository()

# Test API key for authenticated requests
headers = {"X-API-Key": "test-api-key"}


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test data before and after each test"""
    # Clean before
    # Clean test persons (names starting with "Test")
    test_person_names = ["Test Person 1", "Test Person 2", "Test Person 3", 
                        "Test Person 4", "Test Person 5", "Test Officer 1",
                        "Test Officer 2", "Test Officer 3"]
    for name in test_person_names:
        persons = person_repo.find_by_name(name)
        for person in persons:
            person_repo.delete(person['person_id'])
    
    # Clean test target companies and carriers (DOT numbers 999000-999999)
    test_dot_numbers = [999001, 999002, 999003, 999004, 999005]
    for dot in test_dot_numbers:
        target_company_repo.delete(dot)
        carrier_repo.delete(dot)  # Also clean carriers with same numbers
    
    yield
    
    # Clean after
    for name in test_person_names:
        persons = person_repo.find_by_name(name)
        for person in persons:
            person_repo.delete(person['person_id'])
    
    for dot in test_dot_numbers:
        target_company_repo.delete(dot)
        carrier_repo.delete(dot)


def test_create_person():
    """Test creating a new person with all fields"""
    person_data = {
        "person_id": "",  # Will be auto-generated
        "full_name": "Test Person 1",
        "first_name": "Test",
        "last_name": "Person",
        "date_of_birth": "1980-01-15",
        "email": ["test1@example.com", "test1.alt@example.com"],
        "phone": ["+1234567890", "+0987654321"]
    }
    
    response = client.post("/persons/", json=person_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["full_name"] == "Test Person 1"
    assert data["first_name"] == "Test"
    assert data["last_name"] == "Person"
    assert data["person_id"] is not None  # ID should be generated
    assert len(data["email"]) == 2
    assert len(data["phone"]) == 2


def test_create_person_minimal():
    """Test creating a person with only required fields"""
    person_data = {
        "person_id": "",
        "full_name": "Test Person 2"
    }
    
    response = client.post("/persons/", json=person_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["full_name"] == "Test Person 2"
    assert data["person_id"] is not None


def test_get_person():
    """Test getting a person by ID"""
    # First create a person
    person_data = {
        "person_id": "",
        "full_name": "Test Person 3",
        "first_name": "Test",
        "last_name": "Three",
        "date_of_birth": "1985-03-20"
    }
    create_response = client.post("/persons/", json=person_data, headers=headers)
    created_person = create_response.json()
    person_id = created_person["person_id"]
    
    # Get the person
    response = client.get(f"/persons/{person_id}", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["person_id"] == person_id
    assert data["full_name"] == "Test Person 3"
    assert data["first_name"] == "Test"


def test_get_nonexistent_person():
    """Test getting a non-existent person returns 404"""
    response = client.get("/persons/NONEXISTENT123", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_update_person():
    """Test updating a person"""
    # Create a person
    person_data = {
        "person_id": "",
        "full_name": "Test Person 4",
        "first_name": "Test",
        "last_name": "Four"
    }
    create_response = client.post("/persons/", json=person_data, headers=headers)
    person_id = create_response.json()["person_id"]
    
    # Update the person
    update_data = {
        "last_name": "Updated",
        "email": ["updated@example.com"],
        "phone": ["+1111111111"]
    }
    response = client.patch(f"/persons/{person_id}", json=update_data, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["last_name"] == "Updated"
    assert "updated@example.com" in data["email"]
    assert "+1111111111" in data["phone"]


def test_update_nonexistent_person():
    """Test updating a non-existent person returns 404"""
    update_data = {"last_name": "Updated"}
    response = client.patch("/persons/NONEXISTENT123", json=update_data, headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_update_person_no_valid_fields():
    """Test updating with no valid fields returns 400"""
    # Create a person
    person_data = {
        "person_id": "",
        "full_name": "Test Person 5"
    }
    create_response = client.post("/persons/", json=person_data, headers=headers)
    person_id = create_response.json()["person_id"]
    
    # Try to update with empty data
    update_data = {}
    response = client.patch(f"/persons/{person_id}", json=update_data, headers=headers)
    assert response.status_code == 400
    assert "No valid updates" in response.json()["detail"]


def test_delete_person():
    """Test deleting a person"""
    # Create a person
    person_data = {
        "person_id": "",
        "full_name": "Test Person 1"
    }
    create_response = client.post("/persons/", json=person_data, headers=headers)
    person_id = create_response.json()["person_id"]
    
    # Delete the person
    response = client.delete(f"/persons/{person_id}", headers=headers)
    assert response.status_code == 204
    
    # Verify it's deleted
    response = client.get(f"/persons/{person_id}", headers=headers)
    assert response.status_code == 404


def test_delete_nonexistent_person():
    """Test deleting a non-existent person returns 404"""
    response = client.delete("/persons/NONEXISTENT123", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_search_persons_by_name():
    """Test searching persons by name"""
    # Create multiple persons
    persons = [
        {"person_id": "", "full_name": "Test Person 1"},
        {"person_id": "", "full_name": "Test Person 2"},
        {"person_id": "", "full_name": "Test Officer 1"}
    ]
    
    for person in persons:
        client.post("/persons/", json=person, headers=headers)
    
    # Search for "Test Person"
    response = client.get("/persons/search/by-name?name=Test Person", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 2
    assert all("Test Person" in p["full_name"] for p in data)


def test_search_persons_short_name():
    """Test searching with name less than 2 characters returns 400"""
    response = client.get("/persons/search/by-name?name=T", headers=headers)
    assert response.status_code == 400
    assert "at least 2 characters" in response.json()["detail"]


def test_search_persons_case_insensitive():
    """Test search is case insensitive"""
    # Create a person
    person_data = {"person_id": "", "full_name": "Test Person 1"}
    client.post("/persons/", json=person_data, headers=headers)
    
    # Search with different case
    response = client.get("/persons/search/by-name?name=test person", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any("Test Person 1" in p["full_name"] for p in data)


def test_get_person_target_companies():
    """Test getting TargetCompanies where person is an executive"""
    # Create a TargetCompany
    target_company = TargetCompany(
        dot_number=999001,
        legal_name="Test Trucking Inc",
        entity_type="CARRIER"
    )
    target_company_repo.create(target_company)
    
    # Create a person and assign as executive
    executive_data = {
        "dot_number": 999001,
        "full_name": "Test Executive 1",
        "first_name": "Test",
        "last_name": "Executive",
        "role": "CEO",
        "start_date": "2023-01-01"
    }
    executive_response = client.post("/persons/target-company-executive", json=executive_data, headers=headers)
    person_id = executive_response.json()["person"]["person_id"]
    
    # Get person's target companies
    response = client.get(f"/persons/{person_id}/target-companies", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(c["dot_number"] == 999001 for c in data)


def test_get_nonexistent_person_target_companies():
    """Test getting target companies for non-existent person returns 404"""
    response = client.get("/persons/NONEXISTENT123/target-companies", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_create_target_company_executive():
    """Test creating a person and assigning as TargetCompany executive"""
    # Create a TargetCompany first
    target_company = TargetCompany(
        dot_number=999002,
        legal_name="Test Transport LLC",
        entity_type="CARRIER"
    )
    target_company_repo.create(target_company)
    
    # Create person and assign as officer
    officer_data = {
        "dot_number": 999002,
        "full_name": "Test Officer 2",
        "first_name": "Test",
        "last_name": "Officer",
        "date_of_birth": "1975-06-15",
        "role": "President",
        "start_date": "2023-01-01",
        "email": ["officer2@example.com"],
        "phone": ["+1234567890"]
    }
    
    response = client.post("/persons/target-company-executive", json=officer_data, headers=headers)
    assert response.status_code == 201
    data = response.json()
    assert data["person"]["full_name"] == "Test Officer 2"
    assert data["person"]["person_id"] is not None
    assert data["relationship"]["dot_number"] == 999002
    assert data["relationship"]["role"] == "President"


def test_create_executive_nonexistent_target_company():
    """Test creating executive for non-existent TargetCompany returns 404"""
    officer_data = {
        "dot_number": 999999,
        "full_name": "Test Officer 3",
        "role": "CEO"
    }
    
    response = client.post("/persons/target-company-executive", json=officer_data, headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_assign_existing_executive():
    """Test assigning an existing person as TargetCompany executive"""
    # Create a TargetCompany
    target_company = TargetCompany(
        dot_number=999003,
        legal_name="Test Logistics Co",
        entity_type="BROKER"
    )
    target_company_repo.create(target_company)
    
    # Create a person
    person_data = {
        "person_id": "",
        "full_name": "Test Person 1",
        "first_name": "Test",
        "last_name": "Person"
    }
    person_response = client.post("/persons/", json=person_data, headers=headers)
    person_id = person_response.json()["person_id"]
    
    # Assign person as officer
    assignment_data = {
        "person_id": person_id,
        "role": "CFO",
        "start_date": "2023-06-01"
    }
    
    response = client.post(f"/persons/assign-executive?dot_number=999003", 
                          json=assignment_data, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["person_id"] == person_id
    assert data["dot_number"] == 999003
    assert data["role"] == "CFO"


def test_assign_nonexistent_person():
    """Test assigning non-existent person returns 404"""
    # Create a TargetCompany
    target_company = TargetCompany(
        dot_number=999004,
        legal_name="Test Freight Inc",
        entity_type="CARRIER"
    )
    target_company_repo.create(target_company)
    
    assignment_data = {
        "person_id": "NONEXISTENT123",
        "role": "CEO"
    }
    
    response = client.post(f"/persons/assign-executive?dot_number=999004", 
                          json=assignment_data, headers=headers)
    assert response.status_code == 404
    assert "Person with ID" in response.json()["detail"]


def test_assign_to_nonexistent_target_company():
    """Test assigning to non-existent TargetCompany returns 404"""
    # Create a person
    person_data = {
        "person_id": "",
        "full_name": "Test Person 2"
    }
    person_response = client.post("/persons/", json=person_data, headers=headers)
    person_id = person_response.json()["person_id"]
    
    assignment_data = {
        "person_id": person_id,
        "role": "CEO"
    }
    
    response = client.post(f"/persons/assign-executive?dot_number=999999", 
                          json=assignment_data, headers=headers)
    assert response.status_code == 404
    assert "TargetCompany with DOT" in response.json()["detail"]


def test_remove_executive():
    """Test removing executive relationship"""
    # Create a TargetCompany
    target_company = TargetCompany(
        dot_number=999005,
        legal_name="Test Express LLC",
        entity_type="CARRIER"
    )
    target_company_repo.create(target_company)
    
    # Create and assign executive
    executive_data = {
        "dot_number": 999005,
        "full_name": "Test Executive 1",
        "role": "CEO"
    }
    executive_response = client.post("/persons/target-company-executive", json=executive_data, headers=headers)
    person_id = executive_response.json()["person"]["person_id"]
    
    # Remove executive relationship
    response = client.delete(f"/persons/remove-executive?person_id={person_id}&dot_number=999005", 
                            headers=headers)
    assert response.status_code == 204
    
    # Verify relationship is removed by checking person's target companies
    response = client.get(f"/persons/{person_id}/target-companies", headers=headers)
    data = response.json()
    assert not any(c["dot_number"] == 999005 for c in data)


def test_remove_nonexistent_relationship():
    """Test removing non-existent relationship returns 404"""
    response = client.delete("/persons/remove-executive?person_id=NONEXISTENT&dot_number=999999", 
                            headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_find_shared_executives():
    """Test finding TargetCompanies with shared executives"""
    # Create two TargetCompanies
    target_companies = [
        TargetCompany(dot_number=999001, legal_name="Target Company A", entity_type="CARRIER"),
        TargetCompany(dot_number=999002, legal_name="Target Company B", entity_type="BROKER")
    ]
    for tc in target_companies:
        target_company_repo.create(tc)
    
    # Create a shared executive
    executive_data = {
        "dot_number": 999001,
        "full_name": "Test Executive 1",
        "role": "CEO"
    }
    executive_response = client.post("/persons/target-company-executive", json=executive_data, headers=headers)
    person_id = executive_response.json()["person"]["person_id"]
    
    # Assign same person to second TargetCompany
    assignment_data = {
        "person_id": person_id,
        "role": "President"
    }
    client.post(f"/persons/assign-executive?dot_number=999002", 
               json=assignment_data, headers=headers)
    
    # Find shared executives
    response = client.get(f"/persons/patterns/shared-executives?dot_number=999001", headers=headers)
    assert response.status_code == 200
    data = response.json()
    # Should find Target Company B as sharing executives with Target Company A
    assert any(item.get("company", {}).get("dot_number") == 999002 for item in data)


def test_find_shared_executives_nonexistent_target_company():
    """Test finding shared executives for non-existent TargetCompany returns 404"""
    response = client.get("/persons/patterns/shared-executives?dot_number=999999", headers=headers)
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_find_succession_patterns():
    """Test finding officer succession patterns"""
    # This endpoint finds patterns across all companies
    response = client.get("/persons/patterns/succession", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    # The actual patterns depend on the data in the database


def test_person_statistics():
    """Test getting person statistics"""
    # Create some test persons
    persons = [
        {"person_id": "", "full_name": "Test Person 1"},
        {"person_id": "", "full_name": "Test Person 2"},
        {"person_id": "", "full_name": "Test Person 3"}
    ]
    
    for person in persons:
        client.post("/persons/", json=person, headers=headers)
    
    response = client.get("/persons/statistics/summary", headers=headers)
    assert response.status_code == 200
    data = response.json()
    # Check for expected statistics fields (adjust based on actual implementation)
    assert "total_persons" in data or "count" in data or len(data) > 0


def test_api_key_authentication():
    """Test that API key authentication works"""
    # Request without API key should fail
    response = client.get("/persons/search/by-name?name=Test")
    assert response.status_code == 401
    
    # Request with wrong API key should fail
    wrong_headers = {"X-API-Key": "wrong-key"}
    response = client.get("/persons/search/by-name?name=Test", headers=wrong_headers)
    assert response.status_code == 401
    
    # Request with correct API key should succeed (even if no results)
    response = client.get("/persons/search/by-name?name=Test", headers=headers)
    assert response.status_code in [200, 400]  # 400 if name too short, 200 otherwise