# api/routes/person_routes.py
from typing import Dict, List, Optional
from datetime import date
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from models.person import Person
from repositories.person_repository import PersonRepository
from repositories.company_repository import CompanyRepository


router = APIRouter(prefix="/persons", tags=["persons"])
person_repo = PersonRepository()
company_repo = CompanyRepository()


class PersonUpdate(BaseModel):
    """Model for partial person updates"""
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    email: Optional[List[str]] = None
    phone: Optional[List[str]] = None


class OfficerAssignment(BaseModel):
    """Model for assigning a person as company officer"""
    person_id: str
    role: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class CompanyOfficer(BaseModel):
    """Model for creating company officer relationship"""
    dot_number: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    role: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    email: Optional[List[str]] = None
    phone: Optional[List[str]] = None


@router.post("/", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_person(person: Person):
    """Create a new person"""
    result = person_repo.create(person)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create person"
        )
    return result


@router.get("/{person_id}", response_model=Dict)
async def get_person(person_id: str):
    """Get a person by ID"""
    person = person_repo.get_by_id(person_id)
    if not person:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {person_id} not found"
        )
    return person


@router.get("/search/by-name", response_model=List[Dict])
async def search_persons_by_name(
    name: str = Query(..., description="Name to search for")
):
    """Search persons by name"""
    if len(name) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Search term must be at least 2 characters"
        )
    
    persons = person_repo.find_by_name(name)
    return persons


@router.patch("/{person_id}", response_model=Dict)
async def update_person(person_id: str, updates: PersonUpdate):
    """Update a person's properties"""
    # Check if person exists
    if not person_repo.get_by_id(person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {person_id} not found"
        )
    
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid updates provided"
        )
    
    result = person_repo.update(person_id, update_data)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update person"
        )
    
    return result


@router.delete("/{person_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_person(person_id: str):
    """Delete a person"""
    success = person_repo.delete(person_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {person_id} not found"
        )
    return None


@router.get("/{person_id}/companies", response_model=List[Dict])
async def get_person_companies(person_id: str):
    """Get all companies associated with a person"""
    # Check if person exists
    if not person_repo.get_by_id(person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {person_id} not found"
        )
    
    companies = person_repo.get_companies(person_id)
    return companies


# Company-Person Relationship Endpoints

@router.post("/company-officer", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_company_officer(officer: CompanyOfficer):
    """Create a person and assign them as company officer in one operation"""
    # Check if company exists
    if not company_repo.exists(officer.dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {officer.dot_number} not found"
        )
    
    # Create or find person
    person = Person(
        full_name=officer.full_name,
        first_name=officer.first_name,
        last_name=officer.last_name,
        date_of_birth=officer.date_of_birth,
        email=officer.email or [],
        phone=officer.phone or []
    )
    
    person_result = person_repo.find_or_create(person)
    
    # Create relationship
    success = person_repo.add_to_company(
        person_id=person_result['person_id'],
        dot_number=officer.dot_number,
        role=officer.role,
        start_date=officer.start_date,
        end_date=officer.end_date
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create officer relationship"
        )
    
    return {
        "person": person_result,
        "relationship": {
            "dot_number": officer.dot_number,
            "role": officer.role,
            "start_date": officer.start_date,
            "end_date": officer.end_date
        }
    }


@router.post("/assign-officer", response_model=Dict)
async def assign_officer_to_company(assignment: OfficerAssignment, dot_number: int):
    """Assign an existing person as officer to a company"""
    # Check if company exists
    if not company_repo.exists(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {dot_number} not found"
        )
    
    # Check if person exists
    if not person_repo.get_by_id(assignment.person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {assignment.person_id} not found"
        )
    
    success = person_repo.add_to_company(
        person_id=assignment.person_id,
        dot_number=dot_number,
        role=assignment.role,
        start_date=assignment.start_date,
        end_date=assignment.end_date
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create officer relationship"
        )
    
    return {
        "message": "Officer assigned successfully",
        "person_id": assignment.person_id,
        "dot_number": dot_number,
        "role": assignment.role
    }


@router.delete("/remove-officer", status_code=status.HTTP_204_NO_CONTENT)
async def remove_officer_from_company(person_id: str, dot_number: int):
    """Remove officer relationship between person and company"""
    success = person_repo.remove_from_company(person_id, dot_number)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Officer relationship not found"
        )
    return None


@router.get("/patterns/shared-officers", response_model=List[Dict])
async def find_companies_with_shared_officers(dot_number: int):
    """Find companies that share officers with the given company"""
    # Check if company exists
    if not company_repo.exists(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {dot_number} not found"
        )
    
    shared = person_repo.find_shared_officers(dot_number)
    return shared


@router.get("/patterns/succession", response_model=List[Dict])
async def find_officer_succession_patterns():
    """Find suspicious officer succession patterns across companies"""
    patterns = person_repo.find_officer_succession_patterns()
    return patterns


@router.get("/statistics/summary", response_model=Dict)
async def get_person_statistics():
    """Get aggregate statistics about persons"""
    stats = person_repo.get_statistics()
    return stats