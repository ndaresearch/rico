# api/routes/person_routes.py
from typing import Dict, List, Optional
from datetime import date
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from models.person import Person
from repositories.person_repository import PersonRepository
from repositories.target_company_repository import TargetCompanyRepository
from repositories.carrier_repository import CarrierRepository


router = APIRouter(
    prefix="/persons", 
    tags=["persons"],
    responses={404: {"description": "Person not found"}}
)
person_repo = PersonRepository()
target_company_repo = TargetCompanyRepository()
carrier_repo = CarrierRepository()


class PersonUpdate(BaseModel):
    """Model for partial person updates"""
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    email: Optional[List[str]] = None
    phone: Optional[List[str]] = None


class ExecutiveAssignment(BaseModel):
    """Model for assigning a person as TargetCompany executive"""
    person_id: str
    role: str  # CEO, CFO, COO, etc.
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class OfficerAssignment(BaseModel):
    """Model for assigning a person as Carrier primary officer"""
    person_id: str
    usdot: int


class TargetCompanyExecutive(BaseModel):
    """Model for creating TargetCompany executive relationship"""
    dot_number: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    role: str  # CEO, CFO, COO, etc.
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


@router.delete("/remove-executive", status_code=status.HTTP_204_NO_CONTENT)
async def remove_executive_from_target_company(person_id: str, dot_number: int):
    """Remove executive relationship between person and TargetCompany"""
    success = person_repo.remove_from_target_company(person_id, dot_number)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Executive relationship not found"
        )
    return None


@router.delete("/remove-officer", status_code=status.HTTP_204_NO_CONTENT)
async def remove_officer_from_carrier(person_id: str, usdot: int):
    """Remove officer relationship between person and Carrier"""
    success = person_repo.remove_from_carrier(person_id, usdot)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Officer relationship not found"
        )
    return None


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


@router.get("/{person_id}/target-companies", response_model=List[Dict])
async def get_person_target_companies(person_id: str):
    """Get all TargetCompanies where person is an executive"""
    # Check if person exists
    if not person_repo.get_by_id(person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {person_id} not found"
        )
    
    companies = person_repo.get_target_companies(person_id)
    return companies


@router.get("/{person_id}/carriers", response_model=List[Dict])
async def get_person_carriers(person_id: str):
    """Get all Carriers managed by this person"""
    # Check if person exists
    if not person_repo.get_by_id(person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {person_id} not found"
        )
    
    carriers = person_repo.get_carriers(person_id)
    return carriers


# TargetCompany-Person Executive Relationships

@router.post("/target-company-executive", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_target_company_executive(executive: TargetCompanyExecutive):
    """Create a person and assign them as TargetCompany executive in one operation"""
    # Check if TargetCompany exists
    if not target_company_repo.get_by_dot_number(executive.dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TargetCompany with DOT number {executive.dot_number} not found"
        )
    
    # Create or find person
    person = Person(
        person_id="",  # Empty string, will be auto-generated by repository
        full_name=executive.full_name,
        first_name=executive.first_name,
        last_name=executive.last_name,
        date_of_birth=executive.date_of_birth,
        email=executive.email or [],
        phone=executive.phone or []
    )
    
    person_result = person_repo.find_or_create(person)
    
    # Create relationship
    success = person_repo.add_to_target_company(
        person_id=person_result['person_id'],
        dot_number=executive.dot_number,
        role=executive.role,
        start_date=executive.start_date,
        end_date=executive.end_date
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create executive relationship"
        )
    
    return {
        "person": person_result,
        "relationship": {
            "dot_number": executive.dot_number,
            "role": executive.role,
            "start_date": executive.start_date,
            "end_date": executive.end_date
        }
    }


@router.post("/assign-executive", response_model=Dict)
async def assign_executive_to_target_company(assignment: ExecutiveAssignment, dot_number: int):
    """Assign an existing person as executive to a TargetCompany"""
    # Check if TargetCompany exists
    if not target_company_repo.get_by_dot_number(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TargetCompany with DOT number {dot_number} not found"
        )
    
    # Check if person exists
    if not person_repo.get_by_id(assignment.person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {assignment.person_id} not found"
        )
    
    success = person_repo.add_to_target_company(
        person_id=assignment.person_id,
        dot_number=dot_number,
        role=assignment.role,
        start_date=assignment.start_date,
        end_date=assignment.end_date
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create executive relationship"
        )
    
    return {
        "message": "Executive assigned successfully",
        "person_id": assignment.person_id,
        "dot_number": dot_number,
        "role": assignment.role
    }


# Carrier-Person Officer Relationships

@router.post("/assign-officer", response_model=Dict)
async def assign_officer_to_carrier(assignment: OfficerAssignment):
    """Assign an existing person as primary officer to a Carrier"""
    # Check if Carrier exists
    if not carrier_repo.get_by_usdot(assignment.usdot):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {assignment.usdot} not found"
        )
    
    # Check if person exists
    if not person_repo.get_by_id(assignment.person_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person with ID {assignment.person_id} not found"
        )
    
    # Use carrier repository's link_to_officer method
    success = carrier_repo.link_to_officer(
        usdot=assignment.usdot,
        person_id=assignment.person_id
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create officer relationship"
        )
    
    return {
        "message": "Officer assigned to carrier successfully",
        "person_id": assignment.person_id,
        "usdot": assignment.usdot
    }




@router.get("/patterns/shared-executives", response_model=List[Dict])
async def find_target_companies_with_shared_executives(dot_number: int):
    """Find TargetCompanies that share executives with the given TargetCompany"""
    # Check if TargetCompany exists
    if not target_company_repo.get_by_dot_number(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TargetCompany with DOT number {dot_number} not found"
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