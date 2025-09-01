from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, Response, status
from pydantic import BaseModel

from models.carrier import Carrier
from repositories.carrier_repository import CarrierRepository


router = APIRouter(prefix="/carriers", tags=["carriers"])
repo = CarrierRepository()


class CarrierUpdate(BaseModel):
    """Model for partial carrier updates"""
    carrier_name: Optional[str] = None
    primary_officer: Optional[str] = None
    insurance_provider: Optional[str] = None
    insurance_amount: Optional[float] = None
    trucks: Optional[int] = None
    mcs150_drivers: Optional[int] = None
    violations: Optional[int] = None
    crashes: Optional[int] = None
    driver_oos_rate: Optional[float] = None
    vehicle_oos_rate: Optional[float] = None


class ContractRequest(BaseModel):
    """Model for creating carrier-target company contract"""
    target_dot_number: int
    active: bool = True


class OfficerLinkRequest(BaseModel):
    """Model for linking carrier to officer"""
    officer_name: Optional[str] = None
    person_id: Optional[str] = None


@router.post("/", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_carrier(carrier: Carrier):
    """Create a new carrier"""
    if repo.exists(carrier.usdot):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Carrier with USDOT {carrier.usdot} already exists"
        )
    
    result = repo.create(carrier)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create carrier"
        )
    
    return result


@router.get("/{usdot}", response_model=Dict)
async def get_carrier(usdot: int):
    """Get a carrier by USDOT number"""
    carrier = repo.get_by_usdot(usdot)
    if not carrier:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    return carrier


@router.get("/", response_model=List[Dict])
async def get_carriers(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    jb_carrier: Optional[bool] = Query(None, description="Filter by JB Hunt carrier status"),
    min_violations: Optional[int] = Query(None, ge=0, description="Minimum violations")
):
    """Get all carriers with pagination and filters"""
    filters = {}
    if jb_carrier is not None:
        filters['jb_carrier'] = jb_carrier
    if min_violations is not None:
        filters['min_violations'] = min_violations
    
    carriers = repo.get_all(skip=skip, limit=limit, filters=filters)
    return carriers


@router.patch("/{usdot}", response_model=Dict)
async def update_carrier(usdot: int, updates: CarrierUpdate):
    """Update a carrier's properties"""
    if not repo.exists(usdot):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid updates provided"
        )
    
    result = repo.update(usdot, update_data)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update carrier"
        )
    
    return result


@router.delete("/{usdot}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_carrier(usdot: int):
    """Delete a carrier"""
    success = repo.delete(usdot)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    return None


@router.post("/{usdot}/contract")
async def create_carrier_contract(usdot: int, contract: ContractRequest, response: Response):
    """Create a contract between carrier and target company (idempotent)"""
    if not repo.exists(usdot):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    
    # Check if relationship already exists
    check_query = """
    MATCH (tc:TargetCompany {dot_number: $dot_number})-[r:CONTRACTS_WITH]->(c:Carrier {usdot: $usdot})
    RETURN count(r) as count
    """
    result = repo.execute_query(check_query, {"usdot": usdot, "dot_number": contract.target_dot_number})
    exists = result and result[0]['count'] > 0
    
    success = repo.create_contract_with_target(
        usdot=usdot,
        dot_number=contract.target_dot_number,
        active=contract.active
    )
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to create contract. Check if target company exists."
        )
    
    # Set appropriate status code
    response.status_code = status.HTTP_200_OK if exists else status.HTTP_201_CREATED
    
    message = "Contract already exists (updated)" if exists else "Contract created successfully"
    return {"message": message}


@router.post("/bulk", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def bulk_create_carriers(carriers: List[Carrier]):
    """Bulk create multiple carriers"""
    if not carriers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No carriers provided"
        )
    
    if len(carriers) > 1000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum 1000 carriers per bulk request"
        )
    
    # Check for duplicates in request
    usdots = [c.usdot for c in carriers]
    if len(usdots) != len(set(usdots)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Duplicate USDOT numbers in request"
        )
    
    # Check if any already exist
    existing = []
    for usdot in usdots:
        if repo.exists(usdot):
            existing.append(usdot)
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Carriers with these USDOT numbers already exist: {existing}"
        )
    
    result = repo.bulk_create(carriers)
    return result


@router.post("/{usdot}/insurance", response_model=Dict)
async def link_carrier_to_insurance(
    usdot: int,
    provider_name: str = Query(..., description="Name of the insurance provider"),
    amount: Optional[float] = Query(None, description="Insurance coverage amount")
):
    """Create INSURED_BY relationship between carrier and insurance provider"""
    # Check if carrier exists
    if not repo.exists(usdot):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    
    # Import here to avoid circular dependency
    from repositories.insurance_provider_repository import InsuranceProviderRepository
    insurance_repo = InsuranceProviderRepository()
    
    # Check if insurance provider exists
    provider = insurance_repo.get_by_name(provider_name)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Insurance provider '{provider_name}' not found"
        )
    
    # Create the relationship
    success = repo.link_to_insurance_provider(usdot, provider_name, amount)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create insurance relationship"
        )
    
    return {
        "message": "Insurance relationship created successfully",
        "carrier_usdot": usdot,
        "insurance_provider": provider_name,
        "coverage_amount": amount
    }


@router.post("/{usdot}/officer", response_model=Dict)
async def link_carrier_to_officer(usdot: int, request: OfficerLinkRequest, response: Response):
    """
    Link a carrier to an officer (Person entity) - idempotent.
    Provide either officer_name (to create/find) or person_id (existing person).
    """
    # Check if carrier exists
    if not repo.exists(usdot):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    
    # Validate request - need either name or ID
    if not request.officer_name and not request.person_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Must provide either officer_name or person_id"
        )
    
    # Import here to avoid circular dependency
    from repositories.person_repository import PersonRepository
    from models.person import Person
    
    person_repo = PersonRepository()
    
    # Get or create the person
    if request.person_id:
        # Use existing person
        person = person_repo.get_by_id(request.person_id)
        if not person:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Person with ID {request.person_id} not found"
            )
        person_id = request.person_id
        officer_name = person['full_name']
    else:
        # Create or find person by name
        person_model = Person(
            person_id="",  # Will be auto-generated
            full_name=request.officer_name,
            source=["API"]
        )
        person = person_repo.find_or_create(person_model)
        person_id = person['person_id']
        officer_name = request.officer_name
    
    # Check if relationship already exists (for proper status code)
    check_query = """
    MATCH (c:Carrier {usdot: $usdot})-[r:MANAGED_BY]->(p:Person {person_id: $person_id})
    RETURN count(r) as count
    """
    
    result = repo.execute_query(check_query, {"usdot": usdot, "person_id": person_id})
    exists = result and result[0]['count'] > 0
    
    # For backward compatibility with tests that expect 409 on duplicate
    if exists:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Carrier {usdot} is already linked to person {officer_name}"
        )
    
    # Create the relationship (MERGE will handle duplicates)
    success = repo.link_to_officer(usdot, person_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create officer relationship"
        )
    
    # Also update the carrier's primary_officer field if provided
    if request.officer_name:
        repo.update(usdot, {"primary_officer": request.officer_name})
    
    # Set appropriate status code
    response.status_code = status.HTTP_201_CREATED
    
    return {
        "message": "Officer relationship created successfully",
        "carrier_usdot": usdot,
        "person_id": person_id,
        "officer_name": officer_name
    }