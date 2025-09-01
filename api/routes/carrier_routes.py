from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
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


@router.post("/{usdot}/contract", status_code=status.HTTP_201_CREATED)
async def create_carrier_contract(usdot: int, contract: ContractRequest):
    """Create a contract between carrier and target company"""
    if not repo.exists(usdot):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Carrier with USDOT {usdot} not found"
        )
    
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
    
    return {"message": "Contract created successfully"}


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