from typing import Dict, List
from fastapi import APIRouter, HTTPException, Query, status

from models.insurance_provider import InsuranceProvider
from repositories.insurance_provider_repository import InsuranceProviderRepository


router = APIRouter(
    prefix="/insurance-providers", 
    tags=["insurance-providers"],
    responses={404: {"description": "Insurance provider not found"}}
)
repo = InsuranceProviderRepository()


@router.post("/", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_insurance_provider(provider: InsuranceProvider):
    """Create a new insurance provider"""
    if repo.exists_by_name(provider.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Insurance provider with name '{provider.name}' already exists"
        )
    
    result = repo.create(provider)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create insurance provider"
        )
    
    return result


@router.get("/{provider_id}", response_model=Dict)
async def get_insurance_provider(provider_id: str):
    """Get an insurance provider by ID"""
    provider = repo.get_by_id(provider_id)
    if not provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Insurance provider with ID {provider_id} not found"
        )
    return provider


@router.get("/", response_model=List[Dict])
async def get_insurance_providers(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return")
):
    """Get all insurance providers with pagination"""
    providers = repo.get_all(skip=skip, limit=limit)
    return providers


@router.get("/{provider_id}/carriers", response_model=List[Dict])
async def get_provider_carriers(provider_id: str):
    """Get all carriers insured by this provider"""
    if not repo.exists_by_id(provider_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Insurance provider with ID {provider_id} not found"
        )
    
    carriers = repo.get_carriers(provider_id)
    return carriers