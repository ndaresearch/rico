from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from models.target_company import TargetCompany
from repositories.target_company_repository import TargetCompanyRepository


router = APIRouter(
    prefix="/target-companies", 
    tags=["target-companies"],
    responses={404: {"description": "Target company not found"}}
)
repo = TargetCompanyRepository()


class TargetCompanyUpdate(BaseModel):
    """Model for partial target company updates"""
    mc_number: Optional[str] = None
    legal_name: Optional[str] = None
    entity_type: Optional[str] = None
    authority_status: Optional[str] = None
    safety_rating: Optional[str] = None
    total_drivers: Optional[int] = None
    total_trucks: Optional[int] = None
    total_trailers: Optional[int] = None
    risk_score: Optional[float] = None


@router.post("/", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_target_company(company: TargetCompany):
    """Create a new target company"""
    if repo.exists(company.dot_number):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Target company with DOT number {company.dot_number} already exists"
        )
    
    result = repo.create(company)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create target company"
        )
    
    return result


@router.get("/{dot_number}", response_model=Dict)
async def get_target_company(dot_number: int):
    """Get a target company by DOT number"""
    company = repo.get_by_dot_number(dot_number)
    if not company:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target company with DOT number {dot_number} not found"
        )
    return company


@router.get("/", response_model=List[Dict])
async def get_target_companies(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type")
):
    """Get all target companies with pagination"""
    filters = {}
    if entity_type:
        filters['entity_type'] = entity_type
    
    companies = repo.get_all(skip=skip, limit=limit, filters=filters)
    return companies


@router.patch("/{dot_number}", response_model=Dict)
async def update_target_company(dot_number: int, updates: TargetCompanyUpdate):
    """Update a target company's properties"""
    if not repo.exists(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target company with DOT number {dot_number} not found"
        )
    
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid updates provided"
        )
    
    result = repo.update(dot_number, update_data)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update target company"
        )
    
    return result


@router.delete("/{dot_number}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_target_company(dot_number: int):
    """Delete a target company"""
    success = repo.delete(dot_number)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target company with DOT number {dot_number} not found"
        )
    return None


@router.get("/{dot_number}/carriers", response_model=List[Dict])
async def get_target_company_carriers(dot_number: int):
    """Get all carriers contracted with this target company"""
    if not repo.exists(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target company with DOT number {dot_number} not found"
        )
    
    carriers = repo.get_carriers(dot_number)
    return carriers