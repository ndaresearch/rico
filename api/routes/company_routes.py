from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, status
from pydantic import BaseModel

from models.company import Company
from repositories.company_repository import CompanyRepository


router = APIRouter(prefix="/companies", tags=["companies"])
repo = CompanyRepository()


class CompanyUpdate(BaseModel):
    """Model for partial company updates"""
    mc_number: Optional[str] = None
    legal_name: Optional[str] = None
    entity_type: Optional[str] = None
    authority_status: Optional[str] = None
    safety_rating: Optional[str] = None
    operation_classification: Optional[str] = None
    total_drivers: Optional[int] = None
    total_trucks: Optional[int] = None
    total_trailers: Optional[int] = None
    chameleon_risk_score: Optional[float] = None
    safety_risk_score: Optional[float] = None
    financial_risk_score: Optional[float] = None


class CompanyFilters(BaseModel):
    """Query filters for company search"""
    authority_status: Optional[str] = None
    safety_rating: Optional[str] = None
    entity_type: Optional[str] = None
    min_trucks: Optional[int] = None
    chameleon_risk_threshold: Optional[float] = None


@router.post("/", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def create_company(company: Company):
    """Create a new company"""
    # Check if company already exists
    if repo.exists(company.dot_number):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Company with DOT number {company.dot_number} already exists"
        )
    
    result = repo.create(company)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create company"
        )
    
    return result


@router.get("/{dot_number}", response_model=Dict)
async def get_company(dot_number: int):
    """Get a company by DOT number"""
    company = repo.get_by_dot_number(dot_number)
    if not company:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {dot_number} not found"
        )
    return company


@router.get("/", response_model=List[Dict])
async def get_companies(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    authority_status: Optional[str] = Query(None, description="Filter by authority status"),
    safety_rating: Optional[str] = Query(None, description="Filter by safety rating"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    min_trucks: Optional[int] = Query(None, ge=0, description="Minimum number of trucks"),
    chameleon_risk_threshold: Optional[float] = Query(None, ge=0, le=1, description="Minimum chameleon risk score")
):
    """Get all companies with pagination and filters"""
    filters = {}
    if authority_status:
        filters['authority_status'] = authority_status
    if safety_rating:
        filters['safety_rating'] = safety_rating
    if entity_type:
        filters['entity_type'] = entity_type
    if min_trucks is not None:
        filters['min_trucks'] = min_trucks
    if chameleon_risk_threshold is not None:
        filters['chameleon_risk_threshold'] = chameleon_risk_threshold
    
    companies = repo.get_all(skip=skip, limit=limit, filters=filters)
    return companies


@router.patch("/{dot_number}", response_model=Dict)
async def update_company(dot_number: int, updates: CompanyUpdate):
    """Update a company's properties"""
    # Check if company exists
    if not repo.exists(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {dot_number} not found"
        )
    
    # Convert to dict and remove None values
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
            detail="Failed to update company"
        )
    
    return result


@router.delete("/{dot_number}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_company(dot_number: int):
    """Delete a company"""
    success = repo.delete(dot_number)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {dot_number} not found"
        )
    return None


@router.get("/{dot_number}/similar", response_model=List[Dict])
async def find_similar_companies(
    dot_number: int,
    threshold: float = Query(0.7, ge=0, le=1, description="Similarity threshold")
):
    """Find companies similar to the given company (potential chameleons)"""
    # Check if company exists
    if not repo.exists(dot_number):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Company with DOT number {dot_number} not found"
        )
    
    similar = repo.find_similar_companies(dot_number, threshold)
    return similar


@router.get("/statistics/summary", response_model=Dict)
async def get_company_statistics():
    """Get aggregate statistics about all companies"""
    stats = repo.get_statistics()
    return stats


@router.post("/bulk", response_model=Dict, status_code=status.HTTP_201_CREATED)
async def bulk_create_companies(companies: List[Company]):
    """Bulk create multiple companies"""
    if not companies:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No companies provided"
        )
    
    if len(companies) > 1000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum 1000 companies per bulk request"
        )
    
    # Check for duplicates in the request
    dot_numbers = [c.dot_number for c in companies]
    if len(dot_numbers) != len(set(dot_numbers)):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Duplicate DOT numbers in request"
        )
    
    # Check if any already exist
    existing = []
    for dot in dot_numbers:
        if repo.exists(dot):
            existing.append(dot)
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Companies with these DOT numbers already exist: {existing}"
        )
    
    result = repo.bulk_create(companies)
    return result