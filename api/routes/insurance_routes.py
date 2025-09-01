"""
Insurance-related API routes for the RICO fraud detection system.
Provides endpoints for insurance policy management, fraud detection,
and SearchCarriers enrichment.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from datetime import date
import asyncio

from models.insurance_policy import InsurancePolicy
from models.insurance_event import InsuranceEvent
from repositories.insurance_policy_repository import InsurancePolicyRepository
from repositories.carrier_repository import CarrierRepository
from services.searchcarriers_client import SearchCarriersClient
from scripts.import.searchcarriers_insurance_enrichment import SearchCarriersInsuranceEnrichment

router = APIRouter(prefix="/insurance", tags=["Insurance"])

# Repository instances
policy_repo = InsurancePolicyRepository()
carrier_repo = CarrierRepository()


@router.post("/policies/", response_model=dict, status_code=201)
def create_insurance_policy(policy: InsurancePolicy):
    """Create a new insurance policy.
    
    Args:
        policy: InsurancePolicy model with all required fields
        
    Returns:
        dict: Created policy with confirmation
        
    Raises:
        HTTPException: If policy already exists or creation fails
    """
    # Check if policy already exists
    existing = policy_repo.get_by_id(policy.policy_id)
    if existing:
        raise HTTPException(status_code=409, detail=f"Policy {policy.policy_id} already exists")
    
    # Create the policy
    created = policy_repo.create(policy)
    if not created:
        raise HTTPException(status_code=500, detail="Failed to create insurance policy")
    
    # Create relationships if carrier exists
    if carrier_repo.exists(policy.carrier_usdot):
        policy_repo.create_carrier_relationship(
            policy.policy_id,
            policy.carrier_usdot,
            policy.effective_date,
            policy.cancellation_date or policy.expiration_date
        )
    
    return {"message": "Insurance policy created", "policy": created}


@router.get("/policies/{policy_id}", response_model=dict)
def get_insurance_policy(policy_id: str):
    """Get an insurance policy by ID.
    
    Args:
        policy_id: Unique policy identifier
        
    Returns:
        dict: Policy data
        
    Raises:
        HTTPException: If policy not found
    """
    policy = policy_repo.get_by_id(policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail=f"Policy {policy_id} not found")
    
    return policy


@router.get("/carriers/{carrier_usdot}/policies", response_model=List[dict])
def get_carrier_insurance_policies(
    carrier_usdot: int,
    active_only: bool = Query(False, description="Return only active policies"),
    include_expired: bool = Query(True, description="Include expired policies")
):
    """Get all insurance policies for a specific carrier.
    
    Args:
        carrier_usdot: Carrier's USDOT number
        active_only: If True, only return active policies
        include_expired: If False, exclude expired policies
        
    Returns:
        list: List of insurance policies
    """
    policies = policy_repo.get_by_carrier(carrier_usdot, active_only, include_expired)
    return policies


@router.get("/carriers/{carrier_usdot}/timeline", response_model=List[dict])
def get_carrier_insurance_timeline(carrier_usdot: int):
    """Get complete insurance timeline for a carrier including policies and events.
    
    Args:
        carrier_usdot: Carrier's USDOT number
        
    Returns:
        list: Chronologically ordered timeline of insurance policies and events
    """
    timeline = policy_repo.get_carrier_insurance_timeline(carrier_usdot)
    return timeline


@router.post("/carriers/{carrier_usdot}/enrich", response_model=dict)
async def enrich_carrier_insurance(
    carrier_usdot: int,
    background_tasks: BackgroundTasks
):
    """Trigger SearchCarriers API enrichment for a specific carrier.
    
    Args:
        carrier_usdot: Carrier's USDOT number
        background_tasks: FastAPI background task handler
        
    Returns:
        dict: Enrichment status
        
    Raises:
        HTTPException: If carrier not found
    """
    # Check if carrier exists
    carrier = carrier_repo.get_by_usdot(carrier_usdot)
    if not carrier:
        raise HTTPException(status_code=404, detail=f"Carrier {carrier_usdot} not found")
    
    # Run enrichment in background
    async def enrich_task():
        enricher = SearchCarriersInsuranceEnrichment()
        await enricher.enrich_carrier(carrier)
    
    background_tasks.add_task(enrich_task)
    
    return {
        "message": f"Enrichment started for carrier {carrier_usdot}",
        "carrier_name": carrier['carrier_name'],
        "status": "processing"
    }


@router.get("/fraud/coverage-gaps", response_model=List[dict])
def get_insurance_coverage_gaps(
    min_gap_days: int = Query(30, description="Minimum gap size in days"),
    carrier_usdot: Optional[int] = Query(None, description="Filter by specific carrier")
):
    """Detect insurance coverage gaps across carriers.
    
    Args:
        min_gap_days: Minimum gap size to report (default 30 days)
        carrier_usdot: Optional filter for specific carrier
        
    Returns:
        list: Coverage gaps with details
    """
    if carrier_usdot:
        gaps = policy_repo.detect_coverage_gaps(carrier_usdot, min_gap_days)
    else:
        gaps = carrier_repo.detect_insurance_gaps(min_gap_days)
    
    return gaps


@router.get("/fraud/insurance-shopping", response_model=List[dict])
def detect_insurance_shopping(
    months_window: int = Query(12, description="Time window in months"),
    min_providers: int = Query(3, description="Minimum number of providers to flag")
):
    """Detect carriers with frequent insurance provider changes.
    
    Args:
        months_window: Time window to check (default 12 months)
        min_providers: Minimum providers to flag as shopping (default 3)
        
    Returns:
        list: Carriers showing insurance shopping patterns
    """
    shopping_patterns = carrier_repo.detect_insurance_shopping_patterns(months_window, min_providers)
    return shopping_patterns


@router.get("/fraud/underinsured", response_model=List[dict])
def find_underinsured_carriers(
    cargo_type: str = Query("GENERAL_FREIGHT", description="Type of cargo for minimum requirements")
):
    """Find carriers operating with insurance below federal minimums.
    
    Args:
        cargo_type: Type of cargo (affects minimum requirements)
        
    Returns:
        list: Underinsured carriers with coverage details
    """
    underinsured = carrier_repo.find_underinsured_operations(cargo_type)
    return underinsured


@router.get("/fraud/risk-scores", response_model=List[dict])
def get_insurance_fraud_risk_scores():
    """Calculate comprehensive fraud risk scores for all carriers.
    
    Returns:
        list: Carriers with risk scores and contributing factors
    """
    risk_scores = carrier_repo.get_insurance_fraud_risk_scores()
    return risk_scores


@router.get("/fraud/chameleon-patterns", response_model=List[dict])
def detect_chameleon_carriers():
    """Detect potential chameleon carriers based on insurance and officer patterns.
    
    Returns:
        list: Potential chameleon carriers with suspicious patterns
    """
    patterns = carrier_repo.find_chameleon_carrier_patterns()
    return patterns


@router.post("/events/", response_model=dict, status_code=201)
def create_insurance_event(event: InsuranceEvent):
    """Create an insurance event for tracking state changes.
    
    Args:
        event: InsuranceEvent model
        
    Returns:
        dict: Created event with confirmation
    """
    created = policy_repo.create_insurance_event(event)
    if not created:
        raise HTTPException(status_code=500, detail="Failed to create insurance event")
    
    return {"message": "Insurance event created", "event": created}


@router.get("/compliance/check/{carrier_usdot}", response_model=dict)
async def check_carrier_compliance(carrier_usdot: int):
    """Check if a carrier meets insurance compliance requirements.
    
    Args:
        carrier_usdot: Carrier's USDOT number
        
    Returns:
        dict: Compliance status and violations
    """
    client = SearchCarriersClient()
    compliance = await client.check_insurance_compliance(carrier_usdot)
    return compliance


@router.post("/bulk-enrich/high-risk", response_model=dict)
async def bulk_enrich_high_risk_carriers(
    limit: int = Query(10, description="Maximum number of carriers to process"),
    background_tasks: BackgroundTasks
):
    """Enrich high-risk carriers (violations > 20 or crashes > 5) with insurance data.
    
    Args:
        limit: Maximum carriers to process
        background_tasks: FastAPI background task handler
        
    Returns:
        dict: Enrichment status
    """
    # Get high-risk carriers
    high_risk = carrier_repo.get_high_risk_carriers()[:limit]
    
    if not high_risk:
        return {"message": "No high-risk carriers found", "count": 0}
    
    # Run enrichment in background
    async def enrich_task():
        enricher = SearchCarriersInsuranceEnrichment()
        await enricher.enrich_high_risk_carriers(limit)
    
    background_tasks.add_task(enrich_task)
    
    return {
        "message": f"Started enrichment for {len(high_risk)} high-risk carriers",
        "carriers": [{"usdot": c['usdot'], "name": c['carrier_name']} for c in high_risk],
        "status": "processing"
    }


@router.get("/statistics/summary", response_model=dict)
def get_insurance_statistics():
    """Get summary statistics for insurance data and fraud patterns.
    
    Returns:
        dict: Comprehensive statistics
    """
    # Get various fraud patterns
    gaps = carrier_repo.detect_insurance_gaps(30)
    shopping = carrier_repo.detect_insurance_shopping_patterns(12, 3)
    underinsured = carrier_repo.find_underinsured_operations()
    risk_scores = carrier_repo.get_insurance_fraud_risk_scores()
    
    # Calculate statistics
    high_risk_count = sum(1 for r in risk_scores if r['risk_score'] > 50)
    
    return {
        "total_carriers": len(carrier_repo.get_all()),
        "carriers_with_gaps": len(gaps),
        "average_gap_days": sum(g['gap_days'] for g in gaps) / len(gaps) if gaps else 0,
        "insurance_shopping_carriers": len(shopping),
        "underinsured_carriers": len(underinsured),
        "high_risk_carriers": high_risk_count,
        "top_risks": risk_scores[:5] if risk_scores else []
    }