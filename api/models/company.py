from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Company(BaseModel):
    # Primary Identifiers
    dot_number: int
    mc_number: Optional[str] = None
    duns_number: Optional[str] = None
    ein: Optional[str] = None

    # Basic Information
    legal_name: str
    dba_name: List[str] = Field(default_factory=list)
    entity_type: Optional[str] = None

    # Status Information
    authority_status: Optional[str] = None
    safety_rating: Optional[str] = None
    operation_classification: Optional[str] = None
    
    # Corporate Structure
    company_type: Optional[str] = None
    operating_model: Optional[str] = None
    parent_dot_model: Optional[int] = None
    ultimate_parent_id: Optional[str] = None
    consolidation_level: Optional[int] = None
    is_publicly_traded: Optional[bool] = None
    parent_company_name: Optional[str] = None
    sec_cik: Optional[str] = None
    known_subsidiaries: List[str] = Field(default_factory=list)

    # Operational Metrics
    total_drivers: Optional[int] = None
    total_trucks: Optional[int] = None
    total_trailers: Optional[int] = None

    # Risk Scores (calculated)
    chameleon_risk_score: Optional[float] = None
    safety_risk_score: Optional[float] = None
    financial_risk_score: Optional[float] = None

    # Temporal
    created_date: Optional[date] = None
    last_updated: Optional[datetime] = None
    mcs150_date: Optional[date] = None

    # Financial
    insurance_minimum: Optional[float] = None
    cargo_carried: List[str] = Field(default_factory=list)

    # Metadata
    data_completeness_score: Optional[float] = None