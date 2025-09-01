from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class TargetCompany(BaseModel):
    # Primary Identifiers
    dot_number: int
    mc_number: Optional[str] = None
    
    # Basic Information
    legal_name: str
    dba_name: List[str] = Field(default_factory=list)
    entity_type: str  # "BROKER", "FREIGHT_FORWARDER", "CARRIER", etc.
    
    # Status Information
    authority_status: Optional[str] = None
    safety_rating: Optional[str] = None
    
    # Operational Metrics
    total_drivers: Optional[int] = None
    total_trucks: Optional[int] = None
    total_trailers: Optional[int] = None
    
    # Risk Scores (calculated)
    risk_score: Optional[float] = None
    
    # Temporal
    created_date: Optional[date] = None
    last_updated: Optional[datetime] = None
    
    # Metadata
    data_source: Optional[str] = None  # Where this data came from