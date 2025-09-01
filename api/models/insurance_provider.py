from datetime import date, datetime
from typing import Optional
import uuid

from pydantic import BaseModel, Field


class InsuranceProvider(BaseModel):
    # Primary Identifier
    provider_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Basic Information
    name: str  # Unique name of the insurance provider
    
    # Contact Information (if available)
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    website: Optional[str] = None
    
    # Temporal
    created_date: Optional[date] = None
    last_updated: Optional[datetime] = None
    
    # Metadata
    total_carriers_insured: Optional[int] = None  # Count of carriers using this provider
    data_source: Optional[str] = None  # Where this data came from