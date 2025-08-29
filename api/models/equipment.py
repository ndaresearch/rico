from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class Equipment(BaseModel):
    # Primary Identifier
    vin: str

    # Vehicle Information
    type: Optional[str] = None
    make: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None

    # Registration
    title_state: Optional[str] = None
    title_number: Optional[str] = None
    registration_states: List[str] = Field(default_factory=list)

    # Status
    status: Optional[str] = None

    # Financial
    lien_holder: Optional[str] = None
    purchase_price: Optional[float] = None
    purchase_date: Optional[date] = None

    # Lease Information
    under_lease: Optional[bool] = None
    lease_company: Optional[str] = None

    # Risk Indicators
    incident_count: Optional[int] = None
    inspection_failure_rate: Optional[float] = None

    # Metadata
    first_seen: Optional[date] = None
    last_verified: Optional[date] = None