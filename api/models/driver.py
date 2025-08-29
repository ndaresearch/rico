from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class Driver(BaseModel):
    # Identifiers
    cdl_number: str
    driver_id: Optional[str] = None

    # License Info
    cdl_state: Optional[str] = None
    cdl_class: Optional[str] = None
    endorsements: List[str] = Field(default_factory=list)
    medical_cert_expiry: Optional[date] = None

    # Status
    status: Optional[str] = None

    # Financial (if available)
    reported_annual_income: Optional[float] = None
    debt_obligations: Optional[float] = None

    # Training
    cdl_school_graduation: Optional[date] = None

    # Metadata
    first_employment_date: Optional[date] = None
    last_known_employment: Optional[date] = None