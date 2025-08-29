from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Authority(BaseModel):
    # Identifiers
    authority_id: str
    mc_number: Optional[str] = None
    dot_number: Optional[int] = None

    # Authority Details
    authority_type: Optional[str] = None
    commodity_types: List[str] = Field(default_factory=list)

    # Status
    status: Optional[str] = None

    # Important Dates
    granted_date: Optional[date] = None
    effective_date: Optional[date] = None
    revoked_date: Optional[date] = None
    reinstate_date: Optional[date] = None

    # Revocation Details
    revocation_reason: Optional[str] = None

    # Metadata
    last_update: Optional[datetime] = None