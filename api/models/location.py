from datetime import date
from typing import Optional

from pydantic import BaseModel


class Location(BaseModel):
    # Identifiers
    location_id: str

    # Address Components
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    country: Optional[str] = None

    # Geocoding
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Location Type
    location_type: Optional[str] = None

    # Risk Indicators
    company_count: Optional[int] = None

    # Metadata
    first_seen: Optional[date] = None
    verified: Optional[bool] = None