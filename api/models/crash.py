from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Crash(BaseModel):
    # Identifiers
    report_number: str
    report_state: Optional[str] = None
    usdot: int

    # Incident Details
    crash_date: datetime
    severity: Optional[str] = None
    tow_away: bool = False

    # Counts
    fatalities: Optional[int] = None
    injuries: Optional[int] = None
    vehicles_involved: Optional[int] = None

    # Conditions
    weather: Optional[str] = None
    road_condition: Optional[str] = None
    light_condition: Optional[str] = None

    # Location
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Fault
    preventable: Optional[bool] = None
    citation_issued: Optional[bool] = None