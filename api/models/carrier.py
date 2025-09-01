from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class Carrier(BaseModel):
    # Primary Identifier
    usdot: int
    
    # JB Hunt Association
    jb_carrier: bool = False
    carrier_name: str
    
    # Management
    primary_officer: str
    
    # Insurance
    insurance_provider: Optional[str] = None
    insurance_amount: Optional[float] = None  # Parsed from string like "$1 Million"
    
    # Fleet Metrics
    trucks: Optional[int] = None
    mcs150_drivers: Optional[int] = None
    mcs150_miles: Optional[int] = None
    ampd: Optional[int] = None  # Average Miles Per Driver
    
    # Safety Metrics
    inspections: Optional[int] = None
    violations: Optional[int] = None
    oos: Optional[int] = None  # Out of Service
    crashes: Optional[int] = None
    driver_oos_rate: Optional[float] = None  # Percentage
    vehicle_oos_rate: Optional[float] = None  # Percentage
    
    # Temporal
    created_date: Optional[date] = None
    last_updated: Optional[datetime] = None
    mcs150_date: Optional[date] = None  # Date of last MCS-150 update
    
    # Metadata
    data_source: Optional[str] = None  # Where this data came from