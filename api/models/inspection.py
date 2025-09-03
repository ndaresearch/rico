from datetime import date
from typing import Optional

from pydantic import BaseModel


class Inspection(BaseModel):
    inspection_id: str
    usdot: int
    inspection_date: date
    level: int
    state: str
    location: Optional[str] = None
    violations_count: int = 0
    oos_violations_count: int = 0
    vehicle_oos: bool = False
    driver_oos: bool = False
    hazmat_oos: bool = False
    result: str