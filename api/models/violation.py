from datetime import date
from typing import Optional

from pydantic import BaseModel


class Violation(BaseModel):
    # Identifiers
    violation_id: str
    inspection_id: Optional[str] = None

    # Violation Details
    code: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None

    # Severity
    severity_weight: Optional[int] = None
    oos_indicator: Optional[bool] = None

    # Date
    violation_date: date

    # Location
    inspection_state: Optional[str] = None
    inspection_level: Optional[int] = None