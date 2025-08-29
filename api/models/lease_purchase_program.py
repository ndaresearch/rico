from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class LeasePurchaseProgram(BaseModel):
    # Identifiers
    program_id: str
    program_name: Optional[str] = None

    # Terms
    weekly_payment: Optional[float] = None
    total_price: Optional[float] = None
    down_payment: Optional[float] = None
    interest_rate: Optional[float] = None
    term_months: Optional[int] = None
    balloon_payment: Optional[float] = None

    # Success Metrics
    total_enrolled: Optional[int] = None
    successful_completions: Optional[int] = None
    failures: Optional[int] = None
    success_rate: Optional[float] = None

    # Dates
    program_start_date: Optional[date] = None
    program_end_date: Optional[date] = None

    # Flags
    predatory_indicators: List[str] = Field(default_factory=list)