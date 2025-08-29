from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class Person(BaseModel):
    # Identifiers
    person_id: str
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None

    # Additional Info
    date_of_birth: Optional[date] = None

    # Contact
    email: List[str] = Field(default_factory=list)
    phone: List[str] = Field(default_factory=list)

    # Metadata
    first_seen: Optional[date] = None
    last_seen: Optional[date] = None
    source: List[str] = Field(default_factory=list)