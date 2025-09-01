from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class Person(BaseModel):
    """Person entity model representing individuals in the trucking network.
    
    Can represent carrier officers, company executives, or other individuals
    relevant to fraud detection analysis.
    """
    
    # Identifiers
    person_id: str = Field(..., description="Unique identifier for the person", example="P-123456")
    full_name: str = Field(..., description="Full name of the person", example="John Smith")
    first_name: Optional[str] = Field(None, description="First name", example="John")
    last_name: Optional[str] = Field(None, description="Last name", example="Smith")

    # Additional Info
    date_of_birth: Optional[date] = Field(None, description="Date of birth")

    # Contact
    email: List[str] = Field(default_factory=list, description="List of email addresses")
    phone: List[str] = Field(default_factory=list, description="List of phone numbers")

    # Metadata
    first_seen: Optional[date] = Field(None, description="Date first observed in the system")
    last_seen: Optional[date] = Field(None, description="Date last observed in the system")
    source: List[str] = Field(default_factory=list, description="Data sources for this person", example=["JB_HUNT", "API"])
    
    class Config:
        json_schema_extra = {
            "example": {
                "person_id": "P-123456",
                "full_name": "John Smith",
                "first_name": "John",
                "last_name": "Smith",
                "email": ["john.smith@example.com"],
                "phone": ["555-0123"],
                "source": ["JB_HUNT_IMPORT", "API"]
            }
        }