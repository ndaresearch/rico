from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class Carrier(BaseModel):
    """Trucking carrier entity model for the RICO graph database.
    
    Represents a trucking company with safety metrics, insurance information,
    and fleet details used for fraud detection analysis.
    """
    
    # Primary Identifier
    usdot: int = Field(..., description="USDOT number - unique identifier for the carrier", example=777001)
    
    # JB Hunt Association
    jb_carrier: bool = Field(False, description="Whether this carrier contracts with JB Hunt")
    carrier_name: str = Field(..., description="Legal name of the carrier", example="ABC Trucking LLC")
    
    # Management
    primary_officer: str = Field(..., description="Name of the primary officer/owner", example="John Smith")
    
    # Insurance
    insurance_provider: Optional[str] = Field(None, description="Name of insurance company", example="Progressive Commercial")
    insurance_amount: Optional[float] = Field(None, description="Insurance coverage amount in dollars", example=1000000.0)
    
    # Fleet Metrics
    trucks: Optional[int] = Field(None, ge=0, description="Number of trucks in fleet", example=25)
    mcs150_drivers: Optional[int] = Field(None, ge=0, description="Number of drivers reported on MCS-150", example=30)
    mcs150_miles: Optional[int] = Field(None, ge=0, description="Annual miles reported on MCS-150", example=2500000)
    ampd: Optional[int] = Field(None, ge=0, description="Average Miles Per Driver", example=83333)
    
    # Safety Metrics
    inspections: Optional[int] = Field(None, ge=0, description="Total number of inspections", example=45)
    violations: Optional[int] = Field(None, ge=0, description="Total number of violations", example=12)
    oos: Optional[int] = Field(None, ge=0, description="Out of Service violations", example=3)
    crashes: Optional[int] = Field(None, ge=0, description="Total number of crashes", example=2)
    driver_oos_rate: Optional[float] = Field(None, ge=0, le=100, description="Driver out-of-service rate percentage", example=5.2)
    vehicle_oos_rate: Optional[float] = Field(None, ge=0, le=100, description="Vehicle out-of-service rate percentage", example=8.7)
    
    # Temporal
    created_date: Optional[date] = Field(None, description="Date carrier was created in system")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    mcs150_date: Optional[date] = Field(None, description="Date of last MCS-150 filing")
    
    # Metadata
    data_source: Optional[str] = Field(None, description="Source of this data", example="JB_HUNT_IMPORT")
    
    class Config:
        json_schema_extra = {
            "example": {
                "usdot": 777001,
                "jb_carrier": True,
                "carrier_name": "ABC Trucking LLC",
                "primary_officer": "John Smith",
                "insurance_provider": "Progressive Commercial",
                "insurance_amount": 1000000.0,
                "trucks": 25,
                "mcs150_drivers": 30,
                "violations": 12,
                "crashes": 2,
                "driver_oos_rate": 5.2,
                "vehicle_oos_rate": 8.7,
                "data_source": "JB_HUNT_IMPORT"
            }
        }