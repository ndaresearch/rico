from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field


class InsurancePolicy(BaseModel):
    """Insurance policy entity model for the RICO graph database.
    
    Represents an insurance policy with temporal data for tracking coverage history,
    detecting gaps, and identifying insurance shopping patterns for fraud detection.
    """
    
    # Primary Identifier
    policy_id: str = Field(
        ..., 
        description="Unique identifier for the insurance policy",
        example="POL-777001-PROG-2024-01"
    )
    
    # Carrier Association
    carrier_usdot: int = Field(
        ...,
        description="USDOT number of the carrier this policy covers",
        example=777001
    )
    
    # Provider Information
    provider_name: str = Field(
        ...,
        description="Name of the insurance provider",
        example="Progressive Commercial"
    )
    provider_id: Optional[str] = Field(
        None,
        description="Insurance provider's unique identifier if available",
        example="PROG-12345"
    )
    
    # Policy Details
    policy_type: str = Field(
        ...,
        description="Type of insurance filing (BMC-91 for property, BMC-32 for passenger)",
        example="BMC-91"
    )
    policy_number: Optional[str] = Field(
        None,
        description="Insurance company's policy number",
        example="COM-2024-777001"
    )
    
    # Coverage Information
    coverage_amount: float = Field(
        ...,
        ge=0,
        description="Total liability coverage amount in dollars",
        example=1000000.0
    )
    cargo_coverage: Optional[float] = Field(
        None,
        ge=0,
        description="Cargo insurance coverage amount if applicable",
        example=100000.0
    )
    
    # Temporal Data
    effective_date: date = Field(
        ...,
        description="Date when the policy becomes effective",
        example="2024-01-01"
    )
    expiration_date: Optional[date] = Field(
        None,
        description="Date when the policy expires",
        example="2025-01-01"
    )
    cancellation_date: Optional[date] = Field(
        None,
        description="Date when the policy was cancelled if applicable",
        example="2024-06-15"
    )
    
    # Cancellation Details
    cancellation_reason: Optional[str] = Field(
        None,
        description="Reason for policy cancellation",
        example="NON_PAYMENT"
    )
    
    # Compliance Status
    filing_status: str = Field(
        ...,
        description="Current filing status (ACTIVE, CANCELLED, LAPSED, PENDING)",
        example="ACTIVE"
    )
    is_compliant: bool = Field(
        True,
        description="Whether the policy meets federal minimum requirements",
        example=True
    )
    
    # Federal Requirements Check
    meets_federal_minimum: bool = Field(
        True,
        description="Whether coverage meets federal minimum for cargo type",
        example=True
    )
    required_minimum: Optional[float] = Field(
        None,
        description="Federal minimum coverage required for this carrier's cargo type",
        example=750000.0
    )
    
    # Metadata
    created_at: datetime = Field(
        default_factory=lambda: datetime.utcnow(),
        description="Timestamp when this record was created"
    )
    updated_at: Optional[datetime] = Field(
        None,
        description="Timestamp when this record was last updated"
    )
    data_source: str = Field(
        "SEARCHCARRIERS_API",
        description="Source of this insurance data",
        example="SEARCHCARRIERS_API"
    )
    
    # SearchCarriers Specific Fields
    searchcarriers_record_id: Optional[str] = Field(
        None,
        description="Original record ID from SearchCarriers API",
        example="SC-INS-123456"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "policy_id": "POL-777001-PROG-2024-01",
                "carrier_usdot": 777001,
                "provider_name": "Progressive Commercial",
                "provider_id": "PROG-12345",
                "policy_type": "BMC-91",
                "policy_number": "COM-2024-777001",
                "coverage_amount": 1000000.0,
                "cargo_coverage": 100000.0,
                "effective_date": "2024-01-01",
                "expiration_date": "2025-01-01",
                "filing_status": "ACTIVE",
                "is_compliant": True,
                "meets_federal_minimum": True,
                "required_minimum": 750000.0,
                "data_source": "SEARCHCARRIERS_API"
            }
        }
    
    def calculate_coverage_gap(self, next_policy: Optional['InsurancePolicy']) -> Optional[int]:
        """Calculate the gap in days between this policy and the next one.
        
        Args:
            next_policy: The subsequent insurance policy
            
        Returns:
            Number of days between policies, None if no gap or next policy doesn't exist
        """
        if not next_policy:
            return None
        
        # Determine end date of this policy
        end_date = self.cancellation_date or self.expiration_date
        if not end_date:
            return None
        
        # Calculate gap
        gap = (next_policy.effective_date - end_date).days
        return gap if gap > 0 else 0
    
    def is_active_on_date(self, check_date: date) -> bool:
        """Check if the policy was active on a specific date.
        
        Args:
            check_date: The date to check
            
        Returns:
            True if the policy was active on the given date
        """
        # Policy must have started
        if check_date < self.effective_date:
            return False
        
        # Check if policy ended before the date
        end_date = self.cancellation_date or self.expiration_date
        if end_date and check_date > end_date:
            return False
        
        # Check filing status
        return self.filing_status == "ACTIVE"
    
    def check_federal_compliance(self, cargo_type: str = "GENERAL_FREIGHT") -> tuple[bool, str]:
        """Check if the policy meets federal minimum requirements.
        
        Args:
            cargo_type: Type of cargo being transported
            
        Returns:
            Tuple of (is_compliant, reason)
        """
        # Federal minimums per 49 CFR § 387.7
        federal_minimums = {
            "GENERAL_FREIGHT": 750000.0,
            "HOUSEHOLD_GOODS": 750000.0,
            "HAZMAT": 5000000.0,  # Can be 1M-5M depending on material
            "PASSENGERS_15_PLUS": 5000000.0,
            "PASSENGERS_UNDER_15": 1500000.0,
            "OIL": 1000000.0
        }
        
        required = federal_minimums.get(cargo_type, 750000.0)
        
        if self.coverage_amount < required:
            return False, f"Coverage ${self.coverage_amount:,.0f} below required ${required:,.0f} for {cargo_type}"
        
        return True, "Meets federal requirements"