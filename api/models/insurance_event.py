from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field


class InsuranceEvent(BaseModel):
    """Insurance event entity model for tracking insurance state changes.
    
    Captures insurance-related events like new policies, cancellations, lapses,
    and renewals to enable temporal analysis and fraud pattern detection.
    """
    
    # Primary Identifier
    event_id: str = Field(
        ...,
        description="Unique identifier for the insurance event",
        example="EVT-777001-2024-01-15-NEW"
    )
    
    # Carrier Association
    carrier_usdot: int = Field(
        ...,
        description="USDOT number of the carrier involved in this event",
        example=777001
    )
    
    # Event Details
    event_type: str = Field(
        ...,
        description="Type of insurance event (NEW_POLICY, CANCELLATION, LAPSE, RENEWAL, PROVIDER_CHANGE, COVERAGE_INCREASE, COVERAGE_DECREASE)",
        example="NEW_POLICY"
    )
    event_date: date = Field(
        ...,
        description="Date when the event occurred",
        example="2024-01-15"
    )
    
    # Provider Changes
    previous_provider: Optional[str] = Field(
        None,
        description="Previous insurance provider name (for PROVIDER_CHANGE events)",
        example="State Farm Commercial"
    )
    new_provider: Optional[str] = Field(
        None,
        description="New insurance provider name (for NEW_POLICY or PROVIDER_CHANGE events)",
        example="Progressive Commercial"
    )
    
    # Coverage Changes
    previous_coverage: Optional[float] = Field(
        None,
        ge=0,
        description="Previous coverage amount in dollars",
        example=750000.0
    )
    new_coverage: Optional[float] = Field(
        None,
        ge=0,
        description="New coverage amount in dollars",
        example=1000000.0
    )
    coverage_change: Optional[float] = Field(
        None,
        description="Change in coverage amount (positive for increase, negative for decrease)",
        example=250000.0
    )
    
    # Gap Analysis
    days_without_coverage: Optional[int] = Field(
        None,
        ge=0,
        description="Number of days without insurance coverage before this event",
        example=45
    )
    
    # Policy References
    previous_policy_id: Optional[str] = Field(
        None,
        description="ID of the previous policy (for transitions)",
        example="POL-777001-STATE-2023-12"
    )
    new_policy_id: Optional[str] = Field(
        None,
        description="ID of the new policy (for NEW_POLICY or RENEWAL events)",
        example="POL-777001-PROG-2024-01"
    )
    
    # Compliance Impact
    compliance_violation: bool = Field(
        False,
        description="Whether this event resulted in a compliance violation",
        example=False
    )
    violation_reason: Optional[str] = Field(
        None,
        description="Reason for compliance violation if applicable",
        example="Coverage gap exceeded 30 days"
    )
    
    # Fraud Indicators
    is_suspicious: bool = Field(
        False,
        description="Whether this event shows suspicious patterns",
        example=False
    )
    fraud_indicators: Optional[list[str]] = Field(
        None,
        description="List of fraud indicators detected",
        example=["rapid_provider_change", "coverage_reduction_before_claim"]
    )
    
    # Event Context
    reason: Optional[str] = Field(
        None,
        description="Stated reason for the event",
        example="NON_PAYMENT"
    )
    notes: Optional[str] = Field(
        None,
        description="Additional notes or context about the event",
        example="Carrier switched providers after rate increase"
    )
    
    # Metadata
    created_at: datetime = Field(
        default_factory=lambda: datetime.utcnow(),
        description="Timestamp when this record was created"
    )
    data_source: str = Field(
        "SEARCHCARRIERS_API",
        description="Source of this event data",
        example="SEARCHCARRIERS_API"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "EVT-777001-2024-01-15-PROVIDER_CHANGE",
                "carrier_usdot": 777001,
                "event_type": "PROVIDER_CHANGE",
                "event_date": "2024-01-15",
                "previous_provider": "State Farm Commercial",
                "new_provider": "Progressive Commercial",
                "previous_coverage": 750000.0,
                "new_coverage": 1000000.0,
                "coverage_change": 250000.0,
                "days_without_coverage": 0,
                "previous_policy_id": "POL-777001-STATE-2023-12",
                "new_policy_id": "POL-777001-PROG-2024-01",
                "compliance_violation": False,
                "is_suspicious": False,
                "data_source": "SEARCHCARRIERS_API"
            }
        }
    
    def calculate_provider_stability_score(self, previous_events: list['InsuranceEvent']) -> float:
        """Calculate a stability score based on provider change frequency.
        
        Lower scores indicate more frequent changes (potential fraud indicator).
        
        Args:
            previous_events: List of previous insurance events for this carrier
            
        Returns:
            Stability score from 0.0 (very unstable) to 1.0 (very stable)
        """
        if not previous_events:
            return 1.0  # No history means we can't determine instability
        
        # Count provider changes in the last 12 months
        provider_changes = sum(
            1 for event in previous_events
            if event.event_type == "PROVIDER_CHANGE"
            and (self.event_date - event.event_date).days <= 365
        )
        
        # Score calculation: 0 changes = 1.0, 1 change = 0.8, 2 = 0.5, 3+ = 0.2
        if provider_changes == 0:
            return 1.0
        elif provider_changes == 1:
            return 0.8
        elif provider_changes == 2:
            return 0.5
        else:
            return 0.2
    
    def detect_fraud_patterns(self) -> list[str]:
        """Detect potential fraud patterns in this event.
        
        Returns:
            List of detected fraud pattern identifiers
        """
        patterns = []
        
        # Pattern 1: Coverage gap violation (> 30 days)
        if self.days_without_coverage and self.days_without_coverage > 30:
            patterns.append("extended_coverage_gap")
        
        # Pattern 2: Rapid provider change (implied by event type and timing)
        if self.event_type == "PROVIDER_CHANGE":
            patterns.append("provider_shopping")
        
        # Pattern 3: Significant coverage reduction
        if self.coverage_change and self.coverage_change < -100000:
            patterns.append("significant_coverage_reduction")
        
        # Pattern 4: Cancellation for non-payment
        if self.event_type == "CANCELLATION" and self.reason == "NON_PAYMENT":
            patterns.append("financial_distress")
        
        # Pattern 5: Multiple lapses
        if self.event_type == "LAPSE":
            patterns.append("coverage_lapse")
        
        return patterns
    
    def calculate_risk_score(self) -> float:
        """Calculate a risk score for this event.
        
        Returns:
            Risk score from 0.0 (low risk) to 1.0 (high risk)
        """
        score = 0.0
        
        # Base scores by event type
        risk_weights = {
            "CANCELLATION": 0.3,
            "LAPSE": 0.4,
            "PROVIDER_CHANGE": 0.2,
            "COVERAGE_DECREASE": 0.2,
            "NEW_POLICY": 0.1,
            "RENEWAL": 0.0,
            "COVERAGE_INCREASE": 0.0
        }
        
        score += risk_weights.get(self.event_type, 0.1)
        
        # Add risk for coverage gaps
        if self.days_without_coverage:
            if self.days_without_coverage > 30:
                score += 0.3
            elif self.days_without_coverage > 7:
                score += 0.1
        
        # Add risk for compliance violations
        if self.compliance_violation:
            score += 0.2
        
        # Add risk for suspicious patterns
        if self.is_suspicious:
            score += 0.1
        
        # Cap at 1.0
        return min(score, 1.0)