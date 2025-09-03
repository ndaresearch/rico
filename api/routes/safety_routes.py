from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, Response, status
from pydantic import BaseModel

from repositories.safety_snapshot_repository import SafetySnapshotRepository
from repositories.inspection_repository import InspectionRepository
from repositories.crash_repository import CrashRepository


router = APIRouter(
    prefix="/carriers", 
    tags=["safety"],
    responses={404: {"description": "Carrier not found"}}
)

safety_repo = SafetySnapshotRepository()
inspection_repo = InspectionRepository()
crash_repo = CrashRepository()


class RiskAssessment(BaseModel):
    """Model for carrier risk assessment"""
    usdot: int
    risk_level: str
    driver_oos_multiplier: float
    vehicle_oos_multiplier: float
    fatal_crashes: int
    injury_crashes: int
    total_crashes: int
    violation_frequency: float
    high_risk_indicators: List[str]


@router.get("/{usdot}/safety-profile",
            response_model=Dict,
            summary="Get carrier safety profile",
            description="Returns the latest safety snapshot with risk flags")
async def get_carrier_safety_profile(usdot: int):
    """Get the latest safety profile for a carrier.
    
    Args:
        usdot: USDOT number of the carrier
        
    Returns:
        dict: Latest SafetySnapshot with risk assessment
        
    Raises:
        HTTPException: 404 if no safety data found
    """
    snapshot = safety_repo.find_latest_by_usdot(usdot)
    
    if not snapshot:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No safety profile found for carrier {usdot}"
        )
    
    # Add risk assessment
    risk_flags = []
    if snapshot.get("driver_oos_rate", 0) > 10.0:
        risk_flags.append("HIGH_DRIVER_OOS")
    if snapshot.get("vehicle_oos_rate", 0) > 40.0:
        risk_flags.append("HIGH_VEHICLE_OOS")
    
    # Check for SMS alerts
    alert_fields = [
        "unsafe_driving_alert", "hours_of_service_alert", "driver_fitness_alert",
        "controlled_substances_alert", "vehicle_maintenance_alert", 
        "hazmat_compliance_alert", "crash_indicator_alert"
    ]
    
    active_alerts = [field.replace("_alert", "").upper() 
                     for field in alert_fields 
                     if snapshot.get(field, False)]
    
    return {
        "snapshot": snapshot,
        "risk_flags": risk_flags,
        "active_alerts": active_alerts,
        "is_high_risk": len(risk_flags) > 0 or len(active_alerts) > 0
    }


@router.get("/{usdot}/crashes",
            response_model=List[Dict],
            summary="Get carrier crashes",
            description="Returns crash history with severity information")
async def get_carrier_crashes(
    usdot: int,
    include_fatal: bool = Query(True, description="Include fatal crashes"),
    include_injury: bool = Query(True, description="Include injury crashes"),
    include_property: bool = Query(True, description="Include property-only crashes")
):
    """Get crash history for a carrier.
    
    Args:
        usdot: USDOT number of the carrier
        include_fatal: Include fatal crashes
        include_injury: Include injury crashes  
        include_property: Include property-only crashes
        
    Returns:
        list: Crash records with severity classification
    """
    all_crashes = crash_repo.find_by_usdot(usdot)
    
    # Filter by severity if requested
    filtered_crashes = []
    for crash in all_crashes:
        if crash.get("fatalities", 0) > 0 and include_fatal:
            crash["severity_category"] = "FATAL"
            filtered_crashes.append(crash)
        elif crash.get("injuries", 0) > 0 and include_injury:
            crash["severity_category"] = "INJURY"
            filtered_crashes.append(crash)
        elif include_property:
            crash["severity_category"] = "PROPERTY"
            filtered_crashes.append(crash)
    
    # Calculate statistics
    stats = crash_repo.calculate_crash_statistics(usdot, months=24)
    
    return {
        "crashes": filtered_crashes,
        "statistics": stats,
        "total_count": len(filtered_crashes)
    }


@router.get("/{usdot}/inspections",
            response_model=Dict,
            summary="Get carrier inspections",
            description="Returns inspection history with violations")
async def get_carrier_inspections(
    usdot: int,
    limit: int = Query(100, description="Maximum number of inspections to return"),
    include_clean: bool = Query(True, description="Include clean inspections"),
    include_violations: bool = Query(True, description="Include inspections with violations"),
    include_oos: bool = Query(True, description="Include out-of-service inspections")
):
    """Get inspection history for a carrier.
    
    Args:
        usdot: USDOT number of the carrier
        limit: Maximum number of inspections to return
        include_clean: Include clean inspections
        include_violations: Include inspections with violations
        include_oos: Include out-of-service inspections
        
    Returns:
        dict: Inspection records with violation details and statistics
    """
    all_inspections = inspection_repo.find_by_usdot(usdot, limit=limit)
    
    # Filter by result type if requested
    filtered_inspections = []
    for inspection in all_inspections:
        result = inspection.get("result", "")
        if result == "Clean" and include_clean:
            filtered_inspections.append(inspection)
        elif result == "Violations" and include_violations:
            filtered_inspections.append(inspection)
        elif result == "OOS" and include_oos:
            filtered_inspections.append(inspection)
    
    # Calculate statistics
    stats = inspection_repo.calculate_violation_rate(usdot, months=24)
    
    # Find repeat violations
    repeat_violations = inspection_repo.find_repeat_violations(usdot)
    
    return {
        "inspections": filtered_inspections,
        "statistics": stats,
        "repeat_violations": repeat_violations,
        "total_count": len(filtered_inspections)
    }


@router.get("/{usdot}/risk-assessment",
            response_model=RiskAssessment,
            summary="Get carrier risk assessment",
            description="Calculate comprehensive risk analysis for a carrier")
async def get_carrier_risk_assessment(usdot: int):
    """Calculate and return comprehensive risk analysis.
    
    Args:
        usdot: USDOT number of the carrier
        
    Returns:
        RiskAssessment: Comprehensive risk analysis including:
            - OOS rate multipliers (rate / national_avg)
            - Fatal/injury crash count
            - Violation frequency
            - Risk classification: "LOW", "MODERATE", "HIGH", "CRITICAL"
    """
    risk_indicators = []
    
    # Get safety snapshot
    safety_snapshot = safety_repo.find_latest_by_usdot(usdot)
    driver_oos_multiplier = 0.0
    vehicle_oos_multiplier = 0.0
    
    if safety_snapshot:
        # Calculate OOS multipliers
        driver_oos_rate = safety_snapshot.get("driver_oos_rate", 0.0)
        vehicle_oos_rate = safety_snapshot.get("vehicle_oos_rate", 0.0)
        driver_oos_multiplier = driver_oos_rate / 5.0  # National avg is 5%
        vehicle_oos_multiplier = vehicle_oos_rate / 20.0  # National avg is 20%
        
        if driver_oos_multiplier > 2.0:
            risk_indicators.append("DRIVER_OOS_2X_NATIONAL")
        if vehicle_oos_multiplier > 2.0:
            risk_indicators.append("VEHICLE_OOS_2X_NATIONAL")
        
        # Check SMS alerts
        if any([safety_snapshot.get(f"{basic}_alert", False) 
                for basic in ["unsafe_driving", "hours_of_service", "driver_fitness",
                             "controlled_substances", "vehicle_maintenance", 
                             "hazmat_compliance", "crash_indicator"]]):
            risk_indicators.append("SMS_ALERTS_ACTIVE")
    
    # Get crash statistics
    crash_stats = crash_repo.calculate_crash_statistics(usdot, months=24)
    fatal_crashes = crash_stats.get("fatal_crashes", 0)
    injury_crashes = crash_stats.get("injury_crashes", 0)
    total_crashes = crash_stats.get("total_crashes", 0)
    
    if fatal_crashes > 0:
        risk_indicators.append("FATAL_CRASHES")
    if injury_crashes > 3:
        risk_indicators.append("HIGH_INJURY_CRASHES")
    
    # Get inspection statistics
    inspection_stats = inspection_repo.calculate_violation_rate(usdot, months=24)
    violation_frequency = inspection_stats.get("avg_violations_per_inspection", 0.0)
    
    if violation_frequency > 20:
        risk_indicators.append("HIGH_VIOLATION_FREQUENCY")
    
    # Determine risk level
    risk_score = (
        driver_oos_multiplier * 2 +
        vehicle_oos_multiplier * 2 +
        fatal_crashes * 10 +
        min(injury_crashes, 10) +
        min(violation_frequency / 5, 10)
    )
    
    if risk_score >= 30:
        risk_level = "CRITICAL"
    elif risk_score >= 20:
        risk_level = "HIGH"
    elif risk_score >= 10:
        risk_level = "MODERATE"
    else:
        risk_level = "LOW"
    
    return RiskAssessment(
        usdot=usdot,
        risk_level=risk_level,
        driver_oos_multiplier=round(driver_oos_multiplier, 2),
        vehicle_oos_multiplier=round(vehicle_oos_multiplier, 2),
        fatal_crashes=fatal_crashes,
        injury_crashes=injury_crashes,
        total_crashes=total_crashes,
        violation_frequency=round(violation_frequency, 2),
        high_risk_indicators=risk_indicators
    )


@router.get("/high-risk",
            response_model=List[Dict],
            summary="Get high-risk carriers",
            description="Returns carriers with high OOS rates or fatal crashes")
async def get_high_risk_carriers(
    limit: int = Query(100, description="Maximum number of carriers to return")
):
    """Get list of high-risk carriers based on safety metrics.
    
    Args:
        limit: Maximum number of carriers to return
        
    Returns:
        list: High-risk carriers with their safety metrics
    """
    # Get carriers with high OOS rates
    high_oos_carriers = safety_repo.find_high_risk_carriers(limit=limit)
    
    # Get carriers with fatal crashes
    high_crash_carriers = crash_repo.find_high_risk_carriers_by_crashes(limit=limit)
    
    # Combine and deduplicate
    high_risk_usdots = set()
    results = []
    
    for carrier_data in high_oos_carriers:
        carrier = carrier_data.get("c", {})
        snapshot = carrier_data.get("latest_snapshot", {})
        usdot = carrier.get("usdot")
        if usdot and usdot not in high_risk_usdots:
            high_risk_usdots.add(usdot)
            results.append({
                "carrier": carrier,
                "risk_type": "HIGH_OOS",
                "safety_snapshot": snapshot
            })
    
    for carrier_data in high_crash_carriers:
        carrier = carrier_data.get("c", {})
        usdot = carrier.get("usdot")
        if usdot and usdot not in high_risk_usdots:
            high_risk_usdots.add(usdot)
            results.append({
                "carrier": carrier,
                "risk_type": "HIGH_CRASH",
                "crash_statistics": {
                    "crash_count": carrier_data.get("crash_count"),
                    "fatal_crashes": carrier_data.get("fatal_crashes"),
                    "total_fatalities": carrier_data.get("total_fatalities"),
                    "total_injuries": carrier_data.get("total_injuries"),
                    "risk_score": carrier_data.get("risk_score")
                }
            })
    
    # Sort by risk severity
    results.sort(key=lambda x: x.get("crash_statistics", {}).get("risk_score", 0) + 
                               x.get("safety_snapshot", {}).get("driver_oos_rate", 0) +
                               x.get("safety_snapshot", {}).get("vehicle_oos_rate", 0),
                 reverse=True)
    
    return results[:limit]