from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class SafetySnapshot(BaseModel):
    usdot: int
    snapshot_date: date
    driver_oos_rate: float
    vehicle_oos_rate: float
    driver_oos_national_avg: float = 5.0
    vehicle_oos_national_avg: float = 20.0
    unsafe_driving_score: Optional[float] = None
    hours_of_service_score: Optional[float] = None
    driver_fitness_score: Optional[float] = None
    controlled_substances_score: Optional[float] = None
    vehicle_maintenance_score: Optional[float] = None
    hazmat_compliance_score: Optional[float] = None
    crash_indicator_score: Optional[float] = None
    unsafe_driving_alert: bool = False
    hours_of_service_alert: bool = False
    driver_fitness_alert: bool = False
    controlled_substances_alert: bool = False
    vehicle_maintenance_alert: bool = False
    hazmat_compliance_alert: bool = False
    crash_indicator_alert: bool = False
    last_update: datetime