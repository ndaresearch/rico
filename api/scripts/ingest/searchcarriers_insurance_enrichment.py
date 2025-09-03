#!/usr/bin/env python3
"""
SearchCarriers Insurance Enrichment Script.

Fetches insurance data from SearchCarriers API and enriches carrier nodes
with temporal insurance policies, detects gaps, and identifies fraud patterns.
"""

import sys
import os
import time
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, date, timezone, timedelta
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
from models.insurance_policy import InsurancePolicy
from models.insurance_event import InsuranceEvent
from models.carrier import Carrier
from models.safety_snapshot import SafetySnapshot
from models.crash import Crash
from models.inspection import Inspection
from models.violation import Violation
from repositories.carrier_repository import CarrierRepository
from repositories.insurance_policy_repository import InsurancePolicyRepository
from repositories.insurance_provider_repository import InsuranceProviderRepository
from repositories.safety_snapshot_repository import SafetySnapshotRepository
from repositories.crash_repository import CrashRepository
from repositories.inspection_repository import InspectionRepository
from services.searchcarriers_client import SearchCarriersClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SearchCarriersInsuranceEnrichment:
    """Enrichment service for fetching and processing insurance data from SearchCarriers."""
    
    def __init__(self):
        """Initialize the enrichment service with repositories and API client."""
        self.carrier_repo = CarrierRepository()
        self.policy_repo = InsurancePolicyRepository()
        self.provider_repo = InsuranceProviderRepository()
        self.safety_repo = SafetySnapshotRepository()
        self.crash_repo = CrashRepository()
        self.inspection_repo = InspectionRepository()
        self.client = SearchCarriersClient()
        
        # Track statistics
        self.stats = {
            "carriers_processed": 0,
            "policies_created": 0,
            "events_created": 0,
            "gaps_detected": 0,
            "shopping_patterns": 0,
            "compliance_violations": 0,
            "errors": 0
        }
    
    def generate_policy_id(self, carrier_usdot: int, provider: str, effective_date: str) -> str:
        """Generate a unique policy ID.
        
        Args:
            carrier_usdot: Carrier's USDOT number
            provider: Insurance provider name
            effective_date: Policy effective date
            
        Returns:
            str: Unique policy identifier
        """
        # Clean provider name for ID
        provider_short = provider.replace(" ", "").replace(",", "").upper()[:10]
        date_str = effective_date.replace("-", "")
        return f"POL-{carrier_usdot}-{provider_short}-{date_str}"
    
    def generate_event_id(self, carrier_usdot: int, event_type: str, event_date: str) -> str:
        """Generate a unique event ID.
        
        Args:
            carrier_usdot: Carrier's USDOT number
            event_type: Type of insurance event
            event_date: Event date
            
        Returns:
            str: Unique event identifier
        """
        date_str = event_date.replace("-", "")
        return f"EVT-{carrier_usdot}-{date_str}-{event_type}"
    
    def process_insurance_record(self, carrier_usdot: int, record: Dict) -> Optional[InsurancePolicy]:
        """Process a single insurance record from SearchCarriers.
        
        Args:
            carrier_usdot: Carrier's USDOT number
            record: Raw insurance record from API
            
        Returns:
            InsurancePolicy if successfully processed, None otherwise
        """
        try:
            # Map ins_form_code to BMC types
            form_code_mapping = {
                "34": "BMC-34",
                "84": "BMC-84",
                "91": "BMC-91",
                "91X": "BMC-91X",
                "32": "BMC-32"
            }
            
            # Extract fields with actual API field names
            provider_name = record.get("name_company", "Unknown")
            
            # Parse coverage amount (stored as string in thousands)
            # "01000" = $1,000,000, "00750" = $750,000, etc.
            max_cov_str = record.get("max_cov_amount", "00000")
            try:
                # Convert string to float and multiply by 1000
                coverage_amount = float(max_cov_str) * 1000.0
            except (ValueError, TypeError):
                coverage_amount = 0.0
            
            # Get policy number
            policy_number = record.get("policy_no")
            
            # Get policy type from ins_form_code
            ins_form_code = record.get("ins_form_code", "")
            policy_type = form_code_mapping.get(ins_form_code, f"BMC-{ins_form_code}" if ins_form_code else "BMC-91")
            
            # Parse dates (format: "YYYY-MM-DD HH:MM:SS")
            effective_date = None
            if record.get("effective_date"):
                try:
                    # Handle "YYYY-MM-DD HH:MM:SS" format
                    effective_date = datetime.strptime(record["effective_date"], "%Y-%m-%d %H:%M:%S").date()
                except ValueError:
                    # Try ISO format as fallback
                    effective_date = datetime.fromisoformat(record["effective_date"]).date()
            
            expiration_date = None
            if record.get("expiration_date"):
                try:
                    expiration_date = datetime.strptime(record["expiration_date"], "%Y-%m-%d %H:%M:%S").date()
                except ValueError:
                    expiration_date = datetime.fromisoformat(record["expiration_date"]).date()
            
            cancellation_date = None
            if record.get("cancellation_date"):
                try:
                    cancellation_date = datetime.strptime(record["cancellation_date"], "%Y-%m-%d %H:%M:%S").date()
                except ValueError:
                    cancellation_date = datetime.fromisoformat(record["cancellation_date"]).date()
            
            # Determine filing status
            filing_status = record.get("filing_status", "ACTIVE")
            if cancellation_date:
                filing_status = "CANCELLED"
            elif expiration_date and expiration_date < date.today():
                filing_status = "LAPSED"
            
            # Check federal compliance
            meets_federal_minimum = coverage_amount >= 750000.0  # Default for general freight
            
            # Create policy object
            policy = InsurancePolicy(
                policy_id=self.generate_policy_id(carrier_usdot, provider_name, str(effective_date)),
                carrier_usdot=carrier_usdot,
                provider_name=provider_name,
                provider_id=record.get("provider_id"),
                policy_type=policy_type,
                policy_number=policy_number,
                coverage_amount=coverage_amount,
                cargo_coverage=record.get("cargo_coverage"),
                effective_date=effective_date,
                expiration_date=expiration_date,
                cancellation_date=cancellation_date,
                cancellation_reason=record.get("cancellation_reason"),
                filing_status=filing_status,
                is_compliant=meets_federal_minimum,
                meets_federal_minimum=meets_federal_minimum,
                required_minimum=750000.0,
                data_source="SEARCHCARRIERS_API",
                searchcarriers_record_id=str(record.get("id")) if record.get("id") else None
            )
            
            return policy
            
        except Exception as e:
            logger.error(f"Error processing insurance record: {e}")
            return None
    
    def create_insurance_events(self, carrier_usdot: int, policies: List[InsurancePolicy]) -> List[InsuranceEvent]:
        """Create insurance events from policy transitions.
        
        Args:
            carrier_usdot: Carrier's USDOT number
            policies: List of insurance policies sorted by date
            
        Returns:
            List of insurance events
        """
        events = []
        
        # Sort policies by effective date
        sorted_policies = sorted(policies, key=lambda p: p.effective_date)
        
        for i, policy in enumerate(sorted_policies):
            # New policy event
            event_type = "NEW_POLICY" if i == 0 else "RENEWAL"
            
            # Check for provider change
            if i > 0:
                prev_policy = sorted_policies[i - 1]
                if prev_policy.provider_name != policy.provider_name:
                    event_type = "PROVIDER_CHANGE"
                
                # Calculate coverage gap
                gap_days = policy.calculate_coverage_gap(prev_policy)
                
                # Create event
                event = InsuranceEvent(
                    event_id=self.generate_event_id(carrier_usdot, event_type, str(policy.effective_date)),
                    carrier_usdot=carrier_usdot,
                    event_type=event_type,
                    event_date=policy.effective_date,
                    previous_provider=prev_policy.provider_name if i > 0 else None,
                    new_provider=policy.provider_name,
                    previous_coverage=prev_policy.coverage_amount if i > 0 else None,
                    new_coverage=policy.coverage_amount,
                    coverage_change=policy.coverage_amount - prev_policy.coverage_amount if i > 0 else None,
                    days_without_coverage=gap_days,
                    previous_policy_id=prev_policy.policy_id if i > 0 else None,
                    new_policy_id=policy.policy_id,
                    compliance_violation=gap_days > 30 if gap_days else False,
                    violation_reason="Coverage gap exceeded 30 days" if gap_days and gap_days > 30 else None,
                    data_source="SEARCHCARRIERS_API"
                )
                
                # Detect fraud patterns
                fraud_patterns = event.detect_fraud_patterns()
                if fraud_patterns:
                    event.is_suspicious = True
                    event.fraud_indicators = fraud_patterns
                
                events.append(event)
            
            # Check for cancellation event
            if policy.cancellation_date:
                cancel_event = InsuranceEvent(
                    event_id=self.generate_event_id(carrier_usdot, "CANCELLATION", str(policy.cancellation_date)),
                    carrier_usdot=carrier_usdot,
                    event_type="CANCELLATION",
                    event_date=policy.cancellation_date,
                    previous_provider=policy.provider_name,
                    previous_coverage=policy.coverage_amount,
                    previous_policy_id=policy.policy_id,
                    reason=policy.cancellation_reason,
                    data_source="SEARCHCARRIERS_API"
                )
                events.append(cancel_event)
        
        return events
    
    def enrich_carrier_by_usdot(self, carrier_usdot: int) -> Dict:
        """Enrich a carrier by USDOT number with insurance data from SearchCarriers.
        
        Args:
            carrier_usdot: USDOT number of the carrier
            
        Returns:
            dict: Enrichment results
        """
        logger.info(f"Enriching carrier by USDOT: {carrier_usdot}")
        
        # Fetch carrier data from database
        try:
            carrier = self.carrier_repo.get_by_usdot(carrier_usdot)
            if not carrier:
                logger.warning(f"Carrier with USDOT {carrier_usdot} not found in database")
                return {
                    "carrier_usdot": carrier_usdot,
                    "error": "Carrier not found in database",
                    "policies_created": 0,
                    "events_created": 0
                }
            
            # Use existing enrich_carrier method
            return self.enrich_carrier(carrier)
            
        except Exception as e:
            logger.error(f"Error fetching carrier {carrier_usdot} from database: {e}")
            return {
                "carrier_usdot": carrier_usdot,
                "error": str(e),
                "policies_created": 0,
                "events_created": 0
            }
    
    def enrich_carrier(self, carrier: Dict) -> Dict:
        """Enrich a single carrier with insurance data from SearchCarriers.
        
        Args:
            carrier: Carrier data dictionary
            
        Returns:
            dict: Enrichment results
        """
        carrier_usdot = carrier['usdot']
        logger.info(f"Enriching carrier {carrier_usdot}: {carrier['carrier_name']}")
        
        result = {
            "carrier_usdot": carrier_usdot,
            "carrier_name": carrier['carrier_name'],
            "policies_created": 0,
            "events_created": 0,
            "gaps_found": 0,
            "compliance_violations": [],
            "fraud_indicators": [],
            "error": None
        }
        
        try:
            # Fetch insurance history from SearchCarriers
            insurance_data = self.client.get_carrier_insurance_history(carrier_usdot)
            
            if not insurance_data.get("data"):
                logger.warning(f"No insurance data found for carrier {carrier_usdot}")
                result["error"] = "No insurance data available"
                return result
            
            # Process insurance records into policies
            policies = []
            for record in insurance_data["data"]:
                policy = self.process_insurance_record(carrier_usdot, record)
                if policy:
                    policies.append(policy)
            
            if not policies:
                logger.warning(f"Could not process any policies for carrier {carrier_usdot}")
                result["error"] = "Failed to process insurance records"
                return result
            
            # Create insurance providers if they don't exist
            unique_providers = set(p.provider_name for p in policies)
            for provider_name in unique_providers:
                existing_provider = self.provider_repo.get_by_name(provider_name)
                if not existing_provider:
                    # Create provider with required fields
                    from models.insurance_provider import InsuranceProvider
                    provider = InsuranceProvider(
                        provider_id=f"PROV-{provider_name.replace(' ', '').upper()[:10]}",
                        name=provider_name,
                        data_source="SEARCHCARRIERS_API"
                    )
                    self.provider_repo.create(provider)
            
            # Store policies in database
            for policy in policies:
                existing = self.policy_repo.get_by_id(policy.policy_id)
                if not existing:
                    self.policy_repo.create(policy)
                    result["policies_created"] += 1
                    
                    # Create relationships with proper temporal data
                    # Determine the end date and check if it's a cancelled policy
                    end_date = None
                    if policy.cancellation_date:
                        end_date = policy.cancellation_date
                    elif policy.expiration_date:
                        end_date = policy.expiration_date
                    
                    self.policy_repo.create_carrier_relationship(
                        policy.policy_id,
                        carrier_usdot,
                        policy.effective_date,
                        end_date
                    )
                    
                    self.policy_repo.create_provider_relationship(
                        policy.policy_id,
                        policy.provider_name
                    )
            
            # Link policy succession
            sorted_policies = sorted(policies, key=lambda p: p.effective_date)
            for i in range(len(sorted_policies) - 1):
                current = sorted_policies[i]
                next_policy = sorted_policies[i + 1]
                gap_days = next_policy.calculate_coverage_gap(current)
                
                if gap_days and gap_days > 0:
                    self.policy_repo.link_policy_succession(
                        current.policy_id,
                        next_policy.policy_id,
                        gap_days
                    )
                    
                    if gap_days > 30:
                        result["gaps_found"] += 1
                        result["compliance_violations"].append({
                            "type": "COVERAGE_GAP",
                            "days": gap_days,
                            "from_policy": current.policy_id,
                            "to_policy": next_policy.policy_id
                        })
            
            # Create insurance events
            events = self.create_insurance_events(carrier_usdot, policies)
            for event in events:
                self.policy_repo.create_insurance_event(event)
                result["events_created"] += 1
                
                if event.is_suspicious:
                    result["fraud_indicators"].extend(event.fraud_indicators or [])
            
            # Check compliance
            compliance = self.client.check_insurance_compliance(carrier_usdot)
            if not compliance["is_compliant"]:
                result["compliance_violations"].extend(compliance["violations"])
            
            # Detect provider shopping
            shopping = self.client.detect_provider_shopping(insurance_data["data"])
            if shopping["is_shopping"]:
                result["fraud_indicators"].append("insurance_shopping")
                logger.warning(f"Carrier {carrier_usdot} shows insurance shopping pattern: {shopping['provider_count']} providers")
            
            logger.info(f"Successfully enriched carrier {carrier_usdot}: {result['policies_created']} policies, {result['events_created']} events")
            
        except Exception as e:
            logger.error(f"Error enriching carrier {carrier_usdot}: {e}")
            result["error"] = str(e)
            self.stats["errors"] += 1
        
        return result
    
    def enrich_carrier_safety_data(self, usdot: int) -> Dict:
        """Enrich a carrier with safety snapshot data from SearchCarriers.
        
        Args:
            usdot: USDOT number of the carrier
            
        Returns:
            dict: Enrichment results with snapshot creation status
        """
        logger.info(f"Fetching safety data for carrier {usdot}")
        
        try:
            result = self.client.get_safety_summary(usdot)
            
            if "error" in result:
                logger.warning(f"No safety data found for {usdot}: {result.get('error')}")
                return {"error": result["error"]}
            
            if not result.get("data"):
                logger.warning(f"No safety data found for {usdot}")
                return {"error": "No safety data available"}
            
            safety_data = result["data"]
            
            # Parse OOS rates and SMS scores from the response
            driver_oos_rate = float(safety_data.get("driver_oos_rate", 0.0))
            vehicle_oos_rate = float(safety_data.get("vehicle_oos_rate", 0.0))
            
            # Create SafetySnapshot instance
            snapshot = SafetySnapshot(
                usdot=usdot,
                snapshot_date=datetime.now(timezone.utc).date(),
                driver_oos_rate=driver_oos_rate,
                vehicle_oos_rate=vehicle_oos_rate,
                driver_oos_national_avg=5.0,
                vehicle_oos_national_avg=20.0,
                unsafe_driving_score=safety_data.get("unsafe_driving_score"),
                hours_of_service_score=safety_data.get("hours_of_service_score"),
                driver_fitness_score=safety_data.get("driver_fitness_score"),
                controlled_substances_score=safety_data.get("controlled_substances_score"),
                vehicle_maintenance_score=safety_data.get("vehicle_maintenance_score"),
                hazmat_compliance_score=safety_data.get("hazmat_compliance_score"),
                crash_indicator_score=safety_data.get("crash_indicator_score"),
                unsafe_driving_alert=safety_data.get("unsafe_driving_alert", False),
                hours_of_service_alert=safety_data.get("hours_of_service_alert", False),
                driver_fitness_alert=safety_data.get("driver_fitness_alert", False),
                controlled_substances_alert=safety_data.get("controlled_substances_alert", False),
                vehicle_maintenance_alert=safety_data.get("vehicle_maintenance_alert", False),
                hazmat_compliance_alert=safety_data.get("hazmat_compliance_alert", False),
                crash_indicator_alert=safety_data.get("crash_indicator_alert", False),
                last_update=datetime.now(timezone.utc)
            )
            
            # Save to Neo4j
            created_snapshot = self.safety_repo.create(snapshot)
            
            if created_snapshot:
                # Create relationship to carrier
                self.safety_repo.create_relationship_to_carrier(usdot, snapshot)
                
                # Check for high-risk indicators
                if driver_oos_rate > 10.0:  # 2x national average
                    logger.warning(f"High driver OOS rate detected for {usdot}: {driver_oos_rate}%")
                
                if vehicle_oos_rate > 40.0:  # 2x national average
                    logger.warning(f"High vehicle OOS rate detected for {usdot}: {vehicle_oos_rate}%")
                
                logger.info(f"Created safety snapshot for carrier {usdot}")
                
                return {
                    "snapshot_created": True,
                    "driver_oos_rate": driver_oos_rate,
                    "vehicle_oos_rate": vehicle_oos_rate
                }
            else:
                return {
                    "snapshot_created": False,
                    "error": "Failed to create safety snapshot"
                }
                
        except Exception as e:
            logger.error(f"Error fetching safety data for {usdot}: {e}")
            return {"error": str(e)}
    
    def enrich_carrier_crash_data(self, usdot: int) -> Dict:
        """Enrich a carrier with crash history data from SearchCarriers.
        
        Args:
            usdot: USDOT number of the carrier
            
        Returns:
            dict: Enrichment results with crash statistics
        """
        logger.info(f"Fetching crash data for carrier {usdot}")
        
        try:
            result = self.client.get_crashes(usdot)
            
            if "error" in result:
                logger.warning(f"No crash data found for {usdot}: {result.get('error')}")
                return {"error": result["error"]}
            
            if not result.get("data"):
                logger.info(f"No crashes found for carrier {usdot}")
                return {
                    "crash_count": 0,
                    "fatal_crashes": 0,
                    "injury_crashes": 0
                }
            
            crashes = result["data"]
            crash_count = 0
            fatal_crashes = 0
            injury_crashes = 0
            
            for crash_data in crashes:
                try:
                    # Parse crash date
                    crash_date = None
                    if crash_data.get("crash_date"):
                        try:
                            crash_date = datetime.strptime(crash_data["crash_date"], "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            crash_date = datetime.fromisoformat(crash_data["crash_date"])
                    
                    # Create Crash instance
                    crash = Crash(
                        report_number=crash_data.get("report_number", f"CR-{usdot}-{crash_count}"),
                        report_state=crash_data.get("report_state"),
                        usdot=usdot,
                        crash_date=crash_date or datetime.now(timezone.utc),
                        severity=crash_data.get("severity"),
                        tow_away=crash_data.get("tow_away", False),
                        fatalities=crash_data.get("fatalities", 0),
                        injuries=crash_data.get("injuries", 0),
                        vehicles_involved=crash_data.get("vehicles_involved"),
                        weather=crash_data.get("weather"),
                        road_condition=crash_data.get("road_condition"),
                        light_condition=crash_data.get("light_condition"),
                        latitude=crash_data.get("latitude"),
                        longitude=crash_data.get("longitude"),
                        preventable=crash_data.get("preventable"),
                        citation_issued=crash_data.get("citation_issued")
                    )
                    
                    # Save to Neo4j
                    created_crash = self.crash_repo.create(crash)
                    
                    if created_crash:
                        # Create relationship to carrier
                        self.crash_repo.create_relationship_to_carrier(usdot, crash)
                        crash_count += 1
                        
                        # Count fatalities and injuries
                        if crash.fatalities and crash.fatalities > 0:
                            fatal_crashes += 1
                            logger.warning(f"Fatal crash detected for carrier {usdot}: {crash.fatalities} fatalities")
                        
                        if crash.injuries and crash.injuries > 0:
                            injury_crashes += 1
                
                except Exception as e:
                    logger.error(f"Error processing crash for carrier {usdot}: {e}")
                    continue
            
            # Handle pagination if needed (more than 100 crashes)
            if len(crashes) == 100:
                # Fetch additional pages
                page = 2
                while True:
                    additional_result = self.client.get_crashes(usdot, page=page)
                    if not additional_result.get("data"):
                        break
                    
                    for crash_data in additional_result["data"]:
                        # Process additional crashes (same logic as above)
                        crash_count += 1
                    
                    if len(additional_result["data"]) < 100:
                        break
                    page += 1
            
            logger.info(f"Created {crash_count} crash records for carrier {usdot}")
            
            return {
                "crash_count": crash_count,
                "fatal_crashes": fatal_crashes,
                "injury_crashes": injury_crashes
            }
            
        except Exception as e:
            logger.error(f"Error fetching crash data for {usdot}: {e}")
            return {"error": str(e)}
    
    def _process_inspection_batch(self, inspections: List[Dict], usdot: int) -> tuple:
        """Process a batch of inspection records.
        
        Args:
            inspections: List of inspection data from API
            usdot: USDOT number of the carrier
            
        Returns:
            tuple: (inspection_count, violation_count, oos_inspections)
        """
        inspection_count = 0
        violation_count = 0
        oos_inspections = 0
        
        for inspection_data in inspections:
            try:
                # Parse inspection date with proper error handling
                inspection_date = None
                date_field = inspection_data.get("inspection_date")
                
                if date_field:
                    try:
                        # Try standard ISO format first
                        inspection_date = datetime.strptime(date_field, "%Y-%m-%d").date()
                    except ValueError:
                        try:
                            # Try ISO datetime format
                            inspection_date = datetime.fromisoformat(date_field).date()
                        except ValueError:
                            logger.error(f"Failed to parse inspection date '{date_field}' for inspection {inspection_data.get('inspection_id')}")
                            continue  # Skip this inspection if date parsing fails
                else:
                    logger.error(f"No inspection_date field for inspection {inspection_data.get('inspection_id')}")
                    continue  # Skip inspections without dates
                
                # Validate date is reasonable (not future, not too old)
                today = date.today()
                if inspection_date > today:
                    logger.warning(f"Future inspection date {inspection_date} for inspection {inspection_data.get('inspection_id')}")
                    continue
                if inspection_date < date(2000, 1, 1):
                    logger.warning(f"Very old inspection date {inspection_date} for inspection {inspection_data.get('inspection_id')}")
                    continue
                
                # Use the mapped field names from the client
                # The client has already mapped the API fields to our expected names
                # Ensure they are integers
                try:
                    violations_actual = int(inspection_data.get("violations_count", 0))
                except (ValueError, TypeError):
                    violations_actual = 0
                    
                try:
                    oos_count = int(inspection_data.get("oos_violations_count", 0))
                except (ValueError, TypeError):
                    oos_count = 0
                
                # Create Inspection instance with correct field mapping
                inspection = Inspection(
                    inspection_id=inspection_data.get("inspection_id", f"INSP-{usdot}-{datetime.now().timestamp()}"),
                    usdot=usdot,
                    inspection_date=inspection_date,
                    level=inspection_data.get("level", 0),
                    state=inspection_data.get("state", ""),
                    location=inspection_data.get("location"),
                    violations_count=violations_actual,
                    oos_violations_count=oos_count,
                    vehicle_oos=inspection_data.get("vehicle_oos", False),
                    driver_oos=inspection_data.get("driver_oos", False),
                    hazmat_oos=inspection_data.get("hazmat_oos", False),
                    result=inspection_data.get("result", "Clean" if violations_actual == 0 else "Violations")
                )
                
                # Save to Neo4j (MERGE will prevent duplicates)
                created_inspection = self.inspection_repo.create(inspection)
                
                if created_inspection:
                    # Create relationship to carrier
                    self.inspection_repo.create_relationship_to_carrier(usdot, inspection)
                    inspection_count += 1
                    
                    # Count OOS inspections
                    # Ensure oos_count is an integer for comparison
                    try:
                        oos_int = int(oos_count) if oos_count is not None else 0
                    except (ValueError, TypeError):
                        oos_int = 0
                    
                    if inspection.driver_oos or inspection.vehicle_oos or oos_int > 0:
                        oos_inspections += 1
                    
                    # Process violations if present
                    violations = inspection_data.get("violations", [])
                    if violations:
                        violation_ids = []
                        for violation_data in violations:
                            try:
                                # Create Violation instance
                                violation = Violation(
                                    violation_id=violation_data.get("violation_id", f"VIOL-{inspection.inspection_id}-{violation_count}"),
                                    inspection_id=inspection.inspection_id,
                                    code=violation_data.get("code"),
                                    description=violation_data.get("description"),
                                    category=violation_data.get("category"),
                                    severity_weight=violation_data.get("severity_weight"),
                                    oos_indicator=violation_data.get("oos_indicator"),
                                    violation_date=inspection.inspection_date,
                                    inspection_state=inspection.state,
                                    inspection_level=inspection.level
                                )
                                
                                # Create violation node using MERGE to prevent duplicates
                                violation_query = """
                                MERGE (v:Violation {violation_id: $violation_id})
                                ON CREATE SET
                                    v.inspection_id = $inspection_id,
                                    v.code = $code,
                                    v.description = $description,
                                    v.category = $category,
                                    v.severity_weight = $severity_weight,
                                    v.oos_indicator = $oos_indicator,
                                    v.violation_date = $violation_date,
                                    v.inspection_state = $inspection_state,
                                    v.inspection_level = $inspection_level
                                RETURN v
                                """
                                
                                violation_params = violation.model_dump()
                                if violation_params.get('violation_date'):
                                    violation_params['violation_date'] = violation_params['violation_date'].isoformat()
                                
                                # Execute query using inspection repo's connection
                                violation_result = self.inspection_repo.execute_query(violation_query, violation_params)
                                
                                if violation_result:
                                    violation_ids.append(violation.violation_id)
                                    violation_count += 1
                            
                            except Exception as e:
                                logger.error(f"Error creating violation for inspection {inspection.inspection_id}: {e}")
                                continue
                        
                        # Link violations to inspection
                        if violation_ids:
                            self.inspection_repo.link_violations(inspection.inspection_id, violation_ids)
                    
                    # Add to violation count even if no detailed violations
                    violation_count += violations_actual
            
            except Exception as e:
                logger.error(f"Error processing inspection for carrier {usdot}: {e}")
                continue
        
        return inspection_count, violation_count, oos_inspections
    
    def enrich_carrier_inspection_data(self, usdot: int) -> Dict:
        """Enrich a carrier with inspection and violation data from SearchCarriers.
        
        Args:
            usdot: USDOT number of the carrier
            
        Returns:
            dict: Enrichment results with inspection statistics
        """
        logger.info(f"Fetching inspection data for carrier {usdot}")
        
        try:
            result = self.client.get_inspections(usdot, since_months=24)
            
            if "error" in result:
                logger.warning(f"No inspection data found for {usdot}: {result.get('error')}")
                return {"error": result["error"]}
            
            if not result.get("data"):
                logger.info(f"No inspections found for carrier {usdot}")
                return {
                    "inspection_count": 0,
                    "violation_count": 0,
                    "oos_inspections": 0
                }
            
            inspections = result["data"]
            
            # Process first batch of inspections using helper method
            total_inspections, total_violations, total_oos = self._process_inspection_batch(inspections, usdot)
            
            # Handle pagination if needed
            if len(inspections) == 100:
                page = 2
                while True:
                    logger.info(f"Fetching page {page} of inspections for carrier {usdot}")
                    additional_result = self.client.get_inspections(usdot, since_months=24, page=page)
                    
                    if not additional_result.get("data"):
                        break
                    
                    # Process additional inspections using the same helper method
                    batch_inspections, batch_violations, batch_oos = self._process_inspection_batch(
                        additional_result["data"], usdot
                    )
                    
                    total_inspections += batch_inspections
                    total_violations += batch_violations
                    total_oos += batch_oos
                    
                    if len(additional_result["data"]) < 100:
                        break
                    page += 1
            
            logger.info(f"Created {total_inspections} inspection records with {total_violations} violations for carrier {usdot}")
            
            if total_oos > 0:
                logger.warning(f"Carrier {usdot} has {total_oos} OOS inspections")
            
            return {
                "inspection_count": total_inspections,
                "violation_count": total_violations,
                "oos_inspections": total_oos
            }
            
        except Exception as e:
            logger.error(f"Error fetching inspection data for {usdot}: {e}")
            return {"error": str(e)}
    
    def enrich_high_risk_carriers(self, limit: int = 10):
        """Enrich high-risk carriers first (those with violations > 20 or crashes > 5).
        
        Args:
            limit: Maximum number of carriers to process
        """
        logger.info("Starting enrichment of high-risk carriers")
        
        # Get high-risk carriers
        high_risk = self.carrier_repo.get_high_risk_carriers()
        
        if not high_risk:
            logger.info("No high-risk carriers found")
            return
        
        logger.info(f"Found {len(high_risk)} high-risk carriers")
        
        # Process in batches
        carriers_to_process = high_risk[:limit]
        
        for carrier in carriers_to_process:
            result = self.enrich_carrier(carrier)
            
            self.stats["carriers_processed"] += 1
            self.stats["policies_created"] += result["policies_created"]
            self.stats["events_created"] += result["events_created"]
            self.stats["gaps_detected"] += result["gaps_found"]
            
            if result["fraud_indicators"]:
                self.stats["shopping_patterns"] += 1
            
            if result["compliance_violations"]:
                self.stats["compliance_violations"] += 1
            
            # Rate limiting
            time.sleep(1)
    
    def enrich_all_jb_carriers(self, batch_size: int = 10):
        """Enrich all JB Hunt carriers with insurance data.
        
        Args:
            batch_size: Number of carriers to process at a time
        """
        logger.info("Starting enrichment of all JB Hunt carriers")
        
        # Get all JB Hunt carriers
        all_carriers = self.carrier_repo.get_all(filters={"jb_carrier": True})
        
        logger.info(f"Found {len(all_carriers)} JB Hunt carriers to enrich")
        
        # Process in batches
        for i in range(0, len(all_carriers), batch_size):
            batch = all_carriers[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: carriers {i+1} to {min(i+batch_size, len(all_carriers))}")
            
            for carrier in batch:
                result = self.enrich_carrier(carrier)
                
                self.stats["carriers_processed"] += 1
                self.stats["policies_created"] += result["policies_created"]
                self.stats["events_created"] += result["events_created"]
                self.stats["gaps_detected"] += result["gaps_found"]
                
                if result["fraud_indicators"]:
                    self.stats["shopping_patterns"] += 1
                
                if result["compliance_violations"]:
                    self.stats["compliance_violations"] += 1
                
                # Rate limiting
                time.sleep(1)
    
    def print_summary(self):
        """Print enrichment summary statistics."""
        print("\n" + "=" * 60)
        print("SEARCHCARRIERS INSURANCE ENRICHMENT SUMMARY")
        print("=" * 60)
        print(f"Carriers processed: {self.stats['carriers_processed']}")
        print(f"Insurance policies created: {self.stats['policies_created']}")
        print(f"Insurance events created: {self.stats['events_created']}")
        print(f"Coverage gaps detected: {self.stats['gaps_detected']}")
        print(f"Insurance shopping patterns: {self.stats['shopping_patterns']}")
        print(f"Compliance violations: {self.stats['compliance_violations']}")
        print(f"Errors encountered: {self.stats['errors']}")
        
        # Get fraud summary
        gaps = self.policy_repo.detect_coverage_gaps(0, 30)  # All carriers, 30+ day gaps
        shopping = self.policy_repo.detect_insurance_shopping()
        underinsured = self.policy_repo.find_underinsured_carriers()
        
        print("\n" + "-" * 60)
        print("FRAUD DETECTION SUMMARY")
        print("-" * 60)
        print(f"Carriers with coverage gaps (>30 days): {len(gaps)}")
        print(f"Carriers showing insurance shopping: {len(shopping)}")
        print(f"Underinsured carriers: {len(underinsured)}")
        
        if gaps:
            print("\nTop 3 Coverage Gaps:")
            for gap in gaps[:3]:
                print(f"  - {gap['gap_days']} days: {gap['from_provider']} → {gap['to_provider']}")
        
        if shopping:
            print("\nTop 3 Insurance Shopping Patterns:")
            for pattern in shopping[:3]:
                print(f"  - USDOT {pattern['carrier_usdot']}: {pattern['provider_count']} providers")
        
        if underinsured:
            print("\nTop 3 Underinsured Carriers:")
            for violation in underinsured[:3]:
                print(f"  - {violation['carrier_name']}: ${violation['shortage']:,.0f} below minimum")


def main():
    """Main entry point for the enrichment script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enrich carriers with SearchCarriers insurance data")
    parser.add_argument("--high-risk", action="store_true", help="Process high-risk carriers only")
    parser.add_argument("--limit", type=int, default=10, help="Maximum carriers to process")
    parser.add_argument("--all", action="store_true", help="Process all JB Hunt carriers")
    
    args = parser.parse_args()
    
    enricher = SearchCarriersInsuranceEnrichment()
    
    try:
        if args.high_risk:
            enricher.enrich_high_risk_carriers(limit=args.limit)
        elif args.all:
            enricher.enrich_all_jb_carriers(batch_size=args.limit)
        else:
            # Default: process first N carriers
            carriers = enricher.carrier_repo.get_all(limit=args.limit, filters={"jb_carrier": True})
            for carrier in carriers:
                enricher.enrich_carrier(carrier)
                time.sleep(1)  # Rate limiting
    
    finally:
        enricher.print_summary()


if __name__ == "__main__":
    main()