"""
Ingestion Orchestrator Service for RICO Data Import.

This service coordinates the entire data ingestion process, managing entity creation,
relationship building, and optional enrichment scheduling.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from models.carrier import Carrier
from models.target_company import TargetCompany
from models.insurance_provider import InsuranceProvider
from models.person import Person
from repositories.carrier_repository import CarrierRepository
from repositories.target_company_repository import TargetCompanyRepository
from repositories.insurance_provider_repository import InsuranceProviderRepository
from repositories.person_repository import PersonRepository
from utils.csv_parser import parse_carriers_csv, validate_carrier_data, extract_unique_values

logger = logging.getLogger(__name__)


class IngestionOrchestrator:
    """
    Orchestrates the data ingestion process for carrier data.
    
    Manages the complete import workflow including validation, entity creation,
    relationship building, and enrichment scheduling.
    """
    
    def __init__(self):
        """Initialize the orchestrator with required repositories."""
        self.carrier_repo = CarrierRepository()
        self.target_repo = TargetCompanyRepository()
        self.insurance_repo = InsuranceProviderRepository()
        self.person_repo = PersonRepository()
        
        # Track statistics for reporting
        self.stats = {
            "total_records": 0,
            "carriers_created": 0,
            "carriers_skipped": 0,
            "carriers_updated": 0,
            "insurance_providers_created": 0,
            "persons_created": 0,
            "relationships_created": 0,
            "validation_errors": 0,
            "errors": []
        }
        
        # Generate unique job ID
        self.job_id = str(uuid.uuid4())
        self.start_time = datetime.now(timezone.utc)
    
    def validate_csv_data(
        self, 
        carriers: List[Dict], 
        skip_invalid: bool = True
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate carrier data from CSV.
        
        Args:
            carriers: List of carrier dictionaries from CSV
            skip_invalid: Whether to skip invalid records or fail
            
        Returns:
            Tuple of (valid carriers, invalid carriers with errors)
            
        Raises:
            ValueError: If skip_invalid is False and validation errors found
        """
        valid_carriers = []
        invalid_carriers = []
        
        for carrier in carriers:
            is_valid, errors = validate_carrier_data(carrier)
            
            if is_valid:
                valid_carriers.append(carrier)
            else:
                invalid_carrier = {
                    "carrier": carrier,
                    "errors": errors,
                    "row_number": carrier.get('row_number', 'unknown')
                }
                invalid_carriers.append(invalid_carrier)
                
                if not skip_invalid:
                    raise ValueError(
                        f"Validation failed for carrier {carrier.get('carrier_name', 'Unknown')} "
                        f"(USDOT: {carrier.get('usdot', 'N/A')}): {'; '.join(errors)}"
                    )
        
        self.stats["validation_errors"] = len(invalid_carriers)
        
        # Log validation summary
        if invalid_carriers:
            logger.warning(
                f"Found {len(invalid_carriers)} invalid carriers during validation. "
                f"{'Skipping' if skip_invalid else 'Failing'}."
            )
            for invalid in invalid_carriers[:5]:  # Log first 5 errors
                logger.debug(
                    f"Row {invalid['row_number']}: {invalid['carrier'].get('carrier_name', 'Unknown')} - "
                    f"{'; '.join(invalid['errors'])}"
                )
        
        return valid_carriers, invalid_carriers
    
    def create_or_verify_target_company(
        self, 
        target_name: str = "JB_HUNT",
        dot_number: int = 39874
    ) -> bool:
        """
        Create or verify target company exists.
        
        Args:
            target_name: Name identifier for target company
            dot_number: DOT number for the target company
            
        Returns:
            True if target company exists or was created successfully
        """
        try:
            # Check if already exists
            existing = self.target_repo.get_by_dot_number(dot_number)
            if existing:
                logger.info(f"Target company {target_name} already exists (DOT: {dot_number})")
                return True
            
            # Create target company
            target = TargetCompany(
                dot_number=dot_number,
                legal_name="J.B. Hunt Transport Services, Inc." if target_name == "JB_HUNT" else target_name,
                entity_type="BROKER",
                authority_status="ACTIVE",
                data_source="INGESTION_API"
            )
            
            result = self.target_repo.create(target)
            if result:
                logger.info(f"Created target company {target_name} (DOT: {dot_number})")
                return True
            else:
                logger.error(f"Failed to create target company {target_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating/verifying target company: {e}")
            self.stats["errors"].append(f"Target company error: {str(e)}")
            return False
    
    def create_entities(self, carriers: List[Dict]) -> Dict[str, int]:
        """
        Create all entities (carriers, insurance providers, persons).
        
        Args:
            carriers: List of validated carrier dictionaries
            
        Returns:
            Dictionary with counts of created entities
        """
        entity_counts = {
            "carriers": 0,
            "insurance_providers": 0,
            "persons": 0
        }
        
        # Extract unique values
        unique_values = extract_unique_values(carriers)
        
        # Create insurance providers
        for provider_name in unique_values['insurance_providers']:
            try:
                existing = self.insurance_repo.get_by_name(provider_name)
                if not existing:
                    provider = InsuranceProvider(
                        provider_id=f"PROV-{provider_name.replace(' ', '').upper()[:10]}-{uuid.uuid4().hex[:6]}",
                        name=provider_name,
                        data_source="CSV_IMPORT"
                    )
                    result = self.insurance_repo.create(provider)
                    if result:
                        entity_counts["insurance_providers"] += 1
                        logger.debug(f"Created insurance provider: {provider_name}")
            except Exception as e:
                logger.error(f"Error creating insurance provider {provider_name}: {e}")
                self.stats["errors"].append(f"Insurance provider '{provider_name}': {str(e)}")
        
        # Create persons (officers)
        for officer_name in unique_values['officers']:
            try:
                # Check if person exists first
                existing_persons = self.person_repo.find_by_name(officer_name)
                if not existing_persons:
                    # Create new person
                    person = Person(
                        person_id="",  # Will be auto-generated
                        full_name=officer_name,
                        source=["CSV_IMPORT"]
                    )
                    result = self.person_repo.create(person)
                    if result:
                        entity_counts["persons"] += 1
                        logger.debug(f"Created person: {officer_name}")
                else:
                    logger.debug(f"Person already exists: {officer_name}")
            except Exception as e:
                logger.error(f"Error creating person {officer_name}: {e}")
                self.stats["errors"].append(f"Person '{officer_name}': {str(e)}")
        
        # Create carriers
        for carrier_data in carriers:
            try:
                # Check if carrier exists
                existing = self.carrier_repo.get_by_usdot(carrier_data['usdot'])
                
                if existing:
                    # Update existing carrier if needed
                    self.stats["carriers_skipped"] += 1
                    logger.debug(f"Carrier {carrier_data['usdot']} already exists, skipping")
                else:
                    # Create new carrier
                    carrier = Carrier(
                        usdot=carrier_data['usdot'],
                        carrier_name=carrier_data['carrier_name'],
                        primary_officer=carrier_data.get('primary_officer'),
                        jb_carrier=carrier_data.get('jb_carrier', False),
                        insurance_provider=carrier_data.get('insurance_provider'),
                        insurance_amount=carrier_data.get('insurance_amount'),
                        trucks=carrier_data.get('trucks'),
                        inspections=carrier_data.get('inspections'),
                        violations=carrier_data.get('violations'),
                        oos=carrier_data.get('oos'),
                        crashes=carrier_data.get('crashes', 0),
                        driver_oos_rate=carrier_data.get('driver_oos_rate'),
                        vehicle_oos_rate=carrier_data.get('vehicle_oos_rate'),
                        mcs150_drivers=carrier_data.get('mcs150_drivers'),
                        mcs150_miles=carrier_data.get('mcs150_miles'),
                        ampd=carrier_data.get('ampd'),
                        data_source=carrier_data.get('data_source', 'CSV_IMPORT')
                    )
                    
                    result = self.carrier_repo.create(carrier)
                    if result:
                        entity_counts["carriers"] += 1
                        logger.debug(f"Created carrier: {carrier_data['carrier_name']} ({carrier_data['usdot']})")
                    else:
                        self.stats["carriers_skipped"] += 1
                        logger.warning(f"Failed to create carrier {carrier_data['usdot']}")
                        
            except Exception as e:
                logger.error(f"Error creating carrier {carrier_data.get('usdot')}: {e}")
                self.stats["errors"].append(
                    f"Carrier '{carrier_data.get('carrier_name', 'Unknown')}' "
                    f"(USDOT: {carrier_data.get('usdot')}): {str(e)}"
                )
                self.stats["carriers_skipped"] += 1
        
        # Update statistics
        self.stats["carriers_created"] = entity_counts["carriers"]
        self.stats["insurance_providers_created"] = entity_counts["insurance_providers"]
        self.stats["persons_created"] = entity_counts["persons"]
        
        return entity_counts
    
    def create_relationships(
        self, 
        carriers: List[Dict],
        target_dot: int = 39874
    ) -> int:
        """
        Create relationships between entities.
        
        Args:
            carriers: List of carrier dictionaries
            target_dot: DOT number of target company
            
        Returns:
            Number of relationships created
        """
        relationships_created = 0
        
        for carrier_data in carriers:
            usdot = carrier_data['usdot']
            
            # Skip if carrier wasn't created
            if not self.carrier_repo.exists(usdot):
                continue
            
            # Create contract with target company
            try:
                success = self.carrier_repo.create_contract_with_target(
                    usdot=usdot,
                    dot_number=target_dot,
                    active=True
                )
                if success:
                    relationships_created += 1
                    logger.debug(f"Created contract: Carrier {usdot} -> Target {target_dot}")
            except Exception as e:
                logger.error(f"Error creating contract for carrier {usdot}: {e}")
                self.stats["errors"].append(f"Contract for USDOT {usdot}: {str(e)}")
            
            # Create insurance relationship
            if carrier_data.get('insurance_provider'):
                try:
                    success = self.carrier_repo.link_to_insurance_provider(
                        usdot=usdot,
                        provider_name=carrier_data['insurance_provider'],
                        amount=carrier_data.get('insurance_amount')
                    )
                    if success:
                        relationships_created += 1
                        logger.debug(f"Created insurance link: Carrier {usdot} -> {carrier_data['insurance_provider']}")
                except Exception as e:
                    logger.error(f"Error creating insurance relationship for carrier {usdot}: {e}")
                    self.stats["errors"].append(f"Insurance link for USDOT {usdot}: {str(e)}")
            
            # Create officer relationship
            if carrier_data.get('primary_officer') and carrier_data['primary_officer'].lower() not in ['n/a', 'na', '']:
                try:
                    # Find the person
                    person = Person(
                        person_id="",
                        full_name=carrier_data['primary_officer'],
                        source=["CSV_IMPORT"]
                    )
                    person_result = self.person_repo.find_or_create(person)
                    
                    if person_result:
                        success = self.carrier_repo.link_to_officer(
                            usdot=usdot,
                            person_id=person_result['person_id']
                        )
                        if success:
                            relationships_created += 1
                            logger.debug(f"Created officer link: Carrier {usdot} -> {carrier_data['primary_officer']}")
                except Exception as e:
                    logger.error(f"Error creating officer relationship for carrier {usdot}: {e}")
                    self.stats["errors"].append(f"Officer link for USDOT {usdot}: {str(e)}")
        
        self.stats["relationships_created"] = relationships_created
        return relationships_created
    
    async def queue_enrichment(self, carriers: List[Dict]) -> Dict:
        """
        Queue carriers for SearchCarriers API enrichment.
        
        Args:
            carriers: List of carrier dictionaries to enrich
            
        Returns:
            Dictionary with enrichment job details
        """
        # This will be called as a background task
        enrichment_job = {
            "job_id": str(uuid.uuid4()),
            "carrier_count": len(carriers),
            "status": "queued",
            "queued_at": datetime.now(timezone.utc).isoformat()
        }
        
        # In a real implementation, this would queue the enrichment job
        # For now, we'll just log it
        logger.info(
            f"Queued {len(carriers)} carriers for enrichment. "
            f"Job ID: {enrichment_job['job_id']}"
        )
        
        # Import here to avoid circular dependency
        try:
            from services.searchcarriers_enrichment_service import enrich_carriers_async
            
            # Process enrichment in background
            carrier_usdots = [c['usdot'] for c in carriers if c.get('usdot')]
            await enrich_carriers_async(carrier_usdots, enrichment_job['job_id'])
            
            enrichment_job["status"] = "processing"
        except ImportError:
            logger.warning("SearchCarriers enrichment service not available")
            enrichment_job["status"] = "unavailable"
        except Exception as e:
            logger.error(f"Error queuing enrichment: {e}")
            enrichment_job["status"] = "error"
            enrichment_job["error"] = str(e)
        
        return enrichment_job
    
    async def ingest_data(
        self,
        csv_content: str,
        target_company: str = "JB_HUNT",
        enable_enrichment: bool = False,
        skip_invalid: bool = True
    ) -> Dict:
        """
        Main ingestion method that orchestrates the entire import process.
        
        Args:
            csv_content: CSV content or file path
            target_company: Target company identifier
            enable_enrichment: Whether to queue SearchCarriers enrichment
            skip_invalid: Whether to skip invalid records or fail
            
        Returns:
            Dictionary with complete ingestion results
        """
        logger.info(f"Starting ingestion job {self.job_id}")
        
        try:
            # Parse CSV data
            carriers, insurance_providers = parse_carriers_csv(csv_content)
            self.stats["total_records"] = len(carriers)
            logger.info(f"Parsed {len(carriers)} carriers from CSV")
            
            # Validate data
            valid_carriers, invalid_carriers = self.validate_csv_data(carriers, skip_invalid)
            
            if not valid_carriers:
                return {
                    "job_id": self.job_id,
                    "status": "failed",
                    "error": "No valid carriers found in CSV",
                    "summary": self.stats,
                    "invalid_records": invalid_carriers
                }
            
            # Create or verify target company
            target_dot = 39874 if target_company == "JB_HUNT" else None
            if target_dot:
                self.create_or_verify_target_company(target_company, target_dot)
            
            # Create entities
            entity_counts = self.create_entities(valid_carriers)
            logger.info(
                f"Created entities - Carriers: {entity_counts['carriers']}, "
                f"Insurance: {entity_counts['insurance_providers']}, "
                f"Persons: {entity_counts['persons']}"
            )
            
            # Create relationships
            if target_dot:
                relationships = self.create_relationships(valid_carriers, target_dot)
                logger.info(f"Created {relationships} relationships")
            
            # Queue enrichment if enabled
            enrichment_info = None
            if enable_enrichment and valid_carriers:
                enrichment_info = await self.queue_enrichment(valid_carriers)
            
            # Calculate execution time
            execution_time = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            
            # Prepare response
            response = {
                "job_id": self.job_id,
                "status": "completed" if not self.stats["errors"] else "completed_with_errors",
                "execution_time_seconds": execution_time,
                "summary": self.stats,
                "errors": self.stats["errors"][:100] if self.stats["errors"] else [],  # Limit errors in response
                "invalid_records": invalid_carriers[:10] if invalid_carriers else []  # Limit invalid records
            }
            
            if enrichment_info:
                response["enrichment"] = enrichment_info
            
            logger.info(
                f"Ingestion job {self.job_id} completed in {execution_time:.2f} seconds. "
                f"Status: {response['status']}"
            )
            
            return response
            
        except ValueError as e:
            # Re-raise validation errors for proper HTTP status
            logger.error(f"Ingestion job {self.job_id} validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Ingestion job {self.job_id} failed: {e}")
            return {
                "job_id": self.job_id,
                "status": "failed",
                "error": str(e),
                "summary": self.stats
            }


async def enrich_carriers_async(carrier_usdots: List[int], job_id: str):
    """
    Placeholder for async enrichment function.
    
    This would be implemented in a separate service module that handles
    the actual SearchCarriers API calls.
    
    Args:
        carrier_usdots: List of carrier USDOT numbers to enrich
        job_id: Job ID for tracking
    """
    logger.info(f"Starting enrichment for job {job_id} with {len(carrier_usdots)} carriers")
    # Actual implementation would call SearchCarriers API
    pass