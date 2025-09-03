"""
SearchCarriers Enrichment Service.

This module provides the bridge between the ingestion orchestrator and the
SearchCarriers enrichment script. It handles async execution of enrichment
tasks in the background.
"""

import sys
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import settings
from database import db

logger = logging.getLogger(__name__)


async def enrich_carriers_async(carrier_usdots: List[int], job_id: str, enrichment_options: Dict = None) -> Dict:
    """
    Asynchronously enrich carriers with SearchCarriers data.
    
    This function is called by the ingestion orchestrator when enrichment
    is enabled. It processes carriers in the background, fetching insurance
    history, safety data, crash data, and inspection records.
    
    Args:
        carrier_usdots: List of carrier USDOT numbers to enrich
        job_id: Job ID for tracking the enrichment process
        enrichment_options: Dict with options:
            - safety_data: bool - Fetch OOS rates & SMS scores
            - crash_data: bool - Fetch crash history
            - inspection_data: bool - Fetch inspections & violations
            - insurance_data: bool - Fetch insurance history
        
    Returns:
        Dictionary with enrichment results and statistics
    """
    logger.info(f"Starting enrichment job {job_id} for {len(carrier_usdots)} carriers")
    
    # Default options if not provided
    if enrichment_options is None:
        enrichment_options = {
            "safety_data": True,
            "crash_data": True,
            "inspection_data": True,
            "insurance_data": True
        }
    
    start_time = datetime.now(timezone.utc)
    results = {
        "job_id": job_id,
        "status": "processing",
        "carriers_processed": 0,
        "policies_created": 0,
        "events_created": 0,
        "gaps_detected": 0,
        "safety_snapshots_created": 0,
        "crashes_found": 0,
        "fatal_crashes": 0,
        "injury_crashes": 0,
        "inspections_created": 0,
        "violations_created": 0,
        "high_risk_carriers": [],
        "errors": [],
        "started_at": start_time.isoformat()
    }
    
    # Check if API token is configured
    if not settings.search_carriers_api_token:
        logger.warning("SearchCarriers API token not configured, skipping enrichment")
        results["status"] = "skipped"
        results["error"] = "API token not configured"
        return results
    
    try:
        # Import the enrichment script
        from scripts.ingest.searchcarriers_insurance_enrichment import SearchCarriersInsuranceEnrichment
        
        # Create enrichment instance
        enricher = SearchCarriersInsuranceEnrichment()
        
        # Process carriers in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, len(carrier_usdots), batch_size):
            batch = carrier_usdots[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: carriers {i+1} to {min(i+batch_size, len(carrier_usdots))}")
            
            # Process each carrier in the batch
            for usdot in batch:
                try:
                    carrier_result = {"usdot": usdot}
                    
                    # Enrich with insurance data if requested
                    if enrichment_options.get("insurance_data", True):
                        insurance_result = await asyncio.to_thread(
                            enricher.enrich_carrier_by_usdot,
                            usdot
                        )
                        if insurance_result and isinstance(insurance_result, dict):
                            results["policies_created"] += insurance_result.get("policies_created", 0)
                            results["events_created"] += insurance_result.get("events_created", 0)
                            results["gaps_detected"] += insurance_result.get("gaps_found", 0)
                            carrier_result["insurance"] = insurance_result
                            # Check for errors in the result
                            if insurance_result.get("error"):
                                carrier_result["error"] = insurance_result["error"]
                    
                    # Enrich with safety data if requested
                    if enrichment_options.get("safety_data", False):
                        safety_result = await asyncio.to_thread(
                            enricher.enrich_carrier_safety_data,
                            usdot
                        )
                        if safety_result and isinstance(safety_result, dict):
                            if safety_result.get("snapshot_created"):
                                results["safety_snapshots_created"] += 1
                            
                            # Check if high risk based on OOS rates
                            if safety_result.get("driver_oos_rate", 0) > 10.0 or \
                               safety_result.get("vehicle_oos_rate", 0) > 40.0:
                                results["high_risk_carriers"].append(usdot)
                            
                            carrier_result["safety"] = safety_result
                    
                    # Enrich with crash data if requested
                    if enrichment_options.get("crash_data", False):
                        crash_result = await asyncio.to_thread(
                            enricher.enrich_carrier_crash_data,
                            usdot
                        )
                        if crash_result and isinstance(crash_result, dict):
                            results["crashes_found"] += crash_result.get("crash_count", 0)
                            results["fatal_crashes"] += crash_result.get("fatal_crashes", 0)
                            results["injury_crashes"] += crash_result.get("injury_crashes", 0)
                            
                            # High risk if fatal crashes
                            if crash_result.get("fatal_crashes", 0) > 0:
                                if usdot not in results["high_risk_carriers"]:
                                    results["high_risk_carriers"].append(usdot)
                            
                            carrier_result["crashes"] = crash_result
                    
                    # Enrich with inspection data if requested
                    if enrichment_options.get("inspection_data", False):
                        inspection_result = await asyncio.to_thread(
                            enricher.enrich_carrier_inspection_data,
                            usdot
                        )
                        if inspection_result and isinstance(inspection_result, dict):
                            results["inspections_created"] += inspection_result.get("inspection_count", 0)
                            results["violations_created"] += inspection_result.get("violation_count", 0)
                            carrier_result["inspections"] = inspection_result
                    
                    # Update statistics
                    results["carriers_processed"] += 1
                    
                    if carrier_result.get("error"):
                        results["errors"].append({
                            "usdot": usdot,
                            "error": carrier_result["error"],
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                    
                    logger.info(f"Enriched carrier {usdot} with requested data types")
                    
                except Exception as e:
                    logger.error(f"Error enriching carrier {usdot}: {e}")
                    results["carriers_processed"] += 1  # Still count as processed
                    results["errors"].append({
                        "usdot": usdot,
                        "error": str(e),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
            
            # Add delay between batches to respect rate limits
            if i + batch_size < len(carrier_usdots):
                await asyncio.sleep(2)  # 2 second delay between batches
        
        # Calculate execution time
        execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        # Update final status
        results["status"] = "completed" if not results["errors"] else "completed_with_errors"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        results["execution_time_seconds"] = execution_time
        
        logger.info(
            f"Enrichment job {job_id} completed in {execution_time:.2f} seconds. "
            f"Processed: {results['carriers_processed']}, "
            f"Policies: {results['policies_created']}, "
            f"Events: {results['events_created']}, "
            f"Errors: {len(results['errors'])}"
        )
        
    except ImportError as e:
        logger.error(f"Failed to import enrichment module: {e}")
        results["status"] = "failed"
        results["error"] = f"Import error: {str(e)}"
    except Exception as e:
        logger.error(f"Enrichment job {job_id} failed: {e}")
        results["status"] = "failed"
        results["error"] = str(e)
    
    return results


async def get_enrichment_status(job_id: str) -> Dict:
    """
    Get the status of an enrichment job.
    
    Args:
        job_id: Job ID to check
        
    Returns:
        Dictionary with job status and statistics
    """
    # In a production system, this would query a job tracking database
    # For now, return a placeholder
    return {
        "job_id": job_id,
        "status": "unknown",
        "message": "Job tracking not yet implemented"
    }


async def cancel_enrichment(job_id: str) -> bool:
    """
    Cancel an ongoing enrichment job.
    
    Args:
        job_id: Job ID to cancel
        
    Returns:
        Boolean indicating if cancellation was successful
    """
    logger.info(f"Cancelling enrichment job {job_id}")
    # In a production system, this would set a cancellation flag
    # that the enrichment process checks periodically
    return True