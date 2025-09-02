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


async def enrich_carriers_async(carrier_usdots: List[int], job_id: str) -> Dict:
    """
    Asynchronously enrich carriers with SearchCarriers insurance data.
    
    This function is called by the ingestion orchestrator when enrichment
    is enabled. It processes carriers in the background, fetching insurance
    history and creating temporal relationships.
    
    Args:
        carrier_usdots: List of carrier USDOT numbers to enrich
        job_id: Job ID for tracking the enrichment process
        
    Returns:
        Dictionary with enrichment results and statistics
    """
    logger.info(f"Starting enrichment job {job_id} for {len(carrier_usdots)} carriers")
    
    start_time = datetime.now(timezone.utc)
    results = {
        "job_id": job_id,
        "status": "processing",
        "carriers_processed": 0,
        "policies_created": 0,
        "events_created": 0,
        "gaps_detected": 0,
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
                    # Enrich the carrier
                    result = await asyncio.to_thread(
                        enricher.enrich_carrier_by_usdot,
                        usdot
                    )
                    
                    # Update statistics
                    results["carriers_processed"] += 1
                    results["policies_created"] += result.get("policies_created", 0)
                    results["events_created"] += result.get("events_created", 0)
                    results["gaps_detected"] += result.get("gaps_found", 0)
                    
                    if result.get("error"):
                        results["errors"].append({
                            "usdot": usdot,
                            "error": result["error"],
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                    
                    logger.info(f"Enriched carrier {usdot}: {result.get('policies_created', 0)} policies created")
                    
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