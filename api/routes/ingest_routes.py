"""
Data Ingestion Routes for RICO API.

Provides endpoints for bulk data import from CSV files with optional enrichment.
"""

import io
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse

from services.ingest_orchestrator import IngestionOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ingest",
    tags=["ingestion"],
    responses={
        400: {"description": "Bad request - invalid parameters"},
        404: {"description": "File not found"},
        422: {"description": "Unprocessable entity - invalid CSV format"},
        500: {"description": "Internal server error"}
    }
)


@router.post(
    "/",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    summary="Ingest carrier data from CSV",
    description="""
    Ingest carrier data from a CSV file into the graph database.
    
    This endpoint orchestrates the complete data import process:
    1. Parses and validates CSV data
    2. Creates entities (carriers, insurance providers, persons)
    3. Establishes relationships between entities
    4. Optionally queues enrichment from external APIs
    
    The operation is atomic for entity creation but uses MERGE for relationships
    to ensure idempotency.
    
    ## CSV Format
    Required columns:
    - dot_number: USDOT number (integer)
    - Carrier: Carrier name (string)
    
    Optional columns:
    - JB Carrier: Whether carrier contracts with JB Hunt (yes/no)
    - Primary Officer: Name of primary officer
    - Insurance: Insurance provider name
    - Amount: Insurance coverage amount
    - Trucks: Number of trucks
    - Violations: Number of violations
    - Crashes: Number of crashes
    - And various other operational metrics
    
    ## Response
    Returns a comprehensive summary including:
    - Job ID for tracking
    - Counts of created entities
    - Any validation or processing errors
    - Enrichment status if enabled
    """
)
async def ingest_data(
    background_tasks: BackgroundTasks,
    file: Optional[UploadFile] = File(None, description="CSV file to upload"),
    file_path: Optional[str] = Query(None, description="Path to CSV file on server"),
    target_company: str = Query("JB_HUNT", description="Target company identifier"),
    enable_enrichment: bool = Query(False, description="Enable SearchCarriers API enrichment"),
    skip_invalid: bool = Query(True, description="Skip invalid records instead of failing")
):
    """
    Ingest carrier data from CSV file.
    
    Args:
        background_tasks: FastAPI background tasks for async operations
        file: Optional uploaded CSV file
        file_path: Optional path to CSV file on server
        target_company: Target company for relationship creation
        enable_enrichment: Whether to queue SearchCarriers enrichment
        skip_invalid: Whether to skip invalid records or fail on first error
        
    Returns:
        Dictionary with ingestion results including job ID, statistics, and any errors
        
    Raises:
        HTTPException: For various error conditions
    """
    # Validate input - need either file or file_path
    if not file and not file_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either 'file' upload or 'file_path' parameter is required"
        )
    
    if file and file_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide either 'file' upload or 'file_path', not both"
        )
    
    # Prepare CSV content
    csv_content = None
    
    try:
        if file:
            # Validate file type
            if not file.filename.endswith('.csv'):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid file type. Expected CSV, got: {file.filename}"
                )
            
            # Read uploaded file
            content = await file.read()
            
            # Check file size (limit to 10MB)
            if len(content) > 10 * 1024 * 1024:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="File too large. Maximum size is 10MB"
                )
            
            # Decode content
            try:
                csv_content = content.decode('utf-8')
            except UnicodeDecodeError:
                # Try with different encoding
                try:
                    csv_content = content.decode('latin-1')
                except UnicodeDecodeError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Unable to decode CSV file. Please ensure it's in UTF-8 or Latin-1 encoding"
                    )
            
            logger.info(f"Received CSV upload: {file.filename} ({len(content)} bytes)")
            
        elif file_path:
            # Validate and read file from path
            path = Path(file_path)
            
            # Security check - prevent directory traversal
            if ".." in file_path or file_path.startswith("/etc") or file_path.startswith("/sys"):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access to this file path is not allowed"
                )
            
            if not path.exists():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File not found: {file_path}"
                )
            
            if not path.is_file():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Path is not a file: {file_path}"
                )
            
            if not path.suffix.lower() == '.csv':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid file type. Expected CSV, got: {path.suffix}"
                )
            
            # Check file size
            if path.stat().st_size > 10 * 1024 * 1024:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="File too large. Maximum size is 10MB"
                )
            
            # Read file
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
            except UnicodeDecodeError:
                # Try with different encoding
                with open(path, 'r', encoding='latin-1') as f:
                    csv_content = f.read()
            
            logger.info(f"Reading CSV from path: {file_path}")
        
        # Create orchestrator and process data
        orchestrator = IngestionOrchestrator()
        
        # Process ingestion
        if enable_enrichment:
            # Run with enrichment in background
            async def ingest_with_enrichment():
                return await orchestrator.ingest_data(
                    csv_content=csv_content,
                    target_company=target_company,
                    enable_enrichment=True,
                    skip_invalid=skip_invalid
                )
            
            # Add to background tasks
            background_tasks.add_task(ingest_with_enrichment)
            
            # Return immediate response
            return {
                "job_id": orchestrator.job_id,
                "status": "processing",
                "message": "Ingestion started. Data will be processed in the background.",
                "enrichment": {
                    "enabled": True,
                    "status": "queued"
                }
            }
        else:
            # Run synchronously without enrichment
            result = await orchestrator.ingest_data(
                csv_content=csv_content,
                target_company=target_company,
                enable_enrichment=False,
                skip_invalid=skip_invalid
            )
            
            return result
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValueError as e:
        # CSV parsing or validation errors
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"CSV processing error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during ingestion: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error during ingestion: {str(e)}"
        )


@router.get(
    "/status/{job_id}",
    response_model=dict,
    summary="Get ingestion job status",
    description="Get the status of a running or completed ingestion job"
)
async def get_job_status(job_id: str):
    """
    Get the status of an ingestion job.
    
    Args:
        job_id: UUID of the ingestion job
        
    Returns:
        Dictionary with job status and details
        
    Note:
        This is a placeholder endpoint. In a production system, this would
        query a job tracking system or database to get real-time status.
    """
    # Placeholder implementation
    # In production, this would query a job tracking system
    return {
        "job_id": job_id,
        "status": "unknown",
        "message": "Job status tracking not yet implemented"
    }


@router.get(
    "/sample-csv",
    response_model=dict,
    summary="Get sample CSV format",
    description="Get a sample CSV format for carrier data ingestion"
)
async def get_sample_csv():
    """
    Get a sample CSV format with example data.
    
    Returns:
        Dictionary with CSV headers and sample rows
    """
    sample = {
        "description": "Sample CSV format for carrier data ingestion",
        "headers": [
            "dot_number", "JB Carrier", "Carrier", "Primary Officer",
            " Insurance", "Amount", " Trucks ", " Inspections ",
            " Violations ", " OOS ", " Crashes ", "Driver OOS Rate",
            "Vehicle OOS Rate", " MCS150 Drivers ", " MCS150 Miles ", " AMPD "
        ],
        "sample_rows": [
            {
                "dot_number": "1234567",
                "JB Carrier": "Yes",
                "Carrier": "Sample Trucking LLC",
                "Primary Officer": "John Doe",
                " Insurance": "State National",
                "Amount": "$1 Million",
                " Trucks ": "50",
                " Inspections ": "100",
                " Violations ": "10",
                " OOS ": "5",
                " Crashes ": "0",
                "Driver OOS Rate": "2.5%",
                "Vehicle OOS Rate": "10.0%",
                " MCS150 Drivers ": "55",
                " MCS150 Miles ": "1,000,000",
                " AMPD ": "18,182"
            }
        ],
        "notes": [
            "Column names may have leading/trailing spaces",
            "Insurance amount can be in various formats ($1 Million, $750k, numeric)",
            "Percentages should include the % sign",
            "Numbers can include commas",
            "Use 'n/a' or leave blank for missing values"
        ]
    }
    
    return sample