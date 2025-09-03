"""
Data Ingestion Routes for RICO API.

Provides endpoints for bulk data import from CSV files with optional enrichment.
"""

import base64
import io
import logging
from pathlib import Path
from typing import Optional, Dict

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from fastapi.responses import JSONResponse

from models.ingest_request import IngestRequest, IngestResponse
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
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest carrier data from CSV",
    description="""
    Ingest carrier data from a CSV file into the graph database.
    
    This endpoint accepts JSON with either base64-encoded CSV content or a server file path.
    
    This endpoint orchestrates the complete data import process:
    1. Parses and validates CSV data
    2. Creates entities (carriers, insurance providers, persons)
    3. Establishes relationships between entities
    4. Optionally queues enrichment from external APIs
    
    The operation is atomic for entity creation but uses MERGE for relationships
    to ensure idempotency.
    
    ## Request Format
    Provide either:
    - `csv_content`: Base64-encoded CSV string
    - `file_path`: Path to CSV file on server
    
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
    request: IngestRequest
) -> IngestResponse:
    """
    Ingest carrier data from CSV file.
    
    Args:
        background_tasks: FastAPI background tasks for async operations
        request: JSON request with CSV content or file path
        
    Returns:
        IngestResponse with ingestion results including job ID, statistics, and any errors
        
    Raises:
        HTTPException: For various error conditions
    """
    
    # Prepare CSV content
    csv_content = None
    
    try:
        if request.csv_content:
            # Decode base64 CSV content
            try:
                csv_content = request.get_csv_content()
                if not csv_content:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Failed to decode CSV content"
                    )
                
                # Check content size (limit to 10MB)
                if len(csv_content.encode('utf-8')) > 10 * 1024 * 1024:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="CSV content too large. Maximum size is 10MB"
                    )
                
                logger.info(f"Received base64 CSV content ({len(csv_content)} characters)")
                
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=str(e)
                )
            
        elif request.file_path:
            # Validate and read file from path
            path = Path(request.file_path)
            
            # Security check - prevent directory traversal
            if ".." in request.file_path or request.file_path.startswith("/etc") or request.file_path.startswith("/sys"):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access to this file path is not allowed"
                )
            
            if not path.exists():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File not found: {request.file_path}"
                )
            
            if not path.is_file():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Path is not a file: {request.file_path}"
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
            
            logger.info(f"Reading CSV from path: {request.file_path}")
        
        # Create orchestrator and process data
        orchestrator = IngestionOrchestrator()
        
        # Process ingestion
        if request.enable_enrichment:
            # Run with enrichment in background
            async def ingest_with_enrichment():
                return await orchestrator.ingest_data(
                    csv_content=csv_content,
                    target_company=request.target_company,
                    enable_enrichment=True,
                    skip_invalid=request.skip_invalid
                )
            
            # Add to background tasks
            background_tasks.add_task(ingest_with_enrichment)
            
            # Return immediate response
            return IngestResponse(
                job_id=orchestrator.job_id,
                status="processing",
                message="Ingestion started. Data will be processed in the background.",
                enrichment={
                    "enabled": True,
                    "status": "queued"
                }
            )
        else:
            # Run synchronously without enrichment
            result = await orchestrator.ingest_data(
                csv_content=csv_content,
                target_company=request.target_company,
                enable_enrichment=False,
                skip_invalid=request.skip_invalid
            )
            
            return IngestResponse(**result)
            
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


@router.get(
    "/sample",
    response_model=dict,
    summary="Get sample JSON request",
    description="Get sample JSON request format for the ingestion endpoint"
)
async def get_sample_request():
    """
    Get sample JSON request formats for the ingestion endpoint.
    
    Returns:
        Dictionary with example requests for different scenarios
    """
    # Sample CSV for base64 encoding
    sample_csv = """dot_number,JB Carrier,Carrier,Primary Officer, Insurance,Amount, Trucks
1234567,Yes,Test Carrier LLC,John Doe,State National,$1 Million,25
7654321,Yes,Another Carrier Inc,Jane Smith,Test Insurance,$750k,15"""
    
    # Encode to base64
    encoded_csv = base64.b64encode(sample_csv.encode('utf-8')).decode('utf-8')
    
    return {
        "description": "Sample JSON requests for the /ingest/ endpoint",
        "examples": [
            {
                "name": "With base64 CSV content (no enrichment)",
                "request": {
                    "csv_content": encoded_csv,
                    "target_company": "JB_HUNT",
                    "enable_enrichment": False,
                    "skip_invalid": True
                },
                "expected_response": {
                    "job_id": "uuid-string",
                    "status": "completed",
                    "summary": {
                        "total_records": 2,
                        "carriers_created": 2,
                        "insurance_providers_created": 2,
                        "persons_created": 2,
                        "relationships_created": 6
                    }
                }
            },
            {
                "name": "With file path (with enrichment)",
                "request": {
                    "file_path": "api/csv/real_data/jb_hunt_carriers.csv",
                    "target_company": "JB_HUNT",
                    "enable_enrichment": True,
                    "skip_invalid": True
                },
                "expected_response": {
                    "job_id": "uuid-string",
                    "status": "processing",
                    "message": "Ingestion started. Data will be processed in the background.",
                    "enrichment": {
                        "enabled": True,
                        "status": "queued"
                    }
                }
            },
            {
                "name": "Minimal request",
                "request": {
                    "csv_content": encoded_csv
                },
                "note": "Uses default values: target_company='JB_HUNT', enable_enrichment=False, skip_invalid=True"
            }
        ],
        "curl_examples": [
            {
                "description": "Test without enrichment",
                "command": 'curl -X POST "http://localhost:8000/ingest/" -H "Content-Type: application/json" -H "X-API-Key: your-api-key" -d \'{"csv_content": "' + encoded_csv + '", "enable_enrichment": false}\''
            },
            {
                "description": "Test with enrichment (returns immediately)",
                "command": 'curl -X POST "http://localhost:8000/ingest/" -H "Content-Type: application/json" -H "X-API-Key: your-api-key" -d \'{"file_path": "api/csv/real_data/jb_hunt_carriers.csv", "enable_enrichment": true}\''
            }
        ]
    }