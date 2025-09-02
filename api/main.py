import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import settings
from database import db
from routes.person_routes import router as person_router
from routes.target_company_routes import router as target_company_router
from routes.carrier_routes import router as carrier_router
from routes.insurance_provider_routes import router as insurance_provider_router
from routes.insurance_routes import router as insurance_router
from routes.ingest_routes import router as ingest_router

# Configure logging based on settings
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(settings.log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Key Authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Security(api_key_header)):
    """Verify API key for authentication.
    
    Args:
        api_key: The API key from the X-API-Key header
        
    Returns:
        True if authentication successful
        
    Raises:
        HTTPException: If API key is invalid or missing
    """
    if not settings.api_key:  # If no API key is set, allow all requests (dev mode)
        return True
    if not api_key or api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown events."""
    # Startup
    logger.info("Starting RICO API...")
    if not db.verify_connectivity():
        logger.warning("Cannot connect to Neo4j database")
    else:
        logger.info("Successfully connected to Neo4j database")
    
    yield
    
    # Shutdown
    logger.info("Shutting down RICO API...")
    db.close()


# OpenAPI tags for better documentation organization
tags_metadata = [
    {
        "name": "health",
        "description": "Health check endpoints for monitoring API status",
    },
    {
        "name": "carriers",
        "description": "Operations related to trucking carriers - create, read, update, delete carriers and manage relationships",
    },
    {
        "name": "target-companies",
        "description": "Operations for managing large companies that contract with carriers (e.g., JB Hunt)",
    },
    {
        "name": "insurance-providers",
        "description": "Manage insurance provider entities that insure carriers",
    },
    {
        "name": "persons",
        "description": "Manage person entities - officers, executives, and other individuals in the trucking network",
    },
    {
        "name": "Insurance",
        "description": "Insurance policy management, fraud detection, and SearchCarriers API enrichment",
    },
    {
        "name": "ingestion",
        "description": "Bulk data import from CSV files with validation, entity creation, and optional enrichment",
    },
]

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="""
    ## Overview
    RICO (Risk Intelligence for Carrier Operations) is a graph-based fraud detection system 
    for the trucking industry. It models relationships between carriers, companies, 
    insurance providers, and people to detect fraudulent patterns.
    
    ## Features
    - **Graph Database**: Neo4j-powered relationship modeling
    - **Fraud Detection**: Identify suspicious patterns in carrier networks
    - **Real-time Analysis**: Query complex relationships efficiently
    - **RESTful API**: Easy integration with external systems
    
    ## Authentication
    Use the `X-API-Key` header for authentication. Contact admin for API key.
    """,
    version=settings.app_version,
    lifespan=lifespan,
    openapi_tags=tags_metadata,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    debug=settings.debug
)

# Configure CORS for Cloudflare Worker
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your Cloudflare Worker domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint (no auth required)
@app.get("/health", 
         tags=["health"],
         summary="Health Check",
         description="Check the health status of the API and database connectivity",
         response_description="Health status information")
async def health_check():
    """Check API and database health status.
    
    Returns:
        dict: Health status with API status, database status, and version
    """
    db_status = "healthy" if db.verify_connectivity() else "unhealthy"
    return {
        "status": "healthy",
        "database": db_status,
        "version": settings.app_version
    }

# Root endpoint
@app.get("/",
         summary="API Information",
         description="Get basic information about the RICO API",
         response_description="API metadata")
async def root():
    """Get basic API information.
    
    Returns:
        dict: API name, version, and documentation URL
    """
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "documentation": "/docs"
    }

# Include routers with authentication
app.include_router(
    person_router,
    dependencies=[Depends(verify_api_key)]
)

# New entity routers
app.include_router(
    target_company_router,
    dependencies=[Depends(verify_api_key)]
)

app.include_router(
    carrier_router,
    dependencies=[Depends(verify_api_key)]
)

app.include_router(
    insurance_provider_router,
    dependencies=[Depends(verify_api_key)]
)

app.include_router(
    insurance_router,
    dependencies=[Depends(verify_api_key)]
)

app.include_router(
    ingest_router,
    dependencies=[Depends(verify_api_key)]
)

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Handle 404 Not Found errors.
    
    Args:
        request: The incoming request
        exc: The exception that was raised
    """
    # If it's an HTTPException with a detail, preserve it
    if hasattr(exc, 'detail'):
        return JSONResponse(
            status_code=404,
            content={"detail": exc.detail}
        )
    return JSONResponse(
        status_code=404,
        content={"error": "Resource not found", "path": str(request.url)}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Handle 500 Internal Server errors.
    
    Args:
        request: The incoming request  
        exc: The exception that was raised
    """
    logger.error(f"Internal server error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )