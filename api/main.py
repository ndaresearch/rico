import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from database import db
from routes.company_routes import router as company_router
from routes.person_routes import router as person_router
from routes.target_company_routes import router as target_company_router
from routes.carrier_routes import router as carrier_router
from routes.insurance_provider_routes import router as insurance_provider_router

load_dotenv()

# API Key Authentication
API_KEY = os.getenv("API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: Optional[str] = Security(api_key_header)):
    """Verify API key for authentication"""
    if not API_KEY:  # If no API key is set, allow all requests (dev mode)
        return True
    if not api_key or api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key"
        )
    return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    print("Starting RICO API...")
    if not db.verify_connectivity():
        print("WARNING: Cannot connect to Neo4j database")
    else:
        print("Successfully connected to Neo4j database")
    
    yield
    
    # Shutdown
    print("Shutting down RICO API...")
    db.close()


# Create FastAPI app
app = FastAPI(
    title="RICO Graph API",
    description="Graph-based fraud detection system for trucking industry",
    version="1.0.0",
    lifespan=lifespan
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
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_status = "healthy" if db.verify_connectivity() else "unhealthy"
    return {
        "status": "healthy",
        "database": db_status,
        "version": "1.0.0"
    }

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "RICO Graph API",
        "version": "1.0.0",
        "documentation": "/docs"
    }

# Include routers with authentication
# Old Company router (keeping temporarily for backwards compatibility)
app.include_router(
    company_router,
    dependencies=[Depends(verify_api_key)]
)

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

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
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
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )