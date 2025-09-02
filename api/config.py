"""Configuration management using Pydantic Settings."""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support.
    
    All settings can be overridden using environment variables or .env file.
    """
    
    # Neo4j Database Configuration
    neo4j_uri: str = Field(
        default="bolt://localhost:7687",
        description="Neo4j database connection URI"
    )
    neo4j_user: str = Field(
        default="neo4j",
        description="Neo4j username"
    )
    neo4j_password: str = Field(
        ...,
        description="Neo4j password (required)"
    )
    
    # API Configuration
    api_key: Optional[str] = Field(
        default=None,
        description="API key for authentication. If not set, authentication is disabled (dev mode)"
    )
    
    # External API Configuration
    search_carriers_api_token: Optional[str] = Field(
        default=None,
        description="SearchCarriers API token for insurance enrichment"
    )
    
    # Application Settings
    app_name: str = Field(
        default="RICO Graph API",
        description="Application name"
    )
    app_version: str = Field(
        default="1.0.0",
        description="Application version"
    )
    debug: bool = Field(
        default=False,
        description="Debug mode flag"
    )
    
    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    log_file: str = Field(
        default="logs/api.log",
        description="Path to log file"
    )
    
    class Config:
        """Pydantic config."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
        # Allow extra fields from environment
        extra = "ignore"
        
        # Example values for documentation
        json_schema_extra = {
            "example": {
                "neo4j_uri": "bolt://localhost:7687",
                "neo4j_user": "neo4j",
                "neo4j_password": "secure_password",
                "api_key": "your_api_key_here",
                "debug": False,
                "log_level": "INFO"
            }
        }


# Create a singleton instance
settings = Settings()