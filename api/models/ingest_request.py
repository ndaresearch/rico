"""
Pydantic models for data ingestion API requests and responses.
"""

import base64
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class IngestRequest(BaseModel):
    """
    Request model for CSV data ingestion endpoint.
    
    Accepts either base64-encoded CSV content or a file path on the server.
    Exactly one input method must be provided.
    """
    
    csv_content: Optional[str] = Field(
        None,
        description="Base64-encoded CSV content. Use this for direct data upload."
    )
    
    file_path: Optional[str] = Field(
        None,
        description="Path to CSV file on server. Use this for server-side files."
    )
    
    target_company: str = Field(
        "JB_HUNT",
        description="Target company identifier for relationship creation"
    )
    
    enable_enrichment: bool = Field(
        False,
        description="Enable SearchCarriers API enrichment. If true, returns immediately with 'processing' status."
    )
    
    skip_invalid: bool = Field(
        True,
        description="Skip invalid records instead of failing the entire import"
    )
    
    @model_validator(mode='after')
    def validate_exclusive_input(self):
        """Ensure exactly one input method is provided."""
        has_content = self.csv_content is not None
        has_path = self.file_path is not None
        
        if not has_content and not has_path:
            raise ValueError("Either 'csv_content' or 'file_path' must be provided")
        
        if has_content and has_path:
            raise ValueError("Provide either 'csv_content' or 'file_path', not both")
        
        return self
    
    @field_validator('csv_content')
    @classmethod
    def validate_base64(cls, v: Optional[str]) -> Optional[str]:
        """Validate that csv_content is valid base64."""
        if v is None:
            return v
        
        try:
            # Try to decode to ensure it's valid base64
            base64.b64decode(v, validate=True)
            return v
        except Exception:
            raise ValueError("csv_content must be valid base64-encoded string")
    
    def get_csv_content(self) -> Optional[str]:
        """
        Decode and return the CSV content if provided via base64.
        
        Returns:
            Decoded CSV string or None if using file_path
        """
        if self.csv_content:
            try:
                decoded = base64.b64decode(self.csv_content)
                # Try UTF-8 first, then Latin-1
                try:
                    return decoded.decode('utf-8')
                except UnicodeDecodeError:
                    return decoded.decode('latin-1')
            except Exception as e:
                raise ValueError(f"Failed to decode CSV content: {str(e)}")
        return None
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "examples": [
                {
                    "csv_content": "ZG90X251bWJlcixKQiBDYXJyaWVyLENhcnJpZXIsUHJpbWFyeSBPZmZpY2VyLCBJbnN1cmFuY2UsQW1vdW50CjEyMzQ1NixZZXMsVGVzdCBDYXJyaWVyIExMQyxKb2huIERvZSxTdGF0ZSBOYXRpb25hbCwkMSBNaWxsaW9u",
                    "target_company": "JB_HUNT",
                    "enable_enrichment": False,
                    "skip_invalid": True
                },
                {
                    "file_path": "api/csv/real_data/jb_hunt_carriers.csv",
                    "target_company": "JB_HUNT",
                    "enable_enrichment": True,
                    "skip_invalid": True
                }
            ]
        }


class IngestResponse(BaseModel):
    """
    Response model for data ingestion endpoint.
    """
    
    job_id: str = Field(..., description="Unique identifier for the ingestion job")
    
    status: str = Field(
        ...,
        description="Job status: 'processing', 'completed', 'completed_with_errors', or 'failed'"
    )
    
    message: Optional[str] = Field(
        None,
        description="Human-readable status message"
    )
    
    summary: Optional[dict] = Field(
        None,
        description="Summary statistics of the ingestion process"
    )
    
    enrichment: Optional[dict] = Field(
        None,
        description="Enrichment job information if enrichment was enabled"
    )
    
    error: Optional[str] = Field(
        None,
        description="Main error message if the job failed"
    )
    
    errors: Optional[list] = Field(
        None,
        description="List of errors encountered during processing"
    )
    
    invalid_records: Optional[list] = Field(
        None,
        description="List of invalid records that were skipped"
    )
    
    execution_time_seconds: Optional[float] = Field(
        None,
        description="Total execution time in seconds"
    )