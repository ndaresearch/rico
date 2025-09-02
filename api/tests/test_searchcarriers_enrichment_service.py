"""
Unit tests for SearchCarriers enrichment service.

Tests the async enrichment service that bridges the orchestrator
and the SearchCarriers API client.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timezone
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.searchcarriers_enrichment_service import (
    enrich_carriers_async,
    get_enrichment_status,
    cancel_enrichment
)


class TestSearchCarriersEnrichmentService:
    """Test suite for SearchCarriers enrichment service."""
    
    @pytest.fixture
    def mock_enricher(self):
        """Create a mock enrichment instance."""
        enricher = Mock()
        enricher.enrich_carrier_by_usdot = Mock()
        return enricher
    
    @pytest.fixture
    def sample_carrier_usdots(self):
        """Sample USDOT numbers for testing."""
        return [3487141, 3330908, 2440672]
    
    @pytest.fixture
    def mock_settings(self):
        """Mock settings with API token."""
        settings = Mock()
        settings.search_carriers_api_token = "test_token_123"
        return settings
    
    @pytest.fixture
    def successful_enrichment_result(self):
        """Mock successful enrichment result."""
        return {
            "carrier_usdot": 3487141,
            "carrier_name": "Test Carrier",
            "policies_created": 3,
            "events_created": 2,
            "gaps_found": 1,
            "compliance_violations": [],
            "fraud_indicators": [],
            "error": None
        }
    
    @pytest.fixture
    def failed_enrichment_result(self):
        """Mock failed enrichment result."""
        return {
            "carrier_usdot": 9999999,
            "error": "Carrier not found",
            "policies_created": 0,
            "events_created": 0,
            "gaps_found": 0
        }
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_success(
        self, 
        mock_enricher, 
        sample_carrier_usdots,
        mock_settings,
        successful_enrichment_result
    ):
        """Test successful enrichment of multiple carriers."""
        # Mock the enrichment class and its method
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                MockEnricher.return_value = mock_enricher
                mock_enricher.enrich_carrier_by_usdot.return_value = successful_enrichment_result
                
                # Run enrichment
                result = await enrich_carriers_async(sample_carrier_usdots, "test_job_123")
                
                # Verify results
                assert result["job_id"] == "test_job_123"
                assert result["status"] == "completed"
                assert result["carriers_processed"] == 3
                assert result["policies_created"] == 9  # 3 carriers * 3 policies each
                assert result["events_created"] == 6  # 3 carriers * 2 events each
                assert result["gaps_detected"] == 3  # 3 carriers * 1 gap each
                assert len(result["errors"]) == 0
                
                # Verify enricher was called for each USDOT
                assert mock_enricher.enrich_carrier_by_usdot.call_count == 3
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_no_token(self, sample_carrier_usdots):
        """Test enrichment when API token is not configured."""
        mock_settings = Mock()
        mock_settings.search_carriers_api_token = None
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            result = await enrich_carriers_async(sample_carrier_usdots, "test_job_124")
            
            assert result["status"] == "skipped"
            assert result["error"] == "API token not configured"
            assert result["carriers_processed"] == 0
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_with_errors(
        self,
        mock_enricher,
        sample_carrier_usdots,
        mock_settings,
        successful_enrichment_result,
        failed_enrichment_result
    ):
        """Test enrichment with some carriers failing."""
        # Update the failed result to match the second USDOT
        failed_result = failed_enrichment_result.copy()
        failed_result["carrier_usdot"] = sample_carrier_usdots[1]  # 3330908
        
        # Mock different results for different carriers
        mock_enricher.enrich_carrier_by_usdot.side_effect = [
            successful_enrichment_result,
            failed_result,
            successful_enrichment_result
        ]
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                MockEnricher.return_value = mock_enricher
                
                result = await enrich_carriers_async(sample_carrier_usdots, "test_job_125")
                
                assert result["status"] == "completed_with_errors"
                assert result["carriers_processed"] == 3
                assert result["policies_created"] == 6  # 2 successful * 3 policies
                assert len(result["errors"]) == 1
                assert result["errors"][0]["usdot"] == sample_carrier_usdots[1]  # 3330908
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_import_error(
        self,
        sample_carrier_usdots,
        mock_settings
    ):
        """Test handling of import errors."""
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment', side_effect=ImportError("Module not found")):
                result = await enrich_carriers_async(sample_carrier_usdots, "test_job_126")
                
                assert result["status"] == "failed"
                assert "Import error" in result["error"]
                assert result["carriers_processed"] == 0
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_exception(
        self,
        mock_enricher,
        sample_carrier_usdots,
        mock_settings
    ):
        """Test handling of unexpected exceptions."""
        mock_enricher.enrich_carrier_by_usdot.side_effect = Exception("API error")
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                MockEnricher.return_value = mock_enricher
                
                result = await enrich_carriers_async(sample_carrier_usdots, "test_job_127")
                
                assert result["status"] == "completed_with_errors"
                assert result["carriers_processed"] == 3
                assert len(result["errors"]) == 3  # All carriers failed
                assert all("API error" in error["error"] for error in result["errors"])
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_batch_processing(
        self,
        mock_enricher,
        mock_settings,
        successful_enrichment_result
    ):
        """Test that carriers are processed in batches."""
        # Create 25 USDOT numbers (should be 3 batches of 10, 10, 5)
        large_usdot_list = list(range(1000000, 1000025))
        mock_enricher.enrich_carrier_by_usdot.return_value = successful_enrichment_result
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                MockEnricher.return_value = mock_enricher
                
                # Mock asyncio.sleep to avoid actual delays in tests
                with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                    result = await enrich_carriers_async(large_usdot_list, "test_job_128")
                    
                    # Should have 2 sleep calls (after first 2 batches, not after the last)
                    assert mock_sleep.call_count == 2
                    assert result["carriers_processed"] == 25
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_empty_list(self, mock_settings):
        """Test enrichment with empty carrier list."""
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                result = await enrich_carriers_async([], "test_job_129")
                
                assert result["status"] == "completed"
                assert result["carriers_processed"] == 0
                assert result["policies_created"] == 0
                assert result["events_created"] == 0
    
    @pytest.mark.asyncio
    async def test_get_enrichment_status(self):
        """Test getting enrichment job status."""
        result = await get_enrichment_status("test_job_130")
        
        assert result["job_id"] == "test_job_130"
        assert result["status"] == "unknown"
        assert "not yet implemented" in result["message"]
    
    @pytest.mark.asyncio
    async def test_cancel_enrichment(self):
        """Test cancelling an enrichment job."""
        result = await cancel_enrichment("test_job_131")
        assert result is True
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_timing(
        self,
        mock_enricher,
        sample_carrier_usdots,
        mock_settings,
        successful_enrichment_result
    ):
        """Test that execution time is properly calculated."""
        mock_enricher.enrich_carrier_by_usdot.return_value = successful_enrichment_result
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                MockEnricher.return_value = mock_enricher
                
                result = await enrich_carriers_async(sample_carrier_usdots, "test_job_132")
                
                assert "execution_time_seconds" in result
                assert result["execution_time_seconds"] >= 0
                assert "started_at" in result
                assert "completed_at" in result
    
    @pytest.mark.asyncio
    async def test_enrich_carriers_async_thread_safety(
        self,
        mock_enricher,
        sample_carrier_usdots,
        mock_settings,
        successful_enrichment_result
    ):
        """Test that enrichment uses asyncio.to_thread for blocking operations."""
        mock_enricher.enrich_carrier_by_usdot.return_value = successful_enrichment_result
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment') as MockEnricher:
                MockEnricher.return_value = mock_enricher
                
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.return_value = successful_enrichment_result
                    
                    result = await enrich_carriers_async(sample_carrier_usdots, "test_job_133")
                    
                    # Verify to_thread was called for each carrier
                    assert mock_to_thread.call_count == 3
                    assert result["carriers_processed"] == 3