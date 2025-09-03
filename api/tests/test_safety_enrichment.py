"""
Unit tests for enhanced SearchCarriers enrichment service with safety data.

Tests the enrichment orchestration with safety, crash, and inspection data options.
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


class TestSafetyEnrichmentService:
    """Test suite for enhanced enrichment service with safety data."""
    
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
    def mock_enricher(self):
        """Create a mock enricher with all methods."""
        enricher = Mock()
        
        # Mock insurance enrichment
        enricher.enrich_carrier_by_usdot = Mock(return_value={
            "carrier_usdot": 3487141,
            "policies_created": 3,
            "events_created": 2,
            "gaps_found": 1,
            "error": None
        })
        
        # Mock safety enrichment
        enricher.enrich_carrier_safety_data = Mock(return_value={
            "snapshot_created": True,
            "driver_oos_rate": 12.5,
            "vehicle_oos_rate": 45.0,
            "sms_alerts": 2
        })
        
        # Mock crash enrichment
        enricher.enrich_carrier_crash_data = Mock(return_value={
            "crash_count": 5,
            "fatal_crashes": 1,
            "injury_crashes": 2,
            "property_crashes": 2
        })
        
        # Mock inspection enrichment
        enricher.enrich_carrier_inspection_data = Mock(return_value={
            "inspection_count": 10,
            "violation_count": 25,
            "oos_count": 3
        })
        
        return enricher
    
    @pytest.mark.asyncio
    async def test_enrich_with_all_options(self, sample_carrier_usdots, mock_settings, mock_enricher):
        """Test enrichment with all data types enabled."""
        enrichment_options = {
            "safety_data": True,
            "crash_data": True,
            "inspection_data": True,
            "insurance_data": True
        }
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment', 
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    # Configure async mock to return enricher results
                    mock_to_thread.side_effect = [
                        # For carrier 1
                        mock_enricher.enrich_carrier_by_usdot.return_value,
                        mock_enricher.enrich_carrier_safety_data.return_value,
                        mock_enricher.enrich_carrier_crash_data.return_value,
                        mock_enricher.enrich_carrier_inspection_data.return_value,
                        # For carrier 2
                        mock_enricher.enrich_carrier_by_usdot.return_value,
                        mock_enricher.enrich_carrier_safety_data.return_value,
                        mock_enricher.enrich_carrier_crash_data.return_value,
                        mock_enricher.enrich_carrier_inspection_data.return_value,
                        # For carrier 3
                        mock_enricher.enrich_carrier_by_usdot.return_value,
                        mock_enricher.enrich_carrier_safety_data.return_value,
                        mock_enricher.enrich_carrier_crash_data.return_value,
                        mock_enricher.enrich_carrier_inspection_data.return_value
                    ]
                    
                    result = await enrich_carriers_async(
                        sample_carrier_usdots, 
                        "test_job_123",
                        enrichment_options
                    )
        
        # Verify results
        assert result["job_id"] == "test_job_123"
        assert result["status"] == "completed"
        assert result["carriers_processed"] == 3
        assert result["policies_created"] == 9  # 3 carriers * 3 policies each
        assert result["safety_snapshots_created"] == 3
        assert result["crashes_found"] == 15  # 3 carriers * 5 crashes each
        assert result["fatal_crashes"] == 3  # 3 carriers * 1 fatal each
        assert result["inspections_created"] == 30  # 3 carriers * 10 inspections each
        assert result["violations_created"] == 75  # 3 carriers * 25 violations each
    
    @pytest.mark.asyncio
    async def test_enrich_safety_only(self, sample_carrier_usdots, mock_settings, mock_enricher):
        """Test enrichment with only safety data enabled."""
        enrichment_options = {
            "safety_data": True,
            "crash_data": False,
            "inspection_data": False,
            "insurance_data": False
        }
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.return_value = mock_enricher.enrich_carrier_safety_data.return_value
                    
                    result = await enrich_carriers_async(
                        [3487141],  # Single carrier
                        "test_job_456",
                        enrichment_options
                    )
        
        assert result["safety_snapshots_created"] == 1
        assert result["policies_created"] == 0  # Insurance not enabled
        assert result["crashes_found"] == 0  # Crashes not enabled
        assert result["inspections_created"] == 0  # Inspections not enabled
    
    @pytest.mark.asyncio
    async def test_high_risk_carrier_identification(self, mock_settings, mock_enricher):
        """Test identification of high-risk carriers based on OOS rates."""
        enrichment_options = {"safety_data": True}
        
        # Configure high-risk OOS rates
        mock_enricher.enrich_carrier_safety_data.side_effect = [
            {"snapshot_created": True, "driver_oos_rate": 15.0, "vehicle_oos_rate": 30.0},  # High driver OOS
            {"snapshot_created": True, "driver_oos_rate": 8.0, "vehicle_oos_rate": 50.0},   # High vehicle OOS
            {"snapshot_created": True, "driver_oos_rate": 5.0, "vehicle_oos_rate": 20.0}    # Normal
        ]
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.side_effect = mock_enricher.enrich_carrier_safety_data.side_effect
                    
                    result = await enrich_carriers_async(
                        [1111111, 2222222, 3333333],
                        "test_job_789",
                        enrichment_options
                    )
        
        assert len(result["high_risk_carriers"]) == 2
        assert 1111111 in result["high_risk_carriers"]  # High driver OOS
        assert 2222222 in result["high_risk_carriers"]  # High vehicle OOS
        assert 3333333 not in result["high_risk_carriers"]  # Normal rates
    
    @pytest.mark.asyncio
    async def test_fatal_crash_high_risk(self, mock_settings, mock_enricher):
        """Test identification of high-risk carriers based on fatal crashes."""
        enrichment_options = {"crash_data": True}
        
        # Configure crash data with fatalities
        mock_enricher.enrich_carrier_crash_data.side_effect = [
            {"crash_count": 5, "fatal_crashes": 2, "injury_crashes": 1},  # Fatal crashes
            {"crash_count": 3, "fatal_crashes": 0, "injury_crashes": 3},  # No fatal
            {"crash_count": 2, "fatal_crashes": 1, "injury_crashes": 0}   # Fatal crash
        ]
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.side_effect = mock_enricher.enrich_carrier_crash_data.side_effect
                    
                    result = await enrich_carriers_async(
                        [4444444, 5555555, 6666666],
                        "test_job_999",
                        enrichment_options
                    )
        
        assert len(result["high_risk_carriers"]) == 2
        assert 4444444 in result["high_risk_carriers"]  # Fatal crashes
        assert 6666666 in result["high_risk_carriers"]  # Fatal crash
        assert 5555555 not in result["high_risk_carriers"]  # No fatal crashes
    
    @pytest.mark.asyncio
    async def test_batch_processing_with_delays(self, sample_carrier_usdots, mock_settings, mock_enricher):
        """Test batch processing with rate limiting delays."""
        # Use 15 carriers to test batching (batch size is 10)
        large_usdot_list = list(range(1000000, 1000015))
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.return_value = mock_enricher.enrich_carrier_by_usdot.return_value
                    
                    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                        result = await enrich_carriers_async(
                            large_usdot_list,
                            "test_batch_job",
                            {"insurance_data": True}
                        )
        
        # Verify batch delay was applied (15 carriers = 2 batches, 1 delay between)
        mock_sleep.assert_called_once_with(2)
        assert result["carriers_processed"] == 15
    
    @pytest.mark.asyncio
    async def test_error_handling_continues_processing(self, sample_carrier_usdots, mock_settings, mock_enricher):
        """Test that errors in one carrier don't stop processing of others."""
        # Configure one carrier to fail
        mock_enricher.enrich_carrier_by_usdot.side_effect = [
            {"policies_created": 3, "events_created": 2, "gaps_found": 1},
            Exception("API Error for carrier 2"),
            {"policies_created": 2, "events_created": 1, "gaps_found": 0}
        ]
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.side_effect = mock_enricher.enrich_carrier_by_usdot.side_effect
                    
                    result = await enrich_carriers_async(
                        sample_carrier_usdots,
                        "test_error_job",
                        {"insurance_data": True}
                    )
        
        assert result["carriers_processed"] == 3  # All carriers counted
        assert len(result["errors"]) == 1  # One error recorded
        assert result["errors"][0]["usdot"] == sample_carrier_usdots[1]
        assert "API Error" in result["errors"][0]["error"]
        assert result["status"] == "completed_with_errors"
    
    @pytest.mark.asyncio
    async def test_no_api_token_skips_enrichment(self, sample_carrier_usdots):
        """Test that missing API token skips enrichment gracefully."""
        mock_settings = Mock()
        mock_settings.search_carriers_api_token = None  # No token
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            result = await enrich_carriers_async(
                sample_carrier_usdots,
                "test_no_token_job",
                {}
            )
        
        assert result["status"] == "skipped"
        assert result["error"] == "API token not configured"
        assert result["carriers_processed"] == 0
    
    @pytest.mark.asyncio
    async def test_default_options_when_none_provided(self, mock_settings, mock_enricher):
        """Test that default options are used when none provided."""
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    # All methods should be called with default options
                    mock_to_thread.side_effect = [
                        mock_enricher.enrich_carrier_by_usdot.return_value,
                        mock_enricher.enrich_carrier_safety_data.return_value,
                        mock_enricher.enrich_carrier_crash_data.return_value,
                        mock_enricher.enrich_carrier_inspection_data.return_value
                    ]
                    
                    result = await enrich_carriers_async(
                        [3487141],
                        "test_default_job",
                        None  # No options provided
                    )
        
        # All enrichment types should have been attempted
        assert mock_to_thread.call_count == 4  # All 4 enrichment types
    
    @pytest.mark.asyncio
    async def test_enrichment_statistics_accumulation(self, mock_settings, mock_enricher):
        """Test that statistics are correctly accumulated across carriers."""
        mock_enricher.enrich_carrier_inspection_data.side_effect = [
            {"inspection_count": 10, "violation_count": 25},
            {"inspection_count": 15, "violation_count": 40},
            {"inspection_count": 5, "violation_count": 10}
        ]
        
        with patch('services.searchcarriers_enrichment_service.settings', mock_settings):
            with patch('scripts.ingest.searchcarriers_insurance_enrichment.SearchCarriersInsuranceEnrichment',
                      return_value=mock_enricher):
                with patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.side_effect = mock_enricher.enrich_carrier_inspection_data.side_effect
                    
                    result = await enrich_carriers_async(
                        [7777777, 8888888, 9999999],
                        "test_stats_job",
                        {"inspection_data": True}
                    )
        
        assert result["inspections_created"] == 30  # 10 + 15 + 5
        assert result["violations_created"] == 75  # 25 + 40 + 10