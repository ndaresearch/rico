"""
Unit tests for SearchCarriers client safety methods.

Tests the new safety-related API client methods including safety summary,
crashes, inspections, and out-of-service orders.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.searchcarriers_client import SearchCarriersClient


class TestSearchCarriersClientSafety:
    """Test suite for SearchCarriers client safety methods."""
    
    @pytest.fixture
    def client(self):
        """Create a SearchCarriers client with mocked API key."""
        with patch.dict('os.environ', {'SEARCH_CARRIERS_API_TOKEN': 'test_token_123'}):
            return SearchCarriersClient()
    
    @pytest.fixture
    def mock_safety_summary_response(self):
        """Mock safety summary API response."""
        return {
            "data": {
                "driver_oos_rate": 8.5,
                "vehicle_oos_rate": 35.2,
                "unsafe_driving_score": 75.5,
                "hours_of_service_score": 62.3,
                "driver_fitness_score": None,
                "controlled_substances_score": 45.0,
                "vehicle_maintenance_score": 88.9,
                "hazmat_compliance_score": None,
                "crash_indicator_score": 92.1,
                "unsafe_driving_alert": True,
                "hours_of_service_alert": False,
                "vehicle_maintenance_alert": True
            }
        }
    
    @pytest.fixture
    def mock_crashes_response(self):
        """Mock crashes API response."""
        return {
            "data": [
                {
                    "report_number": "CR2023001",
                    "crash_date": "2023-06-15T14:30:00",
                    "fatalities": 1,
                    "injuries": 2,
                    "vehicles_involved": 3,
                    "state": "TX",
                    "preventable": True
                },
                {
                    "report_number": "CR2023002",
                    "crash_date": "2023-08-20T09:15:00",
                    "fatalities": 0,
                    "injuries": 1,
                    "vehicles_involved": 2,
                    "state": "CA",
                    "preventable": False
                },
                {
                    "report_number": "CR2023003",
                    "crash_date": "2023-10-05T16:45:00",
                    "fatalities": 0,
                    "injuries": 0,
                    "vehicles_involved": 2,
                    "state": "FL",
                    "preventable": False
                }
            ]
        }
    
    @pytest.fixture
    def mock_inspections_response(self):
        """Mock inspections API response."""
        return {
            "data": [
                {
                    "inspection_id": "INS2023001",
                    "inspection_date": "2023-09-15",
                    "level": 2,
                    "state": "TX",
                    "violations_count": 3,
                    "oos_count": 1,
                    "vehicle_oos": True,
                    "driver_oos": False
                },
                {
                    "inspection_id": "INS2023002",
                    "inspection_date": "2023-10-20",
                    "level": 1,
                    "state": "CA",
                    "violations_count": 0,
                    "oos_count": 0,
                    "vehicle_oos": False,
                    "driver_oos": False
                }
            ]
        }
    
    @pytest.fixture
    def mock_oos_orders_response(self):
        """Mock out-of-service orders API response."""
        return {
            "data": [
                {
                    "order_id": "OOS2023001",
                    "order_date": "2023-07-10",
                    "violation_code": "393.75A",
                    "description": "Tire tread depth",
                    "vehicle_oos": True,
                    "driver_oos": False
                },
                {
                    "order_id": "OOS2023002",
                    "order_date": "2023-09-15",
                    "violation_code": "395.3A1",
                    "description": "Hours of service violation",
                    "vehicle_oos": False,
                    "driver_oos": True
                }
            ]
        }
    
    def test_get_safety_summary_success(self, client, mock_safety_summary_response):
        """Test successful retrieval of safety summary."""
        with patch.object(client, '_make_request', return_value=mock_safety_summary_response):
            result = client.get_safety_summary(3487141)
            
            assert "data" in result
            assert result["data"]["driver_oos_rate"] == 8.5
            assert result["data"]["vehicle_oos_rate"] == 35.2
            assert result["data"]["unsafe_driving_alert"] is True
            assert result["data"]["dot_number"] == 3487141
            assert "fetched_at" in result["data"]
            assert result["data"]["driver_oos_high_risk"] is False  # 8.5 < 10
            assert result["data"]["vehicle_oos_high_risk"] is False  # 35.2 < 40
    
    def test_get_safety_summary_high_risk_flags(self, client):
        """Test high risk flags are set correctly for dangerous OOS rates."""
        response = {
            "data": {
                "driver_oos_rate": 15.0,  # > 10% threshold
                "vehicle_oos_rate": 45.0   # > 40% threshold
            }
        }
        
        with patch.object(client, '_make_request', return_value=response):
            result = client.get_safety_summary(9999999)
            
            assert result["data"]["driver_oos_high_risk"] is True
            assert result["data"]["vehicle_oos_high_risk"] is True
    
    def test_get_crashes_success(self, client, mock_crashes_response):
        """Test successful retrieval of crash history."""
        with patch.object(client, '_make_request', return_value=mock_crashes_response):
            result = client.get_crashes(3487141)
            
            assert "data" in result
            assert len(result["data"]) == 3
            
            # Check first crash (fatal)
            crash1 = result["data"][0]
            assert crash1["report_number"] == "CR2023001"
            assert crash1["fatalities"] == 1
            assert crash1["severity_level"] == "FATAL"
            assert crash1["dot_number"] == 3487141
            
            # Check second crash (injury)
            crash2 = result["data"][1]
            assert crash2["injuries"] == 1
            assert crash2["severity_level"] == "INJURY"
            
            # Check third crash (property only)
            crash3 = result["data"][2]
            assert crash3["fatalities"] == 0
            assert crash3["injuries"] == 0
            assert crash3["severity_level"] == "PROPERTY"
    
    def test_get_crashes_pagination(self, client):
        """Test crash retrieval with pagination parameters."""
        with patch.object(client, '_make_request') as mock_request:
            client.get_crashes(3487141, page=2, per_page=50)
            
            mock_request.assert_called_once_with(
                "/v1/company/3487141/crashes",
                {"page": 2, "perPage": 50}
            )
    
    def test_get_inspections_success(self, client, mock_inspections_response):
        """Test successful retrieval of inspection records."""
        with patch.object(client, '_make_request', return_value=mock_inspections_response):
            result = client.get_inspections(3487141)
            
            assert "data" in result
            assert len(result["data"]) == 2
            
            # Check first inspection (with violations and OOS)
            inspection1 = result["data"][0]
            assert inspection1["inspection_id"] == "INS2023001"
            assert inspection1["violations_count"] == 3
            assert inspection1["result"] == "OOS"
            assert inspection1["dot_number"] == 3487141
            
            # Check second inspection (clean)
            inspection2 = result["data"][1]
            assert inspection2["violations_count"] == 0
            assert inspection2["result"] == "Clean"
    
    def test_get_inspections_categorization(self, client):
        """Test inspection result categorization logic."""
        response = {
            "data": [
                {"oos_count": 2, "violations_count": 5},  # OOS
                {"oos_count": 0, "violations_count": 3},  # Violations
                {"oos_count": 0, "violations_count": 0}   # Clean
            ]
        }
        
        with patch.object(client, '_make_request', return_value=response):
            result = client.get_inspections(3487141)
            
            assert result["data"][0]["result"] == "OOS"
            assert result["data"][1]["result"] == "Violations"
            assert result["data"][2]["result"] == "Clean"
    
    def test_get_out_of_service_orders_success(self, client, mock_oos_orders_response):
        """Test successful retrieval of out-of-service orders."""
        with patch.object(client, '_make_request', return_value=mock_oos_orders_response):
            result = client.get_out_of_service_orders(3487141)
            
            assert "data" in result
            assert len(result["data"]) == 2
            
            # All OOS orders should be marked as critical
            for order in result["data"]:
                assert order["is_critical"] is True
                assert order["dot_number"] == 3487141
                assert "fetched_at" in order
    
    def test_get_safety_summary_not_found(self, client):
        """Test handling of 404 response for safety summary."""
        response = {"data": [], "error": "Not found"}
        
        with patch.object(client, '_make_request', return_value=response):
            result = client.get_safety_summary(9999999)
            
            assert result["error"] == "Not found"
            assert result["data"] == []
    
    def test_get_crashes_empty_response(self, client):
        """Test handling of empty crash data."""
        response = {"data": []}
        
        with patch.object(client, '_make_request', return_value=response):
            result = client.get_crashes(9999999)
            
            assert result["data"] == []
    
    def test_rate_limiting_in_safety_methods(self, client):
        """Test that rate limiting is applied to safety method calls."""
        with patch.object(client, '_rate_limit') as mock_rate_limit:
            with patch.object(client, 'session') as mock_session:
                mock_response = Mock()
                mock_response.json.return_value = {"data": {}}
                mock_response.raise_for_status = Mock()
                mock_session.get.return_value = mock_response
                
                client.get_safety_summary(3487141)
                client.get_crashes(3487141)
                client.get_inspections(3487141)
                client.get_out_of_service_orders(3487141)
                
                assert mock_rate_limit.call_count == 4
    
    def test_error_handling_in_safety_methods(self, client):
        """Test error handling in safety methods."""
        with patch.object(client, 'session') as mock_session:
            mock_session.get.side_effect = Exception("API Error")
            
            with pytest.raises(Exception) as exc_info:
                client.get_safety_summary(3487141)
            
            assert "API Error" in str(exc_info.value)
    
    def test_since_months_parameter(self, client):
        """Test since_months parameter in safety summary and inspections."""
        with patch.object(client, '_make_request') as mock_request:
            # Test safety summary
            client.get_safety_summary(3487141, since_months=12)
            mock_request.assert_called_with(
                "/v1/company/3487141/safety-summary",
                {"sinceMonths": 12}
            )
            
            # Test inspections
            client.get_inspections(3487141, since_months=36)
            mock_request.assert_called_with(
                "/v1/company/3487141/inspections",
                {"sinceMonths": 36, "page": 1, "perPage": 100}
            )
    
    def test_metadata_added_to_responses(self, client):
        """Test that metadata is correctly added to all safety responses."""
        base_response = {"data": {"test": "value"}}
        
        with patch.object(client, '_make_request', return_value=base_response):
            with patch('services.searchcarriers_client.datetime') as mock_datetime:
                mock_now = Mock()
                mock_now.isoformat.return_value = "2023-11-01T12:00:00"
                mock_datetime.now.return_value = mock_now
                
                result = client.get_safety_summary(3487141)
                
                assert result["data"]["dot_number"] == 3487141
                assert result["data"]["fetched_at"] == "2023-11-01T12:00:00"