"""
Unit tests for safety API routes.

Tests all safety-related endpoints including safety profiles, crashes,
inspections, and risk assessments with mocked repositories.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi import HTTPException
from fastapi.testclient import TestClient
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import app
from routes.safety_routes import RiskAssessment


client = TestClient(app)
headers = {"X-API-Key": "test-api-key"}


class TestSafetyRoutes:
    """Test suite for safety-related API routes."""
    
    @pytest.fixture
    def mock_safety_repo(self):
        """Create a mock SafetySnapshotRepository."""
        repo = Mock()
        repo.find_latest_by_usdot = Mock()
        repo.find_high_risk_carriers = Mock()
        repo.find_carriers_with_alerts = Mock()
        return repo
    
    @pytest.fixture
    def mock_inspection_repo(self):
        """Create a mock InspectionRepository."""
        repo = Mock()
        repo.find_by_usdot = Mock()
        repo.calculate_violation_rate = Mock()
        repo.find_repeat_violations = Mock()
        return repo
    
    @pytest.fixture
    def mock_crash_repo(self):
        """Create a mock CrashRepository."""
        repo = Mock()
        repo.find_by_usdot = Mock()
        repo.calculate_crash_statistics = Mock()
        repo.find_high_risk_carriers_by_crashes = Mock()
        return repo
    
    @pytest.fixture
    def sample_safety_snapshot(self):
        """Create a sample safety snapshot."""
        return {
            "usdot": 3487141,
            "snapshot_date": "2023-11-01",
            "driver_oos_rate": 12.5,
            "vehicle_oos_rate": 45.0,
            "unsafe_driving_score": 85.0,
            "unsafe_driving_alert": True,
            "vehicle_maintenance_alert": True,
            "hours_of_service_alert": False
        }
    
    @pytest.fixture
    def sample_crashes(self):
        """Create sample crash data."""
        return [
            {
                "report_number": "CR001",
                "crash_date": "2023-10-15",
                "fatalities": 1,
                "injuries": 2
            },
            {
                "report_number": "CR002",
                "crash_date": "2023-08-20",
                "fatalities": 0,
                "injuries": 1
            }
        ]
    
    @pytest.fixture
    def sample_inspections(self):
        """Create sample inspection data."""
        return [
            {
                "inspection_id": "INS001",
                "inspection_date": "2023-10-20",
                "violations_count": 3,
                "result": "OOS"
            },
            {
                "inspection_id": "INS002",
                "inspection_date": "2023-09-15",
                "violations_count": 0,
                "result": "Clean"
            }
        ]
    
    def test_get_carrier_safety_profile_success(self, mock_safety_repo, sample_safety_snapshot):
        """Test successful retrieval of carrier safety profile."""
        mock_safety_repo.find_latest_by_usdot.return_value = sample_safety_snapshot
        
        with patch('routes.safety_routes.safety_repo', mock_safety_repo):
            response = client.get("/carriers/3487141/safety-profile", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["snapshot"]["usdot"] == 3487141
        assert data["snapshot"]["driver_oos_rate"] == 12.5
        assert "HIGH_DRIVER_OOS" in data["risk_flags"]  # 12.5 > 10
        assert "HIGH_VEHICLE_OOS" in data["risk_flags"]  # 45 > 40
        assert "UNSAFE_DRIVING" in data["active_alerts"]
        assert "VEHICLE_MAINTENANCE" in data["active_alerts"]
        assert data["is_high_risk"] is True
    
    def test_get_carrier_safety_profile_not_found(self, mock_safety_repo):
        """Test 404 when no safety profile found."""
        mock_safety_repo.find_latest_by_usdot.return_value = None
        
        with patch('routes.safety_routes.safety_repo', mock_safety_repo):
            response = client.get("/carriers/9999999/safety-profile", headers=headers)
        
        assert response.status_code == 404
        assert "No safety profile found" in response.json()["detail"]
    
    def test_get_carrier_crashes_with_statistics(self, mock_crash_repo, sample_crashes):
        """Test crash endpoint returns crashes and statistics."""
        mock_crash_repo.find_by_usdot.return_value = sample_crashes
        mock_crash_repo.calculate_crash_statistics.return_value = {
            "total_crashes": 15,
            "fatal_crashes": 2,
            "injury_crashes": 8,
            "preventable_crashes": 5
        }
        
        with patch('routes.safety_routes.crash_repo', mock_crash_repo):
            response = client.get("/carriers/3487141/crashes", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data["crashes"]) == 2
        assert data["crashes"][0]["severity_category"] == "FATAL"
        assert data["crashes"][1]["severity_category"] == "INJURY"
        assert data["statistics"]["total_crashes"] == 15
        assert data["total_count"] == 2
    
    def test_get_carrier_crashes_filtering(self, mock_crash_repo):
        """Test crash filtering by severity type."""
        all_crashes = [
            {"fatalities": 1, "injuries": 0},  # Fatal
            {"fatalities": 0, "injuries": 2},  # Injury
            {"fatalities": 0, "injuries": 0}   # Property
        ]
        mock_crash_repo.find_by_usdot.return_value = all_crashes
        mock_crash_repo.calculate_crash_statistics.return_value = {}
        
        with patch('routes.safety_routes.crash_repo', mock_crash_repo):
            # Test excluding property-only crashes
            response = client.get("/carriers/3487141/crashes?include_property=false", headers=headers)
            data = response.json()
            assert len(data["crashes"]) == 2
            
            # Test fatal only
            response = client.get("/carriers/3487141/crashes?include_injury=false&include_property=false", headers=headers)
            data = response.json()
            assert len(data["crashes"]) == 1
            assert data["crashes"][0]["severity_category"] == "FATAL"
    
    def test_get_carrier_inspections_with_repeat_violations(self, mock_inspection_repo, sample_inspections):
        """Test inspection endpoint with repeat violation analysis."""
        mock_inspection_repo.find_by_usdot.return_value = sample_inspections
        mock_inspection_repo.calculate_violation_rate.return_value = {
            "total_inspections": 20,
            "avg_violations_per_inspection": 2.5,
            "clean_inspection_rate": 40.0
        }
        mock_inspection_repo.find_repeat_violations.return_value = [
            {
                "violation_code": "393.75A",
                "description": "Tire tread depth",
                "inspection_count": 5
            }
        ]
        
        with patch('routes.safety_routes.inspection_repo', mock_inspection_repo):
            response = client.get("/carriers/3487141/inspections", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data["inspections"]) == 2
        assert data["statistics"]["avg_violations_per_inspection"] == 2.5
        assert len(data["repeat_violations"]) == 1
        assert data["repeat_violations"][0]["violation_code"] == "393.75A"
    
    def test_get_carrier_inspections_filtering(self, mock_inspection_repo):
        """Test inspection filtering by result type."""
        all_inspections = [
            {"result": "Clean"},
            {"result": "Violations"},
            {"result": "OOS"}
        ]
        mock_inspection_repo.find_by_usdot.return_value = all_inspections
        mock_inspection_repo.calculate_violation_rate.return_value = {}
        mock_inspection_repo.find_repeat_violations.return_value = []
        
        with patch('routes.safety_routes.inspection_repo', mock_inspection_repo):
            # Test excluding clean inspections
            response = client.get("/carriers/3487141/inspections?include_clean=false", headers=headers)
            data = response.json()
            assert len(data["inspections"]) == 2
            
            # Test OOS only
            response = client.get("/carriers/3487141/inspections?include_clean=false&include_violations=false", headers=headers)
            data = response.json()
            assert len(data["inspections"]) == 1
            assert data["inspections"][0]["result"] == "OOS"
    
    def test_get_carrier_risk_assessment_critical(self, mock_safety_repo, mock_crash_repo, mock_inspection_repo):
        """Test risk assessment for critical risk carrier."""
        # High OOS rates
        mock_safety_repo.find_latest_by_usdot.return_value = {
            "driver_oos_rate": 15.0,  # 3x national
            "vehicle_oos_rate": 50.0,  # 2.5x national
            "unsafe_driving_alert": True
        }
        
        # Fatal crashes
        mock_crash_repo.calculate_crash_statistics.return_value = {
            "fatal_crashes": 2,
            "injury_crashes": 5,
            "total_crashes": 10
        }
        
        # High violations
        mock_inspection_repo.calculate_violation_rate.return_value = {
            "avg_violations_per_inspection": 25.0
        }
        
        with patch('routes.safety_routes.safety_repo', mock_safety_repo):
            with patch('routes.safety_routes.crash_repo', mock_crash_repo):
                with patch('routes.safety_routes.inspection_repo', mock_inspection_repo):
                    response = client.get("/carriers/3487141/risk-assessment", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["risk_level"] == "CRITICAL"
        assert data["driver_oos_multiplier"] == 3.0
        assert data["vehicle_oos_multiplier"] == 2.5
        assert data["fatal_crashes"] == 2
        assert "DRIVER_OOS_2X_NATIONAL" in data["high_risk_indicators"]
        assert "VEHICLE_OOS_2X_NATIONAL" in data["high_risk_indicators"]
        assert "FATAL_CRASHES" in data["high_risk_indicators"]
        assert "HIGH_VIOLATION_FREQUENCY" in data["high_risk_indicators"]
    
    def test_get_carrier_risk_assessment_low(self, mock_safety_repo, mock_crash_repo, mock_inspection_repo):
        """Test risk assessment for low risk carrier."""
        # Normal OOS rates
        mock_safety_repo.find_latest_by_usdot.return_value = {
            "driver_oos_rate": 3.0,
            "vehicle_oos_rate": 15.0
        }
        
        # No serious crashes
        mock_crash_repo.calculate_crash_statistics.return_value = {
            "fatal_crashes": 0,
            "injury_crashes": 1,
            "total_crashes": 2
        }
        
        # Low violations
        mock_inspection_repo.calculate_violation_rate.return_value = {
            "avg_violations_per_inspection": 2.0
        }
        
        with patch('routes.safety_routes.safety_repo', mock_safety_repo):
            with patch('routes.safety_routes.crash_repo', mock_crash_repo):
                with patch('routes.safety_routes.inspection_repo', mock_inspection_repo):
                    response = client.get("/carriers/3487141/risk-assessment", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["risk_level"] == "LOW"
        assert data["driver_oos_multiplier"] == 0.6
        assert data["vehicle_oos_multiplier"] == 0.75
        assert len(data["high_risk_indicators"]) == 0
    
    def test_get_high_risk_carriers(self, mock_safety_repo, mock_crash_repo):
        """Test high-risk carriers endpoint."""
        mock_safety_repo.find_high_risk_carriers.return_value = [
            {
                "c": {"usdot": 1111111, "carrier_name": "High OOS Co"},
                "latest_snapshot": {"driver_oos_rate": 15.0, "vehicle_oos_rate": 45.0}
            }
        ]
        
        mock_crash_repo.find_high_risk_carriers_by_crashes.return_value = [
            {
                "c": {"usdot": 2222222, "carrier_name": "Crash Co"},
                "crash_count": 10,
                "fatal_crashes": 2,
                "total_fatalities": 3,
                "total_injuries": 15,
                "risk_score": 75
            }
        ]
        
        with patch('routes.safety_routes.safety_repo', mock_safety_repo):
            with patch('routes.safety_routes.crash_repo', mock_crash_repo):
                response = client.get("/carriers/high-risk?limit=10", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        assert len(data) == 2
        
        # Check OOS high-risk carrier
        oos_carrier = next(c for c in data if c["risk_type"] == "HIGH_OOS")
        assert oos_carrier["carrier"]["usdot"] == 1111111
        assert oos_carrier["safety_snapshot"]["driver_oos_rate"] == 15.0
        
        # Check crash high-risk carrier
        crash_carrier = next(c for c in data if c["risk_type"] == "HIGH_CRASH")
        assert crash_carrier["carrier"]["usdot"] == 2222222
        assert crash_carrier["crash_statistics"]["fatal_crashes"] == 2
        assert crash_carrier["crash_statistics"]["risk_score"] == 75
    
    def test_risk_assessment_no_safety_data(self, mock_safety_repo, mock_crash_repo, mock_inspection_repo):
        """Test risk assessment when safety snapshot is missing."""
        mock_safety_repo.find_latest_by_usdot.return_value = None
        mock_crash_repo.calculate_crash_statistics.return_value = {
            "fatal_crashes": 0,
            "injury_crashes": 0,
            "total_crashes": 1
        }
        mock_inspection_repo.calculate_violation_rate.return_value = {
            "avg_violations_per_inspection": 1.0
        }
        
        with patch('routes.safety_routes.safety_repo', mock_safety_repo):
            with patch('routes.safety_routes.crash_repo', mock_crash_repo):
                with patch('routes.safety_routes.inspection_repo', mock_inspection_repo):
                    response = client.get("/carriers/3487141/risk-assessment", headers=headers)
        
        assert response.status_code == 200
        data = response.json()
        
        # Should still calculate risk without safety snapshot
        assert data["driver_oos_multiplier"] == 0.0
        assert data["vehicle_oos_multiplier"] == 0.0
        assert data["risk_level"] == "LOW"
    
    def test_pagination_parameters(self, mock_inspection_repo):
        """Test that limit parameter is properly passed to repositories."""
        mock_inspection_repo.find_by_usdot.return_value = []
        mock_inspection_repo.calculate_violation_rate.return_value = {}
        mock_inspection_repo.find_repeat_violations.return_value = []
        
        with patch('routes.safety_routes.inspection_repo', mock_inspection_repo):
            response = client.get("/carriers/3487141/inspections?limit=50", headers=headers)
        
        assert response.status_code == 200
        mock_inspection_repo.find_by_usdot.assert_called_once_with(3487141, limit=50)