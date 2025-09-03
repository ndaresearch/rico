"""
Unit tests for safety-related repositories.

Tests CRUD operations and relationship management for SafetySnapshot,
Inspection, and Crash repositories with mocked Neo4j operations.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import date, datetime, timezone
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from models.safety_snapshot import SafetySnapshot
from models.inspection import Inspection
from models.crash import Crash
from repositories.safety_snapshot_repository import SafetySnapshotRepository
from repositories.inspection_repository import InspectionRepository
from repositories.crash_repository import CrashRepository


class TestSafetySnapshotRepository:
    """Test suite for SafetySnapshot repository."""
    
    @pytest.fixture
    def repo(self):
        """Create a SafetySnapshotRepository instance."""
        return SafetySnapshotRepository()
    
    @pytest.fixture
    def sample_snapshot(self):
        """Create a sample SafetySnapshot."""
        return SafetySnapshot(
            usdot=3487141,
            snapshot_date=date(2023, 11, 1),
            driver_oos_rate=12.5,
            vehicle_oos_rate=45.0,
            unsafe_driving_score=85.0,
            hours_of_service_score=72.0,
            unsafe_driving_alert=True,
            vehicle_maintenance_alert=True,
            last_update=datetime.now(timezone.utc)
        )
    
    def test_create_safety_snapshot(self, repo, sample_snapshot):
        """Test creating a new safety snapshot."""
        expected_result = [{"s": {"usdot": 3487141, "driver_oos_rate": 12.5}}]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.create(sample_snapshot)
            
            assert result["usdot"] == 3487141
            assert result["driver_oos_rate"] == 12.5
            
            # Verify query was called with correct parameters
            repo.execute_query.assert_called_once()
            call_args = repo.execute_query.call_args
            assert "CREATE (s:SafetySnapshot" in call_args[0][0]
            assert call_args[0][1]["usdot"] == 3487141
            assert call_args[0][1]["driver_oos_rate"] == 12.5
    
    def test_find_latest_by_usdot(self, repo):
        """Test finding the latest safety snapshot for a carrier."""
        expected_result = [{
            "s": {
                "usdot": 3487141,
                "snapshot_date": "2023-11-01",
                "driver_oos_rate": 12.5,
                "vehicle_oos_rate": 45.0
            }
        }]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_latest_by_usdot(3487141)
            
            assert result["usdot"] == 3487141
            assert result["driver_oos_rate"] == 12.5
            assert result["vehicle_oos_rate"] == 45.0
            
            # Verify query
            repo.execute_query.assert_called_once()
            call_args = repo.execute_query.call_args
            assert "ORDER BY s.snapshot_date DESC" in call_args[0][0]
            assert "LIMIT 1" in call_args[0][0]
    
    def test_find_high_risk_carriers(self, repo):
        """Test finding carriers with high OOS rates."""
        expected_result = [
            {
                "c": {"usdot": 3487141, "carrier_name": "High Risk Carrier"},
                "latest_snapshot": {"driver_oos_rate": 15.0, "vehicle_oos_rate": 50.0}
            },
            {
                "c": {"usdot": 2440672, "carrier_name": "Another Risk"},
                "latest_snapshot": {"driver_oos_rate": 11.0, "vehicle_oos_rate": 42.0}
            }
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_high_risk_carriers(limit=50)
            
            assert len(result) == 2
            assert result[0]["c"]["usdot"] == 3487141
            assert result[0]["latest_snapshot"]["driver_oos_rate"] == 15.0
            
            # Verify query checks for high OOS rates
            call_args = repo.execute_query.call_args
            assert "driver_oos_rate > 10.0 OR" in call_args[0][0]
            assert "vehicle_oos_rate > 40.0" in call_args[0][0]
    
    def test_create_relationship_to_carrier(self, repo, sample_snapshot):
        """Test creating HAS_SAFETY_SNAPSHOT relationship."""
        expected_result = [{"r": {}}]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.create_relationship_to_carrier(3487141, sample_snapshot)
            
            assert result is True
            
            # Verify relationship query
            call_args = repo.execute_query.call_args
            assert "MERGE (c)-[r:HAS_SAFETY_SNAPSHOT" in call_args[0][0]
            assert call_args[0][1]["usdot"] == 3487141
    
    def test_find_carriers_with_alerts(self, repo):
        """Test finding carriers with SMS BASIC alerts."""
        expected_result = [
            {
                "c": {"usdot": 3487141, "carrier_name": "Alert Carrier"},
                "latest_snapshot": {
                    "unsafe_driving_alert": True,
                    "vehicle_maintenance_alert": True
                }
            }
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            # Test with specific alert type
            result = repo.find_carriers_with_alerts("unsafe_driving")
            assert len(result) == 1
            
            # Test with all alerts
            result = repo.find_carriers_with_alerts()
            assert len(result) == 1


class TestInspectionRepository:
    """Test suite for Inspection repository."""
    
    @pytest.fixture
    def repo(self):
        """Create an InspectionRepository instance."""
        return InspectionRepository()
    
    @pytest.fixture
    def sample_inspection(self):
        """Create a sample Inspection."""
        return Inspection(
            inspection_id="INS2023001",
            usdot=3487141,
            inspection_date=date(2023, 10, 15),
            level=2,
            state="TX",
            location="Houston",
            violations_count=3,
            oos_violations_count=1,
            vehicle_oos=True,
            driver_oos=False,
            result="OOS"
        )
    
    def test_create_inspection(self, repo, sample_inspection):
        """Test creating a new inspection."""
        expected_result = [{"i": {
            "inspection_id": "INS2023001",
            "usdot": 3487141,
            "violations_count": 3
        }}]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.create(sample_inspection)
            
            assert result["inspection_id"] == "INS2023001"
            assert result["violations_count"] == 3
    
    def test_find_by_usdot(self, repo):
        """Test finding inspections by USDOT number."""
        expected_result = [
            {"i": {"inspection_id": "INS2023001", "violations_count": 3}},
            {"i": {"inspection_id": "INS2023002", "violations_count": 0}}
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_by_usdot(3487141, limit=50)
            
            assert len(result) == 2
            assert result[0]["inspection_id"] == "INS2023001"
            assert result[1]["violations_count"] == 0
    
    def test_link_violations(self, repo):
        """Test linking violations to an inspection."""
        expected_result = [{"count": 3}]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            count = repo.link_violations("INS2023001", ["V001", "V002", "V003"])
            
            assert count == 3
            
            # Verify query creates FOUND relationships
            call_args = repo.execute_query.call_args
            assert "MERGE (i)-[r:FOUND]->(v)" in call_args[0][0]
            assert call_args[0][1]["violation_ids"] == ["V001", "V002", "V003"]
    
    def test_find_oos_inspections(self, repo):
        """Test finding out-of-service inspections."""
        expected_result = [
            {"i": {"inspection_id": "INS2023001", "vehicle_oos": True}},
            {"i": {"inspection_id": "INS2023002", "driver_oos": True}}
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_oos_inspections(usdot=3487141)
            
            assert len(result) == 2
            
            # Verify query checks OOS flags
            call_args = repo.execute_query.call_args
            assert "i.vehicle_oos = true OR i.driver_oos = true OR i.hazmat_oos = true" in call_args[0][0]
    
    def test_calculate_violation_rate(self, repo):
        """Test calculating violation rate statistics."""
        expected_result = [{
            "total_inspections": 20,
            "total_violations": 45,
            "total_oos": 5,
            "clean_inspections": 8,
            "avg_violations_per_inspection": 2.25,
            "avg_oos_per_inspection": 0.25,
            "clean_inspection_rate": 40.0
        }]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.calculate_violation_rate(3487141, months=24)
            
            assert result["total_inspections"] == 20
            assert result["avg_violations_per_inspection"] == 2.25
            assert result["clean_inspection_rate"] == 40.0
    
    def test_find_repeat_violations(self, repo):
        """Test finding repeat violation patterns."""
        expected_result = [
            {
                "violation_code": "393.75A",
                "description": "Tire tread depth",
                "inspection_count": 5,
                "dates": ["2023-01-15", "2023-03-20", "2023-05-10", "2023-07-22", "2023-09-30"]
            },
            {
                "violation_code": "395.3A1",
                "description": "Hours of service",
                "inspection_count": 3,
                "dates": ["2023-02-10", "2023-06-15", "2023-10-05"]
            }
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_repeat_violations(3487141)
            
            assert len(result) == 2
            assert result[0]["inspection_count"] == 5
            assert result[0]["violation_code"] == "393.75A"


class TestCrashRepository:
    """Test suite for Crash repository."""
    
    @pytest.fixture
    def repo(self):
        """Create a CrashRepository instance."""
        return CrashRepository()
    
    @pytest.fixture
    def sample_crash(self):
        """Create a sample Crash."""
        return Crash(
            report_number="CR2023001",
            report_state="TX",
            usdot=3487141,
            crash_date=datetime(2023, 10, 15, 14, 30),
            severity="Fatal",
            tow_away=True,
            fatalities=1,
            injuries=2,
            vehicles_involved=3,
            preventable=True,
            citation_issued=True
        )
    
    def test_create_crash(self, repo, sample_crash):
        """Test creating a new crash record."""
        expected_result = [{"cr": {
            "report_number": "CR2023001",
            "usdot": 3487141,
            "fatalities": 1
        }}]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.create(sample_crash)
            
            assert result["report_number"] == "CR2023001"
            assert result["fatalities"] == 1
    
    def test_find_fatal_crashes(self, repo):
        """Test finding fatal crashes."""
        expected_result = [
            {"cr": {"report_number": "CR2023001", "fatalities": 2}},
            {"cr": {"report_number": "CR2023002", "fatalities": 1}}
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_fatal_crashes(usdot=3487141)
            
            assert len(result) == 2
            assert all(crash["fatalities"] > 0 for crash in result)
    
    def test_calculate_crash_statistics(self, repo):
        """Test calculating crash statistics."""
        expected_result = [{
            "total_crashes": 15,
            "total_fatalities": 2,
            "total_injuries": 12,
            "fatal_crashes": 2,
            "injury_crashes": 8,
            "tow_away_crashes": 10,
            "preventable_crashes": 5,
            "preventable_rate": 33.33
        }]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.calculate_crash_statistics(3487141, months=24)
            
            assert result["total_crashes"] == 15
            assert result["fatal_crashes"] == 2
            assert result["preventable_rate"] == 33.33
    
    def test_find_crash_clusters(self, repo):
        """Test finding crash clusters within time windows."""
        expected_result = [
            {
                "cluster_start": "2023-07-01",
                "crash_reports": ["CR001", "CR002", "CR003"],
                "cluster_size": 3
            }
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_crash_clusters(3487141, days_window=30)
            
            assert len(result) == 1
            assert result[0]["cluster_size"] == 3
            assert len(result[0]["crash_reports"]) == 3
    
    def test_find_high_risk_carriers_by_crashes(self, repo):
        """Test finding high-risk carriers based on crash history."""
        expected_result = [
            {
                "c": {"usdot": 3487141, "carrier_name": "High Risk Co"},
                "crash_count": 10,
                "total_fatalities": 2,
                "total_injuries": 15,
                "fatal_crashes": 2,
                "risk_score": 65
            }
        ]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.find_high_risk_carriers_by_crashes(limit=10)
            
            assert len(result) == 1
            assert result[0]["risk_score"] == 65
            assert result[0]["fatal_crashes"] == 2
    
    def test_create_relationship_to_carrier(self, repo, sample_crash):
        """Test creating INVOLVED_IN relationship."""
        expected_result = [{"r": {}}]
        
        with patch.object(repo, 'execute_query', return_value=expected_result):
            result = repo.create_relationship_to_carrier(3487141, sample_crash)
            
            assert result is True
            
            # Verify relationship query
            call_args = repo.execute_query.call_args
            assert "MERGE (c)-[r:INVOLVED_IN]->(cr)" in call_args[0][0]