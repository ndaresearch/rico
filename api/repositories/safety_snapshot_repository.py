from typing import Dict, List, Optional
from datetime import datetime, timezone

from database import BaseRepository
from models.safety_snapshot import SafetySnapshot


class SafetySnapshotRepository(BaseRepository):
    """Repository for SafetySnapshot entity operations in Neo4j graph database.
    
    Handles CRUD operations for safety snapshots and manages their relationships
    with carriers.
    """
    
    def create(self, snapshot: SafetySnapshot) -> Dict:
        """Create a new safety snapshot node in the graph database.
        
        Args:
            snapshot: SafetySnapshot model with all required properties
            
        Returns:
            dict: Created safety snapshot node data or None if creation fails
        """
        query = """
        CREATE (s:SafetySnapshot {
            usdot: $usdot,
            snapshot_date: $snapshot_date,
            driver_oos_rate: $driver_oos_rate,
            vehicle_oos_rate: $vehicle_oos_rate,
            driver_oos_national_avg: $driver_oos_national_avg,
            vehicle_oos_national_avg: $vehicle_oos_national_avg,
            unsafe_driving_score: $unsafe_driving_score,
            hours_of_service_score: $hours_of_service_score,
            driver_fitness_score: $driver_fitness_score,
            controlled_substances_score: $controlled_substances_score,
            vehicle_maintenance_score: $vehicle_maintenance_score,
            hazmat_compliance_score: $hazmat_compliance_score,
            crash_indicator_score: $crash_indicator_score,
            unsafe_driving_alert: $unsafe_driving_alert,
            hours_of_service_alert: $hours_of_service_alert,
            driver_fitness_alert: $driver_fitness_alert,
            controlled_substances_alert: $controlled_substances_alert,
            vehicle_maintenance_alert: $vehicle_maintenance_alert,
            hazmat_compliance_alert: $hazmat_compliance_alert,
            crash_indicator_alert: $crash_indicator_alert,
            last_update: $last_update
        })
        RETURN s
        """
        
        # Convert dates to strings for Neo4j
        params = snapshot.model_dump()
        if params.get('snapshot_date'):
            params['snapshot_date'] = params['snapshot_date'].isoformat()
        if params.get('last_update'):
            params['last_update'] = params['last_update'].isoformat()
        else:
            params['last_update'] = datetime.now(timezone.utc).isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['s'] if result else None
    
    def update(self, usdot: int, snapshot: SafetySnapshot) -> Dict:
        """Update an existing safety snapshot for a carrier.
        
        Args:
            usdot: The USDOT number of the carrier
            snapshot: SafetySnapshot model with updated data
            
        Returns:
            dict: Updated safety snapshot data
        """
        query = """
        MATCH (s:SafetySnapshot {usdot: $usdot, snapshot_date: $snapshot_date})
        SET s.driver_oos_rate = $driver_oos_rate,
            s.vehicle_oos_rate = $vehicle_oos_rate,
            s.unsafe_driving_score = $unsafe_driving_score,
            s.hours_of_service_score = $hours_of_service_score,
            s.driver_fitness_score = $driver_fitness_score,
            s.controlled_substances_score = $controlled_substances_score,
            s.vehicle_maintenance_score = $vehicle_maintenance_score,
            s.hazmat_compliance_score = $hazmat_compliance_score,
            s.crash_indicator_score = $crash_indicator_score,
            s.unsafe_driving_alert = $unsafe_driving_alert,
            s.hours_of_service_alert = $hours_of_service_alert,
            s.driver_fitness_alert = $driver_fitness_alert,
            s.controlled_substances_alert = $controlled_substances_alert,
            s.vehicle_maintenance_alert = $vehicle_maintenance_alert,
            s.hazmat_compliance_alert = $hazmat_compliance_alert,
            s.crash_indicator_alert = $crash_indicator_alert,
            s.last_update = $last_update
        RETURN s
        """
        
        params = snapshot.model_dump()
        params['usdot'] = usdot
        if params.get('snapshot_date'):
            params['snapshot_date'] = params['snapshot_date'].isoformat()
        params['last_update'] = datetime.now(timezone.utc).isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['s'] if result else None
    
    def find_by_usdot(self, usdot: int) -> List[Dict]:
        """Get all safety snapshots for a carrier.
        
        Args:
            usdot: The USDOT number to search for
            
        Returns:
            list: All safety snapshots for the carrier, ordered by date
        """
        query = """
        MATCH (s:SafetySnapshot {usdot: $usdot})
        RETURN s
        ORDER BY s.snapshot_date DESC
        """
        result = self.execute_query(query, {"usdot": usdot})
        return [record['s'] for record in result]
    
    def find_latest_by_usdot(self, usdot: int) -> Optional[Dict]:
        """Get the most recent safety snapshot for a carrier.
        
        Args:
            usdot: The USDOT number to search for
            
        Returns:
            dict: Most recent safety snapshot or None if not found
        """
        query = """
        MATCH (s:SafetySnapshot {usdot: $usdot})
        RETURN s
        ORDER BY s.snapshot_date DESC
        LIMIT 1
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['s'] if result else None
    
    def create_relationship_to_carrier(self, usdot: int, snapshot: SafetySnapshot) -> bool:
        """Create a HAS_SAFETY_SNAPSHOT relationship between carrier and snapshot.
        
        Args:
            usdot: The USDOT number of the carrier
            snapshot: The safety snapshot to link
            
        Returns:
            bool: True if relationship created successfully
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (s:SafetySnapshot {usdot: $usdot, snapshot_date: $snapshot_date})
        MERGE (c)-[r:HAS_SAFETY_SNAPSHOT {fetched_date: $fetched_date}]->(s)
        RETURN r
        """
        
        params = {
            "usdot": usdot,
            "snapshot_date": snapshot.snapshot_date.isoformat(),
            "fetched_date": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return len(result) > 0
    
    def find_high_risk_carriers(self, limit: int = 100) -> List[Dict]:
        """Find carriers with high OOS rates (>2x national average).
        
        Args:
            limit: Maximum number of carriers to return
            
        Returns:
            list: High-risk carriers with their latest safety metrics
        """
        query = """
        MATCH (s:SafetySnapshot)
        WHERE s.driver_oos_rate > 10.0 OR s.vehicle_oos_rate > 40.0
        WITH s.usdot as usdot, s
        ORDER BY s.snapshot_date DESC
        WITH usdot, COLLECT(s)[0] as latest_snapshot
        MATCH (c:Carrier {usdot: usdot})
        RETURN c, latest_snapshot
        ORDER BY latest_snapshot.driver_oos_rate DESC, latest_snapshot.vehicle_oos_rate DESC
        LIMIT $limit
        """
        
        result = self.execute_query(query, {"limit": limit})
        return result
    
    def find_carriers_with_alerts(self, alert_type: str = None) -> List[Dict]:
        """Find carriers with active SMS BASIC alerts.
        
        Args:
            alert_type: Optional specific alert type to filter by
            
        Returns:
            list: Carriers with active alerts and their snapshots
        """
        if alert_type:
            query = f"""
            MATCH (s:SafetySnapshot)
            WHERE s.{alert_type}_alert = true
            WITH s.usdot as usdot, s
            ORDER BY s.snapshot_date DESC
            WITH usdot, COLLECT(s)[0] as latest_snapshot
            WHERE latest_snapshot.{alert_type}_alert = true
            MATCH (c:Carrier {{usdot: usdot}})
            RETURN c, latest_snapshot
            """
        else:
            query = """
            MATCH (s:SafetySnapshot)
            WHERE s.unsafe_driving_alert = true OR
                  s.hours_of_service_alert = true OR
                  s.driver_fitness_alert = true OR
                  s.controlled_substances_alert = true OR
                  s.vehicle_maintenance_alert = true OR
                  s.hazmat_compliance_alert = true OR
                  s.crash_indicator_alert = true
            WITH s.usdot as usdot, s
            ORDER BY s.snapshot_date DESC
            WITH usdot, COLLECT(s)[0] as latest_snapshot
            MATCH (c:Carrier {usdot: usdot})
            RETURN c, latest_snapshot
            """
        
        result = self.execute_query(query)
        return result