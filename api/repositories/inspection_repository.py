from typing import Dict, List, Optional
from datetime import datetime, timezone

from database import BaseRepository
from models.inspection import Inspection


class InspectionRepository(BaseRepository):
    """Repository for Inspection entity operations in Neo4j graph database.
    
    Handles CRUD operations for inspections and manages their relationships
    with carriers and violations.
    """
    
    def create(self, inspection: Inspection) -> Dict:
        """Create a new inspection node in the graph database.
        
        Args:
            inspection: Inspection model with all required properties
            
        Returns:
            dict: Created inspection node data or None if creation fails
        """
        query = """
        CREATE (i:Inspection {
            inspection_id: $inspection_id,
            usdot: $usdot,
            inspection_date: $inspection_date,
            level: $level,
            state: $state,
            location: $location,
            violations_count: $violations_count,
            oos_violations_count: $oos_violations_count,
            vehicle_oos: $vehicle_oos,
            driver_oos: $driver_oos,
            hazmat_oos: $hazmat_oos,
            result: $result
        })
        RETURN i
        """
        
        # Convert dates to strings for Neo4j
        params = inspection.model_dump()
        if params.get('inspection_date'):
            params['inspection_date'] = params['inspection_date'].isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['i'] if result else None
    
    def find_by_usdot(self, usdot: int, limit: int = 100) -> List[Dict]:
        """Get inspection records for a carrier.
        
        Args:
            usdot: The USDOT number to search for
            limit: Maximum number of inspections to return
            
        Returns:
            list: Inspection records for the carrier, ordered by date
        """
        query = """
        MATCH (i:Inspection {usdot: $usdot})
        RETURN i
        ORDER BY i.inspection_date DESC
        LIMIT $limit
        """
        result = self.execute_query(query, {"usdot": usdot, "limit": limit})
        return [record['i'] for record in result]
    
    def find_by_inspection_id(self, inspection_id: str) -> Optional[Dict]:
        """Get a specific inspection by ID.
        
        Args:
            inspection_id: The inspection ID to search for
            
        Returns:
            dict: Inspection data or None if not found
        """
        query = """
        MATCH (i:Inspection {inspection_id: $inspection_id})
        RETURN i
        """
        result = self.execute_query(query, {"inspection_id": inspection_id})
        return result[0]['i'] if result else None
    
    def create_relationship_to_carrier(self, usdot: int, inspection: Inspection) -> bool:
        """Create an UNDERWENT relationship between carrier and inspection.
        
        Args:
            usdot: The USDOT number of the carrier
            inspection: The inspection to link
            
        Returns:
            bool: True if relationship created successfully
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (i:Inspection {inspection_id: $inspection_id})
        MERGE (c)-[r:UNDERWENT]->(i)
        RETURN r
        """
        
        params = {
            "usdot": usdot,
            "inspection_id": inspection.inspection_id
        }
        
        result = self.execute_query(query, params)
        return len(result) > 0
    
    def link_violations(self, inspection_id: str, violation_ids: List[str]) -> int:
        """Create FOUND relationships between inspection and violations.
        
        Args:
            inspection_id: The inspection ID
            violation_ids: List of violation IDs to link
            
        Returns:
            int: Number of relationships created
        """
        query = """
        MATCH (i:Inspection {inspection_id: $inspection_id})
        MATCH (v:Violation)
        WHERE v.violation_id IN $violation_ids
        MERGE (i)-[r:FOUND]->(v)
        RETURN COUNT(r) as count
        """
        
        params = {
            "inspection_id": inspection_id,
            "violation_ids": violation_ids
        }
        
        result = self.execute_query(query, params)
        return result[0]['count'] if result else 0
    
    def find_oos_inspections(self, usdot: int = None) -> List[Dict]:
        """Find inspections that resulted in out-of-service orders.
        
        Args:
            usdot: Optional USDOT number to filter by carrier
            
        Returns:
            list: Inspections with OOS violations
        """
        if usdot:
            query = """
            MATCH (i:Inspection {usdot: $usdot})
            WHERE i.vehicle_oos = true OR i.driver_oos = true OR i.hazmat_oos = true
            RETURN i
            ORDER BY i.inspection_date DESC
            """
            params = {"usdot": usdot}
        else:
            query = """
            MATCH (i:Inspection)
            WHERE i.vehicle_oos = true OR i.driver_oos = true OR i.hazmat_oos = true
            RETURN i
            ORDER BY i.inspection_date DESC
            LIMIT 1000
            """
            params = {}
        
        result = self.execute_query(query, params)
        return [record['i'] for record in result]
    
    def find_clean_inspections(self, usdot: int) -> List[Dict]:
        """Find inspections with no violations for a carrier.
        
        Args:
            usdot: The USDOT number of the carrier
            
        Returns:
            list: Clean inspection records
        """
        query = """
        MATCH (i:Inspection {usdot: $usdot})
        WHERE i.violations_count = 0
        RETURN i
        ORDER BY i.inspection_date DESC
        """
        
        result = self.execute_query(query, {"usdot": usdot})
        return [record['i'] for record in result]
    
    def calculate_violation_rate(self, usdot: int, months: int = 24) -> Dict:
        """Calculate violation rate for a carrier over a time period.
        
        Args:
            usdot: The USDOT number of the carrier
            months: Number of months to look back
            
        Returns:
            dict: Statistics about violation rates
        """
        query = """
        MATCH (i:Inspection {usdot: $usdot})
        WHERE i.inspection_date >= date() - duration('P' + $months + 'M')
        WITH COUNT(i) as total_inspections,
             SUM(i.violations_count) as total_violations,
             SUM(i.oos_violations_count) as total_oos,
             SUM(CASE WHEN i.violations_count = 0 THEN 1 ELSE 0 END) as clean_inspections
        RETURN total_inspections,
               total_violations,
               total_oos,
               clean_inspections,
               CASE WHEN total_inspections > 0 
                    THEN toFloat(total_violations) / total_inspections 
                    ELSE 0.0 END as avg_violations_per_inspection,
               CASE WHEN total_inspections > 0 
                    THEN toFloat(total_oos) / total_inspections 
                    ELSE 0.0 END as avg_oos_per_inspection,
               CASE WHEN total_inspections > 0 
                    THEN toFloat(clean_inspections) / total_inspections * 100 
                    ELSE 0.0 END as clean_inspection_rate
        """
        
        result = self.execute_query(query, {"usdot": usdot, "months": str(months)})
        return result[0] if result else {
            "total_inspections": 0,
            "total_violations": 0,
            "total_oos": 0,
            "clean_inspections": 0,
            "avg_violations_per_inspection": 0.0,
            "avg_oos_per_inspection": 0.0,
            "clean_inspection_rate": 0.0
        }
    
    def find_repeat_violations(self, usdot: int) -> List[Dict]:
        """Find patterns of repeat violations for a carrier.
        
        Args:
            usdot: The USDOT number of the carrier
            
        Returns:
            list: Violations that appear in multiple inspections
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})-[:UNDERWENT]->(i:Inspection)-[:FOUND]->(v:Violation)
        WITH v.code as violation_code, 
             v.description as description,
             COUNT(DISTINCT i) as inspection_count,
             COLLECT(i.inspection_date) as dates
        WHERE inspection_count > 1
        RETURN violation_code, 
               description, 
               inspection_count, 
               dates
        ORDER BY inspection_count DESC
        """
        
        result = self.execute_query(query, {"usdot": usdot})
        return result