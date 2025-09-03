from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta

from database import BaseRepository
from models.crash import Crash


class CrashRepository(BaseRepository):
    """Repository for Crash entity operations in Neo4j graph database.
    
    Handles CRUD operations for crashes and manages their relationships
    with carriers.
    """
    
    def create(self, crash: Crash) -> Dict:
        """Create a new crash node in the graph database.
        
        Args:
            crash: Crash model with all required properties
            
        Returns:
            dict: Created crash node data or None if creation fails
        """
        query = """
        CREATE (cr:Crash {
            report_number: $report_number,
            report_state: $report_state,
            usdot: $usdot,
            crash_date: $crash_date,
            severity: $severity,
            tow_away: $tow_away,
            fatalities: $fatalities,
            injuries: $injuries,
            vehicles_involved: $vehicles_involved,
            weather: $weather,
            road_condition: $road_condition,
            light_condition: $light_condition,
            latitude: $latitude,
            longitude: $longitude,
            preventable: $preventable,
            citation_issued: $citation_issued
        })
        RETURN cr
        """
        
        # Convert dates to strings for Neo4j
        params = crash.model_dump()
        if params.get('crash_date'):
            params['crash_date'] = params['crash_date'].isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['cr'] if result else None
    
    def find_by_usdot(self, usdot: int) -> List[Dict]:
        """Get all crashes for a carrier.
        
        Args:
            usdot: The USDOT number to search for
            
        Returns:
            list: All crashes for the carrier, ordered by date
        """
        query = """
        MATCH (cr:Crash {usdot: $usdot})
        RETURN cr
        ORDER BY cr.crash_date DESC
        """
        result = self.execute_query(query, {"usdot": usdot})
        return [record['cr'] for record in result]
    
    def find_by_report_number(self, report_number: str) -> Optional[Dict]:
        """Get a specific crash by report number.
        
        Args:
            report_number: The crash report number
            
        Returns:
            dict: Crash data or None if not found
        """
        query = """
        MATCH (cr:Crash {report_number: $report_number})
        RETURN cr
        """
        result = self.execute_query(query, {"report_number": report_number})
        return result[0]['cr'] if result else None
    
    def create_relationship_to_carrier(self, usdot: int, crash: Crash) -> bool:
        """Create an INVOLVED_IN relationship between carrier and crash.
        
        Args:
            usdot: The USDOT number of the carrier
            crash: The crash to link
            
        Returns:
            bool: True if relationship created successfully
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (cr:Crash {report_number: $report_number})
        MERGE (c)-[r:INVOLVED_IN]->(cr)
        RETURN r
        """
        
        params = {
            "usdot": usdot,
            "report_number": crash.report_number
        }
        
        result = self.execute_query(query, params)
        return len(result) > 0
    
    def find_fatal_crashes(self, usdot: int = None) -> List[Dict]:
        """Find crashes with fatalities.
        
        Args:
            usdot: Optional USDOT number to filter by carrier
            
        Returns:
            list: Fatal crashes
        """
        if usdot:
            query = """
            MATCH (cr:Crash {usdot: $usdot})
            WHERE cr.fatalities > 0
            RETURN cr
            ORDER BY cr.crash_date DESC
            """
            params = {"usdot": usdot}
        else:
            query = """
            MATCH (cr:Crash)
            WHERE cr.fatalities > 0
            RETURN cr
            ORDER BY cr.crash_date DESC
            LIMIT 1000
            """
            params = {}
        
        result = self.execute_query(query, params)
        return [record['cr'] for record in result]
    
    def find_injury_crashes(self, usdot: int = None) -> List[Dict]:
        """Find crashes with injuries.
        
        Args:
            usdot: Optional USDOT number to filter by carrier
            
        Returns:
            list: Injury crashes
        """
        if usdot:
            query = """
            MATCH (cr:Crash {usdot: $usdot})
            WHERE cr.injuries > 0
            RETURN cr
            ORDER BY cr.crash_date DESC
            """
            params = {"usdot": usdot}
        else:
            query = """
            MATCH (cr:Crash)
            WHERE cr.injuries > 0
            RETURN cr
            ORDER BY cr.crash_date DESC
            LIMIT 1000
            """
            params = {}
        
        result = self.execute_query(query, params)
        return [record['cr'] for record in result]
    
    def find_tow_away_crashes(self, usdot: int) -> List[Dict]:
        """Find crashes that required tow-away.
        
        Args:
            usdot: The USDOT number of the carrier
            
        Returns:
            list: Tow-away crashes
        """
        query = """
        MATCH (cr:Crash {usdot: $usdot})
        WHERE cr.tow_away = true
        RETURN cr
        ORDER BY cr.crash_date DESC
        """
        
        result = self.execute_query(query, {"usdot": usdot})
        return [record['cr'] for record in result]
    
    def find_preventable_crashes(self, usdot: int) -> List[Dict]:
        """Find crashes that were preventable.
        
        Args:
            usdot: The USDOT number of the carrier
            
        Returns:
            list: Preventable crashes
        """
        query = """
        MATCH (cr:Crash {usdot: $usdot})
        WHERE cr.preventable = true
        RETURN cr
        ORDER BY cr.crash_date DESC
        """
        
        result = self.execute_query(query, {"usdot": usdot})
        return [record['cr'] for record in result]
    
    def calculate_crash_statistics(self, usdot: int, months: int = 24) -> Dict:
        """Calculate crash statistics for a carrier over a time period.
        
        Args:
            usdot: The USDOT number of the carrier
            months: Number of months to look back
            
        Returns:
            dict: Crash statistics
        """
        query = """
        MATCH (cr:Crash {usdot: $usdot})
        WHERE cr.crash_date >= date() - duration('P' + $months + 'M')
        WITH COUNT(cr) as total_crashes,
             SUM(cr.fatalities) as total_fatalities,
             SUM(cr.injuries) as total_injuries,
             SUM(CASE WHEN cr.fatalities > 0 THEN 1 ELSE 0 END) as fatal_crashes,
             SUM(CASE WHEN cr.injuries > 0 THEN 1 ELSE 0 END) as injury_crashes,
             SUM(CASE WHEN cr.tow_away = true THEN 1 ELSE 0 END) as tow_away_crashes,
             SUM(CASE WHEN cr.preventable = true THEN 1 ELSE 0 END) as preventable_crashes
        RETURN total_crashes,
               total_fatalities,
               total_injuries,
               fatal_crashes,
               injury_crashes,
               tow_away_crashes,
               preventable_crashes,
               CASE WHEN total_crashes > 0 
                    THEN toFloat(preventable_crashes) / total_crashes * 100 
                    ELSE 0.0 END as preventable_rate
        """
        
        result = self.execute_query(query, {"usdot": usdot, "months": str(months)})
        return result[0] if result else {
            "total_crashes": 0,
            "total_fatalities": 0,
            "total_injuries": 0,
            "fatal_crashes": 0,
            "injury_crashes": 0,
            "tow_away_crashes": 0,
            "preventable_crashes": 0,
            "preventable_rate": 0.0
        }
    
    def find_crashes_by_severity(self, min_fatalities: int = 0, min_injuries: int = 0) -> List[Dict]:
        """Find crashes meeting severity thresholds.
        
        Args:
            min_fatalities: Minimum number of fatalities
            min_injuries: Minimum number of injuries
            
        Returns:
            list: Crashes meeting the severity criteria
        """
        query = """
        MATCH (cr:Crash)
        WHERE cr.fatalities >= $min_fatalities OR cr.injuries >= $min_injuries
        RETURN cr
        ORDER BY cr.fatalities DESC, cr.injuries DESC, cr.crash_date DESC
        LIMIT 1000
        """
        
        params = {
            "min_fatalities": min_fatalities,
            "min_injuries": min_injuries
        }
        
        result = self.execute_query(query, params)
        return [record['cr'] for record in result]
    
    def find_crash_clusters(self, usdot: int, days_window: int = 30) -> List[Dict]:
        """Find clusters of crashes within a time window.
        
        Args:
            usdot: The USDOT number of the carrier
            days_window: Number of days for clustering window
            
        Returns:
            list: Crash clusters with counts
        """
        query = """
        MATCH (cr1:Crash {usdot: $usdot})
        MATCH (cr2:Crash {usdot: $usdot})
        WHERE cr1.report_number < cr2.report_number
          AND abs(duration.between(date(cr1.crash_date), date(cr2.crash_date)).days) <= $days_window
        WITH cr1.crash_date as cluster_start,
             COLLECT(DISTINCT cr2.report_number) + [cr1.report_number] as crash_reports,
             COUNT(DISTINCT cr2) + 1 as cluster_size
        WHERE cluster_size >= 2
        RETURN cluster_start, crash_reports, cluster_size
        ORDER BY cluster_size DESC, cluster_start DESC
        """
        
        params = {
            "usdot": usdot,
            "days_window": days_window
        }
        
        result = self.execute_query(query, params)
        return result
    
    def find_high_risk_carriers_by_crashes(self, limit: int = 100) -> List[Dict]:
        """Find carriers with the most severe crash histories.
        
        Args:
            limit: Maximum number of carriers to return
            
        Returns:
            list: High-risk carriers based on crash data
        """
        query = """
        MATCH (cr:Crash)
        WHERE cr.crash_date >= date() - duration('P24M')
        WITH cr.usdot as usdot,
             COUNT(cr) as crash_count,
             SUM(cr.fatalities) as total_fatalities,
             SUM(cr.injuries) as total_injuries,
             SUM(CASE WHEN cr.fatalities > 0 THEN 1 ELSE 0 END) as fatal_crashes
        WHERE crash_count > 0
        MATCH (c:Carrier {usdot: usdot})
        RETURN c, 
               crash_count, 
               total_fatalities, 
               total_injuries, 
               fatal_crashes,
               (total_fatalities * 10 + total_injuries * 2 + crash_count) as risk_score
        ORDER BY risk_score DESC
        LIMIT $limit
        """
        
        result = self.execute_query(query, {"limit": limit})
        return result