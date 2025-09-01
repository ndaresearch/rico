from typing import Dict, List, Optional
from datetime import datetime, timezone

from database import BaseRepository
from models.carrier import Carrier


class CarrierRepository(BaseRepository):
    """Repository for Carrier entity operations"""
    
    def create(self, carrier: Carrier) -> Dict:
        """Create a new carrier node"""
        query = """
        CREATE (c:Carrier {
            usdot: $usdot,
            jb_carrier: $jb_carrier,
            carrier_name: $carrier_name,
            primary_officer: $primary_officer,
            insurance_provider: $insurance_provider,
            insurance_amount: $insurance_amount,
            trucks: $trucks,
            mcs150_drivers: $mcs150_drivers,
            mcs150_miles: $mcs150_miles,
            ampd: $ampd,
            inspections: $inspections,
            violations: $violations,
            oos: $oos,
            crashes: $crashes,
            driver_oos_rate: $driver_oos_rate,
            vehicle_oos_rate: $vehicle_oos_rate,
            created_date: $created_date,
            last_updated: $last_updated,
            mcs150_date: $mcs150_date,
            data_source: $data_source
        })
        RETURN c
        """
        
        # Convert dates to strings for Neo4j
        params = carrier.model_dump()
        if params.get('created_date'):
            params['created_date'] = params['created_date'].isoformat()
        if params.get('mcs150_date'):
            params['mcs150_date'] = params['mcs150_date'].isoformat()
        if params.get('last_updated'):
            params['last_updated'] = params['last_updated'].isoformat()
        else:
            params['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['c'] if result else None
    
    def get_by_usdot(self, usdot: int) -> Optional[Dict]:
        """Get a carrier by USDOT number"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        RETURN c
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['c'] if result else None
    
    def get_all(self, skip: int = 0, limit: int = 100, filters: Dict = None) -> List[Dict]:
        """Get all carriers with pagination and filters"""
        where_clauses = []
        params = {"skip": skip, "limit": limit}
        
        if filters:
            if filters.get('jb_carrier') is not None:
                where_clauses.append("c.jb_carrier = $jb_carrier")
                params['jb_carrier'] = filters['jb_carrier']
            
            if filters.get('min_trucks'):
                where_clauses.append("c.trucks >= $min_trucks")
                params['min_trucks'] = filters['min_trucks']
            
            if filters.get('min_violations'):
                where_clauses.append("c.violations >= $min_violations")
                params['min_violations'] = filters['min_violations']
            
            if filters.get('min_crashes'):
                where_clauses.append("c.crashes >= $min_crashes")
                params['min_crashes'] = filters['min_crashes']
            
            if filters.get('min_driver_oos_rate'):
                where_clauses.append("c.driver_oos_rate >= $min_driver_oos_rate")
                params['min_driver_oos_rate'] = filters['min_driver_oos_rate']
            
            if filters.get('insurance_provider'):
                where_clauses.append("c.insurance_provider = $insurance_provider")
                params['insurance_provider'] = filters['insurance_provider']
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        MATCH (c:Carrier)
        {where_clause}
        RETURN c
        ORDER BY c.usdot
        SKIP $skip
        LIMIT $limit
        """
        
        result = self.execute_query(query, params)
        return [record['c'] for record in result]
    
    def update(self, usdot: int, updates: Dict) -> Optional[Dict]:
        """Update a carrier's properties"""
        # Build SET clause dynamically
        set_clauses = []
        params = {"usdot": usdot}
        
        for key, value in updates.items():
            if value is not None:
                set_clauses.append(f"c.{key} = ${key}")
                # Convert dates to strings
                if key in ['created_date', 'mcs150_date', 'last_updated']:
                    params[key] = value.isoformat() if hasattr(value, 'isoformat') else value
                else:
                    params[key] = value
        
        # Always update last_updated
        set_clauses.append("c.last_updated = $last_updated")
        params['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        if not set_clauses:
            return None
        
        set_clause = ", ".join(set_clauses)
        
        query = f"""
        MATCH (c:Carrier {{usdot: $usdot}})
        SET {set_clause}
        RETURN c
        """
        
        result = self.execute_query(query, params)
        return result[0]['c'] if result else None
    
    def delete(self, usdot: int) -> bool:
        """Delete a carrier and its relationships"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        DETACH DELETE c
        RETURN count(c) as deleted
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['deleted'] > 0 if result else False
    
    def exists(self, usdot: int) -> bool:
        """Check if a carrier exists"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        RETURN count(c) > 0 as exists
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['exists'] if result else False
    
    def get_statistics(self) -> Dict:
        """Get carrier statistics"""
        query = """
        MATCH (c:Carrier)
        RETURN 
            count(c) as total_carriers,
            count(CASE WHEN c.jb_carrier = true THEN 1 END) as jb_carriers,
            avg(c.trucks) as avg_trucks,
            avg(c.violations) as avg_violations,
            avg(c.crashes) as avg_crashes,
            avg(c.driver_oos_rate) as avg_driver_oos_rate,
            avg(c.vehicle_oos_rate) as avg_vehicle_oos_rate,
            count(CASE WHEN c.crashes > 0 THEN 1 END) as carriers_with_crashes,
            count(CASE WHEN c.violations > 10 THEN 1 END) as high_violation_carriers
        """
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def create_contract_with_target(self, usdot: int, dot_number: int, 
                                   contract_start: Optional[str] = None,
                                   contract_end: Optional[str] = None,
                                   active: bool = True) -> bool:
        """Create a CONTRACTS_WITH relationship between a carrier and target company"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (tc:TargetCompany {dot_number: $dot_number})
        CREATE (tc)-[r:CONTRACTS_WITH {
            start_date: $start_date,
            end_date: $end_date,
            active: $active,
            created_at: $created_at
        }]->(c)
        RETURN r
        """
        
        params = {
            "usdot": usdot,
            "dot_number": dot_number,
            "start_date": contract_start,
            "end_date": contract_end,
            "active": active,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def link_to_insurance_provider(self, usdot: int, provider_name: str, 
                                  amount: Optional[float] = None) -> bool:
        """Create INSURED_BY relationship to insurance provider"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (ip:InsuranceProvider {name: $provider_name})
        CREATE (c)-[r:INSURED_BY {
            amount: $amount,
            created_at: $created_at
        }]->(ip)
        RETURN r
        """
        
        params = {
            "usdot": usdot,
            "provider_name": provider_name,
            "amount": amount,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def link_to_officer(self, usdot: int, person_id: str) -> bool:
        """Create MANAGED_BY relationship to a person (officer)"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (p:Person {person_id: $person_id})
        CREATE (c)-[r:MANAGED_BY {
            created_at: $created_at
        }]->(p)
        RETURN r
        """
        
        params = {
            "usdot": usdot,
            "person_id": person_id,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def get_high_risk_carriers(self, threshold: float = 0.2) -> List[Dict]:
        """Get carriers with high OOS rates or multiple crashes"""
        query = """
        MATCH (c:Carrier)
        WHERE c.driver_oos_rate > $threshold 
           OR c.vehicle_oos_rate > $threshold 
           OR c.crashes > 5
        RETURN c
        ORDER BY c.crashes DESC, c.driver_oos_rate DESC
        """
        result = self.execute_query(query, {"threshold": threshold})
        return [record['c'] for record in result]
    
    def bulk_create(self, carriers: List[Carrier]) -> Dict:
        """Bulk create carriers"""
        query = """
        UNWIND $carriers as carrier
        CREATE (c:Carrier)
        SET c = carrier
        RETURN count(c) as created
        """
        
        # Convert all carriers to dict with proper date formatting
        carriers_data = []
        for carrier in carriers:
            data = carrier.model_dump()
            if data.get('created_date'):
                data['created_date'] = data['created_date'].isoformat()
            if data.get('mcs150_date'):
                data['mcs150_date'] = data['mcs150_date'].isoformat()
            if data.get('last_updated'):
                data['last_updated'] = data['last_updated'].isoformat()
            else:
                data['last_updated'] = datetime.now(timezone.utc).isoformat()
            carriers_data.append(data)
        
        result = self.execute_query(query, {"carriers": carriers_data})
        return {"created": result[0]['created']} if result else {"created": 0}