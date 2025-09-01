from typing import Dict, List, Optional
from datetime import datetime, timezone

from database import BaseRepository
from models.target_company import TargetCompany


class TargetCompanyRepository(BaseRepository):
    """Repository for TargetCompany entity operations"""
    
    def create(self, target_company: TargetCompany) -> Dict:
        """Create a new target company node"""
        query = """
        CREATE (tc:TargetCompany {
            dot_number: $dot_number,
            legal_name: $legal_name,
            mc_number: $mc_number,
            dba_name: $dba_name,
            entity_type: $entity_type,
            authority_status: $authority_status,
            safety_rating: $safety_rating,
            total_drivers: $total_drivers,
            total_trucks: $total_trucks,
            total_trailers: $total_trailers,
            risk_score: $risk_score,
            created_date: $created_date,
            last_updated: $last_updated,
            data_source: $data_source
        })
        RETURN tc
        """
        
        # Convert dates to strings for Neo4j
        params = target_company.model_dump()
        if params.get('created_date'):
            params['created_date'] = params['created_date'].isoformat()
        if params.get('last_updated'):
            params['last_updated'] = params['last_updated'].isoformat()
        else:
            params['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['tc'] if result else None
    
    def get_by_dot_number(self, dot_number: int) -> Optional[Dict]:
        """Get a target company by DOT number"""
        query = """
        MATCH (tc:TargetCompany {dot_number: $dot_number})
        RETURN tc
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result[0]['tc'] if result else None
    
    def get_all(self, skip: int = 0, limit: int = 100, filters: Dict = None) -> List[Dict]:
        """Get all target companies with pagination and filters"""
        where_clauses = []
        params = {"skip": skip, "limit": limit}
        
        if filters:
            if filters.get('authority_status'):
                where_clauses.append("tc.authority_status = $authority_status")
                params['authority_status'] = filters['authority_status']
            
            if filters.get('safety_rating'):
                where_clauses.append("tc.safety_rating = $safety_rating")
                params['safety_rating'] = filters['safety_rating']
            
            if filters.get('entity_type'):
                where_clauses.append("tc.entity_type = $entity_type")
                params['entity_type'] = filters['entity_type']
            
            if filters.get('min_trucks'):
                where_clauses.append("tc.total_trucks >= $min_trucks")
                params['min_trucks'] = filters['min_trucks']
            
            if filters.get('risk_threshold'):
                where_clauses.append("tc.risk_score >= $risk_threshold")
                params['risk_threshold'] = filters['risk_threshold']
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        MATCH (tc:TargetCompany)
        {where_clause}
        RETURN tc
        ORDER BY tc.dot_number
        SKIP $skip
        LIMIT $limit
        """
        
        result = self.execute_query(query, params)
        return [record['tc'] for record in result]
    
    def update(self, dot_number: int, updates: Dict) -> Optional[Dict]:
        """Update a target company's properties"""
        # Build SET clause dynamically
        set_clauses = []
        params = {"dot_number": dot_number}
        
        for key, value in updates.items():
            if value is not None:
                set_clauses.append(f"tc.{key} = ${key}")
                # Convert dates to strings
                if key in ['created_date', 'last_updated']:
                    params[key] = value.isoformat() if hasattr(value, 'isoformat') else value
                else:
                    params[key] = value
        
        # Always update last_updated
        set_clauses.append("tc.last_updated = $last_updated")
        params['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        if not set_clauses:
            return None
        
        set_clause = ", ".join(set_clauses)
        
        query = f"""
        MATCH (tc:TargetCompany {{dot_number: $dot_number}})
        SET {set_clause}
        RETURN tc
        """
        
        result = self.execute_query(query, params)
        return result[0]['tc'] if result else None
    
    def delete(self, dot_number: int) -> bool:
        """Delete a target company and its relationships"""
        query = """
        MATCH (tc:TargetCompany {dot_number: $dot_number})
        DETACH DELETE tc
        RETURN count(tc) as deleted
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result[0]['deleted'] > 0 if result else False
    
    def exists(self, dot_number: int) -> bool:
        """Check if a target company exists"""
        query = """
        MATCH (tc:TargetCompany {dot_number: $dot_number})
        RETURN count(tc) > 0 as exists
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result[0]['exists'] if result else False
    
    def get_statistics(self) -> Dict:
        """Get target company statistics"""
        query = """
        MATCH (tc:TargetCompany)
        RETURN 
            count(tc) as total_companies,
            avg(tc.total_drivers) as avg_drivers,
            avg(tc.total_trucks) as avg_trucks,
            avg(tc.risk_score) as avg_risk_score,
            count(CASE WHEN tc.authority_status = 'ACTIVE' THEN 1 END) as active_companies,
            count(CASE WHEN tc.risk_score > 0.7 THEN 1 END) as high_risk_companies
        """
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def get_carriers(self, dot_number: int) -> List[Dict]:
        """Get all carriers contracted with this target company"""
        query = """
        MATCH (tc:TargetCompany {dot_number: $dot_number})-[:CONTRACTS_WITH]->(c:Carrier)
        RETURN c
        ORDER BY c.carrier_name
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return [record['c'] for record in result]
    
    def bulk_create(self, target_companies: List[TargetCompany]) -> Dict:
        """Bulk create target companies"""
        query = """
        UNWIND $companies as company
        CREATE (tc:TargetCompany)
        SET tc = company
        RETURN count(tc) as created
        """
        
        # Convert all companies to dict with proper date formatting
        companies_data = []
        for company in target_companies:
            data = company.model_dump()
            if data.get('created_date'):
                data['created_date'] = data['created_date'].isoformat()
            if data.get('last_updated'):
                data['last_updated'] = data['last_updated'].isoformat()
            else:
                data['last_updated'] = datetime.now(timezone.utc).isoformat()
            companies_data.append(data)
        
        result = self.execute_query(query, {"companies": companies_data})
        return {"created": result[0]['created']} if result else {"created": 0}