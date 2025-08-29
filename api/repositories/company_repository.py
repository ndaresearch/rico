from typing import Dict, List, Optional
from datetime import datetime, timezone

from database import BaseRepository
from models.company import Company


class CompanyRepository(BaseRepository):
    """Repository for Company entity operations"""
    
    def create(self, company: Company) -> Dict:
        """Create a new company node"""
        query = """
        CREATE (c:Company {
            dot_number: $dot_number,
            legal_name: $legal_name,
            mc_number: $mc_number,
            duns_number: $duns_number,
            ein: $ein,
            dba_name: $dba_name,
            entity_type: $entity_type,
            authority_status: $authority_status,
            safety_rating: $safety_rating,
            operation_classification: $operation_classification,
            company_type: $company_type,
            operating_model: $operating_model,
            parent_dot_model: $parent_dot_model,
            ultimate_parent_id: $ultimate_parent_id,
            consolidation_level: $consolidation_level,
            is_publicly_traded: $is_publicly_traded,
            parent_company_name: $parent_company_name,
            sec_cik: $sec_cik,
            known_subsidiaries: $known_subsidiaries,
            total_drivers: $total_drivers,
            total_trucks: $total_trucks,
            total_trailers: $total_trailers,
            chameleon_risk_score: $chameleon_risk_score,
            safety_risk_score: $safety_risk_score,
            financial_risk_score: $financial_risk_score,
            created_date: $created_date,
            last_updated: $last_updated,
            mcs150_date: $mcs150_date,
            insurance_minimum: $insurance_minimum,
            cargo_carried: $cargo_carried,
            data_completeness_score: $data_completeness_score
        })
        RETURN c
        """
        
        # Convert dates to strings for Neo4j
        params = company.model_dump()
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
    
    def get_by_dot_number(self, dot_number: int) -> Optional[Dict]:
        """Get a company by DOT number"""
        query = """
        MATCH (c:Company {dot_number: $dot_number})
        RETURN c
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result[0]['c'] if result else None
    
    def get_all(self, skip: int = 0, limit: int = 100, filters: Dict = None) -> List[Dict]:
        """Get all companies with pagination and filters"""
        where_clauses = []
        params = {"skip": skip, "limit": limit}
        
        if filters:
            if filters.get('authority_status'):
                where_clauses.append("c.authority_status = $authority_status")
                params['authority_status'] = filters['authority_status']
            
            if filters.get('safety_rating'):
                where_clauses.append("c.safety_rating = $safety_rating")
                params['safety_rating'] = filters['safety_rating']
            
            if filters.get('entity_type'):
                where_clauses.append("c.entity_type = $entity_type")
                params['entity_type'] = filters['entity_type']
            
            if filters.get('min_trucks'):
                where_clauses.append("c.total_trucks >= $min_trucks")
                params['min_trucks'] = filters['min_trucks']
            
            if filters.get('chameleon_risk_threshold'):
                where_clauses.append("c.chameleon_risk_score >= $chameleon_risk_threshold")
                params['chameleon_risk_threshold'] = filters['chameleon_risk_threshold']
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        query = f"""
        MATCH (c:Company)
        {where_clause}
        RETURN c
        ORDER BY c.dot_number
        SKIP $skip
        LIMIT $limit
        """
        
        result = self.execute_query(query, params)
        return [record['c'] for record in result]
    
    def update(self, dot_number: int, updates: Dict) -> Optional[Dict]:
        """Update a company's properties"""
        # Build SET clause dynamically
        set_clauses = []
        params = {"dot_number": dot_number}
        
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
        MATCH (c:Company {{dot_number: $dot_number}})
        SET {set_clause}
        RETURN c
        """
        
        result = self.execute_query(query, params)
        return result[0]['c'] if result else None
    
    def delete(self, dot_number: int) -> bool:
        """Delete a company and its relationships"""
        query = """
        MATCH (c:Company {dot_number: $dot_number})
        DETACH DELETE c
        RETURN count(c) as deleted
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result[0]['deleted'] > 0 if result else False
    
    def exists(self, dot_number: int) -> bool:
        """Check if a company exists"""
        query = """
        MATCH (c:Company {dot_number: $dot_number})
        RETURN count(c) > 0 as exists
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result[0]['exists'] if result else False
    
    def get_statistics(self) -> Dict:
        """Get company statistics"""
        query = """
        MATCH (c:Company)
        RETURN 
            count(c) as total_companies,
            avg(c.total_drivers) as avg_drivers,
            avg(c.total_trucks) as avg_trucks,
            avg(c.chameleon_risk_score) as avg_chameleon_risk,
            count(CASE WHEN c.authority_status = 'ACTIVE' THEN 1 END) as active_companies,
            count(CASE WHEN c.chameleon_risk_score > 0.7 THEN 1 END) as high_risk_companies
        """
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def find_similar_companies(self, dot_number: int, threshold: float = 0.8) -> List[Dict]:
        """Find companies with similar names or addresses (potential chameleons)"""
        query = """
        MATCH (c1:Company {dot_number: $dot_number})
        MATCH (c2:Company)
        WHERE c1 <> c2
        AND (
            // Similar names (using simple string matching - in production use fuzzy matching)
            toLower(c1.legal_name) CONTAINS toLower(substring(c2.legal_name, 0, 10))
            OR toLower(c2.legal_name) CONTAINS toLower(substring(c1.legal_name, 0, 10))
            // Or share officers (this will work when we add Person relationships)
            OR EXISTS {
                MATCH (c1)-[:HAS_OFFICER]->(p:Person)<-[:HAS_OFFICER]-(c2)
            }
            // Or share equipment (this will work when we add Equipment relationships)
            OR EXISTS {
                MATCH (c1)-[:OPERATES]->(e:Equipment)<-[:OPERATES]-(c2)
            }
        )
        RETURN c2, 
               CASE 
                   WHEN toLower(c1.legal_name) = toLower(c2.legal_name) THEN 1.0
                   WHEN toLower(c1.legal_name) CONTAINS toLower(c2.legal_name) THEN 0.9
                   ELSE 0.7
               END as similarity_score
        ORDER BY similarity_score DESC
        LIMIT 10
        """
        result = self.execute_query(query, {"dot_number": dot_number, "threshold": threshold})
        return result
    
    def bulk_create(self, companies: List[Company]) -> Dict:
        """Bulk create companies"""
        query = """
        UNWIND $companies as company
        CREATE (c:Company)
        SET c = company
        RETURN count(c) as created
        """
        
        # Convert all companies to dict with proper date formatting
        companies_data = []
        for company in companies:
            data = company.model_dump()
            if data.get('created_date'):
                data['created_date'] = data['created_date'].isoformat()
            if data.get('mcs150_date'):
                data['mcs150_date'] = data['mcs150_date'].isoformat()
            if data.get('last_updated'):
                data['last_updated'] = data['last_updated'].isoformat()
            else:
                data['last_updated'] = datetime.now(timezone.utc).isoformat()
            companies_data.append(data)
        
        result = self.execute_query(query, {"companies": companies_data})
        return {"created": result[0]['created']} if result else {"created": 0}