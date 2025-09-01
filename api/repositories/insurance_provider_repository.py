from typing import Dict, List, Optional
from datetime import datetime, timezone

from database import BaseRepository
from models.insurance_provider import InsuranceProvider


class InsuranceProviderRepository(BaseRepository):
    """Repository for InsuranceProvider entity operations"""
    
    def create(self, provider: InsuranceProvider) -> Dict:
        """Create a new insurance provider node"""
        query = """
        CREATE (ip:InsuranceProvider {
            provider_id: $provider_id,
            name: $name,
            contact_phone: $contact_phone,
            contact_email: $contact_email,
            website: $website,
            created_date: $created_date,
            last_updated: $last_updated,
            total_carriers_insured: $total_carriers_insured,
            data_source: $data_source
        })
        RETURN ip
        """
        
        # Convert dates to strings for Neo4j
        params = provider.model_dump()
        if params.get('created_date'):
            params['created_date'] = params['created_date'].isoformat()
        if params.get('last_updated'):
            params['last_updated'] = params['last_updated'].isoformat()
        else:
            params['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['ip'] if result else None
    
    def get_by_id(self, provider_id: str) -> Optional[Dict]:
        """Get an insurance provider by ID"""
        query = """
        MATCH (ip:InsuranceProvider {provider_id: $provider_id})
        RETURN ip
        """
        result = self.execute_query(query, {"provider_id": provider_id})
        return result[0]['ip'] if result else None
    
    def get_by_name(self, name: str) -> Optional[Dict]:
        """Get an insurance provider by name"""
        query = """
        MATCH (ip:InsuranceProvider {name: $name})
        RETURN ip
        """
        result = self.execute_query(query, {"name": name})
        return result[0]['ip'] if result else None
    
    def get_or_create(self, name: str) -> Dict:
        """Get an existing provider by name or create a new one"""
        existing = self.get_by_name(name)
        if existing:
            return existing
        
        # Create new provider with just the name
        provider = InsuranceProvider(name=name)
        return self.create(provider)
    
    def get_all(self, skip: int = 0, limit: int = 100) -> List[Dict]:
        """Get all insurance providers with pagination"""
        query = """
        MATCH (ip:InsuranceProvider)
        RETURN ip
        ORDER BY ip.name
        SKIP $skip
        LIMIT $limit
        """
        
        params = {"skip": skip, "limit": limit}
        result = self.execute_query(query, params)
        return [record['ip'] for record in result]
    
    def update(self, provider_id: str, updates: Dict) -> Optional[Dict]:
        """Update an insurance provider's properties"""
        # Build SET clause dynamically
        set_clauses = []
        params = {"provider_id": provider_id}
        
        for key, value in updates.items():
            if value is not None:
                set_clauses.append(f"ip.{key} = ${key}")
                # Convert dates to strings
                if key in ['created_date', 'last_updated']:
                    params[key] = value.isoformat() if hasattr(value, 'isoformat') else value
                else:
                    params[key] = value
        
        # Always update last_updated
        set_clauses.append("ip.last_updated = $last_updated")
        params['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        if not set_clauses:
            return None
        
        set_clause = ", ".join(set_clauses)
        
        query = f"""
        MATCH (ip:InsuranceProvider {{provider_id: $provider_id}})
        SET {set_clause}
        RETURN ip
        """
        
        result = self.execute_query(query, params)
        return result[0]['ip'] if result else None
    
    def delete(self, provider_id: str) -> bool:
        """Delete an insurance provider and its relationships"""
        query = """
        MATCH (ip:InsuranceProvider {provider_id: $provider_id})
        DETACH DELETE ip
        RETURN count(ip) as deleted
        """
        result = self.execute_query(query, {"provider_id": provider_id})
        return result[0]['deleted'] > 0 if result else False
    
    def exists_by_name(self, name: str) -> bool:
        """Check if an insurance provider exists by name"""
        query = """
        MATCH (ip:InsuranceProvider {name: $name})
        RETURN count(ip) > 0 as exists
        """
        result = self.execute_query(query, {"name": name})
        return result[0]['exists'] if result else False
    
    def exists_by_id(self, provider_id: str) -> bool:
        """Check if an insurance provider exists by ID"""
        query = """
        MATCH (ip:InsuranceProvider {provider_id: $provider_id})
        RETURN count(ip) > 0 as exists
        """
        result = self.execute_query(query, {"provider_id": provider_id})
        return result[0]['exists'] if result else False
    
    def get_carriers(self, provider_id: str) -> List[Dict]:
        """Get all carriers insured by this provider"""
        query = """
        MATCH (ip:InsuranceProvider {provider_id: $provider_id})<-[:INSURED_BY]-(c:Carrier)
        RETURN c
        ORDER BY c.carrier_name
        """
        result = self.execute_query(query, {"provider_id": provider_id})
        return [record['c'] for record in result]
    
    def get_carriers_by_name(self, name: str) -> List[Dict]:
        """Get all carriers insured by this provider (by name)"""
        query = """
        MATCH (ip:InsuranceProvider {name: $name})<-[:INSURED_BY]-(c:Carrier)
        RETURN c
        ORDER BY c.carrier_name
        """
        result = self.execute_query(query, {"name": name})
        return [record['c'] for record in result]
    
    def update_carrier_count(self, provider_id: str) -> Dict:
        """Update the total_carriers_insured count for a provider"""
        query = """
        MATCH (ip:InsuranceProvider {provider_id: $provider_id})
        OPTIONAL MATCH (ip)<-[:INSURED_BY]-(c:Carrier)
        WITH ip, count(c) as carrier_count
        SET ip.total_carriers_insured = carrier_count,
            ip.last_updated = $last_updated
        RETURN ip
        """
        
        params = {
            "provider_id": provider_id,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return result[0]['ip'] if result else None
    
    def get_statistics(self) -> Dict:
        """Get insurance provider statistics"""
        query = """
        MATCH (ip:InsuranceProvider)
        OPTIONAL MATCH (ip)<-[:INSURED_BY]-(c:Carrier)
        WITH ip, count(c) as carriers_per_provider
        RETURN 
            count(DISTINCT ip) as total_providers,
            avg(carriers_per_provider) as avg_carriers_per_provider,
            max(carriers_per_provider) as max_carriers_per_provider,
            count(CASE WHEN carriers_per_provider = 0 THEN 1 END) as providers_without_carriers,
            count(CASE WHEN carriers_per_provider > 10 THEN 1 END) as major_providers
        """
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def bulk_create(self, providers: List[InsuranceProvider]) -> Dict:
        """Bulk create insurance providers"""
        query = """
        UNWIND $providers as provider
        CREATE (ip:InsuranceProvider)
        SET ip = provider
        RETURN count(ip) as created
        """
        
        # Convert all providers to dict with proper date formatting
        providers_data = []
        for provider in providers:
            data = provider.model_dump()
            if data.get('created_date'):
                data['created_date'] = data['created_date'].isoformat()
            if data.get('last_updated'):
                data['last_updated'] = data['last_updated'].isoformat()
            else:
                data['last_updated'] = datetime.now(timezone.utc).isoformat()
            providers_data.append(data)
        
        result = self.execute_query(query, {"providers": providers_data})
        return {"created": result[0]['created']} if result else {"created": 0}