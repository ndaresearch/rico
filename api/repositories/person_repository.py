# api/repositories/person_repository.py
from typing import Dict, List, Optional
from datetime import datetime, date, timezone
import hashlib

from database import BaseRepository
from models.person import Person


class PersonRepository(BaseRepository):
    """Repository for Person entity operations"""
    
    def _generate_person_id(self, full_name: str, dob: Optional[date] = None) -> str:
        """Generate a consistent person_id based on name and DOB"""
        # Normalize name: lowercase, remove extra spaces
        normalized_name = " ".join(full_name.lower().split())
        
        # Include DOB if available for better uniqueness
        if dob:
            id_string = f"{normalized_name}:{dob.isoformat()}"
        else:
            id_string = normalized_name
        
        # Create hash for consistent ID
        return "P" + hashlib.md5(id_string.encode()).hexdigest()[:10].upper()
    
    def create(self, person: Person) -> Dict:
        """Create a new person node"""
        # Generate person_id if not provided
        if not person.person_id:
            person.person_id = self._generate_person_id(
                person.full_name, 
                person.date_of_birth
            )
        
        query = """
        CREATE (p:Person {
            person_id: $person_id,
            full_name: $full_name,
            first_name: $first_name,
            last_name: $last_name,
            date_of_birth: $date_of_birth,
            email: $email,
            phone: $phone,
            first_seen: $first_seen,
            last_seen: $last_seen,
            source: $source
        })
        RETURN p
        """
        
        params = person.model_dump()
        # Convert dates to strings
        if params.get('date_of_birth'):
            params['date_of_birth'] = params['date_of_birth'].isoformat()
        if params.get('first_seen'):
            params['first_seen'] = params['first_seen'].isoformat()
        else:
            params['first_seen'] = date.today().isoformat()
        if params.get('last_seen'):
            params['last_seen'] = params['last_seen'].isoformat()
        else:
            params['last_seen'] = date.today().isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['p'] if result else None
    
    def get_by_id(self, person_id: str) -> Optional[Dict]:
        """Get a person by their ID"""
        query = """
        MATCH (p:Person {person_id: $person_id})
        RETURN p
        """
        result = self.execute_query(query, {"person_id": person_id})
        return result[0]['p'] if result else None
    
    def find_by_name(self, full_name: str) -> List[Dict]:
        """Find persons by name (fuzzy matching)"""
        query = """
        MATCH (p:Person)
        WHERE toLower(p.full_name) CONTAINS toLower($name_search)
        RETURN p
        ORDER BY p.full_name
        LIMIT 10
        """
        result = self.execute_query(query, {"name_search": full_name})
        return [record['p'] for record in result]
    
    def find_or_create(self, person: Person) -> Dict:
        """Find existing person or create new one"""
        # Generate consistent person_id
        if not person.person_id:
            person.person_id = self._generate_person_id(
                person.full_name,
                person.date_of_birth
            )
        
        # Try to find existing
        existing = self.get_by_id(person.person_id)
        if existing:
            # Update last_seen
            self.update(person.person_id, {"last_seen": date.today()})
            return existing
        
        # Create new
        return self.create(person)
    
    def update(self, person_id: str, updates: Dict) -> Optional[Dict]:
        """Update a person's properties"""
        set_clauses = []
        params = {"person_id": person_id}
        
        for key, value in updates.items():
            if value is not None:
                set_clauses.append(f"p.{key} = ${key}")
                if key in ['date_of_birth', 'first_seen', 'last_seen']:
                    params[key] = value.isoformat() if hasattr(value, 'isoformat') else value
                else:
                    params[key] = value
        
        if not set_clauses:
            return None
        
        set_clause = ", ".join(set_clauses)
        
        query = f"""
        MATCH (p:Person {{person_id: $person_id}})
        SET {set_clause}
        RETURN p
        """
        
        result = self.execute_query(query, params)
        return result[0]['p'] if result else None
    
    def delete(self, person_id: str) -> bool:
        """Delete a person and their relationships"""
        query = """
        MATCH (p:Person {person_id: $person_id})
        DETACH DELETE p
        RETURN count(p) as deleted
        """
        result = self.execute_query(query, {"person_id": person_id})
        return result[0]['deleted'] > 0 if result else False
    
    def get_companies(self, person_id: str) -> List[Dict]:
        """DEPRECATED: Use get_target_companies() or get_carriers() instead"""
        # For backwards compatibility, return empty list
        return []
    
    def get_target_companies(self, person_id: str) -> List[Dict]:
        """Get all TargetCompanies where person is an executive"""
        query = """
        MATCH (p:Person {person_id: $person_id})<-[r:HAS_EXECUTIVE]-(tc:TargetCompany)
        RETURN tc, r.role as role, r.start_date as start_date, r.end_date as end_date
        ORDER BY r.start_date DESC
        """
        result = self.execute_query(query, {"person_id": person_id})
        
        # Flatten the response by merging company data with relationship data
        flattened = []
        for record in result:
            company_data = record.get('tc', {})
            # Merge company properties with relationship properties
            flattened_record = {**company_data}
            flattened_record['role'] = record.get('role')
            flattened_record['start_date'] = record.get('start_date')
            flattened_record['end_date'] = record.get('end_date')
            flattened.append(flattened_record)
        
        return flattened
    
    def get_carriers(self, person_id: str) -> List[Dict]:
        """Get all Carriers managed by this person"""
        query = """
        MATCH (p:Person {person_id: $person_id})<-[r:MANAGED_BY]-(c:Carrier)
        RETURN c, r.created_at as since
        ORDER BY c.carrier_name
        """
        result = self.execute_query(query, {"person_id": person_id})
        
        # Flatten the response
        flattened = []
        for record in result:
            carrier_data = record.get('c', {})
            flattened_record = {**carrier_data}
            flattened_record['managing_since'] = record.get('since')
            flattened.append(flattened_record)
        
        return flattened
    
    def add_to_company(self, person_id: str, dot_number: int, role: str, 
                       start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """DEPRECATED: Use add_to_target_company() instead"""
        # Redirect to new method for backwards compatibility
        return self.add_to_target_company(person_id, dot_number, role, start_date, end_date)
    
    def add_to_target_company(self, person_id: str, dot_number: int, role: str,
                              start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """Create HAS_EXECUTIVE relationship between TargetCompany and Person"""
        query = """
        MATCH (tc:TargetCompany {dot_number: $dot_number})
        MATCH (p:Person {person_id: $person_id})
        CREATE (tc)-[r:HAS_EXECUTIVE {
            role: $role,
            start_date: $start_date,
            end_date: $end_date,
            created_at: $created_at
        }]->(p)
        RETURN r
        """
        
        params = {
            "dot_number": dot_number,
            "person_id": person_id,
            "role": role,
            "start_date": start_date.isoformat() if start_date else date.today().isoformat(),
            "end_date": end_date.isoformat() if end_date else None,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return len(result) > 0
    
    def remove_from_company(self, person_id: str, dot_number: int) -> bool:
        """DEPRECATED: Use remove_from_target_company() instead"""
        return self.remove_from_target_company(person_id, dot_number)
    
    def remove_from_target_company(self, person_id: str, dot_number: int) -> bool:
        """Remove HAS_EXECUTIVE relationship"""
        query = """
        MATCH (tc:TargetCompany {dot_number: $dot_number})-[r:HAS_EXECUTIVE]->(p:Person {person_id: $person_id})
        DELETE r
        RETURN count(r) as deleted
        """
        result = self.execute_query(query, {
            "person_id": person_id,
            "dot_number": dot_number
        })
        return result[0]['deleted'] > 0 if result else False
    
    def remove_from_carrier(self, person_id: str, usdot: int) -> bool:
        """Remove MANAGED_BY relationship"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})-[r:MANAGED_BY]->(p:Person {person_id: $person_id})
        DELETE r
        RETURN count(r) as deleted
        """
        result = self.execute_query(query, {
            "person_id": person_id,
            "usdot": usdot
        })
        return result[0]['deleted'] > 0 if result else False
    
    def find_shared_officers(self, dot_number: int) -> List[Dict]:
        """Find TargetCompanies that share executives with the given TargetCompany"""
        query = """
        MATCH (tc1:TargetCompany {dot_number: $dot_number})-[:HAS_EXECUTIVE]->(p:Person)
        MATCH (tc2:TargetCompany)-[:HAS_EXECUTIVE]->(p)
        WHERE tc1 <> tc2
        RETURN DISTINCT tc2 as company, 
               collect(DISTINCT p.full_name) as shared_executives,
               count(DISTINCT p) as executive_count
        ORDER BY executive_count DESC
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result
    
    def find_officer_succession_patterns(self) -> List[Dict]:
        """Find suspicious executive succession patterns (same person, sequential companies)"""
        query = """
        MATCH (p:Person)<-[r1:HAS_EXECUTIVE]-(tc1:TargetCompany)
        MATCH (p)<-[r2:HAS_EXECUTIVE]-(tc2:TargetCompany)
        WHERE tc1 <> tc2
        AND r1.end_date IS NOT NULL
        AND r2.start_date IS NOT NULL
        AND duration.between(date(r1.end_date), date(r2.start_date)).months <= 6
        RETURN p.full_name as person,
               tc1.legal_name as company1,
               tc1.dot_number as dot1,
               r1.end_date as left_date,
               tc2.legal_name as company2,
               tc2.dot_number as dot2,
               r2.start_date as joined_date
        ORDER BY p.full_name, r1.end_date
        """
        result = self.execute_query(query)
        return result
    
    def get_statistics(self) -> Dict:
        """Get person statistics"""
        query = """
        MATCH (p:Person)
        WITH count(p) as total_persons
        OPTIONAL MATCH (p:Person)<-[:HAS_EXECUTIVE]-(tc:TargetCompany)
        WITH total_persons, count(DISTINCT p) as executives
        OPTIONAL MATCH (p:Person)<-[:MANAGED_BY]-(c:Carrier)
        WITH total_persons, executives, count(DISTINCT p) as officers
        OPTIONAL MATCH (p:Person)<-[:HAS_EXECUTIVE]-(tc:TargetCompany)
        WITH total_persons, executives, officers, p, count(DISTINCT tc) as target_count
        WHERE target_count > 1
        WITH total_persons, executives, officers, count(DISTINCT p) as multi_target_persons
        OPTIONAL MATCH (p:Person)<-[:MANAGED_BY]-(c:Carrier)
        WITH total_persons, executives, officers, multi_target_persons, p, count(DISTINCT c) as carrier_count
        WHERE carrier_count > 1
        RETURN total_persons,
               executives as persons_as_executives,
               officers as persons_as_officers,
               multi_target_persons as persons_with_multiple_target_companies,
               count(DISTINCT p) as persons_with_multiple_carriers
        """
        result = self.execute_query(query)
        
        # Return defaults if empty result
        if not result:
            return {
                "total_persons": 0,
                "persons_as_executives": 0,
                "persons_as_officers": 0,
                "persons_with_multiple_target_companies": 0,
                "persons_with_multiple_carriers": 0
            }
        
        return result[0]