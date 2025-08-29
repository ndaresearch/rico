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
        """Get all companies associated with a person"""
        query = """
        MATCH (p:Person {person_id: $person_id})<-[r:HAS_OFFICER]-(c:Company)
        RETURN c, r.role as role, r.start_date as start_date, r.end_date as end_date
        ORDER BY r.start_date DESC
        """
        result = self.execute_query(query, {"person_id": person_id})
        
        # Flatten the response by merging company data with relationship data
        flattened = []
        for record in result:
            company_data = record.get('c', {})
            # Merge company properties with relationship properties
            flattened_record = {**company_data}
            flattened_record['role'] = record.get('role')
            flattened_record['start_date'] = record.get('start_date')
            flattened_record['end_date'] = record.get('end_date')
            flattened.append(flattened_record)
        
        return flattened
    
    def add_to_company(self, person_id: str, dot_number: int, role: str, 
                       start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """Create HAS_OFFICER relationship between Company and Person"""
        query = """
        MATCH (c:Company {dot_number: $dot_number})
        MATCH (p:Person {person_id: $person_id})
        CREATE (c)-[r:HAS_OFFICER {
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
        """Remove HAS_OFFICER relationship"""
        query = """
        MATCH (c:Company {dot_number: $dot_number})-[r:HAS_OFFICER]->(p:Person {person_id: $person_id})
        DELETE r
        RETURN count(r) as deleted
        """
        result = self.execute_query(query, {
            "person_id": person_id,
            "dot_number": dot_number
        })
        return result[0]['deleted'] > 0 if result else False
    
    def find_shared_officers(self, dot_number: int) -> List[Dict]:
        """Find companies that share officers with the given company"""
        query = """
        MATCH (c1:Company {dot_number: $dot_number})-[:HAS_OFFICER]->(p:Person)
        MATCH (c2:Company)-[:HAS_OFFICER]->(p)
        WHERE c1 <> c2
        RETURN DISTINCT c2 as company, 
               collect(DISTINCT p.full_name) as shared_officers,
               count(DISTINCT p) as officer_count
        ORDER BY officer_count DESC
        """
        result = self.execute_query(query, {"dot_number": dot_number})
        return result
    
    def find_officer_succession_patterns(self) -> List[Dict]:
        """Find suspicious officer succession patterns (same person, sequential companies)"""
        query = """
        MATCH (p:Person)<-[r1:HAS_OFFICER]-(c1:Company)
        MATCH (p)<-[r2:HAS_OFFICER]-(c2:Company)
        WHERE c1 <> c2
        AND r1.end_date IS NOT NULL
        AND r2.start_date IS NOT NULL
        AND duration.between(date(r1.end_date), date(r2.start_date)).months <= 6
        RETURN p.full_name as person,
               c1.legal_name as company1,
               c1.dot_number as dot1,
               r1.end_date as left_date,
               c2.legal_name as company2,
               c2.dot_number as dot2,
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
        MATCH (p:Person)<-[:HAS_OFFICER]-(c:Company)
        WITH total_persons, count(DISTINCT p) as persons_with_companies
        MATCH (p:Person)<-[:HAS_OFFICER]-(c:Company)
        WITH total_persons, persons_with_companies, p, count(DISTINCT c) as company_count
        WHERE company_count > 1
        RETURN total_persons,
               persons_with_companies,
               count(DISTINCT p) as persons_with_multiple_companies,
               max(company_count) as max_companies_per_person
        """
        result = self.execute_query(query)
        
        # Return defaults if empty result
        if not result:
            return {
                "total_persons": 0,
                "persons_with_companies": 0,
                "persons_with_multiple_companies": 0,
                "max_companies_per_person": 0
            }
        
        return result[0]