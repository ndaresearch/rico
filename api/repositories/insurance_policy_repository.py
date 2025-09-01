from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, date

from database import BaseRepository
from models.insurance_policy import InsurancePolicy
from models.insurance_event import InsuranceEvent


class InsurancePolicyRepository(BaseRepository):
    """Repository for InsurancePolicy entity operations in Neo4j graph database.
    
    Handles CRUD operations for insurance policies and manages temporal relationships
    with carriers, providers, and policy transitions for fraud detection.
    """
    
    def create(self, policy: InsurancePolicy) -> Dict:
        """Create a new insurance policy node in the graph database.
        
        Args:
            policy: InsurancePolicy model with all required properties
            
        Returns:
            dict: Created policy node data or None if creation fails
        """
        query = """
        CREATE (ip:InsurancePolicy {
            policy_id: $policy_id,
            carrier_usdot: $carrier_usdot,
            provider_name: $provider_name,
            provider_id: $provider_id,
            policy_type: $policy_type,
            policy_number: $policy_number,
            coverage_amount: $coverage_amount,
            cargo_coverage: $cargo_coverage,
            effective_date: $effective_date,
            expiration_date: $expiration_date,
            cancellation_date: $cancellation_date,
            cancellation_reason: $cancellation_reason,
            filing_status: $filing_status,
            is_compliant: $is_compliant,
            meets_federal_minimum: $meets_federal_minimum,
            required_minimum: $required_minimum,
            created_at: $created_at,
            updated_at: $updated_at,
            data_source: $data_source,
            searchcarriers_record_id: $searchcarriers_record_id
        })
        RETURN ip
        """
        
        params = policy.model_dump()
        # Convert dates to strings for Neo4j
        for date_field in ['effective_date', 'expiration_date', 'cancellation_date']:
            if params.get(date_field):
                params[date_field] = params[date_field].isoformat()
        
        # Convert datetimes to strings
        if params.get('created_at'):
            params['created_at'] = params['created_at'].isoformat()
        else:
            params['created_at'] = datetime.now(timezone.utc).isoformat()
        
        if params.get('updated_at'):
            params['updated_at'] = params['updated_at'].isoformat()
        
        result = self.execute_query(query, params)
        return result[0]['ip'] if result else None
    
    def get_by_id(self, policy_id: str) -> Optional[Dict]:
        """Get an insurance policy by its ID.
        
        Args:
            policy_id: The unique policy identifier
            
        Returns:
            dict: Policy data if found, None otherwise
        """
        query = """
        MATCH (ip:InsurancePolicy {policy_id: $policy_id})
        RETURN ip
        """
        result = self.execute_query(query, {"policy_id": policy_id})
        return result[0]['ip'] if result else None
    
    def get_by_carrier(self, carrier_usdot: int, 
                      active_only: bool = False,
                      include_expired: bool = True) -> List[Dict]:
        """Get all insurance policies for a specific carrier.
        
        Args:
            carrier_usdot: The USDOT number of the carrier
            active_only: If True, only return currently active policies
            include_expired: If False, exclude expired policies
            
        Returns:
            list: List of policy dictionaries
        """
        where_clauses = ["ip.carrier_usdot = $carrier_usdot"]
        
        if active_only:
            where_clauses.append("ip.filing_status = 'ACTIVE'")
        
        if not include_expired:
            where_clauses.append("(ip.expiration_date IS NULL OR ip.expiration_date >= $today)")
        
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
        MATCH (ip:InsurancePolicy)
        WHERE {where_clause}
        RETURN ip
        ORDER BY ip.effective_date DESC
        """
        
        params = {"carrier_usdot": carrier_usdot}
        if not include_expired:
            params["today"] = date.today().isoformat()
        
        result = self.execute_query(query, params)
        return [record['ip'] for record in result]
    
    def create_carrier_relationship(self, policy_id: str, carrier_usdot: int,
                                  from_date: date, to_date: Optional[date] = None) -> bool:
        """Create HAD_INSURANCE relationship between carrier and policy with temporal data.
        
        Args:
            policy_id: The policy identifier
            carrier_usdot: The carrier's USDOT number
            from_date: Start date of the insurance coverage
            to_date: End date of the insurance coverage (if ended)
            
        Returns:
            bool: True if relationship created, False otherwise
        """
        # Calculate status
        status = "ACTIVE"
        if to_date:
            if to_date < date.today():
                status = "EXPIRED"
        
        # Calculate duration days
        duration_days = -1  # Active policy
        if to_date:
            duration_days = (to_date - from_date).days
        
        query = """
        MATCH (c:Carrier {usdot: $carrier_usdot})
        MATCH (ip:InsurancePolicy {policy_id: $policy_id})
        MERGE (c)-[r:HAD_INSURANCE]->(ip)
        ON CREATE SET 
            r.from_date = $from_date,
            r.to_date = $to_date,
            r.status = $status,
            r.duration_days = $duration_days,
            r.created_at = $created_at
        ON MATCH SET 
            r.to_date = $to_date,
            r.status = $status,
            r.duration_days = $duration_days,
            r.updated_at = $updated_at
        RETURN r
        """
        
        now = datetime.now(timezone.utc).isoformat()
        params = {
            "carrier_usdot": carrier_usdot,
            "policy_id": policy_id,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat() if to_date else None,
            "status": status,
            "duration_days": duration_days,
            "created_at": now,
            "updated_at": now
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def create_provider_relationship(self, policy_id: str, provider_name: str) -> bool:
        """Create PROVIDED_BY relationship between policy and insurance provider.
        
        Args:
            policy_id: The policy identifier
            provider_name: Name of the insurance provider
            
        Returns:
            bool: True if relationship created, False otherwise
        """
        query = """
        MATCH (ip:InsurancePolicy {policy_id: $policy_id})
        MATCH (prov:InsuranceProvider {name: $provider_name})
        MERGE (ip)-[r:PROVIDED_BY]->(prov)
        ON CREATE SET r.created_at = $created_at
        RETURN r
        """
        
        params = {
            "policy_id": policy_id,
            "provider_name": provider_name,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def link_policy_succession(self, previous_policy_id: str, next_policy_id: str,
                              gap_days: int = 0) -> bool:
        """Create PRECEDED_BY relationship between consecutive policies.
        
        Args:
            previous_policy_id: ID of the earlier policy
            next_policy_id: ID of the subsequent policy
            gap_days: Number of days between policies
            
        Returns:
            bool: True if relationship created, False otherwise
        """
        query = """
        MATCH (prev:InsurancePolicy {policy_id: $previous_policy_id})
        MATCH (next:InsurancePolicy {policy_id: $next_policy_id})
        MERGE (next)-[r:PRECEDED_BY]->(prev)
        ON CREATE SET 
            r.gap_days = $gap_days,
            r.created_at = $created_at
        RETURN r
        """
        
        params = {
            "previous_policy_id": previous_policy_id,
            "next_policy_id": next_policy_id,
            "gap_days": gap_days,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def detect_coverage_gaps(self, carrier_usdot: int, 
                            gap_threshold_days: int = 30) -> List[Dict]:
        """Detect gaps in insurance coverage for a carrier.
        
        Args:
            carrier_usdot: The carrier's USDOT number
            gap_threshold_days: Minimum gap size to report (default 30 days)
            
        Returns:
            list: List of gap information dictionaries
        """
        query = """
        MATCH (c:Carrier {usdot: $carrier_usdot})-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        WITH ip
        ORDER BY ip.effective_date
        WITH collect(ip) as policies
        UNWIND range(0, size(policies)-2) as i
        WITH policies[i] as p1, policies[i+1] as p2
        WITH p1, p2,
             CASE 
                WHEN p1.cancellation_date IS NOT NULL 
                THEN duration.between(date(p1.cancellation_date), date(p2.effective_date)).days
                WHEN p1.expiration_date IS NOT NULL 
                THEN duration.between(date(p1.expiration_date), date(p2.effective_date)).days
                ELSE 0
             END as gap_days
        WHERE gap_days >= $gap_threshold_days
        RETURN {
            from_policy: p1.policy_id,
            to_policy: p2.policy_id,
            gap_start: COALESCE(p1.cancellation_date, p1.expiration_date),
            gap_end: p2.effective_date,
            gap_days: gap_days,
            from_provider: p1.provider_name,
            to_provider: p2.provider_name
        } as gap
        ORDER BY gap_days DESC
        """
        
        params = {
            "carrier_usdot": carrier_usdot,
            "gap_threshold_days": gap_threshold_days
        }
        
        result = self.execute_query(query, params)
        return [record['gap'] for record in result]
    
    def detect_insurance_shopping(self, months_window: int = 12,
                                 min_provider_count: int = 3) -> List[Dict]:
        """Detect carriers with frequent insurance provider changes.
        
        Args:
            months_window: Time window in months to check
            min_provider_count: Minimum number of different providers to flag
            
        Returns:
            list: List of carriers with insurance shopping patterns
        """
        query = """
        MATCH (c:Carrier)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        WHERE ip.effective_date >= date() - duration({months: $months_window})
        WITH c, COUNT(DISTINCT ip.provider_name) as provider_count,
             COLLECT(DISTINCT ip.provider_name) as providers,
             COLLECT(ip.effective_date) as policy_dates
        WHERE provider_count >= $min_provider_count
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            provider_count: provider_count,
            providers: providers,
            policy_dates: policy_dates,
            risk_score: toFloat(provider_count) / $months_window
        } as shopping_pattern
        ORDER BY provider_count DESC
        """
        
        params = {
            "months_window": months_window,
            "min_provider_count": min_provider_count
        }
        
        result = self.execute_query(query, params)
        return [record['shopping_pattern'] for record in result]
    
    def find_underinsured_carriers(self, cargo_type: str = "GENERAL_FREIGHT") -> List[Dict]:
        """Find carriers with insurance coverage below federal minimums.
        
        Args:
            cargo_type: Type of cargo to check requirements for
            
        Returns:
            list: List of underinsured carriers
        """
        # Federal minimums per 49 CFR § 387.7
        federal_minimums = {
            "GENERAL_FREIGHT": 750000.0,
            "HOUSEHOLD_GOODS": 750000.0,
            "HAZMAT": 5000000.0,
            "PASSENGERS_15_PLUS": 5000000.0,
            "PASSENGERS_UNDER_15": 1500000.0,
            "OIL": 1000000.0
        }
        
        required_minimum = federal_minimums.get(cargo_type, 750000.0)
        
        query = """
        MATCH (c:Carrier)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        WHERE ip.filing_status = 'ACTIVE' 
          AND ip.coverage_amount < $required_minimum
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            policy_id: ip.policy_id,
            provider: ip.provider_name,
            coverage_amount: ip.coverage_amount,
            shortage: $required_minimum - ip.coverage_amount,
            required_minimum: $required_minimum
        } as violation
        ORDER BY violation.shortage DESC
        """
        
        params = {"required_minimum": required_minimum}
        
        result = self.execute_query(query, params)
        return [record['violation'] for record in result]
    
    def create_insurance_event(self, event: InsuranceEvent) -> Dict:
        """Create an insurance event node and link it to the carrier.
        
        Args:
            event: InsuranceEvent model
            
        Returns:
            dict: Created event node data
        """
        query = """
        MATCH (c:Carrier {usdot: $carrier_usdot})
        CREATE (ie:InsuranceEvent {
            event_id: $event_id,
            carrier_usdot: $carrier_usdot,
            event_type: $event_type,
            event_date: $event_date,
            previous_provider: $previous_provider,
            new_provider: $new_provider,
            previous_coverage: $previous_coverage,
            new_coverage: $new_coverage,
            coverage_change: $coverage_change,
            days_without_coverage: $days_without_coverage,
            previous_policy_id: $previous_policy_id,
            new_policy_id: $new_policy_id,
            compliance_violation: $compliance_violation,
            violation_reason: $violation_reason,
            is_suspicious: $is_suspicious,
            fraud_indicators: $fraud_indicators,
            reason: $reason,
            notes: $notes,
            created_at: $created_at,
            data_source: $data_source
        })
        CREATE (c)-[:INSURANCE_EVENT]->(ie)
        RETURN ie
        """
        
        params = event.model_dump()
        # Convert date to string
        if params.get('event_date'):
            params['event_date'] = params['event_date'].isoformat()
        if params.get('created_at'):
            params['created_at'] = params['created_at'].isoformat()
        else:
            params['created_at'] = datetime.now(timezone.utc).isoformat()
        
        # Convert list to string array for Neo4j
        if params.get('fraud_indicators'):
            params['fraud_indicators'] = params['fraud_indicators']
        
        result = self.execute_query(query, params)
        return result[0]['ie'] if result else None
    
    def get_carrier_insurance_timeline(self, carrier_usdot: int) -> List[Dict]:
        """Get complete insurance timeline for a carrier including policies and events.
        
        Args:
            carrier_usdot: The carrier's USDOT number
            
        Returns:
            list: Chronologically ordered list of insurance policies and events
        """
        query = """
        MATCH (c:Carrier {usdot: $carrier_usdot})
        OPTIONAL MATCH (c)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        OPTIONAL MATCH (c)-[:INSURANCE_EVENT]->(ie:InsuranceEvent)
        WITH 
            COLLECT(DISTINCT {
                type: 'policy',
                date: ip.effective_date,
                data: ip
            }) + 
            COLLECT(DISTINCT {
                type: 'event',
                date: ie.event_date,
                data: ie
            }) as timeline
        UNWIND timeline as item
        WITH item
        WHERE item.data IS NOT NULL
        RETURN item
        ORDER BY item.date
        """
        
        result = self.execute_query(query, {"carrier_usdot": carrier_usdot})
        return [record['item'] for record in result]
    
    def bulk_create(self, policies: List[InsurancePolicy]) -> Dict:
        """Bulk create insurance policies.
        
        Args:
            policies: List of InsurancePolicy models
            
        Returns:
            dict: Summary of created policies
        """
        query = """
        UNWIND $policies as policy
        CREATE (ip:InsurancePolicy)
        SET ip = policy
        RETURN count(ip) as created
        """
        
        # Convert all policies to dict with proper date formatting
        policies_data = []
        for policy in policies:
            data = policy.model_dump()
            for date_field in ['effective_date', 'expiration_date', 'cancellation_date']:
                if data.get(date_field):
                    data[date_field] = data[date_field].isoformat()
            if data.get('created_at'):
                data['created_at'] = data['created_at'].isoformat()
            else:
                data['created_at'] = datetime.now(timezone.utc).isoformat()
            if data.get('updated_at'):
                data['updated_at'] = data['updated_at'].isoformat()
            policies_data.append(data)
        
        result = self.execute_query(query, {"policies": policies_data})
        return {"created": result[0]['created']} if result else {"created": 0}