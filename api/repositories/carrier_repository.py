from typing import Dict, List, Optional
from datetime import datetime, timezone, date

from database import BaseRepository
from models.carrier import Carrier


class CarrierRepository(BaseRepository):
    """Repository for Carrier entity operations in Neo4j graph database.
    
    Handles CRUD operations for carriers and manages their relationships
    with target companies, insurance providers, and officers.
    """
    
    def create(self, carrier: Carrier) -> Dict:
        """Create a new carrier node in the graph database.
        
        Args:
            carrier: Carrier model with all required properties
            
        Returns:
            dict: Created carrier node data or None if creation fails
        """
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
        """Get a carrier by USDOT number.
        
        Args:
            usdot: The USDOT number to search for
            
        Returns:
            dict: Carrier data if found, None otherwise
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        RETURN c
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['c'] if result else None
    
    def get_all(self, skip: int = 0, limit: int = 100, filters: Dict = None) -> List[Dict]:
        """Get all carriers with pagination and optional filters.
        
        Args:
            skip: Number of records to skip (for pagination)
            limit: Maximum number of records to return
            filters: Optional dict with filter criteria:
                - jb_carrier: bool - Filter by JB Hunt carrier status
                - min_trucks: int - Minimum number of trucks
                - min_violations: int - Minimum number of violations
                - min_crashes: int - Minimum number of crashes
                - min_driver_oos_rate: float - Minimum driver OOS rate
                - insurance_provider: str - Filter by insurance provider name
                
        Returns:
            list: List of carrier dictionaries matching the criteria
        """
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
        """Update a carrier's properties.
        
        Args:
            usdot: The USDOT number of the carrier to update
            updates: Dictionary of properties to update
            
        Returns:
            dict: Updated carrier data if successful, None otherwise
        """
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
        """Delete a carrier and all its relationships.
        
        Uses DETACH DELETE to remove the carrier node and all connected edges.
        
        Args:
            usdot: The USDOT number of the carrier to delete
            
        Returns:
            bool: True if carrier was deleted, False if not found
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        DETACH DELETE c
        RETURN count(c) as deleted
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['deleted'] > 0 if result else False
    
    def exists(self, usdot: int) -> bool:
        """Check if a carrier exists by USDOT number.
        
        Args:
            usdot: The USDOT number to check
            
        Returns:
            bool: True if carrier exists, False otherwise
        """
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        RETURN count(c) > 0 as exists
        """
        result = self.execute_query(query, {"usdot": usdot})
        return result[0]['exists'] if result else False
    
    def get_statistics(self) -> Dict:
        """Get aggregate statistics for all carriers.
        
        Returns:
            dict: Statistics including total carriers, averages for trucks,
                  violations, crashes, and safety rates
        """
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
        MERGE (tc)-[r:CONTRACTS_WITH]->(c)
        ON CREATE SET 
            r.start_date = $start_date,
            r.end_date = $end_date,
            r.active = $active,
            r.created_at = $created_at
        ON MATCH SET 
            r.updated_at = $updated_at,
            r.active = $active
        RETURN r
        """
        
        now = datetime.now(timezone.utc).isoformat()
        params = {
            "usdot": usdot,
            "dot_number": dot_number,
            "start_date": contract_start,
            "end_date": contract_end,
            "active": active,
            "created_at": now,
            "updated_at": now
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def link_to_insurance_provider(self, usdot: int, provider_name: str, 
                                  amount: Optional[float] = None) -> bool:
        """Create or update INSURED_BY relationship to insurance provider"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (ip:InsuranceProvider {name: $provider_name})
        MERGE (c)-[r:INSURED_BY]->(ip)
        ON CREATE SET 
            r.amount = $amount,
            r.created_at = $created_at
        ON MATCH SET 
            r.amount = $amount,
            r.updated_at = $updated_at
        RETURN r
        """
        
        now = datetime.now(timezone.utc).isoformat()
        params = {
            "usdot": usdot,
            "provider_name": provider_name,
            "amount": amount,
            "created_at": now,
            "updated_at": now
        }
        
        result = self.execute_query(query, params)
        return bool(result)
    
    def link_to_officer(self, usdot: int, person_id: str) -> bool:
        """Create MANAGED_BY relationship to a person (officer)"""
        query = """
        MATCH (c:Carrier {usdot: $usdot})
        MATCH (p:Person {person_id: $person_id})
        MERGE (c)-[r:MANAGED_BY]->(p)
        ON CREATE SET 
            r.created_at = $created_at
        ON MATCH SET 
            r.updated_at = $updated_at
        RETURN r
        """
        
        now = datetime.now(timezone.utc).isoformat()
        params = {
            "usdot": usdot,
            "person_id": person_id,
            "created_at": now,
            "updated_at": now
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
    
    def detect_insurance_gaps(self, min_gap_days: int = 30) -> List[Dict]:
        """Detect carriers with insurance coverage gaps.
        
        Args:
            min_gap_days: Minimum gap size in days to report (default 30)
            
        Returns:
            list: Carriers with coverage gaps and details
        """
        query = """
        MATCH (c:Carrier)-[:HAD_INSURANCE]->(ip1:InsurancePolicy)
        OPTIONAL MATCH (ip1)<-[:PRECEDED_BY {gap_days: gap}]-(ip2:InsurancePolicy)
        WHERE gap >= $min_gap_days
        WITH c, ip1, ip2, gap
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            gap_days: gap,
            from_policy: ip1.policy_id,
            to_policy: ip2.policy_id,
            from_provider: ip1.provider_name,
            to_provider: ip2.provider_name,
            violation_count: c.violations,
            crash_count: c.crashes
        } as gap_info
        ORDER BY gap DESC
        """
        
        result = self.execute_query(query, {"min_gap_days": min_gap_days})
        return [record['gap_info'] for record in result]
    
    def detect_insurance_shopping_patterns(self, months: int = 12, min_providers: int = 3) -> List[Dict]:
        """Detect carriers with frequent insurance provider changes.
        
        Args:
            months: Time window in months
            min_providers: Minimum number of providers to flag as shopping
            
        Returns:
            list: Carriers showing insurance shopping behavior
        """
        query = """
        MATCH (c:Carrier)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        WHERE ip.effective_date >= date() - duration({months: $months})
        WITH c, COUNT(DISTINCT ip.provider_name) as provider_count,
             COLLECT(DISTINCT ip.provider_name) as providers,
             COLLECT(DISTINCT ip.effective_date) as dates
        WHERE provider_count >= $min_providers
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            provider_count: provider_count,
            providers: providers,
            policy_dates: dates,
            violations: c.violations,
            crashes: c.crashes,
            risk_score: toFloat(provider_count) / toFloat($months)
        } as shopping_info
        ORDER BY provider_count DESC
        """
        
        params = {
            "months": months,
            "min_providers": min_providers
        }
        
        result = self.execute_query(query, params)
        return [record['shopping_info'] for record in result]
    
    def find_underinsured_operations(self, cargo_type: str = "GENERAL_FREIGHT") -> List[Dict]:
        """Find carriers operating with insurance below federal minimums.
        
        Args:
            cargo_type: Type of cargo for determining minimum requirements
            
        Returns:
            list: Underinsured carriers with coverage details
        """
        # Federal minimums per 49 CFR § 387.7
        federal_minimums = {
            "GENERAL_FREIGHT": 750000.0,
            "HOUSEHOLD_GOODS": 750000.0,
            "HAZMAT": 5000000.0,
            "PASSENGERS_15_PLUS": 5000000.0,
            "PASSENGERS_UNDER_15": 1500000.0
        }
        
        min_coverage = federal_minimums.get(cargo_type, 750000.0)
        
        query = """
        MATCH (c:Carrier)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        WHERE ip.filing_status = 'ACTIVE'
          AND ip.coverage_amount < $min_coverage
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            current_coverage: ip.coverage_amount,
            required_minimum: $min_coverage,
            shortage: $min_coverage - ip.coverage_amount,
            provider: ip.provider_name,
            policy_id: ip.policy_id,
            violations: c.violations,
            crashes: c.crashes
        } as underinsured_info
        ORDER BY underinsured_info.shortage DESC
        """
        
        result = self.execute_query(query, {"min_coverage": min_coverage})
        return [record['underinsured_info'] for record in result]
    
    def get_insurance_fraud_risk_scores(self) -> List[Dict]:
        """Calculate comprehensive fraud risk scores for all carriers based on insurance patterns.
        
        Returns:
            list: Carriers with calculated risk scores and contributing factors
        """
        query = """
        MATCH (c:Carrier)
        OPTIONAL MATCH (c)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
        OPTIONAL MATCH (c)-[:INSURANCE_EVENT]->(ie:InsuranceEvent)
        WITH c,
             COUNT(DISTINCT ip) as policy_count,
             COUNT(DISTINCT ip.provider_name) as provider_count,
             COUNT(DISTINCT CASE WHEN ie.event_type = 'CANCELLATION' THEN ie END) as cancellations,
             COUNT(DISTINCT CASE WHEN ie.compliance_violation = true THEN ie END) as violations,
             MAX(CASE WHEN EXISTS((ip)<-[:PRECEDED_BY {gap_days: gap}]-()) THEN gap ELSE 0 END) as max_gap
        WITH c, 
             policy_count,
             provider_count,
             cancellations,
             violations,
             max_gap,
             // Calculate risk score (0-100)
             CASE
                WHEN policy_count = 0 THEN 100  // No insurance is highest risk
                ELSE (
                    (CASE WHEN provider_count > 3 THEN 25 ELSE 0 END) +  // Shopping
                    (CASE WHEN cancellations > 2 THEN 25 ELSE cancellations * 10 END) +  // Cancellations
                    (CASE WHEN violations > 0 THEN 25 ELSE 0 END) +  // Compliance violations
                    (CASE WHEN max_gap > 30 THEN 25 WHEN max_gap > 7 THEN 15 ELSE 0 END)  // Gaps
                )
             END as risk_score
        WHERE risk_score > 0
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            risk_score: risk_score,
            policy_count: policy_count,
            provider_count: provider_count,
            cancellations: cancellations,
            compliance_violations: violations,
            max_coverage_gap: max_gap,
            safety_violations: c.violations,
            crashes: c.crashes
        } as risk_info
        ORDER BY risk_score DESC
        """
        
        result = self.execute_query(query)
        return [record['risk_info'] for record in result]
    
    def find_chameleon_carrier_patterns(self) -> List[Dict]:
        """Detect potential chameleon carriers based on insurance and authority patterns.
        
        Returns:
            list: Potential chameleon carriers with suspicious patterns
        """
        query = """
        // Find carriers with similar officers and insurance patterns
        MATCH (c1:Carrier)-[:MANAGED_BY]->(p:Person)<-[:MANAGED_BY]-(c2:Carrier)
        WHERE c1.usdot <> c2.usdot
        OPTIONAL MATCH (c1)-[:HAD_INSURANCE]->(ip1:InsurancePolicy)
        OPTIONAL MATCH (c2)-[:HAD_INSURANCE]->(ip2:InsurancePolicy)
        WHERE ip1.provider_name = ip2.provider_name
        WITH c1, c2, p, COUNT(DISTINCT ip1.provider_name) as shared_providers
        WHERE shared_providers > 0
        RETURN {
            carrier1_usdot: c1.usdot,
            carrier1_name: c1.carrier_name,
            carrier2_usdot: c2.usdot,
            carrier2_name: c2.carrier_name,
            shared_officer: p.name,
            shared_insurance_providers: shared_providers,
            carrier1_violations: c1.violations,
            carrier2_violations: c2.violations
        } as chameleon_pattern
        ORDER BY shared_providers DESC
        """
        
        result = self.execute_query(query)
        return [record['chameleon_pattern'] for record in result]
    
    def get_carriers_without_insurance_on_date(self, check_date: date) -> List[Dict]:
        """Find carriers without active insurance on a specific date.
        
        Args:
            check_date: The date to check for insurance coverage
            
        Returns:
            list: Carriers without insurance on the specified date
        """
        query = """
        MATCH (c:Carrier)
        WHERE NOT EXISTS {
            MATCH (c)-[r:HAD_INSURANCE]->(:InsurancePolicy)
            WHERE date($check_date) >= date(r.from_date)
            AND (r.to_date IS NULL OR date($check_date) <= date(r.to_date))
        }
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            last_known_provider: c.insurance_provider,
            trucks: c.trucks,
            violations: c.violations,
            crashes: c.crashes
        } as uninsured_carrier
        ORDER BY c.trucks DESC
        """
        
        params = {"check_date": check_date.isoformat()}
        result = self.execute_query(query, params)
        return [record['uninsured_carrier'] for record in result]
    
    def get_coverage_timeline(self, carrier_usdot: int) -> List[Dict]:
        """Get complete insurance coverage timeline for a carrier.
        
        Args:
            carrier_usdot: The carrier's USDOT number
            
        Returns:
            list: Chronological list of insurance policies with temporal data
        """
        query = """
        MATCH (c:Carrier {usdot: $carrier_usdot})-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
        RETURN {
            policy_id: ip.policy_id,
            provider_name: ip.provider_name,
            from_date: r.from_date,
            to_date: r.to_date,
            status: r.status,
            duration_days: r.duration_days,
            coverage_amount: ip.coverage_amount,
            policy_type: ip.policy_type
        } as coverage_period
        ORDER BY r.from_date
        """
        
        params = {"carrier_usdot": carrier_usdot}
        result = self.execute_query(query, params)
        return [record['coverage_period'] for record in result]
    
    def find_overlapping_policies(self) -> List[Dict]:
        """Find carriers with overlapping insurance policies.
        
        Returns:
            list: Carriers with overlapping coverage periods
        """
        query = """
        MATCH (c:Carrier)-[r1:HAD_INSURANCE]->(ip1:InsurancePolicy)
        MATCH (c)-[r2:HAD_INSURANCE]->(ip2:InsurancePolicy)
        WHERE id(r1) < id(r2)
          AND r1.from_date < r2.from_date
          AND (r1.to_date IS NULL OR r1.to_date > r2.from_date)
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            policy1_id: ip1.policy_id,
            policy1_provider: ip1.provider_name,
            policy1_from: r1.from_date,
            policy1_to: r1.to_date,
            policy2_id: ip2.policy_id,
            policy2_provider: ip2.provider_name,
            policy2_from: r2.from_date,
            policy2_to: r2.to_date,
            overlap_days: CASE 
                WHEN r1.to_date IS NULL THEN 
                    duration.between(date(r2.from_date), date()).days
                ELSE 
                    duration.between(date(r2.from_date), date(r1.to_date)).days
            END
        } as overlap_info
        ORDER BY overlap_info.overlap_days DESC
        """
        
        result = self.execute_query(query)
        return [record['overlap_info'] for record in result]
    
    def calculate_total_days_without_coverage(self, carrier_usdot: int, 
                                             start_date: date, 
                                             end_date: date) -> int:
        """Calculate total days without insurance coverage in a date range.
        
        Args:
            carrier_usdot: The carrier's USDOT number
            start_date: Start of the period to check
            end_date: End of the period to check
            
        Returns:
            int: Total number of days without coverage
        """
        query = """
        MATCH (c:Carrier {usdot: $carrier_usdot})-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
        WHERE r.from_date <= $end_date 
          AND (r.to_date IS NULL OR r.to_date >= $start_date)
        WITH c, 
             COLLECT({
                 from: CASE 
                     WHEN r.from_date < $start_date THEN $start_date 
                     ELSE r.from_date 
                 END,
                 to: CASE 
                     WHEN r.to_date IS NULL OR r.to_date > $end_date THEN $end_date 
                     ELSE r.to_date 
                 END
             }) as coverage_periods
        
        // Calculate total covered days
        WITH c, coverage_periods,
             duration.between(date($start_date), date($end_date)).days + 1 as total_days
        UNWIND coverage_periods as period
        WITH c, total_days, 
             SUM(duration.between(date(period.from), date(period.to)).days + 1) as covered_days
        
        RETURN total_days - covered_days as days_without_coverage
        """
        
        params = {
            "carrier_usdot": carrier_usdot,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }
        
        result = self.execute_query(query, params)
        if result and result[0].get('days_without_coverage') is not None:
            return result[0]['days_without_coverage']
        return 0
    
    def find_carriers_with_coverage_gaps(self, gap_threshold_days: int = 30) -> List[Dict]:
        """Find carriers with significant gaps in insurance coverage.
        
        Args:
            gap_threshold_days: Minimum gap size to report (default 30 days)
            
        Returns:
            list: Carriers with coverage gaps exceeding threshold
        """
        query = """
        MATCH (c:Carrier)-[r1:HAD_INSURANCE]->(ip1:InsurancePolicy)
        MATCH (c)-[r2:HAD_INSURANCE]->(ip2:InsurancePolicy)
        WHERE r1.to_date IS NOT NULL 
          AND r2.from_date > r1.to_date
          AND id(r1) < id(r2)
        WITH c, r1, r2, ip1, ip2,
             duration.between(date(r1.to_date), date(r2.from_date)).days as gap_days
        WHERE gap_days >= $gap_threshold_days
        WITH c, 
             COLLECT({
                 from_policy: ip1.policy_id,
                 to_policy: ip2.policy_id,
                 gap_start: r1.to_date,
                 gap_end: r2.from_date,
                 gap_days: gap_days,
                 from_provider: ip1.provider_name,
                 to_provider: ip2.provider_name
             }) as gaps,
             MAX(gap_days) as max_gap,
             SUM(gap_days) as total_gap_days
        RETURN {
            carrier_usdot: c.usdot,
            carrier_name: c.carrier_name,
            gap_count: SIZE(gaps),
            max_gap_days: max_gap,
            total_gap_days: total_gap_days,
            gaps: gaps,
            violations: c.violations,
            crashes: c.crashes
        } as gap_info
        ORDER BY total_gap_days DESC
        """
        
        params = {"gap_threshold_days": gap_threshold_days}
        result = self.execute_query(query, params)
        return [record['gap_info'] for record in result]