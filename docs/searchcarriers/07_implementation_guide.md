# SearchCarriers API Implementation Guide for RICO

## Priority Implementation Plan

### Phase 1: Core Insurance Data (Week 1)
**Objective**: Establish insurance tracking and detect coverage gaps

1. **Endpoints to Implement**:
   - `/v2/company/{dotNumber}/insurances` - Current insurance
   - `/v1/authority/{docketNumber}/history` - Authority history

2. **Data to Collect**:
   - Insurance provider name and changes
   - Coverage amounts and types
   - Policy effective/expiration dates
   - BMC filing types
   - Cancellation history

3. **Immediate Value**:
   - Identify underinsured carriers
   - Detect insurance lapses
   - Track provider shopping patterns

### Phase 2: Authority & Compliance (Week 2)
**Objective**: Detect chameleon carriers and authority manipulation

1. **Endpoints to Implement**:
   - `/v1/company/{dotNumber}/authorities` - All authorities
   - `/v1/company/{dotNumber}/safety-summary` - Safety metrics

2. **Data to Collect**:
   - Authority status and types
   - Historical status changes
   - Safety scores and percentiles
   - Out-of-service rates

3. **Immediate Value**:
   - Identify reincarnated bad actors
   - Detect authority shopping
   - Flag high-risk carriers

### Phase 3: Entity Resolution (Week 3)
**Objective**: Map relationships and detect shell companies

1. **Endpoints to Implement**:
   - `/v2/company/{dotNumber}/physical-geo-location` - Addresses
   - `/v1/company/{dotNumber}/equipment` - Equipment/VINs
   - `/v1/search/by-vin/{vin}` - VIN history

2. **Data to Collect**:
   - Physical addresses and coordinates
   - Equipment VINs and details
   - Fleet size metrics
   - Service areas

3. **Immediate Value**:
   - Cluster related entities
   - Track equipment transfers
   - Identify shell networks

### Phase 4: Deep Compliance (Week 4)
**Objective**: Build comprehensive risk profiles

1. **Endpoints to Implement**:
   - `/v1/company/{dotNumber}/inspections` - Full inspection history
   - `/v1/company/{dotNumber}/out-of-service-orders` - OOS orders
   - `/v2/company/{dotNumber}/risk-factors` - SMS ratings

2. **Data to Collect**:
   - Detailed violations
   - Inspection patterns
   - SMS BASIC scores
   - Crash history

## Python Implementation Script

```python
import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
from neo4j import GraphDatabase

class SearchCarriersEnrichment:
    def __init__(self):
        self.api_key = os.getenv('SEARCHCARRIERS_API_KEY')
        self.base_url = 'https://searchcarriers.com/api'
        self.headers = {'Authorization': f'Bearer {self.api_key}'}
        self.neo4j_driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
        )
    
    def enrich_carrier_insurance(self, dot_number: int) -> Dict:
        """Fetch and store insurance data for a carrier"""
        endpoint = f'/v2/company/{dot_number}/insurances'
        
        try:
            response = requests.get(
                f'{self.base_url}{endpoint}',
                headers=self.headers,
                params={'perPage': 100}
            )
            response.raise_for_status()
            
            insurance_data = response.json()
            
            # Process and store in Neo4j
            with self.neo4j_driver.session() as session:
                for insurance in insurance_data.get('data', []):
                    session.run("""
                        MATCH (c:Carrier {usdot: $dot_number})
                        MERGE (i:InsurancePolicy {
                            policy_id: $policy_id,
                            provider: $provider
                        })
                        SET i.coverage_amount = $coverage_amount,
                            i.policy_type = $policy_type,
                            i.effective_date = $effective_date,
                            i.expiration_date = $expiration_date,
                            i.status = $status
                        MERGE (c)-[r:INSURED_BY]->(i)
                        SET r.created_at = timestamp()
                    """, 
                    dot_number=dot_number,
                    policy_id=insurance.get('policy_id'),
                    provider=insurance.get('provider_name'),
                    coverage_amount=insurance.get('coverage_amount'),
                    policy_type=insurance.get('filing_type'),
                    effective_date=insurance.get('effective_date'),
                    expiration_date=insurance.get('expiration_date'),
                    status=insurance.get('status', 'active')
                    )
            
            return insurance_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching insurance for DOT {dot_number}: {e}")
            return {}
    
    def detect_insurance_fraud_patterns(self) -> List[Dict]:
        """Query Neo4j for insurance fraud patterns"""
        patterns = []
        
        with self.neo4j_driver.session() as session:
            # Pattern 1: Frequent insurance changes
            result = session.run("""
                MATCH (c:Carrier)-[r:INSURED_BY]->(i:InsurancePolicy)
                WITH c, COUNT(DISTINCT i.provider) as provider_count,
                     COLLECT(DISTINCT i.provider) as providers
                WHERE provider_count > 3
                RETURN c.usdot as dot_number, 
                       c.carrier_name as name,
                       provider_count,
                       providers
                ORDER BY provider_count DESC
            """)
            
            for record in result:
                patterns.append({
                    'type': 'insurance_shopping',
                    'dot_number': record['dot_number'],
                    'name': record['name'],
                    'provider_count': record['provider_count'],
                    'providers': record['providers']
                })
        
        return patterns
    
    def enrich_authority_history(self, docket_number: str) -> Dict:
        """Fetch authority history to detect chameleon carriers"""
        endpoint = f'/v1/authority/{docket_number}/history'
        
        try:
            response = requests.get(
                f'{self.base_url}{endpoint}',
                headers=self.headers,
                params={'perPage': 100}
            )
            response.raise_for_status()
            
            history_data = response.json()
            
            # Store authority events
            with self.neo4j_driver.session() as session:
                for event in history_data.get('data', []):
                    session.run("""
                        MERGE (a:Authority {docket_number: $docket_number})
                        CREATE (e:AuthorityEvent {
                            event_id: $event_id,
                            event_type: $event_type,
                            event_date: $event_date,
                            description: $description
                        })
                        CREATE (a)-[:STATUS_CHANGED]->(e)
                    """,
                    docket_number=docket_number,
                    event_id=event.get('id'),
                    event_type=event.get('authority_types'),
                    event_date=event.get('event_date'),
                    description=event.get('authority_description')
                    )
            
            return history_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching authority history for {docket_number}: {e}")
            return {}

    def batch_enrich_carriers(self, dot_numbers: List[int], 
                            delay_seconds: int = 1):
        """Batch enrich multiple carriers with rate limiting"""
        results = []
        
        for dot in dot_numbers:
            print(f"Enriching DOT {dot}...")
            
            # Collect all data
            insurance = self.enrich_carrier_insurance(dot)
            
            results.append({
                'dot_number': dot,
                'insurance': insurance
            })
            
            # Rate limiting
            time.sleep(delay_seconds)
        
        return results

# Usage
if __name__ == "__main__":
    enricher = SearchCarriersEnrichment()
    
    # Get carriers from RICO
    carriers_to_enrich = [777001, 777002, 777003]  # From JB Hunt list
    
    # Enrich with SearchCarriers data
    enriched_data = enricher.batch_enrich_carriers(carriers_to_enrich)
    
    # Detect fraud patterns
    fraud_patterns = enricher.detect_insurance_fraud_patterns()
    
    print(f"Found {len(fraud_patterns)} potential fraud patterns")
```

## ROI Analysis

### Immediate Value (Week 1)
- **Insurance Gaps**: Identify 10-15% of carriers with coverage issues
- **Compliance Risk**: Flag carriers operating without proper coverage
- **Cost Savings**: Prevent partnerships with uninsured carriers

### Short-term Value (Month 1)
- **Chameleon Detection**: Identify 5-10% reincarnated bad actors
- **Network Analysis**: Map carrier relationships and shell companies
- **Risk Scoring**: Automated risk assessment for all carriers

### Long-term Value (Quarter 1)
- **Predictive Analytics**: Predict carriers likely to fail
- **Fraud Prevention**: 20-30% reduction in fraud exposure
- **Operational Efficiency**: 50% reduction in manual vetting time

## Success Metrics
1. **Coverage**: % of carriers with complete insurance data
2. **Detection Rate**: Fraud patterns identified per week
3. **Accuracy**: False positive rate < 10%
4. **Timeliness**: Data freshness < 24 hours
5. **ROI**: Cost savings from prevented fraud