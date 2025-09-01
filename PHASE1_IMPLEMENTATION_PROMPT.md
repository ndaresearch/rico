# SearchCarriers Integration Phase 1: Insurance Data Foundation

## Project Context
You are implementing Phase 1 of the RICO fraud detection system's SearchCarriers API integration. RICO is a Neo4j graph database system that tracks trucking carriers contracting with JB Hunt to detect fraudulent patterns. The system currently has basic carrier and insurance provider nodes but lacks temporal insurance tracking and compliance monitoring capabilities.

## Your Mission
Implement comprehensive insurance data collection and fraud detection by integrating SearchCarriers API endpoints to track insurance histories, detect coverage gaps, identify "insurance shopping" patterns, and monitor compliance with federal regulations.

## Current System Architecture
- **Database**: Neo4j graph database
- **API**: FastAPI with Python
- **Existing Entities**: Carrier, InsuranceProvider, TargetCompany, Person
- **Existing Relationships**: CONTRACTS_WITH, INSURED_BY, MANAGED_BY, HAS_EXECUTIVE
- **Import Pattern**: Scripts in `/api/scripts/ingest/` using Pydantic models

## Phase 1 Implementation Tasks

### Task 1: Analyze SearchCarriers Documentation
1. Read all documentation in `/docs/searchcarriers/`, especially:
   - `03_insurance_endpoints.md` - Insurance data endpoints
   - `04_authority_endpoints.md` - Authority history endpoint
   - `07_implementation_guide.md` - Integration patterns

2. Identify the exact data fields needed:
   - Insurance provider details and history
   - Policy types (BMC-91 for property, BMC-32 for passenger)
   - Coverage amounts and dates (effective/expiration)
   - Cancellation and lapse information
   - Filing compliance status
   - Authority status changes that affect insurance requirements

### Task 2: Design Enhanced Data Model
Based on the existing patterns in `/api/models/` and `/api/repositories/`, create:

1. **New Entity Models** (follow existing Pydantic patterns):
   ```python
   # InsurancePolicy model
   - policy_id: str (unique identifier)
   - carrier_usdot: int
   - provider_name: str
   - provider_id: Optional[str]
   - policy_type: str (BMC-91, BMC-32, etc.)
   - coverage_amount: float
   - cargo_coverage: Optional[float]
   - effective_date: date
   - expiration_date: Optional[date]
   - cancellation_date: Optional[date]
   - cancellation_reason: Optional[str]
   - filing_status: str (ACTIVE, CANCELLED, LAPSED)
   - is_compliant: bool
   - created_at: datetime
   - updated_at: datetime
   - data_source: str
   ```

   ```python
   # InsuranceEvent model (for tracking changes)
   - event_id: str
   - carrier_usdot: int
   - event_type: str (NEW_POLICY, CANCELLATION, LAPSE, RENEWAL)
   - event_date: date
   - previous_provider: Optional[str]
   - new_provider: Optional[str]
   - coverage_change: Optional[float]
   - days_without_coverage: Optional[int]
   ```

2. **Enhanced Relationships**:
   - `(Carrier)-[:HAD_INSURANCE {from_date, to_date, status}]->(InsurancePolicy)`
   - `(InsurancePolicy)-[:PROVIDED_BY]->(InsuranceProvider)`
   - `(InsurancePolicy)-[:PRECEDED_BY {gap_days}]->(InsurancePolicy)`
   - `(Carrier)-[:INSURANCE_EVENT]->(InsuranceEvent)`

### Task 3: Implement SearchCarriers Client
Create `/api/services/searchcarriers_client.py` following the existing service patterns:

1. **Authentication**: Use environment variable for API key
2. **Rate Limiting**: Implement proper rate limiting per API documentation
3. **Error Handling**: Robust retry logic with exponential backoff
4. **Methods to implement**:
   ```python
   async def get_carrier_insurance_history(dot_number: int)
   async def get_authority_history(docket_number: str)
   async def check_insurance_compliance(dot_number: int)
   async def detect_coverage_gaps(insurance_history: List[Dict])
   ```

### Task 4: Create Import/Enrichment Script
Following patterns in `/api/scripts/ingest/`, create `searchcarriers_insurance_enrichment.py`:

1. **Batch Processing Logic**:
   - Start with highest-risk carriers (violations > 20 or crashes > 5)
   - Process in batches of 10 to respect rate limits
   - Track last_enriched timestamp to avoid redundant API calls

2. **Data Processing Steps**:
   ```python
   # For each carrier:
   1. Fetch current insurance from SearchCarriers
   2. Fetch historical insurance data
   3. Fetch authority history for compliance context
   4. Identify coverage gaps and provider changes
   5. Create InsurancePolicy nodes with temporal data
   6. Create relationships with dates and status
   7. Flag compliance violations
   ```

3. **Gap Detection Algorithm**:
   ```python
   def detect_insurance_gaps(policies: List[Dict]) -> List[Dict]:
       """
       Identify periods > 30 days without coverage
       Flag as federal violation per 49 CFR § 387.7
       """
   ```

4. **Provider Shopping Detection**:
   ```python
   def detect_insurance_shopping(carrier_usdot: int) -> Dict:
       """
       Flag carriers with 3+ providers in 12 months
       Calculate average time with each provider
       """
   ```

### Task 5: Create Fraud Detection Queries
Implement these Cypher queries in a new repository method:

1. **Insurance Gap Detection**:
   ```cypher
   MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
   WHERE r.gap_days > 30
   RETURN c.carrier_name, c.usdot, r.gap_days, ip.cancellation_reason
   ORDER BY r.gap_days DESC
   ```

2. **Insurance Shopping Pattern**:
   ```cypher
   MATCH (c:Carrier)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
   WITH c, COUNT(DISTINCT ip.provider_name) as provider_count
   WHERE provider_count >= 3
   AND duration.between(MIN(ip.effective_date), MAX(ip.expiration_date)).months <= 12
   RETURN c, provider_count
   ```

3. **Underinsured Operations**:
   ```cypher
   MATCH (c:Carrier)-[:HAS_INSURANCE]->(ip:InsurancePolicy)
   WHERE ip.coverage_amount < 750000 
   AND c.cargo_type = 'GENERAL_FREIGHT'
   RETURN c, ip.coverage_amount, 750000 - ip.coverage_amount as shortage
   ```

### Task 6: Testing and Validation
1. Create test cases following existing patterns in `/api/tests/`
2. Test idempotency of import scripts
3. Validate against known insurance violations in the JB Hunt carrier data
4. Ensure no duplicate relationships are created

## Implementation Priorities

### Day 1-2: Foundation
1. Read all SearchCarriers documentation thoroughly
2. Create InsurancePolicy and InsuranceEvent models
3. Set up SearchCarriers client with authentication

### Day 3-4: Data Collection
1. Implement insurance data fetch methods
2. Create enrichment script for batch processing
3. Test with 5 sample carriers

### Day 5: Integration
1. Create temporal relationships in Neo4j
2. Implement gap detection algorithms
3. Run full enrichment on high-risk carriers

### Day 6-7: Fraud Detection
1. Implement fraud detection queries
2. Create compliance monitoring dashboard
3. Generate report of findings

## Success Criteria
- ✅ All 67 JB Hunt carriers have insurance history data
- ✅ Identify at least 5 carriers with coverage gaps > 30 days
- ✅ Detect at least 3 carriers with insurance shopping patterns
- ✅ Flag all carriers with coverage below federal minimums
- ✅ Zero duplicate relationships in the graph
- ✅ All API calls handle rate limits gracefully

## Important Patterns to Follow

### From Existing Codebase
1. **Model Creation**: Use Pydantic with proper validation (see `/api/models/carrier.py`)
2. **Repository Pattern**: Follow BaseRepository pattern (see `/api/repositories/`)
3. **Relationship Creation**: Use MERGE to prevent duplicates
4. **Date Handling**: Convert to ISO format strings for Neo4j
5. **Import Scripts**: Include idempotency checks and progress logging

### SearchCarriers Specific
1. **API Authentication**: Bearer token in headers
2. **Pagination**: Handle `perPage` and `page` parameters
3. **Rate Limiting**: Respect limits (check documentation)
4. **Error Codes**: Handle 404 for non-existent carriers

## Environment Setup
```bash
# Required environment variables
SEARCHCARRIERS_API_KEY=your_api_key
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

## Files to Reference
- `/docs/searchcarriers/03_insurance_endpoints.md` - Insurance API details
- `/api/models/carrier.py` - Example model structure
- `/api/repositories/carrier_repository.py` - Repository pattern
- `/api/scripts/ingest/jb_hunt_carriers_import.py` - Import script pattern
- `/api/scripts/ingest/fix_insurance_relationships.py` - Relationship creation pattern

## Expected Deliverables
1. `InsurancePolicy` and `InsuranceEvent` Pydantic models
2. `InsurancePolicyRepository` with CRUD operations
3. `searchcarriers_client.py` service implementation
4. `searchcarriers_insurance_enrichment.py` import script
5. Enhanced `CarrierRepository` with fraud detection methods
6. Test suite for new functionality
7. Report showing detected insurance violations

## Notes
- The SearchCarriers API provides both current (`/v2`) and historical (`/v1`) endpoints
- BMC-91 filing is required for property carriers, BMC-32 for passenger carriers
- Federal minimum coverage is $750,000 for general freight, higher for hazmat
- A gap of 30+ days in coverage is a federal violation requiring immediate action
- "Insurance shopping" (3+ providers in 12 months) is a strong fraud indicator

Begin by examining the SearchCarriers documentation to understand the exact data structure and plan your implementation approach.