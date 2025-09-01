# Insurance Endpoints

Critical endpoints for gathering insurance data and detecting fraud patterns.

## 1. Get Company Insurances (v2)
**GET** `/v2/company/{dotNumber}/insurances`

Returns current and historical insurance information for a carrier.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 10) |

### Response
Returns array of `Insurance v2` objects with pagination.

### Key Data Points
- Insurance provider details
- Coverage amounts and types
- Policy effective dates
- Filing types (BMC-91, BMC-32)
- Cancellation/lapse information

---

## 2. Get Company Insurances (v1) 
**GET** `/v1/company/{dotNumber}/insurances`

Legacy endpoint for insurance information.

### Parameters
Same as v2 endpoint.

### Response
Returns array of `Insurance v1` objects with pagination.

---

## Insurance Data Value for Fraud Detection

### High Priority Fields
1. **Coverage Gaps**: Periods without active insurance
2. **Provider Changes**: Frequent switching may indicate shopping for lenient providers
3. **Coverage Amounts**: Underinsured operations relative to cargo value
4. **Filing Types**: 
   - BMC-91: Required for property carriers
   - BMC-32: Required for passenger carriers
5. **Cancellation History**: Pattern of cancelled policies

### Fraud Indicators
- Rapid insurance provider changes (< 6 months)
- Coverage amounts below regulatory minimums
- Gaps between policy periods
- Multiple cancellations for non-payment
- Insurance obtained just before authority activation

### Compliance Requirements
- **Minimum Coverage**:
  - General freight: $750,000
  - Hazmat: $1,000,000 - $5,000,000
  - Passengers (15+): $5,000,000
- **Filing Requirements**:
  - Must maintain continuous coverage
  - 30-day notice required for cancellation
  - Immediate suspension for lapse

## Integration Recommendations

### Data Collection Strategy
1. Pull current insurance status for all active carriers
2. Collect historical data to identify patterns
3. Monitor for insurance changes weekly
4. Flag carriers with:
   - Recent insurance changes
   - Coverage below requirements
   - History of lapses

### Graph Model Enhancements
```cypher
// New Insurance Policy Node
(:InsurancePolicy {
  policy_id: string,
  provider_name: string,
  coverage_amount: float,
  policy_type: string, // BMC-91, BMC-32
  effective_date: date,
  expiration_date: date,
  status: string // active, cancelled, lapsed
})

// Temporal Relationship
(:Carrier)-[:HAD_INSURANCE {
  from_date: date,
  to_date: date,
  cancellation_reason: string
}]->(:InsurancePolicy)
```