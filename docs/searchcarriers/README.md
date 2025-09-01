# SearchCarriers API Documentation

Organized documentation for the SearchCarriers API, structured for easy reference and implementation in the RICO fraud detection system.

## Documentation Structure

1. **[01_overview.md](01_overview.md)** - API basics, authentication, and general structure
2. **[02_search_endpoints.md](02_search_endpoints.md)** - Company search and lookup capabilities
3. **[03_insurance_endpoints.md](03_insurance_endpoints.md)** - Insurance data endpoints with fraud detection focus
4. **[04_authority_endpoints.md](04_authority_endpoints.md)** - Authority status and history for chameleon detection
5. **[05_safety_compliance_endpoints.md](05_safety_compliance_endpoints.md)** - Safety metrics, inspections, and violations
6. **[06_company_details_endpoints.md](06_company_details_endpoints.md)** - Detailed company information and equipment
7. **[07_implementation_guide.md](07_implementation_guide.md)** - Phased implementation plan with code examples

## Quick Reference

### High-Value Endpoints for Fraud Detection

| Priority | Endpoint | Purpose | Fraud Indicators |
|----------|----------|---------|------------------|
| **Critical** | `/v2/company/{dot}/insurances` | Insurance coverage | Gaps, frequent changes, underinsured |
| **Critical** | `/v1/authority/{docket}/history` | Authority changes | Reincarnation, status manipulation |
| **High** | `/v1/company/{dot}/safety-summary` | Safety metrics | Sudden improvements, high violations |
| **High** | `/v1/company/{dot}/equipment` | Equipment/VINs | Shared equipment, transfers |
| **High** | `/v2/company/{dot}/physical-geo-location` | Address clustering | Shell companies, virtual offices |
| **Medium** | `/v1/company/{dot}/inspections` | Inspection history | Pattern avoidance, repeat violations |
| **Medium** | `/v1/search/by-vin/{vin}` | VIN history | Equipment from failed carriers |

### Key Data Fields by Category

#### Insurance Data
- Provider name and history
- Coverage amounts (BMC-91, BMC-32)
- Policy dates (effective, expiration)
- Cancellation/lapse history
- Filing types and compliance

#### Authority Data
- Authority types (MC, FF, MX)
- Status (Active, Revoked, Suspended)
- Historical changes and dates
- Related authorities
- Compliance violations

#### Safety Data
- SMS BASIC scores (7 categories)
- Out-of-service rates
- Violation codes and severity
- Inspection frequency
- Crash history

#### Entity Data
- Physical addresses
- Equipment VINs
- Fleet size (trucks, trailers, drivers)
- Service areas
- Officer information

## Implementation Priorities

### Week 1: Foundation
- Set up API authentication
- Implement insurance endpoints
- Create InsurancePolicy nodes in Neo4j
- Build insurance gap detection

### Week 2: Authority Tracking
- Implement authority history endpoint
- Create Authority and AuthorityEvent nodes
- Build chameleon detection queries

### Week 3: Entity Resolution
- Implement location and equipment endpoints
- Create clustering algorithms
- Map carrier relationships

### Week 4: Risk Scoring
- Implement safety and inspection endpoints
- Build comprehensive risk scores
- Create automated alerts

## Fraud Detection Patterns

### 1. Insurance Fraud
- Coverage gaps > 30 days
- 3+ providers in 12 months
- Coverage below minimums
- Cancelled for non-payment

### 2. Chameleon Carriers
- New authority < 90 days after revocation
- Same address/equipment as failed carrier
- Rapid authority status changes
- Multiple dormant authorities

### 3. Safety Manipulation
- Sudden score improvements
- Inspection avoidance patterns
- Repeat violations after warnings
- High out-of-service rates

### 4. Shell Companies
- Multiple carriers at same address
- Shared equipment across entities
- Common officers
- Minimal reported assets

## API Integration Best Practices

1. **Rate Limiting**: Respect API limits (check current limits)
2. **Batch Processing**: Group requests efficiently
3. **Error Handling**: Implement retry logic with backoff
4. **Data Validation**: Verify data consistency
5. **Incremental Updates**: Track last update timestamps
6. **Caching**: Store static data locally

## Support & Resources

- API Token Generation: https://searchcarriers.com/settings/api-tokens
- Contact: garrett@searchcarriers.com
- Base URL: https://searchcarriers.com/api

## Next Steps

1. Review the [Implementation Guide](07_implementation_guide.md)
2. Set up API authentication
3. Start with Phase 1 (Insurance Data)
4. Build fraud detection queries
5. Monitor and refine patterns