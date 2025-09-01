# Authority & Operating Status Endpoints

Critical for identifying chameleon carriers and authority manipulation.

## 1. Get Authority History
**GET** `/v1/authority/{docketNumber}/history`

Retrieves the complete history for a given authority (MC, FF, or MX number).

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `docketNumber` | string | Yes | MC, FF, or MX number (in path) |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 100) |

### Response
Returns array of `AuthorityHistory v1` objects showing:
- Authority status changes over time
- Authority types (common, contract, broker, freight forwarder)
- Associated DOT numbers
- Activation/deactivation dates

---

## 2. Get Company Authorities
**GET** `/v1/company/{dotNumber}/authorities`

Returns all authorities associated with a specific carrier.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 10) |

### Response
Returns array of `AuthorityResource v1` objects.

---

## Authority Fraud Patterns

### Chameleon Carrier Indicators
1. **Reincarnation Pattern**:
   - Company shuts down after violations
   - New authority activated within 30 days
   - Same officers/addresses/equipment
   
2. **Authority Shopping**:
   - Multiple authorities under different names
   - Authorities activated/deactivated in sequence
   - Avoiding suspension by switching authorities

3. **Shell Company Networks**:
   - Multiple authorities at same address
   - Shared equipment across authorities
   - Common officers across entities

### High-Risk Authority Changes
- Authority activated < 6 months
- Previously revoked authority
- Multiple dormant authorities
- Rapid status changes (active/inactive cycling)

### Compliance Red Flags
- Operating beyond authority scope
- Broker authority without bond
- Common carrier without insurance
- Interstate operation without MC number

## Data Integration Strategy

### Priority Data Points
1. **Authority Status**: Active, Inactive, Revoked, Suspended
2. **Authority Types**: 
   - Common Carrier (general freight)
   - Contract Carrier (specific shippers)
   - Broker (arranges transportation)
   - Freight Forwarder (consolidates shipments)
3. **Historical Changes**: Track all status transitions
4. **Related Entities**: Other authorities at same location

### Graph Model Enhancement
```cypher
// Authority Node
(:Authority {
  docket_number: string,
  authority_type: string,
  status: string,
  issue_date: date,
  latest_change: date
})

// Authority History
(:AuthorityEvent {
  event_id: string,
  event_type: string, // activated, suspended, revoked
  event_date: date,
  reason: string
})

// Relationships
(:Carrier)-[:HAS_AUTHORITY]->(:Authority)
(:Authority)-[:STATUS_CHANGED]->(:AuthorityEvent)
(:Authority)-[:PREDECESSOR_OF]->(:Authority) // Chameleon detection
```

### Detection Queries
```cypher
// Find potential chameleon carriers
MATCH (a1:Authority)-[:PREDECESSOR_OF]->(a2:Authority)
WHERE a1.status = 'Revoked' 
  AND a2.issue_date < date(a1.latest_change) + duration('P90D')
RETURN a1, a2

// Find authority shopping patterns
MATCH (c:Carrier)-[:HAS_AUTHORITY]->(a:Authority)
WITH c, COUNT(a) as auth_count
WHERE auth_count > 2
RETURN c, auth_count
```