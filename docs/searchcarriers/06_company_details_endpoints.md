# Company Details Endpoints

Comprehensive company information for entity resolution and relationship mapping.

## 1. Service Areas
**GET** `/v2/company/{dotNumber}/service-areas`

Geographic areas where the carrier operates.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 10) |

### Response
Returns array of service area strings (states/regions).

---

## 2. Physical Geo Location
**GET** `/v2/company/{dotNumber}/physical-geo-location`

Physical address coordinates and location details.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |

### Response
Returns `GeoLocationResource v2` object with:
- Latitude/longitude
- Full address
- Validation status

---

## 3. Truck, Trailer & Driver Count
**GET** `/v2/company/truck-driver-trailer-count`

Self-reported fleet and driver statistics.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | No | DOT number |
| `docketNumber` | string | No | Docket number |

### Response
Returns object with:
- `trucks`: Power unit count
- `trailers`: Trailer count
- `drivers`: Driver count
- `dot_number`: DOT identifier
- `name`: Company name

---

## 4. Equipment
**GET** `/v1/company/{dotNumber}/equipment`

Detailed equipment inventory including VINs.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `equipmentType` | string | No | Filter: truck, trailer, other |
| `vin` | string | No | Filter by specific VIN |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 100) |

### Response
Returns array of `Equipment v1` objects with:
- VIN numbers
- Equipment type
- Make/model/year
- Registration details

---

## Entity Resolution & Fraud Detection

### Address-Based Clustering
Identify related entities through:
1. **Shared Physical Addresses**:
   - Multiple carriers at same location
   - Virtual office addresses
   - Residential addresses for commercial operations

2. **Geographic Anomalies**:
   - Service areas don't match physical location
   - Interstate operation from residential address
   - Clustered carriers in small geographic area

### Fleet Size Indicators
1. **Mismatched Metrics**:
   - High revenue with minimal equipment
   - Driver count exceeds truck count significantly
   - Zero equipment but active operations

2. **Rapid Changes**:
   - Sudden fleet size increases
   - Equipment disappears after violations
   - Fleet transfers between related entities

### Equipment Tracking
1. **VIN Analysis**:
   - Equipment shared across carriers
   - VINs associated with accidents/violations
   - Equipment age vs. reported condition

2. **Chameleon Detection**:
   - Same VINs under new carrier
   - Equipment from revoked carrier
   - Pattern of equipment transfers

## Integration Strategy

### Data Collection Priority
1. **High**: Physical addresses for clustering analysis
2. **High**: Equipment VINs for relationship detection  
3. **Medium**: Fleet size for capacity validation
4. **Low**: Service areas for operational scope

### Graph Model Enhancement
```cypher
// Location Node
(:Location {
  address: string,
  city: string,
  state: string,
  zip: string,
  lat: float,
  lon: float,
  type: string // commercial, residential, virtual
})

// Equipment Node  
(:Equipment {
  vin: string,
  type: string,
  make: string,
  model: string,
  year: integer
})

// Service Area Node
(:ServiceArea {
  state: string,
  region: string
})

// Relationships
(:Carrier)-[:LOCATED_AT]->(:Location)
(:Carrier)-[:OWNS]->(:Equipment)
(:Carrier)-[:SERVICES]->(:ServiceArea)
(:Equipment)-[:TRANSFERRED_FROM]->(:Carrier)
```

### Clustering Queries
```cypher
// Find carriers at same address
MATCH (l:Location)<-[:LOCATED_AT]-(c:Carrier)
WITH l, COLLECT(c) as carriers
WHERE SIZE(carriers) > 1
RETURN l.address, carriers

// Track equipment transfers
MATCH (e:Equipment)-[:TRANSFERRED_FROM]->(c1:Carrier)
MATCH (e)<-[:OWNS]-(c2:Carrier)
WHERE c1 <> c2
RETURN e.vin, c1.name as from_carrier, c2.name as to_carrier
```