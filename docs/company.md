# Company Module API Documentation

## Overview
The Company module provides comprehensive CRUD operations for managing trucking company entities in the RICO graph database. It includes fraud detection capabilities, particularly for identifying chameleon carriers.

## Base URL
```
http://localhost:8000
```

## Authentication
All endpoints require API key authentication via the `X-API-Key` header.

```bash
curl -H "X-API-Key: your-api-key" http://localhost:8000/companies/
```

## Company Entity Structure

### Core Fields
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| dot_number | integer | Yes | Unique DOT identifier |
| mc_number | string | No | Motor Carrier number |
| legal_name | string | Yes | Official company name |
| dba_name | array[string] | No | Doing Business As names |
| entity_type | string | No | CARRIER, BROKER, or CARRIER/BROKER |
| authority_status | string | No | ACTIVE, INACTIVE, or REVOKED |
| safety_rating | string | No | SATISFACTORY, CONDITIONAL, UNSATISFACTORY |

### Operational Metrics
| Field | Type | Description |
|-------|------|-------------|
| total_drivers | integer | Number of employed drivers |
| total_trucks | integer | Number of trucks operated |
| total_trailers | integer | Number of trailers |

### Risk Scores
| Field | Type | Range | Description |
|-------|------|-------|-------------|
| chameleon_risk_score | float | 0-1 | Likelihood of being a chameleon carrier |
| safety_risk_score | float | 0-1 | Safety compliance risk |
| financial_risk_score | float | 0-1 | Financial stability risk |

## Endpoints

### 1. Create Company
**POST** `/companies/`

Creates a new company in the database.

#### Request Body
```json
{
  "dot_number": 123456,
  "mc_number": "MC-123456",
  "legal_name": "Example Trucking Inc",
  "entity_type": "CARRIER",
  "authority_status": "ACTIVE",
  "total_drivers": 25,
  "total_trucks": 15,
  "created_date": "2023-01-01"
}
```

#### Response
- **201 Created**: Company successfully created
- **409 Conflict**: Company with this DOT number already exists
- **500 Internal Server Error**: Database error

#### Example
```bash
curl -X POST http://localhost:8000/companies/ \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "dot_number": 123456,
    "legal_name": "Example Trucking Inc",
    "entity_type": "CARRIER"
  }'
```

### 2. Get Company by DOT Number
**GET** `/companies/{dot_number}`

Retrieves a specific company by its DOT number.

#### Response
- **200 OK**: Company found
- **404 Not Found**: Company doesn't exist

#### Example
```bash
curl -H "X-API-Key: your-api-key" \
  http://localhost:8000/companies/123456
```

### 3. List Companies
**GET** `/companies/`

Lists all companies with pagination and filtering options.

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| skip | integer | 0 | Number of records to skip |
| limit | integer | 100 | Maximum records to return (max: 1000) |
| authority_status | string | - | Filter by authority status |
| safety_rating | string | - | Filter by safety rating |
| entity_type | string | - | Filter by entity type |
| min_trucks | integer | - | Minimum number of trucks |
| chameleon_risk_threshold | float | - | Minimum chameleon risk score |

#### Example
```bash
# Get active carriers with more than 20 trucks
curl -H "X-API-Key: your-api-key" \
  "http://localhost:8000/companies/?authority_status=ACTIVE&min_trucks=20"

# Get high-risk companies
curl -H "X-API-Key: your-api-key" \
  "http://localhost:8000/companies/?chameleon_risk_threshold=0.7"
```

### 4. Update Company
**PATCH** `/companies/{dot_number}`

Updates specific fields of an existing company.

#### Request Body
Only include fields you want to update:
```json
{
  "authority_status": "INACTIVE",
  "total_trucks": 30,
  "safety_rating": "CONDITIONAL"
}
```

#### Response
- **200 OK**: Company updated successfully
- **404 Not Found**: Company doesn't exist
- **400 Bad Request**: No valid updates provided

#### Example
```bash
curl -X PATCH http://localhost:8000/companies/123456 \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "authority_status": "INACTIVE",
    "chameleon_risk_score": 0.85
  }'
```

### 5. Delete Company
**DELETE** `/companies/{dot_number}`

Deletes a company and all its relationships.

#### Response
- **204 No Content**: Company deleted successfully
- **404 Not Found**: Company doesn't exist

#### Example
```bash
curl -X DELETE http://localhost:8000/companies/123456 \
  -H "X-API-Key: your-api-key"
```

### 6. Find Similar Companies
**GET** `/companies/{dot_number}/similar`

Finds companies with similar characteristics (potential chameleons).

#### Query Parameters
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| threshold | float | 0.7 | Similarity threshold (0-1) |

#### Response
Returns array of similar companies with similarity scores:
```json
[
  {
    "c2": {
      "dot_number": 789012,
      "legal_name": "Example Transport LLC",
      "chameleon_risk_score": 0.82
    },
    "similarity_score": 0.9
  }
]
```

#### Example
```bash
curl -H "X-API-Key: your-api-key" \
  "http://localhost:8000/companies/123456/similar?threshold=0.8"
```

### 7. Get Statistics
**GET** `/companies/statistics/summary`

Returns aggregate statistics about all companies.

#### Response
```json
{
  "total_companies": 1250,
  "avg_drivers": 23.5,
  "avg_trucks": 18.2,
  "avg_chameleon_risk": 0.35,
  "active_companies": 980,
  "high_risk_companies": 125
}
```

#### Example
```bash
curl -H "X-API-Key: your-api-key" \
  http://localhost:8000/companies/statistics/summary
```

### 8. Bulk Create Companies
**POST** `/companies/bulk`

Creates multiple companies in a single request.

#### Request Body
Array of company objects (max 1000):
```json
[
  {
    "dot_number": 111111,
    "legal_name": "Company One",
    "entity_type": "CARRIER"
  },
  {
    "dot_number": 222222,
    "legal_name": "Company Two",
    "entity_type": "BROKER"
  }
]
```

#### Response
- **201 Created**: Returns count of created companies
- **409 Conflict**: One or more companies already exist
- **400 Bad Request**: Invalid data or too many companies

#### Example
```bash
curl -X POST http://localhost:8000/companies/bulk \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '[
    {"dot_number": 111111, "legal_name": "Company One", "entity_type": "CARRIER"},
    {"dot_number": 222222, "legal_name": "Company Two", "entity_type": "BROKER"}
  ]'
```

## Data Import

### Generate Test Data
```bash
python scripts/generate_company_test_data.py
```
This creates:
- `companies.csv`: 100 companies with realistic data and suspicious patterns
- `patterns_report.txt`: Documentation of intentional fraud patterns

### Import CSV Data
```bash
python scripts/import_companies.py companies.csv http://localhost:8000 your-api-key
```

## Suspicious Patterns in Test Data

The test data generator creates several fraud patterns:

1. **Chameleon Carriers** (30% of data)
   - Groups of 2-4 companies with similar names
   - Sequential creation dates (6 months apart)
   - Shared EIN numbers
   - Pattern: First company INACTIVE/UNSATISFACTORY, latest ACTIVE

2. **Sequential DOT Numbers**
   - 5 companies with consecutive DOT numbers (900000-900004)
   - All created within days of each other
   - High chameleon risk scores (0.85)

3. **Location Clusters**
   - Multiple companies at the same address
   - Useful for detecting shell companies

4. **Risk Score Distribution**
   - Normal companies: 0.0-0.3 chameleon risk
   - Suspicious companies: 0.7-1.0 chameleon risk

## Testing

### Run Unit Tests
```bash
cd api
pytest tests/test_company_endpoints.py -v
```

### Test Coverage
- ✅ Create company
- ✅ Duplicate prevention
- ✅ Get by DOT number
- ✅ List with pagination
- ✅ Filter by multiple criteria
- ✅ Update fields
- ✅ Delete company
- ✅ Bulk operations
- ✅ API authentication
- ✅ Statistics endpoint

## Neo4j Queries

### Direct Database Queries
```cypher
// Count all companies
MATCH (c:Company) RETURN count(c);

// Find high-risk companies
MATCH (c:Company)
WHERE c.chameleon_risk_score > 0.7
RETURN c.dot_number, c.legal_name, c.chameleon_risk_score
ORDER BY c.chameleon_risk_score DESC;

// Find companies with similar names
MATCH (c1:Company), (c2:Company)
WHERE c1 <> c2 
AND toLower(c1.legal_name) CONTAINS toLower(substring(c2.legal_name, 0, 10))
RETURN c1.legal_name, c2.legal_name;
```

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure Neo4j is running: `docker ps`
   - Check Neo4j logs: `docker logs rico-neo4j`

2. **Authentication Failed**
   - Verify API key in `.env` file
   - Check Neo4j password matches docker-compose.yml

3. **Constraint Violations**
   - DOT numbers must be unique
   - Run cleanup: `MATCH (c:Company) WHERE c.dot_number = X DELETE c`

4. **Performance Issues**
   - Check indexes are created: `SHOW INDEXES`
   - Monitor query execution: `PROFILE <query>`