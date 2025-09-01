# Safety & Compliance Endpoints

Essential for risk assessment and identifying high-risk carriers.

## 1. Safety Summary
**GET** `/v1/company/{dotNumber}/safety-summary`

Comprehensive safety metrics for a carrier.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `sinceMonths` | integer | No | Months to look back (default: 24) |

### Response
Returns `CompanySafetySummary v1` object with safety metrics.

---

## 2. Inspections
**GET** `/v1/company/{dotNumber}/inspections`

Detailed inspection history for a carrier.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 100) |
| `sinceMonths` | integer | No | Months to look back (default: 24) |

### Response
Returns array of `Inspection v1` objects with:
- Inspection dates and locations
- Violation details
- Out-of-service orders
- Severity ratings

---

## 3. Out of Service Orders
**GET** `/v1/company/{dotNumber}/out-of-service-orders`

Returns all out-of-service orders for a carrier.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |
| `page` | integer | No | Page number (default: 1) |
| `perPage` | integer | No | Results per page (default: 10) |

### Response
Returns array of `OutOfServiceOrder v1` objects.

---

## 4. SMS Risk Factors (v2)
**GET** `/v2/company/{dotNumber}/risk-factors`

Safety Measurement System (SMS) ratings and risk factors.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `dotNumber` | integer | Yes | DOT number (in path) |

### Response
Returns array of risk factor objects.

---

## 5. Inspection Details
**GET** `/v1/inspection/details`

Detailed information about a specific inspection.

### Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `inspectionId` | integer | Yes | Inspection ID |

---

## Safety Metrics for Fraud Detection

### Critical Safety Indicators
1. **Out-of-Service Rate**: 
   - National average: ~5% driver, ~20% vehicle
   - > 2x average indicates poor maintenance/training
   
2. **Violation Patterns**:
   - Hours of Service (fatigue fraud)
   - Vehicle maintenance (deferred maintenance)
   - Driver qualification (unlicensed operators)
   
3. **Inspection Frequency**:
   - Low inspection rate may indicate route avoidance
   - Clustered inspections suggest targeted enforcement

### SMS BASIC Categories
1. **Unsafe Driving**: Speeding, reckless driving
2. **Hours-of-Service**: Fatigue-related violations
3. **Driver Fitness**: Medical, license issues
4. **Controlled Substances**: Drug/alcohol violations
5. **Vehicle Maintenance**: Mechanical defects
6. **Hazmat Compliance**: Dangerous goods violations
7. **Crash Indicator**: Crash history

### High-Risk Patterns
- Sudden improvement in scores (data manipulation)
- Violations across multiple BASICs
- Repeat violations after warnings
- Out-of-service orders for same issue
- Crashes with preventable causes

## Integration Recommendations

### Data Collection Priority
1. **Immediate**: Out-of-service orders (highest risk)
2. **Weekly**: Safety scores and new violations
3. **Monthly**: Full inspection history
4. **Quarterly**: SMS percentile rankings

### Graph Model Enhancement
```cypher
// Inspection Node
(:Inspection {
  inspection_id: string,
  inspection_date: date,
  level: integer, // 1-6
  violations_count: integer,
  oos_count: integer
})

// Violation Node
(:Violation {
  violation_code: string,
  description: string,
  severity: integer,
  basic_category: string
})

// Relationships
(:Carrier)-[:UNDERWENT]->(:Inspection)
(:Inspection)-[:FOUND]->(:Violation)
(:Carrier)-[:HAS_SMS_SCORE {
  basic: string,
  percentile: float,
  measure: float
}]->(:SMSCategory)
```

### Risk Scoring Query
```cypher
// Calculate carrier risk score
MATCH (c:Carrier)-[:UNDERWENT]->(i:Inspection)
WHERE i.inspection_date > date() - duration('P24M')
WITH c, 
     COUNT(i) as inspection_count,
     SUM(i.violations_count) as total_violations,
     SUM(i.oos_count) as total_oos
RETURN c.usdot,
       (total_violations * 1.0 / inspection_count) as violation_rate,
       (total_oos * 1.0 / inspection_count) as oos_rate,
       CASE 
         WHEN violation_rate > 3 THEN 'HIGH'
         WHEN violation_rate > 1 THEN 'MEDIUM'
         ELSE 'LOW'
       END as risk_level
```