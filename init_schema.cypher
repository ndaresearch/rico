// ============================================================================
// RICO Graph Database Schema Initialization
// ============================================================================
// 
// Purpose: Initialize the Neo4j database schema for the RICO fraud detection system
// Description: Creates constraints, indexes, and documents relationships for tracking
//              fraudulent patterns in the trucking industry
// 
// Entity Types:
//   - TargetCompany: Large enterprises (e.g., JB Hunt) that contract carriers
//   - Carrier: Individual trucking companies with safety/violation data
//   - Person: Individuals who manage carriers or serve as executives
//   - InsurancePolicy: Individual insurance policies with temporal data
//   - InsuranceProvider: Insurance companies that provide policies
//   - InsuranceEvent: Insurance state changes (new, cancelled, lapsed, etc.)
//
// Last Updated: 2025-09-02
// ============================================================================

// ============================================================================
// CLEANUP SECTION (Commented out - use carefully in development)
// ============================================================================
// WARNING: These commands will delete all data! Only use for complete reset
// MATCH (n) DETACH DELETE n;
// DROP CONSTRAINT carrier_usdot_unique IF EXISTS;
// DROP CONSTRAINT target_company_dot_unique IF EXISTS;
// DROP CONSTRAINT person_id_unique IF EXISTS;
// DROP CONSTRAINT insurance_policy_id_unique IF EXISTS;
// DROP CONSTRAINT insurance_provider_name_unique IF EXISTS;
// DROP CONSTRAINT insurance_event_id_unique IF EXISTS;

// ============================================================================
// CURRENT IMPLEMENTATION - Active Entities and Relationships
// ============================================================================

// ----------------------------------------------------------------------------
// CARRIER CONSTRAINTS AND INDEXES
// ----------------------------------------------------------------------------
// Carrier represents individual trucking companies
// Required properties: usdot (unique identifier), carrier_name, primary_officer
// Optional properties: insurance_provider, insurance_amount, trucks, violations, crashes

CREATE CONSTRAINT carrier_usdot_unique IF NOT EXISTS
FOR (c:Carrier) REQUIRE c.usdot IS UNIQUE;

CREATE INDEX carrier_name_index IF NOT EXISTS
FOR (c:Carrier) ON (c.carrier_name);

CREATE INDEX carrier_violations_index IF NOT EXISTS
FOR (c:Carrier) ON (c.violations);

CREATE INDEX carrier_crashes_index IF NOT EXISTS
FOR (c:Carrier) ON (c.crashes);

CREATE INDEX carrier_driver_oos_index IF NOT EXISTS
FOR (c:Carrier) ON (c.driver_oos_rate);

CREATE INDEX carrier_vehicle_oos_index IF NOT EXISTS
FOR (c:Carrier) ON (c.vehicle_oos_rate);

CREATE INDEX carrier_jb_carrier_index IF NOT EXISTS
FOR (c:Carrier) ON (c.jb_carrier);

// ----------------------------------------------------------------------------
// TARGET COMPANY CONSTRAINTS AND INDEXES
// ----------------------------------------------------------------------------
// TargetCompany represents large enterprises that contract with carriers
// Required properties: dot_number (unique), legal_name
// Optional properties: mc_number, entity_type, authority_status, safety_rating

CREATE CONSTRAINT target_company_dot_unique IF NOT EXISTS
FOR (tc:TargetCompany) REQUIRE tc.dot_number IS UNIQUE;

CREATE INDEX target_company_name_index IF NOT EXISTS
FOR (tc:TargetCompany) ON (tc.legal_name);

CREATE INDEX target_company_mc_index IF NOT EXISTS
FOR (tc:TargetCompany) ON (tc.mc_number);

CREATE INDEX target_company_status_index IF NOT EXISTS
FOR (tc:TargetCompany) ON (tc.authority_status);

// ----------------------------------------------------------------------------
// PERSON CONSTRAINTS AND INDEXES
// ----------------------------------------------------------------------------
// Person represents individuals (carrier officers, company executives)
// Required properties: person_id (unique), full_name
// Optional properties: first_name, last_name, email[], phone[]

CREATE CONSTRAINT person_id_unique IF NOT EXISTS
FOR (p:Person) REQUIRE p.person_id IS UNIQUE;

CREATE INDEX person_name_index IF NOT EXISTS
FOR (p:Person) ON (p.full_name);

CREATE INDEX person_first_name_index IF NOT EXISTS
FOR (p:Person) ON (p.first_name);

CREATE INDEX person_last_name_index IF NOT EXISTS
FOR (p:Person) ON (p.last_name);

// ----------------------------------------------------------------------------
// INSURANCE POLICY CONSTRAINTS AND INDEXES
// ----------------------------------------------------------------------------
// InsurancePolicy represents individual insurance policies with temporal data
// Required properties: policy_id (unique), carrier_usdot, provider_name, coverage_amount
// Optional properties: effective_date, expiration_date, cancellation_date, filing_status

CREATE CONSTRAINT insurance_policy_id_unique IF NOT EXISTS
FOR (ip:InsurancePolicy) REQUIRE ip.policy_id IS UNIQUE;

CREATE INDEX insurance_policy_carrier_index IF NOT EXISTS
FOR (ip:InsurancePolicy) ON (ip.carrier_usdot);

CREATE INDEX insurance_policy_provider_index IF NOT EXISTS
FOR (ip:InsurancePolicy) ON (ip.provider_name);

CREATE INDEX insurance_policy_effective_date_index IF NOT EXISTS
FOR (ip:InsurancePolicy) ON (ip.effective_date);

CREATE INDEX insurance_policy_expiration_date_index IF NOT EXISTS
FOR (ip:InsurancePolicy) ON (ip.expiration_date);

CREATE INDEX insurance_policy_filing_status_index IF NOT EXISTS
FOR (ip:InsurancePolicy) ON (ip.filing_status);

CREATE INDEX insurance_policy_coverage_index IF NOT EXISTS
FOR (ip:InsurancePolicy) ON (ip.coverage_amount);

// ----------------------------------------------------------------------------
// INSURANCE PROVIDER CONSTRAINTS AND INDEXES
// ----------------------------------------------------------------------------
// InsuranceProvider represents insurance companies
// Required properties: name (unique)
// Optional properties: provider_id, contact_phone, contact_email, website

CREATE CONSTRAINT insurance_provider_name_unique IF NOT EXISTS
FOR (prov:InsuranceProvider) REQUIRE prov.name IS UNIQUE;

CREATE INDEX insurance_provider_id_index IF NOT EXISTS
FOR (prov:InsuranceProvider) ON (prov.provider_id);

// ----------------------------------------------------------------------------
// INSURANCE EVENT CONSTRAINTS AND INDEXES
// ----------------------------------------------------------------------------
// InsuranceEvent tracks insurance state changes for temporal analysis
// Required properties: event_id (unique), carrier_usdot, event_type, event_date
// Optional properties: previous_provider, new_provider, coverage changes, compliance info

CREATE CONSTRAINT insurance_event_id_unique IF NOT EXISTS
FOR (ie:InsuranceEvent) REQUIRE ie.event_id IS UNIQUE;

CREATE INDEX insurance_event_carrier_index IF NOT EXISTS
FOR (ie:InsuranceEvent) ON (ie.carrier_usdot);

CREATE INDEX insurance_event_date_index IF NOT EXISTS
FOR (ie:InsuranceEvent) ON (ie.event_date);

CREATE INDEX insurance_event_type_index IF NOT EXISTS
FOR (ie:InsuranceEvent) ON (ie.event_type);

CREATE INDEX insurance_event_suspicious_index IF NOT EXISTS
FOR (ie:InsuranceEvent) ON (ie.is_suspicious);

// ----------------------------------------------------------------------------
// DOCUMENTED RELATIONSHIPS
// ----------------------------------------------------------------------------
// The following relationships are actively used in the system:
//
// 1. (:TargetCompany)-[:CONTRACTS_WITH]->(:Carrier)
//    - Represents carriers that work for target companies
//    - Properties: contract_date, status
//
// 2. (:Carrier)-[:MANAGED_BY]->(:Person)
//    - Links carriers to their primary officers/managers
//    - Properties: role, start_date
//
// 3. (:TargetCompany)-[:HAS_EXECUTIVE]->(:Person)
//    - Links target companies to their executives (CEO, CFO, etc.)
//    - Properties: role, start_date
//
// 4. (:Carrier)-[:HAD_INSURANCE]->(:InsurancePolicy)
//    - Temporal relationship tracking insurance coverage
//    - Properties: from_date, to_date, status, duration_days
//
// 5. (:InsurancePolicy)-[:PROVIDED_BY]->(:InsuranceProvider)
//    - Links policies to their insurance providers
//    - Properties: None
//
// 6. (:InsurancePolicy)-[:PRECEDED_BY]->(:InsurancePolicy)
//    - Links consecutive insurance policies for gap analysis
//    - Properties: gap_days (number of days between policies)
//
// 7. (:Carrier)-[:INSURANCE_EVENT]->(:InsuranceEvent)
//    - Links carriers to insurance-related events
//    - Properties: None

// ============================================================================
// FUTURE IMPLEMENTATION - Planned Entities
// ============================================================================
// These entities are planned for future implementation but not yet active

// ----------------------------------------------------------------------------
// DRIVER (FUTURE)
// ----------------------------------------------------------------------------
// Will represent individual truck drivers
// Planned properties: cdl_number (unique), full_name, status, hire_date

// CREATE CONSTRAINT driver_cdl_unique IF NOT EXISTS
// FOR (d:Driver) REQUIRE d.cdl_number IS UNIQUE;

// CREATE INDEX driver_status_index IF NOT EXISTS
// FOR (d:Driver) ON (d.status);

// ----------------------------------------------------------------------------
// EQUIPMENT (FUTURE)
// ----------------------------------------------------------------------------
// Will represent trucks, trailers, and other equipment
// Planned properties: vin (unique), type, status, year, make, model

// CREATE CONSTRAINT equipment_vin_unique IF NOT EXISTS
// FOR (e:Equipment) REQUIRE e.vin IS UNIQUE;

// CREATE INDEX equipment_status_index IF NOT EXISTS
// FOR (e:Equipment) ON (e.status);

// CREATE INDEX equipment_type_index IF NOT EXISTS
// FOR (e:Equipment) ON (e.type);

// ----------------------------------------------------------------------------
// LOCATION (FUTURE)
// ----------------------------------------------------------------------------
// Will represent physical addresses and terminals
// Planned properties: location_id (unique), street_address, city, state, zip

// CREATE CONSTRAINT location_id_unique IF NOT EXISTS
// FOR (l:Location) REQUIRE l.location_id IS UNIQUE;

// CREATE INDEX location_city_state_index IF NOT EXISTS
// FOR (l:Location) ON (l.city, l.state);

// ----------------------------------------------------------------------------
// AUTHORITY (FUTURE)
// ----------------------------------------------------------------------------
// Will represent operating authorities and permits
// Planned properties: authority_id (unique), mc_number, type, status

// CREATE CONSTRAINT authority_id_unique IF NOT EXISTS
// FOR (a:Authority) REQUIRE a.authority_id IS UNIQUE;

// CREATE INDEX authority_mc_index IF NOT EXISTS
// FOR (a:Authority) ON (a.mc_number);

// CREATE INDEX authority_status_index IF NOT EXISTS
// FOR (a:Authority) ON (a.status);

// ----------------------------------------------------------------------------
// CRASH (FUTURE)
// ----------------------------------------------------------------------------
// Will represent accident/crash records
// Planned properties: report_number (unique), crash_date, severity, fatalities

// CREATE CONSTRAINT crash_report_unique IF NOT EXISTS
// FOR (cr:Crash) REQUIRE cr.report_number IS UNIQUE;

// CREATE INDEX crash_date_index IF NOT EXISTS
// FOR (cr:Crash) ON (cr.crash_date);

// ----------------------------------------------------------------------------
// VIOLATION (FUTURE)
// ----------------------------------------------------------------------------
// Will represent safety violations and citations
// Planned properties: violation_id (unique), code, violation_date, severity

// CREATE CONSTRAINT violation_id_unique IF NOT EXISTS
// FOR (v:Violation) REQUIRE v.violation_id IS UNIQUE;

// CREATE INDEX violation_code_index IF NOT EXISTS
// FOR (v:Violation) ON (v.code);

// CREATE INDEX violation_date_index IF NOT EXISTS
// FOR (v:Violation) ON (v.violation_date);

// ============================================================================
// PATTERN DETECTION QUERIES - Examples from Production Code
// ============================================================================

// ----------------------------------------------------------------------------
// 1. DETECT INSURANCE COVERAGE GAPS
// ----------------------------------------------------------------------------
// Find carriers with gaps in insurance coverage > 30 days
WITH 30 as min_gap_days
MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
WHERE r.to_date < date() 
  AND NOT EXISTS {
    MATCH (c)-[r2:HAD_INSURANCE]->(ip2:InsurancePolicy)
    WHERE r2.from_date <= r.to_date + duration({days: min_gap_days})
      AND r2.from_date > r.to_date
  }
RETURN c.usdot as carrier_usdot,
       c.carrier_name as carrier_name,
       r.to_date as last_coverage_date,
       duration.between(r.to_date, date()).days as days_without_coverage
ORDER BY days_without_coverage DESC
LIMIT 10;

// ----------------------------------------------------------------------------
// 2. IDENTIFY CHAMELEON CARRIERS
// ----------------------------------------------------------------------------
// Find carriers sharing officers and insurance providers (potential chameleons)
MATCH (c1:Carrier)-[:MANAGED_BY]->(p:Person)<-[:MANAGED_BY]-(c2:Carrier)
WHERE c1.usdot <> c2.usdot
OPTIONAL MATCH (c1)-[:HAD_INSURANCE]->(ip1:InsurancePolicy)-[:PROVIDED_BY]->(prov:InsuranceProvider)
OPTIONAL MATCH (c2)-[:HAD_INSURANCE]->(ip2:InsurancePolicy)-[:PROVIDED_BY]->(prov)
WITH c1, c2, p, COUNT(DISTINCT prov) as shared_providers
WHERE shared_providers > 0
RETURN c1.usdot as carrier1_usdot,
       c1.carrier_name as carrier1_name,
       c2.usdot as carrier2_usdot,
       c2.carrier_name as carrier2_name,
       p.full_name as shared_officer,
       shared_providers,
       c1.violations + c2.violations as combined_violations
ORDER BY shared_providers DESC, combined_violations DESC
LIMIT 10;

// ----------------------------------------------------------------------------
// 3. INSURANCE SHOPPING PATTERN
// ----------------------------------------------------------------------------
// Find carriers that frequently change insurance providers
MATCH (c:Carrier)-[:INSURANCE_EVENT]->(ie:InsuranceEvent)
WHERE ie.event_type = 'PROVIDER_CHANGE'
  AND ie.event_date > date() - duration({months: 12})
WITH c, COUNT(ie) as provider_changes
WHERE provider_changes >= 3
RETURN c.usdot as carrier_usdot,
       c.carrier_name as carrier_name,
       provider_changes,
       c.violations as violations,
       c.crashes as crashes
ORDER BY provider_changes DESC
LIMIT 10;

// ----------------------------------------------------------------------------
// 4. RISK SCORING BASED ON INSURANCE PATTERNS
// ----------------------------------------------------------------------------
// Calculate fraud risk scores for carriers
MATCH (c:Carrier)
OPTIONAL MATCH (c)-[:HAD_INSURANCE]->(ip:InsurancePolicy)
OPTIONAL MATCH (c)-[:INSURANCE_EVENT]->(ie:InsuranceEvent)
WITH c,
     COUNT(DISTINCT ip) as policy_count,
     COUNT(DISTINCT ip.provider_name) as provider_count,
     COUNT(DISTINCT CASE WHEN ie.event_type = 'CANCELLATION' THEN ie END) as cancellations,
     COUNT(DISTINCT CASE WHEN ie.compliance_violation = true THEN ie END) as violations
WITH c, 
     policy_count,
     provider_count,
     cancellations,
     violations,
     CASE
        WHEN policy_count = 0 THEN 100
        ELSE (
            (CASE WHEN provider_count > 3 THEN 25 ELSE 0 END) +
            (CASE WHEN cancellations > 2 THEN 25 ELSE cancellations * 10 END) +
            (CASE WHEN violations > 0 THEN 25 ELSE 0 END)
        )
     END as risk_score
WHERE risk_score > 50
RETURN c.usdot as carrier_usdot,
       c.carrier_name as carrier_name,
       risk_score,
       policy_count,
       provider_count,
       cancellations as policy_cancellations,
       violations as compliance_violations
ORDER BY risk_score DESC
LIMIT 20;

// ----------------------------------------------------------------------------
// 5. CARRIERS SHARING MULTIPLE CONNECTIONS
// ----------------------------------------------------------------------------
// Find carriers connected through multiple relationships (officers, insurance)
MATCH (c1:Carrier)-[:MANAGED_BY]->(p:Person)<-[:MANAGED_BY]-(c2:Carrier)
WHERE c1.usdot < c2.usdot
OPTIONAL MATCH (c1)-[:HAD_INSURANCE]->(ip1:InsurancePolicy)-[:PROVIDED_BY]->(prov:InsuranceProvider)<-[:PROVIDED_BY]-(ip2:InsurancePolicy)<-[:HAD_INSURANCE]-(c2)
WITH c1, c2, 
     COLLECT(DISTINCT p.full_name) as shared_officers,
     COLLECT(DISTINCT prov.name) as shared_insurers
WHERE SIZE(shared_officers) > 0 OR SIZE(shared_insurers) > 0
RETURN c1.carrier_name as carrier1,
       c2.carrier_name as carrier2,
       shared_officers,
       shared_insurers,
       c1.violations + c2.violations as total_violations
ORDER BY SIZE(shared_officers) + SIZE(shared_insurers) DESC
LIMIT 10;

// ============================================================================
// MIGRATION NOTES
// ============================================================================
// 
// 1. This schema replaces the deprecated Company node with specialized
//    Carrier and TargetCompany nodes to prevent supernode problems
//
// 2. The HAD_INSURANCE relationship now includes temporal properties
//    (from_date, to_date, status, duration_days) for tracking coverage history
//
// 3. All constraints use IF NOT EXISTS making this script idempotent
//
// 4. The insurance model uses InsurancePolicy nodes (not direct relationships
//    to InsuranceProvider) to capture temporal and policy-specific data
//
// 5. Pattern detection queries are optimized for the current data model
//    and should be used as templates for fraud detection
//
// ============================================================================

// Return confirmation
RETURN "RICO Schema initialization complete - Current implementation ready" as status;