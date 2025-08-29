// init_schema.cypher
// Run this file after Neo4j is up to create constraints and indexes

// ========================================
// CONSTRAINTS (Run these first)
// ========================================

// Unique constraints
CREATE CONSTRAINT company_dot_unique IF NOT EXISTS 
FOR (c:Company) REQUIRE c.dot_number IS UNIQUE;

CREATE CONSTRAINT equipment_vin_unique IF NOT EXISTS 
FOR (e:Equipment) REQUIRE e.vin IS UNIQUE;

CREATE CONSTRAINT driver_cdl_unique IF NOT EXISTS 
FOR (d:Driver) REQUIRE d.cdl_number IS UNIQUE;

CREATE CONSTRAINT person_id_unique IF NOT EXISTS 
FOR (p:Person) REQUIRE p.person_id IS UNIQUE;

CREATE CONSTRAINT authority_id_unique IF NOT EXISTS 
FOR (a:Authority) REQUIRE a.authority_id IS UNIQUE;

CREATE CONSTRAINT location_id_unique IF NOT EXISTS 
FOR (l:Location) REQUIRE l.location_id IS UNIQUE;

CREATE CONSTRAINT crash_report_unique IF NOT EXISTS 
FOR (c:Crash) REQUIRE c.report_number IS UNIQUE;

CREATE CONSTRAINT violation_id_unique IF NOT EXISTS 
FOR (v:Violation) REQUIRE v.violation_id IS UNIQUE;

CREATE CONSTRAINT lease_program_id_unique IF NOT EXISTS 
FOR (lp:LeasePurchaseProgram) REQUIRE lp.program_id IS UNIQUE;

// ========================================
// INDEXES (Run after constraints)
// ========================================

// Node property indexes
CREATE INDEX company_name_index IF NOT EXISTS 
FOR (c:Company) ON (c.legal_name);

CREATE INDEX company_created_index IF NOT EXISTS 
FOR (c:Company) ON (c.created_date);

CREATE INDEX company_status_index IF NOT EXISTS 
FOR (c:Company) ON (c.authority_status);

CREATE INDEX company_safety_rating_index IF NOT EXISTS 
FOR (c:Company) ON (c.safety_rating);

CREATE INDEX person_name_index IF NOT EXISTS 
FOR (p:Person) ON (p.full_name);

CREATE INDEX location_address_index IF NOT EXISTS 
FOR (l:Location) ON (l.street_address);

CREATE INDEX location_city_state_index IF NOT EXISTS 
FOR (l:Location) ON (l.city, l.state);

CREATE INDEX authority_dates_index IF NOT EXISTS 
FOR (a:Authority) ON (a.granted_date, a.revoked_date);

CREATE INDEX authority_status_index IF NOT EXISTS 
FOR (a:Authority) ON (a.status);

CREATE INDEX equipment_type_index IF NOT EXISTS 
FOR (e:Equipment) ON (e.type);

CREATE INDEX equipment_status_index IF NOT EXISTS 
FOR (e:Equipment) ON (e.status);

CREATE INDEX crash_date_index IF NOT EXISTS 
FOR (c:Crash) ON (c.crash_date);

CREATE INDEX crash_severity_index IF NOT EXISTS 
FOR (c:Crash) ON (c.severity);

CREATE INDEX violation_date_index IF NOT EXISTS 
FOR (v:Violation) ON (v.violation_date);

CREATE INDEX violation_category_index IF NOT EXISTS 
FOR (v:Violation) ON (v.category);

// Composite indexes for common query patterns
CREATE INDEX company_type_status_index IF NOT EXISTS 
FOR (c:Company) ON (c.entity_type, c.authority_status);

// ========================================
// RELATIONSHIP INDEXES (Neo4j 5.x)
// ========================================

CREATE INDEX operates_dates IF NOT EXISTS 
FOR ()-[o:OPERATES]->() ON (o.start_date, o.end_date);

CREATE INDEX employed_dates IF NOT EXISTS 
FOR ()-[e:EMPLOYED_BY]->() ON (e.start_date, e.end_date);

CREATE INDEX officer_dates IF NOT EXISTS 
FOR ()-[h:HAS_OFFICER]->() ON (h.start_date, h.end_date);

CREATE INDEX settlement_date IF NOT EXISTS 
FOR ()-[s:WEEKLY_SETTLEMENT]->() ON (s.date);

CREATE INDEX located_at_current IF NOT EXISTS 
FOR ()-[l:LOCATED_AT]->() ON (l.is_current);

CREATE INDEX leased_dates IF NOT EXISTS 
FOR ()-[l:LEASED]->() ON (l.start_date, l.end_date);

// ========================================
// VERIFY SETUP
// ========================================

// Show all constraints
SHOW CONSTRAINTS;

// Show all indexes
SHOW INDEXES;