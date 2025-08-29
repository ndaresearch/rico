// Drop existing constraints and indexes (for clean setup)
CALL apoc.schema.assert({}, {});

// Create constraints for Company
CREATE CONSTRAINT company_dot_unique IF NOT EXISTS
FOR (c:Company) REQUIRE c.dot_number IS UNIQUE;

CREATE INDEX company_mc_index IF NOT EXISTS
FOR (c:Company) ON (c.mc_number);

CREATE INDEX company_status_index IF NOT EXISTS
FOR (c:Company) ON (c.authority_status);

CREATE INDEX company_ein_index IF NOT EXISTS
FOR (c:Company) ON (c.ein);

CREATE INDEX company_risk_index IF NOT EXISTS
FOR (c:Company) ON (c.chameleon_risk_score);

// Create constraints for Person
CREATE CONSTRAINT person_id_unique IF NOT EXISTS
FOR (p:Person) REQUIRE p.person_id IS UNIQUE;

CREATE INDEX person_name_index IF NOT EXISTS
FOR (p:Person) ON (p.full_name);

// Create constraints for Driver
CREATE CONSTRAINT driver_cdl_unique IF NOT EXISTS
FOR (d:Driver) REQUIRE d.cdl_number IS UNIQUE;

CREATE INDEX driver_status_index IF NOT EXISTS
FOR (d:Driver) ON (d.status);

// Create constraints for Equipment
CREATE CONSTRAINT equipment_vin_unique IF NOT EXISTS
FOR (e:Equipment) REQUIRE e.vin IS UNIQUE;

CREATE INDEX equipment_status_index IF NOT EXISTS
FOR (e:Equipment) ON (e.status);

CREATE INDEX equipment_type_index IF NOT EXISTS
FOR (e:Equipment) ON (e.type);

// Create constraints for Location
CREATE CONSTRAINT location_id_unique IF NOT EXISTS
FOR (l:Location) REQUIRE l.location_id IS UNIQUE;

CREATE INDEX location_address_index IF NOT EXISTS
FOR (l:Location) ON (l.street_address);

CREATE INDEX location_city_state_index IF NOT EXISTS
FOR (l:Location) ON (l.city, l.state);

// Create constraints for Authority
CREATE CONSTRAINT authority_id_unique IF NOT EXISTS
FOR (a:Authority) REQUIRE a.authority_id IS UNIQUE;

CREATE INDEX authority_mc_index IF NOT EXISTS
FOR (a:Authority) ON (a.mc_number);

CREATE INDEX authority_status_index IF NOT EXISTS
FOR (a:Authority) ON (a.status);

// Create constraints for Crash
CREATE CONSTRAINT crash_report_unique IF NOT EXISTS
FOR (cr:Crash) REQUIRE cr.report_number IS UNIQUE;

CREATE INDEX crash_date_index IF NOT EXISTS
FOR (cr:Crash) ON (cr.crash_date);

// Create constraints for Violation
CREATE CONSTRAINT violation_id_unique IF NOT EXISTS
FOR (v:Violation) REQUIRE v.violation_id IS UNIQUE;

CREATE INDEX violation_code_index IF NOT EXISTS
FOR (v:Violation) ON (v.code);

CREATE INDEX violation_date_index IF NOT EXISTS
FOR (v:Violation) ON (v.violation_date);

// Create constraints for LeasePurchaseProgram
CREATE CONSTRAINT lease_program_id_unique IF NOT EXISTS
FOR (lp:LeasePurchaseProgram) REQUIRE lp.program_id IS UNIQUE;

// Create indexes for relationship properties (when we add them)
// These will be used for temporal queries
CALL db.index.fulltext.createNodeIndex("companyNameIndex", ["Company"], ["legal_name", "dba_name"]) IF NOT EXISTS;

// Return confirmation
RETURN "Schema initialization complete" as status;