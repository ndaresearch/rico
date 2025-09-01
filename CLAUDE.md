# CLAUDE.md - RICO Graph Database

## Project Overview
RICO is a fraud detection system for the trucking industry using Neo4j graph database. We're modeling relationships between trucking companies, carriers, insurance providers, and people to detect fraudulent patterns.

## Architecture
- **Database**: Neo4j (graph database)
- **API**: FastAPI with Python
- **Testing**: Real Neo4j test database via Docker
- **Data Source**: JB Hunt carrier CSV data

## Data Model

### Entities
1. **TargetCompany** - Large companies (e.g., JB Hunt) that contract carriers
2. **Carrier** - Individual trucking companies with safety/violation data
3. **InsuranceProvider** - Companies that insure carriers
4. **Person** - Individuals who manage carriers or serve as executives

### Relationships
```
(:TargetCompany)-[:CONTRACTS_WITH]->(:Carrier)
(:TargetCompany)-[:HAS_EXECUTIVE]->(:Person)  # CEO, CFO, etc.
(:Carrier)-[:INSURED_BY]->(:InsuranceProvider)
(:Carrier)-[:MANAGED_BY]->(:Person)           # Primary officers
(:Carrier)-[:HAS_VIOLATION]->(:Violation)     # Future
(:Carrier)-[:INVOLVED_IN]->(:Crash)           # Future
```

## Progress Summary

### ✅ Session 1: Initial Implementation
- Created TargetCompany, Carrier, InsuranceProvider models
- Built repositories with CRUD operations
- Implemented API routes with full test coverage
- Created JB Hunt import script
- **Result**: 67 carriers, 26 insurance providers, all relationships

### ✅ Session 2: Cleanup & Person Relationships
- Deleted old Company implementation (9 files)
- Redesigned Person entity with dual relationships:
  - `HAS_EXECUTIVE` for TargetCompany executives
  - `MANAGED_BY` for Carrier primary officers
- Fixed all Person endpoint tests
- **Result**: 48 tests passing, clean separation of concerns

### ✅ Session 3: Insurance Relationships
- Created insurance relationship fix script
- Added POST `/carriers/{usdot}/insurance` endpoint
- Implemented MERGE for idempotent relationships
- Added 5 comprehensive tests for insurance functionality
- Updated import script to create insurance relationships
- **Result**: 53 tests passing, complete graph connectivity

## Current State
- **Entities**: 67 carriers, 26 insurance providers, 1 target company (JB Hunt)
- **Relationships**: 
  - ✅ Carrier-to-JB Hunt contracts
  - ✅ Carrier-to-InsuranceProvider (ready to create)
  - ⏳ Carrier-to-Person (primary officers - next task)
  - ⏳ TargetCompany-to-Person (executives - next task)
- **Tests**: 53 passing (100% coverage)

## Next Steps
1. **Create Person relationships** from carrier primary_officer data
2. **Add database constraints** for data integrity
3. **Implement Violation and Crash** entities from CSV data
4. **Build fraud detection queries** using graph patterns

## Testing
```bash
./run_tests.sh                    # Run all tests
./run_tests.sh --keep-running     # Keep test DB running
# Test DB: port 7688, Production: port 7687
```

## Import Scripts
```bash
# Import JB Hunt carriers with all relationships
python api/scripts/import/jb_hunt_carriers_import.py

# Fix missing insurance relationships (idempotent)
python api/scripts/import/fix_insurance_relationships.py
```

## Key Design Decisions
1. **Specialized entities** prevent supernode problems
2. **MERGE over CREATE** for idempotent operations
3. **Typed relationships** for clear semantics
4. **Real test database** for accurate testing
5. **No backwards compatibility** - clean break from old model