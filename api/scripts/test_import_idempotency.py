#!/usr/bin/env python3
"""
Test that re-running parts of the import doesn't create duplicates
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import db
from repositories.carrier_repository import CarrierRepository


def simulate_duplicate_contracts():
    """Simulate re-creating existing contracts"""
    carrier_repo = CarrierRepository()
    jb_hunt_dot = 39874
    
    print("Simulating duplicate contract creation for first 5 carriers...")
    
    # Get first 5 carriers
    with db.get_session() as session:
        result = session.run("""
            MATCH (c:Carrier)
            WHERE c.jb_carrier = true
            RETURN c.usdot as usdot
            LIMIT 5
        """)
        carriers = [record['usdot'] for record in result]
    
    # Try to recreate contracts
    for usdot in carriers:
        success = carrier_repo.create_contract_with_target(
            usdot=usdot,
            dot_number=jb_hunt_dot,
            active=True
        )
        print(f"  Carrier {usdot}: {'Success (MERGE handled it)' if success else 'Failed'}")
    
    # Check for duplicates
    with db.get_session() as session:
        result = session.run("""
            MATCH (tc:TargetCompany)-[r:CONTRACTS_WITH]->(c:Carrier)
            WITH tc, c, count(r) as rel_count
            WHERE rel_count > 1
            RETURN count(*) as duplicate_count
        """)
        duplicate_count = result.single()['duplicate_count']
    
    return duplicate_count == 0


def simulate_duplicate_officers():
    """Simulate re-creating existing officer relationships"""
    print("\nSimulating duplicate officer creation for first 5 carriers...")
    
    # Get first 5 carriers with officers
    with db.get_session() as session:
        result = session.run("""
            MATCH (c:Carrier)-[r:MANAGED_BY]->(p:Person)
            RETURN c.usdot as usdot, p.person_id as person_id
            LIMIT 5
        """)
        relationships = [(record['usdot'], record['person_id']) for record in result]
    
    if not relationships:
        print("  No existing officer relationships found")
        return True
    
    carrier_repo = CarrierRepository()
    
    # Try to recreate relationships
    for usdot, person_id in relationships:
        success = carrier_repo.link_to_officer(usdot, person_id)
        print(f"  Carrier {usdot} -> Person {person_id}: {'Success (MERGE handled it)' if success else 'Failed'}")
    
    # Check for duplicates
    with db.get_session() as session:
        result = session.run("""
            MATCH (c:Carrier)-[r:MANAGED_BY]->(p:Person)
            WITH c, p, count(r) as rel_count
            WHERE rel_count > 1
            RETURN count(*) as duplicate_count
        """)
        duplicate_count = result.single()['duplicate_count']
    
    return duplicate_count == 0


def main():
    """Run import idempotency tests"""
    print("=" * 60)
    print("TESTING IMPORT SCRIPT IDEMPOTENCY")
    print("=" * 60)
    print("\nThis simulates what would happen if import scripts were run twice\n")
    
    # Test contract duplication
    if simulate_duplicate_contracts():
        print("\n✅ Contract re-creation handled correctly (no duplicates)")
    else:
        print("\n❌ Contract re-creation created duplicates!")
        return
    
    # Test officer duplication
    if simulate_duplicate_officers():
        print("\n✅ Officer re-creation handled correctly (no duplicates)")
    else:
        print("\n❌ Officer re-creation created duplicates!")
        return
    
    print("\n" + "=" * 60)
    print("✅ Import scripts are safe to run multiple times!")
    print("   MERGE operations prevent duplicate relationships")
    print("=" * 60)


if __name__ == "__main__":
    main()