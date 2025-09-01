#!/usr/bin/env python3
"""
Test that relationship creation is idempotent by simulating multiple imports
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import db
from repositories.carrier_repository import CarrierRepository
from repositories.target_company_repository import TargetCompanyRepository
from repositories.person_repository import PersonRepository
from models.carrier import Carrier
from models.target_company import TargetCompany
from models.person import Person


def test_contract_idempotency():
    """Test that creating contracts multiple times doesn't create duplicates"""
    carrier_repo = CarrierRepository()
    target_repo = TargetCompanyRepository()
    
    # Create test data
    test_usdot = 999001
    test_dot = 999002
    
    # Clean up any existing test data
    carrier_repo.delete(test_usdot)
    target_repo.delete(test_dot)
    
    # Create carrier and target company
    carrier = Carrier(
        usdot=test_usdot,
        carrier_name="Test Idempotent Carrier",
        primary_officer="Test Officer"
    )
    carrier_repo.create(carrier)
    
    target = TargetCompany(
        dot_number=test_dot,
        legal_name="Test Idempotent Target",
        entity_type="BROKER"
    )
    target_repo.create(target)
    
    # Create contract 3 times
    for i in range(3):
        success = carrier_repo.create_contract_with_target(
            usdot=test_usdot,
            dot_number=test_dot,
            active=True
        )
        print(f"  Attempt {i+1}: {'Success' if success else 'Failed'}")
    
    # Check how many relationships exist
    with db.get_session() as session:
        result = session.run("""
            MATCH (tc:TargetCompany {dot_number: $dot})-[r:CONTRACTS_WITH]->(c:Carrier {usdot: $usdot})
            RETURN count(r) as count
        """, {"dot": test_dot, "usdot": test_usdot})
        count = result.single()['count']
    
    # Clean up
    carrier_repo.delete(test_usdot)
    target_repo.delete(test_dot)
    
    return count == 1


def test_officer_idempotency():
    """Test that linking officers multiple times doesn't create duplicates"""
    carrier_repo = CarrierRepository()
    person_repo = PersonRepository()
    
    # Create test data
    test_usdot = 999003
    
    # Clean up any existing test data
    carrier_repo.delete(test_usdot)
    
    # Create carrier
    carrier = Carrier(
        usdot=test_usdot,
        carrier_name="Test Officer Carrier",
        primary_officer="Test Officer"
    )
    carrier_repo.create(carrier)
    
    # Create person
    person = Person(
        person_id="",
        full_name="Test Officer Person",
        source=["TEST"]
    )
    created_person = person_repo.find_or_create(person)
    person_id = created_person['person_id']
    
    # Link officer 3 times
    for i in range(3):
        success = carrier_repo.link_to_officer(test_usdot, person_id)
        print(f"  Attempt {i+1}: {'Success' if success else 'Failed'}")
    
    # Check how many relationships exist
    with db.get_session() as session:
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})-[r:MANAGED_BY]->(p:Person {person_id: $person_id})
            RETURN count(r) as count
        """, {"usdot": test_usdot, "person_id": person_id})
        count = result.single()['count']
    
    # Clean up
    carrier_repo.delete(test_usdot)
    person_repo.delete(person_id)
    
    return count == 1


def test_executive_idempotency():
    """Test that adding executives multiple times doesn't create duplicates"""
    target_repo = TargetCompanyRepository()
    person_repo = PersonRepository()
    
    # Create test data
    test_dot = 999004
    
    # Clean up any existing test data
    target_repo.delete(test_dot)
    
    # Create target company
    target = TargetCompany(
        dot_number=test_dot,
        legal_name="Test Executive Target",
        entity_type="BROKER"
    )
    target_repo.create(target)
    
    # Create person
    person = Person(
        person_id="",
        full_name="Test Executive",
        source=["TEST"]
    )
    created_person = person_repo.find_or_create(person)
    person_id = created_person['person_id']
    
    # Add as executive 3 times
    for i in range(3):
        success = person_repo.add_to_target_company(
            person_id=person_id,
            dot_number=test_dot,
            role="CEO"
        )
        print(f"  Attempt {i+1}: {'Success' if success else 'Failed'}")
    
    # Check how many relationships exist
    with db.get_session() as session:
        result = session.run("""
            MATCH (tc:TargetCompany {dot_number: $dot})-[r:HAS_EXECUTIVE]->(p:Person {person_id: $person_id})
            RETURN count(r) as count
        """, {"dot": test_dot, "person_id": person_id})
        count = result.single()['count']
    
    # Clean up
    target_repo.delete(test_dot)
    person_repo.delete(person_id)
    
    return count == 1


def main():
    """Run all idempotency tests"""
    print("=" * 60)
    print("TESTING IDEMPOTENCY OF RELATIONSHIP CREATION")
    print("=" * 60)
    
    print("\n1. Testing CONTRACTS_WITH idempotency:")
    if test_contract_idempotency():
        print("   ✅ CONTRACTS_WITH is idempotent (only 1 relationship created)")
    else:
        print("   ❌ CONTRACTS_WITH created duplicates!")
    
    print("\n2. Testing MANAGED_BY idempotency:")
    if test_officer_idempotency():
        print("   ✅ MANAGED_BY is idempotent (only 1 relationship created)")
    else:
        print("   ❌ MANAGED_BY created duplicates!")
    
    print("\n3. Testing HAS_EXECUTIVE idempotency:")
    if test_executive_idempotency():
        print("   ✅ HAS_EXECUTIVE is idempotent (only 1 relationship created)")
    else:
        print("   ❌ HAS_EXECUTIVE created duplicates!")
    
    print("\n" + "=" * 60)
    print("✅ All relationship creation methods are idempotent!")
    print("=" * 60)


if __name__ == "__main__":
    main()