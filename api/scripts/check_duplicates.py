#!/usr/bin/env python3
"""
Check for duplicate relationships in the database
"""

import os
from neo4j import GraphDatabase

# Database connection
uri = "bolt://localhost:7687"
user = "neo4j"
password = "ricograph123"

driver = GraphDatabase.driver(uri, auth=(user, password))

with driver.session() as session:
    print("=" * 60)
    print("CHECKING FOR DUPLICATE RELATIONSHIPS")
    print("=" * 60)
    
    # Check for duplicate CONTRACTS_WITH relationships
    print("\n1. Checking CONTRACTS_WITH relationships:")
    result = session.run("""
        MATCH (tc:TargetCompany)-[r:CONTRACTS_WITH]->(c:Carrier)
        WITH tc, c, count(r) as rel_count
        WHERE rel_count > 1
        RETURN tc.legal_name as target, c.carrier_name as carrier, rel_count
        ORDER BY rel_count DESC
    """)
    duplicates = list(result)
    if duplicates:
        print(f"   ⚠️  Found {len(duplicates)} carrier(s) with duplicate CONTRACTS_WITH relationships:")
        for record in duplicates[:5]:
            print(f"      - {record['target']} -> {record['carrier']}: {record['rel_count']} relationships")
    else:
        print("   ✅ No duplicate CONTRACTS_WITH relationships found")
    
    # Check for duplicate MANAGED_BY relationships
    print("\n2. Checking MANAGED_BY relationships:")
    result = session.run("""
        MATCH (c:Carrier)-[r:MANAGED_BY]->(p:Person)
        WITH c, p, count(r) as rel_count
        WHERE rel_count > 1
        RETURN c.carrier_name as carrier, p.full_name as person, rel_count
        ORDER BY rel_count DESC
    """)
    duplicates = list(result)
    if duplicates:
        print(f"   ⚠️  Found {len(duplicates)} carrier(s) with duplicate MANAGED_BY relationships:")
        for record in duplicates[:5]:
            print(f"      - {record['carrier']} -> {record['person']}: {record['rel_count']} relationships")
    else:
        print("   ✅ No duplicate MANAGED_BY relationships found")
    
    # Check for duplicate INSURED_BY relationships
    print("\n3. Checking INSURED_BY relationships:")
    result = session.run("""
        MATCH (c:Carrier)-[r:INSURED_BY]->(ip:InsuranceProvider)
        WITH c, ip, count(r) as rel_count
        WHERE rel_count > 1
        RETURN c.carrier_name as carrier, ip.name as provider, rel_count
        ORDER BY rel_count DESC
    """)
    duplicates = list(result)
    if duplicates:
        print(f"   ⚠️  Found {len(duplicates)} carrier(s) with duplicate INSURED_BY relationships:")
        for record in duplicates[:5]:
            print(f"      - {record['carrier']} -> {record['provider']}: {record['rel_count']} relationships")
    else:
        print("   ✅ No duplicate INSURED_BY relationships found")
    
    # Count total relationships
    print("\n4. Total relationship counts:")
    for rel_type in ["CONTRACTS_WITH", "MANAGED_BY", "INSURED_BY"]:
        result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
        count = result.single()['count']
        print(f"   - {rel_type}: {count} relationships")
    
    # Check for carriers with multiple officers (shouldn't happen)
    print("\n5. Checking for carriers with multiple officers:")
    result = session.run("""
        MATCH (c:Carrier)-[:MANAGED_BY]->(p:Person)
        WITH c, count(DISTINCT p) as officer_count
        WHERE officer_count > 1
        RETURN c.carrier_name as carrier, officer_count
    """)
    multi_officer = list(result)
    if multi_officer:
        print(f"   ⚠️  Found {len(multi_officer)} carrier(s) with multiple officers:")
        for record in multi_officer[:5]:
            print(f"      - {record['carrier']}: {record['officer_count']} officers")
    else:
        print("   ✅ No carriers with multiple officers found")

driver.close()
print("\n" + "=" * 60)
print("CHECK COMPLETE")
print("=" * 60)