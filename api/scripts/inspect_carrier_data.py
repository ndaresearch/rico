#!/usr/bin/env python3
"""
Query script to inspect all data for carrier 3487141 (Tutash Express INC)
"""

import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Neo4jConnection

def inspect_carrier(usdot: int = 3487141):
    """Inspect all data for a specific carrier."""
    
    db = Neo4jConnection()
    
    print(f"\n{'='*60}")
    print(f"Inspecting Data for Carrier DOT: {usdot}")
    print(f"{'='*60}\n")
    
    with db.get_session() as session:
        
        # 1. Get carrier basic info
        print("📋 CARRIER INFORMATION:")
        print("-" * 40)
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})
            RETURN c
        """, usdot=usdot)
        
        carrier = result.single()
        if carrier:
            c = carrier['c']
            for key, value in c.items():
                print(f"  {key}: {value}")
        else:
            print("  ❌ Carrier not found!")
            return
        
        # 2. Count inspections
        print("\n📊 INSPECTION SUMMARY:")
        print("-" * 40)
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})-[:UNDERWENT]->(i:Inspection)
            RETURN 
                COUNT(i) as total_inspections,
                COUNT(DISTINCT i.inspection_date) as unique_dates,
                MIN(i.inspection_date) as earliest_date,
                MAX(i.inspection_date) as latest_date,
                SUM(i.violations_count) as total_violations,
                SUM(i.oos_violations_count) as total_oos_violations,
                SUM(CASE WHEN i.driver_oos THEN 1 ELSE 0 END) as driver_oos_count,
                SUM(CASE WHEN i.vehicle_oos THEN 1 ELSE 0 END) as vehicle_oos_count,
                SUM(CASE WHEN i.result = 'Clean' THEN 1 ELSE 0 END) as clean_inspections,
                SUM(CASE WHEN i.result = 'Violations' THEN 1 ELSE 0 END) as violation_inspections,
                SUM(CASE WHEN i.result = 'OOS' THEN 1 ELSE 0 END) as oos_inspections
        """, usdot=usdot)
        
        stats = result.single()
        if stats:
            print(f"  Total Inspections: {stats['total_inspections']}")
            print(f"  Unique Dates: {stats['unique_dates']}")
            print(f"  Date Range: {stats['earliest_date']} to {stats['latest_date']}")
            print(f"  Total Violations: {stats['total_violations']}")
            print(f"  Total OOS Violations: {stats['total_oos_violations']}")
            print(f"  Driver OOS Events: {stats['driver_oos_count']}")
            print(f"  Vehicle OOS Events: {stats['vehicle_oos_count']}")
            print(f"  Clean Inspections: {stats['clean_inspections']}")
            print(f"  Inspections with Violations: {stats['violation_inspections']}")
            print(f"  OOS Inspections: {stats['oos_inspections']}")
        
        # 3. Sample inspections
        print("\n🔍 SAMPLE INSPECTIONS (First 5):")
        print("-" * 40)
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})-[:UNDERWENT]->(i:Inspection)
            RETURN i.inspection_id as id, 
                   i.inspection_date as date, 
                   i.state as state,
                   i.violations_count as violations,
                   i.oos_violations_count as oos,
                   i.driver_oos as driver_oos,
                   i.vehicle_oos as vehicle_oos,
                   i.result as result
            ORDER BY i.inspection_date DESC
            LIMIT 5
        """, usdot=usdot)
        
        for record in result:
            print(f"\n  ID: {record['id']}")
            print(f"    Date: {record['date']}")
            print(f"    State: {record['state']}")
            print(f"    Violations: {record['violations']}")
            print(f"    OOS Count: {record['oos']}")
            print(f"    Driver OOS: {record['driver_oos']}")
            print(f"    Vehicle OOS: {record['vehicle_oos']}")
            print(f"    Result: {record['result']}")
        
        # 4. Check for duplicates
        print("\n⚠️  DUPLICATE CHECK:")
        print("-" * 40)
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})-[:UNDERWENT]->(i:Inspection)
            WITH i.inspection_id as id, COUNT(*) as count
            WHERE count > 1
            RETURN id, count
            ORDER BY count DESC
            LIMIT 10
        """, usdot=usdot)
        
        duplicates = list(result)
        if duplicates:
            print(f"  Found {len(duplicates)} duplicate inspection IDs:")
            for dup in duplicates:
                print(f"    {dup['id']}: {dup['count']} occurrences")
        else:
            print("  ✅ No duplicate inspections found")
        
        # 5. Inspections by date
        print("\n📅 INSPECTIONS BY MONTH (Last 12 months with data):")
        print("-" * 40)
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})-[:UNDERWENT]->(i:Inspection)
            WITH substring(i.inspection_date, 0, 7) as month,
                 COUNT(*) as count,
                 SUM(i.violations_count) as violations,
                 SUM(CASE WHEN i.driver_oos OR i.vehicle_oos THEN 1 ELSE 0 END) as oos
            ORDER BY month DESC
            LIMIT 12
            RETURN month, count, violations, oos
        """, usdot=usdot)
        
        for record in result:
            print(f"  {record['month']}: {record['count']} inspections, "
                  f"{record['violations']} violations, {record['oos']} OOS")
        
        # 6. Insurance relationships
        print("\n💼 INSURANCE RELATIONSHIPS:")
        print("-" * 40)
        result = session.run("""
            MATCH (c:Carrier {usdot: $usdot})-[:INSURED_BY]->(p:InsuranceProvider)
            RETURN p.name as provider, p.policy_count as policies
        """, usdot=usdot)
        
        providers = list(result)
        if providers:
            for p in providers:
                print(f"  Provider: {p['provider']} ({p['policies']} policies)")
        else:
            print("  No insurance relationships found")
        
        # 7. Check violations
        print("\n🚨 VIOLATIONS (Sample):")
        print("-" * 40)
        result = session.run("""
            MATCH (v:Violation)
            WHERE v.inspection_id STARTS WITH 'INSP-' + $usdot_str
            RETURN v.violation_id as id,
                   v.code as code,
                   v.description as description,
                   v.oos_indicator as oos
            LIMIT 5
        """, usdot_str=str(usdot))
        
        violations = list(result)
        if violations:
            for v in violations:
                print(f"  Code: {v['code']}")
                print(f"    {v['description']}")
                print(f"    OOS: {v['oos']}")
                print()
        else:
            print("  No violations found in database")
    
    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    # You can pass a different DOT number as argument
    if len(sys.argv) > 1:
        inspect_carrier(int(sys.argv[1]))
    else:
        inspect_carrier(3487141)