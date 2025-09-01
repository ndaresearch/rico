#!/usr/bin/env python3
"""
Test script for temporal insurance queries.

This script tests the new temporal query methods added to CarrierRepository
and verifies that the migration was successful.
"""

import sys
import os
from pathlib import Path
from datetime import date, timedelta
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from repositories.carrier_repository import CarrierRepository
from repositories.insurance_policy_repository import InsurancePolicyRepository

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_temporal_queries():
    """Test all temporal query methods."""
    
    carrier_repo = CarrierRepository()
    policy_repo = InsurancePolicyRepository()
    
    print("\n" + "=" * 60)
    print("TESTING TEMPORAL INSURANCE QUERIES")
    print("=" * 60)
    
    # Test 1: Get carriers without insurance on a specific date
    print("\n1. Testing carriers without insurance on today's date...")
    today = date.today()
    uninsured = carrier_repo.get_carriers_without_insurance_on_date(today)
    print(f"   Found {len(uninsured)} carriers without insurance today")
    if uninsured[:3]:
        print("   Sample uninsured carriers:")
        for carrier in uninsured[:3]:
            print(f"     - {carrier['carrier_name']} (USDOT: {carrier['carrier_usdot']})")
    
    # Test 2: Get coverage timeline for a specific carrier
    print("\n2. Testing coverage timeline...")
    # Get a carrier with multiple policies
    carriers_with_policies = carrier_repo.execute_query("""
        MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
        WITH c, COUNT(r) as policy_count
        WHERE policy_count > 1
        RETURN c.usdot as usdot, c.carrier_name as name, policy_count
        ORDER BY policy_count DESC
        LIMIT 1
    """)
    
    if carriers_with_policies:
        test_carrier = carriers_with_policies[0]
        print(f"   Testing with carrier: {test_carrier['name']} (USDOT: {test_carrier['usdot']})")
        print(f"   This carrier has {test_carrier['policy_count']} policies")
        
        timeline = carrier_repo.get_coverage_timeline(test_carrier['usdot'])
        print(f"   Coverage timeline:")
        for period in timeline:
            status = period.get('status', 'UNKNOWN')
            duration = period.get('duration_days', 0)
            duration_str = f"{duration} days" if duration > 0 else "ongoing"
            print(f"     - {period['from_date']} to {period.get('to_date', 'present')}: "
                  f"{period['provider_name']} ({status}, {duration_str})")
    
    # Test 3: Find overlapping policies
    print("\n3. Testing overlapping policies detection...")
    overlaps = carrier_repo.find_overlapping_policies()
    print(f"   Found {len(overlaps)} overlapping policy instances")
    if overlaps[:3]:
        print("   Sample overlaps:")
        for overlap in overlaps[:3]:
            print(f"     - Carrier {overlap['carrier_name']} (USDOT: {overlap['carrier_usdot']}):")
            print(f"       Policy 1: {overlap['policy1_provider']} "
                  f"({overlap['policy1_from']} to {overlap.get('policy1_to', 'present')})")
            print(f"       Policy 2: {overlap['policy2_provider']} "
                  f"({overlap['policy2_from']} to {overlap.get('policy2_to', 'present')})")
            print(f"       Overlap: {overlap.get('overlap_days', 0)} days")
    
    # Test 4: Calculate days without coverage
    print("\n4. Testing days without coverage calculation...")
    if carriers_with_policies:
        test_carrier_usdot = carriers_with_policies[0]['usdot']
        start_date = date.today() - timedelta(days=365)
        end_date = date.today()
        
        days_without = carrier_repo.calculate_total_days_without_coverage(
            test_carrier_usdot, start_date, end_date
        )
        print(f"   Carrier {test_carrier['name']} in the last year:")
        print(f"     Period: {start_date} to {end_date} (365 days)")
        print(f"     Days without coverage: {days_without}")
        print(f"     Coverage percentage: {((365 - days_without) / 365) * 100:.1f}%")
    
    # Test 5: Find carriers with coverage gaps
    print("\n5. Testing coverage gaps detection...")
    gaps = carrier_repo.find_carriers_with_coverage_gaps(gap_threshold_days=1)
    print(f"   Found {len(gaps)} carriers with coverage gaps > 1 day")
    if gaps[:3]:
        print("   Top carriers with gaps:")
        for gap_info in gaps[:3]:
            print(f"     - {gap_info['carrier_name']} (USDOT: {gap_info['carrier_usdot']}):")
            print(f"       Total gaps: {gap_info['gap_count']}")
            print(f"       Max gap: {gap_info['max_gap_days']} days")
            print(f"       Total gap days: {gap_info['total_gap_days']} days")
            if gap_info['gaps']:
                largest_gap = gap_info['gaps'][0]
                print(f"       Largest gap: {largest_gap['gap_start']} to {largest_gap['gap_end']} "
                      f"({largest_gap['gap_days']} days)")
    
    # Test 6: Verify temporal properties exist
    print("\n6. Verifying temporal properties...")
    verification = carrier_repo.execute_query("""
        MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
        RETURN 
            COUNT(r) as total,
            COUNT(r.from_date) as with_from,
            COUNT(r.to_date) as with_to,
            COUNT(r.status) as with_status,
            COUNT(r.duration_days) as with_duration,
            COLLECT(DISTINCT r.status) as statuses
    """)
    
    if verification:
        v = verification[0]
        print(f"   Total relationships: {v['total']}")
        print(f"   With from_date: {v['with_from']} ({v['with_from']/v['total']*100:.1f}%)")
        print(f"   With to_date: {v['with_to']} ({v['with_to']/v['total']*100:.1f}%)")
        print(f"   With status: {v['with_status']} ({v['with_status']/v['total']*100:.1f}%)")
        print(f"   With duration_days: {v['with_duration']} ({v['with_duration']/v['total']*100:.1f}%)")
        print(f"   Distinct statuses: {v['statuses']}")
    
    print("\n" + "=" * 60)
    print("TEMPORAL QUERY TESTS COMPLETED")
    print("=" * 60)


def main():
    """Main entry point."""
    try:
        test_temporal_queries()
        print("\n✅ All temporal query tests completed successfully!")
    except Exception as e:
        logger.error(f"Test failed: {e}")
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()