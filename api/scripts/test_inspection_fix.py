#!/usr/bin/env python3
"""
Test script to verify inspection data parsing fixes for carrier 3487141 (Tutash Express INC).

Expected results:
- ~1,743 inspections (not 1,858 duplicates)
- 897 violations (not all "Clean")
- 227 OOS events (not 0)
- Correct historical dates (not today's date)
"""

import sys
import os
from datetime import date, datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Neo4jConnection
from repositories.carrier_repository import CarrierRepository
from repositories.inspection_repository import InspectionRepository
from services.searchcarriers_client import SearchCarriersClient
from scripts.ingest.searchcarriers_insurance_enrichment import SearchCarriersInsuranceEnrichment

def test_carrier_inspection_data():
    """Test inspection data for carrier 3487141."""
    usdot = 3487141
    carrier_name = "Tutash Express INC"
    
    print(f"\n{'='*60}")
    print(f"Testing Inspection Data Fixes for Carrier {usdot}")
    print(f"Carrier: {carrier_name}")
    print(f"{'='*60}\n")
    
    # Initialize repositories (they use the singleton db connection)
    carrier_repo = CarrierRepository()
    inspection_repo = InspectionRepository()
    
    # Check if carrier exists
    carrier = carrier_repo.get_by_usdot(usdot)
    if not carrier:
        print(f"❌ Carrier {usdot} not found in database")
        print("   Please run the carrier import script first.")
        return False
    
    print(f"✅ Found carrier in database: {carrier.get('name', carrier.get('legal_name', 'Unknown'))}\n")
    
    # Get existing inspections from database
    existing_inspections = inspection_repo.find_by_usdot(usdot, limit=2000)
    print(f"📊 Current Database State:")
    print(f"   - Existing inspections: {len(existing_inspections)}")
    
    if existing_inspections:
        # Check date distribution
        dates = [insp.get('inspection_date') for insp in existing_inspections]
        unique_dates = set(dates)
        today_str = date.today().isoformat()
        today_count = dates.count(today_str)
        
        print(f"   - Unique dates: {len(unique_dates)}")
        print(f"   - Inspections with today's date: {today_count}")
        
        # Check violations
        violations_count = sum(insp.get('violations_count', 0) for insp in existing_inspections)
        clean_count = sum(1 for insp in existing_inspections if insp.get('result') == 'Clean')
        
        print(f"   - Total violations: {violations_count}")
        print(f"   - Clean inspections: {clean_count}")
        
        # Check OOS
        oos_count = sum(1 for insp in existing_inspections 
                       if insp.get('vehicle_oos') or insp.get('driver_oos'))
        print(f"   - OOS inspections: {oos_count}\n")
    
    # Clear existing inspections for clean test
    print("🗑️  Clearing existing inspection data for clean test...")
    clear_query = """
    MATCH (c:Carrier {usdot: $usdot})-[r:UNDERWENT]->(i:Inspection)
    DETACH DELETE i
    """
    inspection_repo.execute_query(clear_query, {"usdot": usdot})
    
    # Also clear violations
    clear_violations_query = """
    MATCH (v:Violation)
    WHERE v.inspection_id STARTS WITH 'INSP-' + $usdot
    DETACH DELETE v
    """
    inspection_repo.execute_query(clear_violations_query, {"usdot": str(usdot)})
    
    print("✅ Cleared existing data\n")
    
    # Run enrichment with fixes
    print("🔄 Running enrichment with fixed parsing logic...")
    enrichment = SearchCarriersInsuranceEnrichment()
    result = enrichment.enrich_carrier_inspection_data(usdot)
    
    if "error" in result:
        print(f"❌ Error during enrichment: {result['error']}")
        return False
    
    print(f"\n📈 Enrichment Results:")
    print(f"   - Inspections created: {result['inspection_count']}")
    print(f"   - Violations found: {result['violation_count']}")
    print(f"   - OOS inspections: {result['oos_inspections']}\n")
    
    # Verify results
    print("🔍 Verifying Results Against Expected Values:")
    print(f"   Expected: ~1,743 inspections, 897 violations, 227 OOS events\n")
    
    # Re-fetch from database to verify
    new_inspections = inspection_repo.find_by_usdot(usdot, limit=2000)
    
    # Check inspection count
    inspection_pass = 1700 <= len(new_inspections) <= 1800
    status = "✅" if inspection_pass else "❌"
    print(f"   {status} Inspection count: {len(new_inspections)} (expected ~1,743)")
    
    # Check for duplicates
    inspection_ids = [insp.get('inspection_id') for insp in new_inspections]
    unique_ids = set(inspection_ids)
    duplicates = len(inspection_ids) - len(unique_ids)
    dup_status = "✅" if duplicates == 0 else "❌"
    print(f"   {dup_status} Duplicate inspections: {duplicates}")
    
    # Check dates
    dates = [insp.get('inspection_date') for insp in new_inspections]
    unique_dates = set(dates)
    today_str = date.today().isoformat()
    today_count = dates.count(today_str)
    date_status = "✅" if today_count < 10 else "❌"
    print(f"   {date_status} Inspections with today's date: {today_count}")
    print(f"       Unique dates: {len(unique_dates)}")
    
    # Sample some dates
    if dates:
        sample_dates = sorted(set(dates))[:5]
        print(f"       Sample dates: {', '.join(sample_dates)}")
    
    # Check violations
    total_violations = sum(insp.get('violations_count', 0) for insp in new_inspections)
    clean_inspections = sum(1 for insp in new_inspections if insp.get('result') == 'Clean')
    violation_status = "✅" if 850 <= total_violations <= 950 else "⚠️"
    print(f"   {violation_status} Total violations: {total_violations} (expected ~897)")
    print(f"       Clean inspections: {clean_inspections}")
    
    # Check OOS
    oos_inspections = sum(1 for insp in new_inspections 
                          if insp.get('vehicle_oos') or insp.get('driver_oos') 
                          or insp.get('oos_violations_count', 0) > 0)
    oos_status = "✅" if 200 <= oos_inspections <= 250 else "⚠️"
    print(f"   {oos_status} OOS inspections: {oos_inspections} (expected ~227)")
    
    # Overall pass/fail
    print(f"\n{'='*60}")
    all_pass = inspection_pass and duplicates == 0 and today_count < 10
    if all_pass:
        print("✅ All critical fixes verified successfully!")
    else:
        print("⚠️  Some issues remain - review the results above")
    print(f"{'='*60}\n")
    
    return all_pass

if __name__ == "__main__":
    success = test_carrier_inspection_data()
    sys.exit(0 if success else 1)