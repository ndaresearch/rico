#!/usr/bin/env python3
"""
Create Person entities from carrier primary_officer data and establish MANAGED_BY relationships.
This script is idempotent - safe to run multiple times.
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import date
from collections import defaultdict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Force reload of environment variables
from dotenv import load_dotenv
load_dotenv(override=True)

from database import db
from models.person import Person
from models.carrier import Carrier
from repositories.person_repository import PersonRepository
from repositories.carrier_repository import CarrierRepository


def get_carriers_with_officers() -> List[Dict]:
    """Get all carriers that have a primary_officer field"""
    query = """
    MATCH (c:Carrier)
    WHERE c.primary_officer IS NOT NULL 
      AND c.primary_officer <> ''
      AND toLower(c.primary_officer) <> 'n/a'
    RETURN c.usdot as usdot, 
           c.carrier_name as carrier_name,
           c.primary_officer as primary_officer
    ORDER BY c.primary_officer
    """
    
    with db.get_session() as session:
        result = session.run(query)
        carriers = [dict(record) for record in result]
    
    return carriers


def analyze_officer_patterns(carriers: List[Dict]) -> Dict:
    """Analyze officer data for patterns and potential fraud indicators"""
    officer_carriers = defaultdict(list)
    
    for carrier in carriers:
        officer_name = carrier['primary_officer'].strip()
        officer_carriers[officer_name].append({
            'usdot': carrier['usdot'],
            'carrier_name': carrier['carrier_name']
        })
    
    # Find officers managing multiple carriers (potential fraud indicator)
    multi_carrier_officers = {
        name: carriers_list 
        for name, carriers_list in officer_carriers.items() 
        if len(carriers_list) > 1
    }
    
    stats = {
        'total_carriers': len(carriers),
        'unique_officers': len(officer_carriers),
        'officers_with_multiple_carriers': len(multi_carrier_officers),
        'multi_carrier_details': multi_carrier_officers
    }
    
    return stats


def create_officers_and_relationships(carriers: List[Dict], dry_run: bool = False) -> Tuple[int, int, int]:
    """
    Create Person entities for officers and MANAGED_BY relationships.
    Returns: (officers_created, officers_found, relationships_created)
    """
    person_repo = PersonRepository()
    carrier_repo = CarrierRepository()
    
    officers_created = 0
    officers_found = 0
    relationships_created = 0
    officer_person_map = {}  # Map officer names to person_ids
    
    # Process each unique officer
    unique_officers = set(c['primary_officer'].strip() for c in carriers)
    
    print(f"\nProcessing {len(unique_officers)} unique officers...")
    
    for officer_name in unique_officers:
        if not officer_name or officer_name.lower() in ['n/a', 'na', '']:
            continue
        
        # Create Person entity
        person = Person(
            person_id="",  # Will be auto-generated
            full_name=officer_name,
            source=["JB_HUNT_CSV"]
        )
        
        if not dry_run:
            # Use find_or_create to handle duplicates
            existing = person_repo.find_or_create(person)
            
            if existing.get('first_seen') == existing.get('last_seen'):
                # Newly created
                officers_created += 1
                print(f"  ✓ Created person: {officer_name} (ID: {existing['person_id']})")
            else:
                # Already existed
                officers_found += 1
                print(f"  - Found existing person: {officer_name} (ID: {existing['person_id']})")
            
            officer_person_map[officer_name] = existing['person_id']
        else:
            print(f"  [DRY RUN] Would create/find person: {officer_name}")
            # Generate ID for dry run
            officer_person_map[officer_name] = person_repo._generate_person_id(officer_name)
    
    # Create MANAGED_BY relationships
    print(f"\nCreating relationships for {len(carriers)} carriers...")
    
    for carrier in carriers:
        officer_name = carrier['primary_officer'].strip()
        
        if officer_name not in officer_person_map:
            continue
        
        person_id = officer_person_map[officer_name]
        
        if not dry_run:
            # Check if relationship already exists
            existing_rel = check_existing_relationship(carrier['usdot'], person_id)
            
            if not existing_rel:
                success = carrier_repo.link_to_officer(carrier['usdot'], person_id)
                if success:
                    relationships_created += 1
                    print(f"  ✓ Linked carrier {carrier['carrier_name']} (USDOT: {carrier['usdot']}) to {officer_name}")
                else:
                    print(f"  ✗ Failed to link carrier {carrier['usdot']} to {officer_name}")
            else:
                print(f"  - Relationship already exists: {carrier['carrier_name']} -> {officer_name}")
        else:
            print(f"  [DRY RUN] Would link carrier {carrier['carrier_name']} to {officer_name}")
    
    return officers_created, officers_found, relationships_created


def check_existing_relationship(usdot: int, person_id: str) -> bool:
    """Check if a MANAGED_BY relationship already exists"""
    query = """
    MATCH (c:Carrier {usdot: $usdot})-[r:MANAGED_BY]->(p:Person {person_id: $person_id})
    RETURN count(r) as count
    """
    
    with db.get_session() as session:
        result = session.run(query, {"usdot": usdot, "person_id": person_id})
        record = result.single()
        return record['count'] > 0 if record else False


def print_fraud_indicators(stats: Dict):
    """Print potential fraud indicators found in the data"""
    if stats['officers_with_multiple_carriers'] > 0:
        print("\n" + "=" * 60)
        print("⚠️  POTENTIAL FRAUD INDICATORS DETECTED")
        print("=" * 60)
        print(f"\nFound {stats['officers_with_multiple_carriers']} officers managing multiple carriers:")
        
        for officer_name, carriers_list in stats['multi_carrier_details'].items():
            print(f"\n  {officer_name} manages {len(carriers_list)} carriers:")
            for carrier in carriers_list:
                print(f"    - {carrier['carrier_name']} (USDOT: {carrier['usdot']})")
    else:
        print("\n✅ No officers found managing multiple carriers (good sign)")


def main():
    """Main function"""
    # Check for dry run flag
    dry_run = '--dry-run' in sys.argv
    
    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No changes will be made")
        print("=" * 60)
    
    print("\n" + "=" * 60)
    print("CREATE OFFICERS FROM CARRIERS")
    print("=" * 60)
    
    # Step 1: Get carriers with officers
    print("\n1. Fetching carriers with primary officers...")
    carriers = get_carriers_with_officers()
    print(f"   ✓ Found {len(carriers)} carriers with primary officers")
    
    if not carriers:
        print("\n⚠️  No carriers with primary officers found. Exiting.")
        return
    
    # Step 2: Analyze patterns
    print("\n2. Analyzing officer patterns...")
    stats = analyze_officer_patterns(carriers)
    print(f"   ✓ Found {stats['unique_officers']} unique officers")
    print(f"   ✓ {stats['officers_with_multiple_carriers']} officers manage multiple carriers")
    
    # Step 3: Create officers and relationships
    print("\n3. Creating Person entities and relationships...")
    officers_created, officers_found, relationships_created = create_officers_and_relationships(
        carriers, dry_run=dry_run
    )
    
    # Step 4: Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if not dry_run:
        print(f"Officers created: {officers_created}")
        print(f"Officers found (already existed): {officers_found}")
        print(f"Relationships created: {relationships_created}")
        print(f"Total carriers processed: {len(carriers)}")
    else:
        print(f"Would create up to {stats['unique_officers']} officers")
        print(f"Would create up to {len(carriers)} relationships")
    
    # Step 5: Print fraud indicators
    print_fraud_indicators(stats)
    
    print("\n✅ Script completed successfully!")


if __name__ == "__main__":
    main()