#!/usr/bin/env python3
"""
Add temporal properties to existing HAD_INSURANCE relationships.

This migration script enhances HAD_INSURANCE relationships with:
- to_date: When the policy ended (from cancellation_date or expiration_date)
- status: ACTIVE, EXPIRED, or CANCELLED
- duration_days: Total days of coverage

The script is idempotent and can be run multiple times safely.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, date, timezone
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
from database import db
from repositories.insurance_policy_repository import InsurancePolicyRepository

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TemporalRelationshipMigration:
    """Migration to add temporal properties to HAD_INSURANCE relationships."""
    
    def __init__(self):
        """Initialize the migration with database connection."""
        self.db = db
        self.policy_repo = InsurancePolicyRepository()
        self.stats = {
            "relationships_updated": 0,
            "active_policies": 0,
            "expired_policies": 0,
            "cancelled_policies": 0,
            "gaps_detected": 0,
            "overlaps_detected": 0,
            "errors": 0
        }
    
    def calculate_status(self, policy_data: dict) -> str:
        """Calculate the status of a policy based on its dates.
        
        Args:
            policy_data: Dictionary containing policy date fields
            
        Returns:
            str: Status - ACTIVE, EXPIRED, or CANCELLED
        """
        cancellation_date = policy_data.get('cancellation_date')
        expiration_date = policy_data.get('expiration_date')
        
        if cancellation_date:
            return "CANCELLED"
        
        if expiration_date:
            # Parse the date if it's a string
            if isinstance(expiration_date, str):
                exp_date = datetime.fromisoformat(expiration_date).date()
            else:
                exp_date = expiration_date
            
            if exp_date < date.today():
                return "EXPIRED"
        
        return "ACTIVE"
    
    def calculate_duration_days(self, from_date: str, to_date: str = None) -> int:
        """Calculate the duration between two dates in days.
        
        Args:
            from_date: Start date (ISO format string)
            to_date: End date (ISO format string) or None
            
        Returns:
            int: Number of days, or -1 if still active
        """
        if not to_date:
            return -1  # Still active
        
        start = datetime.fromisoformat(from_date).date()
        end = datetime.fromisoformat(to_date).date()
        
        return (end - start).days
    
    def update_relationship_properties(self) -> dict:
        """Update all HAD_INSURANCE relationships with temporal properties.
        
        Returns:
            dict: Summary of updates made
        """
        logger.info("Starting temporal properties migration...")
        
        # Query to get all HAD_INSURANCE relationships with their associated policies
        query = """
        MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
        RETURN 
            c.usdot as carrier_usdot,
            ip.policy_id as policy_id,
            ip.effective_date as effective_date,
            ip.expiration_date as expiration_date,
            ip.cancellation_date as cancellation_date,
            ip.filing_status as filing_status,
            r.from_date as existing_from_date,
            r.to_date as existing_to_date,
            id(r) as relationship_id
        """
        
        with self.db.get_session() as session:
            result = session.run(query)
            relationships = list(result)
        
        logger.info(f"Found {len(relationships)} HAD_INSURANCE relationships to process")
        
        # Process each relationship
        for rel in relationships:
            try:
                policy_id = rel['policy_id']
                carrier_usdot = rel['carrier_usdot']
                
                # Determine the to_date
                to_date = None
                if rel['cancellation_date']:
                    to_date = rel['cancellation_date']
                elif rel['expiration_date']:
                    to_date = rel['expiration_date']
                
                # Calculate status
                status = self.calculate_status({
                    'cancellation_date': rel['cancellation_date'],
                    'expiration_date': rel['expiration_date']
                })
                
                # Calculate duration
                from_date = rel['existing_from_date'] or rel['effective_date']
                duration_days = self.calculate_duration_days(from_date, to_date)
                
                # Update the relationship with temporal properties
                update_query = """
                MATCH (c:Carrier {usdot: $carrier_usdot})-[r:HAD_INSURANCE]->(ip:InsurancePolicy {policy_id: $policy_id})
                SET 
                    r.to_date = $to_date,
                    r.status = $status,
                    r.duration_days = $duration_days,
                    r.updated_at = $updated_at
                RETURN r
                """
                
                params = {
                    'carrier_usdot': carrier_usdot,
                    'policy_id': policy_id,
                    'to_date': to_date,
                    'status': status,
                    'duration_days': duration_days,
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }
                
                with self.db.get_session() as session:
                    update_result = session.run(update_query, params)
                    if update_result.single():
                        self.stats["relationships_updated"] += 1
                        
                        # Track status counts
                        if status == "ACTIVE":
                            self.stats["active_policies"] += 1
                        elif status == "EXPIRED":
                            self.stats["expired_policies"] += 1
                        elif status == "CANCELLED":
                            self.stats["cancelled_policies"] += 1
                        
                        if self.stats["relationships_updated"] % 10 == 0:
                            logger.info(f"Processed {self.stats['relationships_updated']} relationships...")
                
            except Exception as e:
                logger.error(f"Error processing relationship for policy {policy_id}: {e}")
                self.stats["errors"] += 1
        
        return self.stats
    
    def detect_gaps_and_overlaps(self) -> dict:
        """Detect coverage gaps and overlapping policies after migration.
        
        Returns:
            dict: Summary of gaps and overlaps found
        """
        logger.info("Detecting coverage gaps and overlaps...")
        
        # Query for gaps
        gap_query = """
        MATCH (c:Carrier)-[r1:HAD_INSURANCE]->(ip1:InsurancePolicy)
        MATCH (c)-[r2:HAD_INSURANCE]->(ip2:InsurancePolicy)
        WHERE r1.to_date IS NOT NULL 
          AND r2.from_date > r1.to_date
          AND id(r1) < id(r2)
        WITH c, r1, r2, 
             duration.between(date(r1.to_date), date(r2.from_date)).days as gap_days
        WHERE gap_days > 0
        RETURN c.usdot as carrier_usdot, 
               c.carrier_name as carrier_name,
               r1.to_date as gap_start, 
               r2.from_date as gap_end,
               gap_days
        ORDER BY gap_days DESC
        """
        
        # Query for overlaps
        overlap_query = """
        MATCH (c:Carrier)-[r1:HAD_INSURANCE]->(ip1:InsurancePolicy)
        MATCH (c)-[r2:HAD_INSURANCE]->(ip2:InsurancePolicy)
        WHERE id(r1) < id(r2)
          AND r1.from_date < r2.from_date
          AND (r1.to_date IS NULL OR r1.to_date > r2.from_date)
        RETURN c.usdot as carrier_usdot,
               c.carrier_name as carrier_name,
               r1.from_date as r1_start,
               r1.to_date as r1_end,
               r2.from_date as r2_start,
               r2.to_date as r2_end,
               ip1.provider_name as provider1,
               ip2.provider_name as provider2
        """
        
        gaps = []
        overlaps = []
        
        with self.db.get_session() as session:
            # Get gaps
            gap_result = session.run(gap_query)
            for record in gap_result:
                gaps.append(dict(record))
                self.stats["gaps_detected"] += 1
            
            # Get overlaps
            overlap_result = session.run(overlap_query)
            for record in overlap_result:
                overlaps.append(dict(record))
                self.stats["overlaps_detected"] += 1
        
        return {"gaps": gaps, "overlaps": overlaps}
    
    def verify_migration(self) -> dict:
        """Verify the migration was successful.
        
        Returns:
            dict: Verification results
        """
        logger.info("Verifying migration...")
        
        verify_query = """
        MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
        RETURN 
            COUNT(r) as total_relationships,
            COUNT(r.from_date) as with_from_date,
            COUNT(r.to_date) as with_to_date,
            COUNT(r.status) as with_status,
            COUNT(r.duration_days) as with_duration,
            COLLECT(DISTINCT r.status) as statuses
        """
        
        with self.db.get_session() as session:
            result = session.run(verify_query)
            verification = dict(result.single())
        
        return verification
    
    def print_summary(self, gaps_overlaps: dict):
        """Print migration summary.
        
        Args:
            gaps_overlaps: Dictionary containing gaps and overlaps data
        """
        print("\n" + "=" * 60)
        print("TEMPORAL PROPERTIES MIGRATION SUMMARY")
        print("=" * 60)
        print(f"Relationships updated: {self.stats['relationships_updated']}")
        print(f"  - Active policies: {self.stats['active_policies']}")
        print(f"  - Expired policies: {self.stats['expired_policies']}")
        print(f"  - Cancelled policies: {self.stats['cancelled_policies']}")
        print(f"Coverage gaps detected: {self.stats['gaps_detected']}")
        print(f"Overlapping policies detected: {self.stats['overlaps_detected']}")
        print(f"Errors encountered: {self.stats['errors']}")
        
        # Show top gaps
        if gaps_overlaps["gaps"]:
            print("\n" + "-" * 60)
            print("TOP COVERAGE GAPS (Days without insurance)")
            print("-" * 60)
            for gap in gaps_overlaps["gaps"][:5]:
                print(f"  Carrier {gap['carrier_usdot']} ({gap['carrier_name']}): "
                      f"{gap['gap_days']} days gap")
                print(f"    From {gap['gap_start']} to {gap['gap_end']}")
        
        # Show overlaps
        if gaps_overlaps["overlaps"]:
            print("\n" + "-" * 60)
            print("OVERLAPPING POLICIES DETECTED")
            print("-" * 60)
            for overlap in gaps_overlaps["overlaps"][:5]:
                print(f"  Carrier {overlap['carrier_usdot']} ({overlap['carrier_name']}):")
                print(f"    {overlap['provider1']}: {overlap['r1_start']} to {overlap['r1_end']}")
                print(f"    {overlap['provider2']}: {overlap['r2_start']} to {overlap['r2_end']}")
    
    def run(self):
        """Execute the complete migration."""
        try:
            # Update relationships
            self.update_relationship_properties()
            
            # Detect gaps and overlaps
            gaps_overlaps = self.detect_gaps_and_overlaps()
            
            # Verify migration
            verification = self.verify_migration()
            
            # Print summary
            self.print_summary(gaps_overlaps)
            
            # Print verification
            print("\n" + "-" * 60)
            print("VERIFICATION")
            print("-" * 60)
            print(f"Total relationships: {verification['total_relationships']}")
            print(f"With from_date: {verification['with_from_date']}")
            print(f"With to_date: {verification['with_to_date']}")
            print(f"With status: {verification['with_status']}")
            print(f"With duration_days: {verification['with_duration']}")
            print(f"Distinct statuses: {verification['statuses']}")
            
            if verification['with_status'] == verification['total_relationships']:
                print("\n✅ Migration completed successfully!")
            else:
                print(f"\n⚠️ Migration partially complete. "
                      f"{verification['total_relationships'] - verification['with_status']} "
                      f"relationships missing temporal properties.")
            
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            print(f"\n❌ Migration failed: {e}")
            return False


def main():
    """Main entry point for the migration script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Add temporal properties to HAD_INSURANCE relationships"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (analyze but don't update)"
    )
    
    args = parser.parse_args()
    
    migration = TemporalRelationshipMigration()
    
    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        # Just detect and report
        with migration.db.get_session() as session:
            result = session.run("""
                MATCH (c:Carrier)-[r:HAD_INSURANCE]->(ip:InsurancePolicy)
                WHERE r.status IS NULL
                RETURN COUNT(r) as missing_temporal
            """)
            count = result.single()['missing_temporal']
            print(f"Found {count} relationships missing temporal properties")
    else:
        migration.run()


if __name__ == "__main__":
    main()