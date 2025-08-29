import csv
import json
import requests
import sys
from typing import List, Dict
import time


def load_csv(filename: str) -> List[Dict]:
    """Load companies from CSV file"""
    companies = []
    
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert JSON strings back to lists
            for key, value in row.items():
                if value and value.startswith('['):
                    try:
                        row[key] = json.loads(value)
                    except:
                        pass
                # Handle None/empty values
                elif value == '' or value == 'None':
                    row[key] = None
                # Convert numeric fields
                elif key in ['dot_number', 'total_drivers', 'total_trucks', 'total_trailers']:
                    row[key] = int(value) if value else None
                elif key in ['chameleon_risk_score', 'safety_risk_score', 'financial_risk_score', 
                            'insurance_minimum', 'data_completeness_score']:
                    row[key] = float(value) if value else None
                elif key == 'is_publicly_traded':
                    row[key] = value.lower() == 'true' if value else None
            
            companies.append(row)
    
    return companies


def import_companies(api_url: str, api_key: str, companies: List[Dict], batch_size: int = 10):
    """Import companies to the API in batches"""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    
    total = len(companies)
    created = 0
    failed = 0
    errors = []
    
    print(f"Importing {total} companies in batches of {batch_size}...")
    
    # Process in batches
    for i in range(0, total, batch_size):
        batch = companies[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        
        print(f"Processing batch {batch_num} ({i+1}-{min(i+batch_size, total)} of {total})...", end="")
        
        try:
            # Use bulk endpoint for efficiency
            response = requests.post(
                f"{api_url}/companies/bulk",
                headers=headers,
                json=batch
            )
            
            if response.status_code == 201:
                data = response.json()
                batch_created = data.get('created', len(batch))
                created += batch_created
                print(f" ✓ Created {batch_created} companies")
            else:
                print(f" ✗ Failed: {response.status_code}")
                
                # Try individual imports for this batch
                print(f"  Retrying individually...")
                for company in batch:
                    try:
                        ind_response = requests.post(
                            f"{api_url}/companies/",
                            headers=headers,
                            json=company
                        )
                        if ind_response.status_code == 201:
                            created += 1
                            print(f"    ✓ Created DOT {company['dot_number']}")
                        elif ind_response.status_code == 409:
                            print(f"    - DOT {company['dot_number']} already exists")
                        else:
                            failed += 1
                            errors.append(f"DOT {company['dot_number']}: {ind_response.text}")
                            print(f"    ✗ Failed DOT {company['dot_number']}: {ind_response.status_code}")
                    except Exception as e:
                        failed += 1
                        errors.append(f"DOT {company['dot_number']}: {str(e)}")
                        print(f"    ✗ Error DOT {company['dot_number']}: {str(e)}")
        
        except Exception as e:
            print(f" ✗ Error: {str(e)}")
            failed += len(batch)
            errors.append(f"Batch {batch_num}: {str(e)}")
        
        # Small delay between batches
        time.sleep(0.1)
    
    print("\n" + "=" * 50)
    print("IMPORT COMPLETE")
    print(f"  Total Companies: {total}")
    print(f"  Successfully Created: {created}")
    print(f"  Failed: {failed}")
    
    if errors:
        print("\nErrors:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors)-10} more errors")
    
    return created, failed


def verify_import(api_url: str, api_key: str):
    """Verify the import by checking statistics"""
    headers = {"X-API-Key": api_key}
    
    try:
        response = requests.get(f"{api_url}/companies/statistics/summary", headers=headers)
        if response.status_code == 200:
            stats = response.json()
            print("\nDatabase Statistics:")
            print(f"  Total Companies: {stats.get('total_companies', 0)}")
            print(f"  Active Companies: {stats.get('active_companies', 0)}")
            print(f"  Average Trucks: {stats.get('avg_trucks', 0):.1f}")
            print(f"  High Risk Companies: {stats.get('high_risk_companies', 0)}")
    except Exception as e:
        print(f"Could not verify import: {str(e)}")


def main():
    """Main import function"""
    # Configuration
    API_URL = "http://localhost:8000"
    API_KEY = "test-api-key-123"  # Get from .env file
    CSV_FILE = "companies.csv"
    
    # Allow command line arguments
    if len(sys.argv) > 1:
        CSV_FILE = sys.argv[1]
    if len(sys.argv) > 2:
        API_URL = sys.argv[2]
    if len(sys.argv) > 3:
        API_KEY = sys.argv[3]
    
    print("RICO Company Data Import")
    print("=" * 50)
    print(f"API URL: {API_URL}")
    print(f"CSV File: {CSV_FILE}")
    print()
    
    # Test API connectivity
    print("Testing API connectivity...", end="")
    try:
        response = requests.get(f"{API_URL}/health")
        if response.status_code == 200:
            print(" ✓ API is accessible")
        else:
            print(" ✗ API returned status", response.status_code)
            sys.exit(1)
    except Exception as e:
        print(f" ✗ Cannot connect to API: {str(e)}")
        sys.exit(1)
    
    # Load CSV data
    print(f"Loading data from {CSV_FILE}...", end="")
    try:
        companies = load_csv(CSV_FILE)
        print(f" ✓ Loaded {len(companies)} companies")
    except FileNotFoundError:
        print(f" ✗ File not found: {CSV_FILE}")
        sys.exit(1)
    except Exception as e:
        print(f" ✗ Error loading CSV: {str(e)}")
        sys.exit(1)
    
    # Import companies
    print()
    created, failed = import_companies(API_URL, API_KEY, companies)
    
    # Verify import
    print()
    verify_import(API_URL, API_KEY)
    
    # Exit with appropriate code
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main() 