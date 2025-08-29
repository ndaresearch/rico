# scripts/generate_company_test_data.py
import csv
import random
from datetime import date, datetime, timedelta
from typing import List, Dict
import json

# Company name components for realistic generation
PREFIXES = ["National", "American", "First", "Global", "United", "Premier", "Express", "Rapid", "Swift", "Eagle"]
CORES = ["Transport", "Logistics", "Freight", "Trucking", "Carriers", "Express", "Shipping", "Hauling", "Transit", "Lines"]
SUFFIXES = ["Inc", "LLC", "Corp", "Company", "Group", "Services", "Solutions", "International", "USA", "Systems"]

# Suspicious name variations (for chameleon patterns)
CHAMELEON_BASES = [
    "Diamond", "Phoenix", "Liberty", "Freedom", "Eagle", "Star", "Crown", "Royal", "Prime", "Elite"
]

# Cities for address generation
CITIES = [
    ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"),
    ("Atlanta", "GA", "30301"),
    ("Miami", "FL", "33101"),
    ("Dallas", "TX", "75201"),
    ("Los Angeles", "CA", "90001"),
    ("New York", "NY", "10001"),
    ("Denver", "CO", "80201"),
    ("Seattle", "WA", "98101")
]

# Cargo types
CARGO_TYPES = [
    ["General Freight"],
    ["Household Goods"],
    ["Metal sheets coils rolls"],
    ["Motor Vehicles"],
    ["Logs Poles Beams Lumber"],
    ["Building Materials"],
    ["Fresh Produce"],
    ["Liquids Gases"],
    ["Hazmat"],
    ["Refrigerated Food"]
]


def generate_dot_number(start=100000):
    """Generate DOT numbers"""
    return start + random.randint(1, 900000)


def generate_mc_number(dot_number):
    """Generate MC number based on DOT"""
    if random.random() > 0.1:  # 90% have MC numbers
        return f"MC-{dot_number % 1000000:06d}"
    return None


def generate_company_name(base=None, variation=0):
    """Generate realistic company names with variations for chameleons"""
    if base:
        # Create variations for potential chameleon carriers
        variations = [
            f"{base} {random.choice(CORES)} {random.choice(SUFFIXES)}",
            f"New {base} {random.choice(CORES)}",
            f"{base} {random.choice(['Express', 'Transport', 'Logistics'])} {random.choice(['II', '2', 'Plus'])}",
            f"All {base} {random.choice(CORES)}",
            f"{base} National {random.choice(['Carriers', 'Transport'])}",
        ]
        return variations[variation % len(variations)]
    else:
        # Generate normal company name
        return f"{random.choice(PREFIXES)} {random.choice(CORES)} {random.choice(SUFFIXES)}"


def generate_companies(num_companies=100):
    """Generate realistic company data with suspicious patterns"""
    companies = []
    used_dot_numbers = set()
    
    # Track chameleon groups (companies that might be related)
    chameleon_groups = {}
    
    # Generate chameleon carriers (30% of companies)
    num_chameleons = int(num_companies * 0.3)
    
    # Create chameleon groups
    for i in range(num_chameleons // 3):  # Each group has ~3 related companies
        base_name = random.choice(CHAMELEON_BASES)
        group_id = f"group_{i}"
        chameleon_groups[group_id] = {
            "base_name": base_name,
            "companies": [],
            "shared_ein": f"EIN-{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
            "shared_address": random.choice(CITIES)
        }
    
    # Generate chameleon companies
    chameleon_count = 0
    for group_id, group_data in chameleon_groups.items():
        num_in_group = random.randint(2, 4)
        
        for variation in range(num_in_group):
            if chameleon_count >= num_chameleons:
                break
                
            dot_number = generate_dot_number()
            while dot_number in used_dot_numbers:
                dot_number = generate_dot_number()
            used_dot_numbers.add(dot_number)
            
            # Create dates showing succession pattern
            base_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 1000))
            created_date = base_date + timedelta(days=variation * 180)  # 6 months apart
            
            # Alternate between active and inactive (shell game pattern)
            if variation == 0:
                authority_status = "INACTIVE"  # First one shut down
                safety_rating = "UNSATISFACTORY"
            elif variation == num_in_group - 1:
                authority_status = "ACTIVE"  # Latest one active
                safety_rating = None  # Too new for rating
            else:
                authority_status = random.choice(["INACTIVE", "REVOKED"])
                safety_rating = "CONDITIONAL"
            
            city, state, zip_code = group_data["shared_address"]
            
            company = {
                "dot_number": dot_number,
                "mc_number": generate_mc_number(dot_number),
                "legal_name": generate_company_name(group_data["base_name"], variation),
                "dba_name": [generate_company_name(group_data["base_name"], variation + 1)] if random.random() > 0.5 else [],
                "entity_type": "CARRIER",
                "authority_status": authority_status,
                "safety_rating": safety_rating,
                "operation_classification": random.choice(["AUTHORIZED_FOR_HIRE", "PRIVATE"]),
                "company_type": random.choice(["LLC", "CORPORATION", "SOLE_PROPRIETORSHIP"]),
                "ein": group_data["shared_ein"] if random.random() > 0.3 else f"EIN-{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
                "total_drivers": random.randint(1, 15),
                "total_trucks": random.randint(1, 10),
                "total_trailers": random.randint(0, 8),
                "chameleon_risk_score": round(0.7 + random.random() * 0.3, 2),  # High risk
                "safety_risk_score": round(0.5 + random.random() * 0.5, 2),
                "financial_risk_score": round(0.6 + random.random() * 0.4, 2),
                "created_date": created_date.isoformat(),
                "mcs150_date": (created_date + timedelta(days=random.randint(30, 365))).isoformat(),
                "insurance_minimum": random.choice([750000, 1000000, 5000000]),
                "cargo_carried": random.choice(CARGO_TYPES),
                "data_completeness_score": round(0.7 + random.random() * 0.3, 2),
                # Suspicious patterns
                "_pattern": "CHAMELEON_CARRIER",
                "_group_id": group_id,
                "_city": city,
                "_state": state,
                "_zip": zip_code
            }
            
            companies.append(company)
            group_data["companies"].append(dot_number)
            chameleon_count += 1
    
    # Generate normal companies (70%)
    for i in range(num_companies - chameleon_count):
        dot_number = generate_dot_number()
        while dot_number in used_dot_numbers:
            dot_number = generate_dot_number()
        used_dot_numbers.add(dot_number)
        
        city, state, zip_code = random.choice(CITIES)
        created_date = date(2018, 1, 1) + timedelta(days=random.randint(0, 2000))
        
        # Normal companies have better metrics
        company = {
            "dot_number": dot_number,
            "mc_number": generate_mc_number(dot_number),
            "legal_name": generate_company_name(),
            "dba_name": [generate_company_name()] if random.random() > 0.7 else [],
            "entity_type": random.choice(["CARRIER", "BROKER", "CARRIER/BROKER"]),
            "authority_status": random.choices(["ACTIVE", "INACTIVE"], weights=[0.8, 0.2])[0],
            "safety_rating": random.choices(["SATISFACTORY", "CONDITIONAL", "UNSATISFACTORY", None], weights=[0.6, 0.2, 0.1, 0.1])[0],
            "operation_classification": random.choice(["AUTHORIZED_FOR_HIRE", "PRIVATE", "EXEMPT_FOR_HIRE"]),
            "company_type": random.choice(["LLC", "CORPORATION", "PARTNERSHIP", "SOLE_PROPRIETORSHIP"]),
            "ein": f"EIN-{random.randint(10, 99)}-{random.randint(1000000, 9999999)}",
            "total_drivers": random.randint(5, 100),
            "total_trucks": random.randint(5, 75),
            "total_trailers": random.randint(0, 50),
            "chameleon_risk_score": round(random.random() * 0.3, 2),  # Low risk
            "safety_risk_score": round(random.random() * 0.4, 2),
            "financial_risk_score": round(random.random() * 0.3, 2),
            "created_date": created_date.isoformat(),
            "mcs150_date": (created_date + timedelta(days=random.randint(30, 730))).isoformat(),
            "insurance_minimum": random.choice([750000, 1000000, 5000000]),
            "cargo_carried": random.choice(CARGO_TYPES),
            "data_completeness_score": round(0.8 + random.random() * 0.2, 2),
            "_pattern": "NORMAL",
            "_city": city,
            "_state": state,
            "_zip": zip_code
        }
        
        # Add some large legitimate carriers
        if random.random() < 0.1:  # 10% are large carriers
            company["total_drivers"] = random.randint(100, 1000)
            company["total_trucks"] = random.randint(100, 750)
            company["total_trailers"] = random.randint(100, 500)
            company["is_publicly_traded"] = True
            company["parent_company_name"] = f"{company['legal_name'].split()[0]} Holdings"
            company["_pattern"] = "LARGE_CARRIER"
        
        companies.append(company)
    
    # Add some companies with sequential DOT numbers (suspicious pattern)
    sequential_start = 900000
    for i in range(5):
        dot_number = sequential_start + i
        if dot_number not in used_dot_numbers:
            city, state, zip_code = random.choice(CITIES)
            company = {
                "dot_number": dot_number,
                "mc_number": f"MC-{dot_number:06d}",
                "legal_name": f"Quick Start Transport {i+1} LLC",
                "entity_type": "CARRIER",
                "authority_status": "ACTIVE",
                "safety_rating": None,  # Too new
                "total_drivers": random.randint(1, 5),
                "total_trucks": random.randint(1, 3),
                "chameleon_risk_score": 0.85,  # Very suspicious
                "created_date": (date.today() - timedelta(days=30+i*7)).isoformat(),
                "_pattern": "SEQUENTIAL_DOT",
                "_city": city,
                "_state": state,
                "_zip": zip_code
            }
            companies.append(company)
    
    return companies, chameleon_groups


def save_to_csv(companies, filename="companies.csv"):
    """Save companies to CSV file"""
    if not companies:
        return
    
    # Get all unique keys across all companies
    all_keys = set()
    for company in companies:
        all_keys.update(company.keys())
    
    # Remove internal pattern tracking fields from CSV
    fieldnames = [k for k in all_keys if not k.startswith("_")]
    fieldnames.sort()
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for company in companies:
            # Write only non-internal fields
            row = {k: v for k, v in company.items() if not k.startswith("_")}
            # Convert lists to JSON strings for CSV
            for key, value in row.items():
                if isinstance(value, list):
                    row[key] = json.dumps(value)
            writer.writerow(row)
    
    print(f"Generated {len(companies)} companies saved to {filename}")


def save_patterns_report(companies, chameleon_groups, filename="patterns_report.txt"):
    """Save a report of the suspicious patterns in the data"""
    with open(filename, 'w') as f:
        f.write("SUSPICIOUS PATTERNS IN GENERATED DATA\n")
        f.write("=" * 50 + "\n\n")
        
        # Count patterns
        pattern_counts = {}
        for company in companies:
            pattern = company.get("_pattern", "UNKNOWN")
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
        
        f.write("PATTERN SUMMARY:\n")
        for pattern, count in pattern_counts.items():
            f.write(f"  {pattern}: {count} companies\n")
        f.write("\n")
        
        # Chameleon groups
        f.write("CHAMELEON CARRIER GROUPS:\n")
        f.write("-" * 30 + "\n")
        for group_id, group_data in chameleon_groups.items():
            f.write(f"\nGroup: {group_id}\n")
            f.write(f"  Base Name: {group_data['base_name']}\n")
            f.write(f"  Shared EIN: {group_data['shared_ein']}\n")
            f.write(f"  Shared Location: {group_data['shared_address']}\n")
            f.write(f"  DOT Numbers: {group_data['companies']}\n")
            
            # Show the progression
            group_companies = [c for c in companies if c.get('_group_id') == group_id]
            for gc in sorted(group_companies, key=lambda x: x['created_date']):
                f.write(f"    - {gc['dot_number']}: {gc['legal_name']} ({gc['authority_status']}) - Created: {gc['created_date']}\n")
        
        # Sequential DOT numbers
        f.write("\n\nSEQUENTIAL DOT NUMBERS:\n")
        f.write("-" * 30 + "\n")
        sequential = [c for c in companies if c.get('_pattern') == 'SEQUENTIAL_DOT']
        for sc in sorted(sequential, key=lambda x: x['dot_number']):
            f.write(f"  {sc['dot_number']}: {sc['legal_name']} - Created: {sc['created_date']}\n")
        
        # High risk companies
        f.write("\n\nHIGH RISK COMPANIES (Chameleon Score > 0.7):\n")
        f.write("-" * 30 + "\n")
        high_risk = [c for c in companies if c.get('chameleon_risk_score', 0) > 0.7]
        for hr in sorted(high_risk, key=lambda x: x.get('chameleon_risk_score', 0), reverse=True)[:10]:
            f.write(f"  {hr['dot_number']}: {hr['legal_name']} - Risk Score: {hr.get('chameleon_risk_score', 0)}\n")
        
        # Location clusters
        f.write("\n\nLOCATION CLUSTERS:\n")
        f.write("-" * 30 + "\n")
        location_counts = {}
        for company in companies:
            location = f"{company.get('_city', 'Unknown')}, {company.get('_state', 'Unknown')}"
            if location not in location_counts:
                location_counts[location] = []
            location_counts[location].append(company['dot_number'])
        
        for location, dots in sorted(location_counts.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
            f.write(f"  {location}: {len(dots)} companies\n")
            if len(dots) > 10:
                f.write(f"    Sample DOTs: {dots[:10]}...\n")
        
    print(f"Patterns report saved to {filename}")


def main():
    """Main function to generate test data"""
    print("Generating realistic trucking company test data...")
    print("-" * 50)
    
    # Generate companies
    companies, chameleon_groups = generate_companies(100)
    
    # Save to CSV
    save_to_csv(companies, "companies.csv")
    
    # Save patterns report
    save_patterns_report(companies, chameleon_groups, "patterns_report.txt")
    
    print("\nGeneration complete!")
    print(f"Files created:")
    print("  - companies.csv: Import this into your API")
    print("  - patterns_report.txt: Documentation of suspicious patterns")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"  Total Companies: {len(companies)}")
    print(f"  Chameleon Groups: {len(chameleon_groups)}")
    print(f"  High Risk Companies: {len([c for c in companies if c.get('chameleon_risk_score', 0) > 0.7])}")
    print(f"  Active Companies: {len([c for c in companies if c.get('authority_status') == 'ACTIVE'])}")
    print(f"  Inactive/Revoked: {len([c for c in companies if c.get('authority_status') in ['INACTIVE', 'REVOKED']])}")


if __name__ == "__main__":
    main()