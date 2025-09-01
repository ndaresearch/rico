#!/usr/bin/env python3
"""Test the JB Hunt import script parsing"""

from scripts.ingest.jb_hunt_carriers_import import load_jb_hunt_carriers

carriers, providers = load_jb_hunt_carriers('csv/real_data/jb_hunt_carriers.csv')
print(f'Loaded {len(carriers)} carriers')
print(f'Found {len(providers)} unique insurance providers')
print('\nFirst 3 carriers:')
for i, c in enumerate(carriers[:3]):
    print(f'\nCarrier {i+1}:')
    print(f'  USDOT: {c["usdot"]}')
    print(f'  Name: {c["carrier_name"]}')
    print(f'  Insurance: {c["insurance_provider"]}')
    print(f'  Amount: ${c["insurance_amount"]:,.0f}' if c["insurance_amount"] else '  Amount: N/A')
    print(f'  Trucks: {c["trucks"]}')
    print(f'  Crashes: {c["crashes"]}')
    print(f'  Driver OOS Rate: {c["driver_oos_rate"]}%' if c["driver_oos_rate"] else '  Driver OOS Rate: N/A')

print(f'\nUnique insurance providers: {", ".join(providers[:5])}...')