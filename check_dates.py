import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
headers = {'apikey': key, 'Authorization': f'Bearer {key}'}

# 1. Get unique dates from daily_supply (most recent)
r1 = requests.get(f'{url}/rest/v1/daily_supply?select=date&order=date.desc&limit=1000', headers=headers)
dates = sorted(set(row['date'] for row in r1.json()), reverse=True)
print("=== daily_supply dates (recent 10) ===")
for d in dates[:10]:
    print(f"  {d}")
print(f"  Total unique dates: {len(dates)}")

# 2. Get unique dates from daily_market
r2 = requests.get(f'{url}/rest/v1/daily_market?select=date&order=date.desc&limit=1000', headers=headers)
dates2 = sorted(set(row['date'] for row in r2.json()), reverse=True)
print("\n=== daily_market dates (recent 10) ===")
for d in dates2[:10]:
    print(f"  {d}")
print(f"  Total unique dates: {len(dates2)}")

# 3. Check 3/23 specifically
r3 = requests.get(f'{url}/rest/v1/daily_supply?date=eq.2026-03-23&limit=5', headers=headers)
print(f"\n=== 3/23 daily_supply: {len(r3.json())} rows ===")

r4 = requests.get(f'{url}/rest/v1/daily_market?date=eq.2026-03-23&limit=5', headers=headers)
print(f"=== 3/23 daily_market: {len(r4.json())} rows ===")

# 4. Check all tables
for table in ['analysis_scores', 'sector_map', 'top3_history']:
    r = requests.get(f'{url}/rest/v1/{table}?limit=1', headers=headers)
    print(f"\n{table}: status={r.status_code}, rows={len(r.json())}")
    if r.json():
        print(f"  columns: {list(r.json()[0].keys())}")
