import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
headers = {'apikey': key, 'Authorization': f'Bearer {key}'}

# 1. Check what dates exist in daily_supply
r1 = requests.get(f'{url}/rest/v1/daily_supply?select=trade_date&order=trade_date.desc&limit=20', headers=headers)
print("=== daily_supply recent dates ===")
dates = set()
for row in r1.json():
    dates.add(row['trade_date'])
for d in sorted(dates, reverse=True):
    print(f"  {d}")

# 2. Check what dates exist in daily_market
r2 = requests.get(f'{url}/rest/v1/daily_market?select=trade_date&order=trade_date.desc&limit=20', headers=headers)
print("\n=== daily_market recent dates ===")
dates2 = set()
for row in r2.json():
    dates2.add(row['trade_date'])
for d in sorted(dates2, reverse=True):
    print(f"  {d}")

# 3. Show sample data for most recent date
print("\n=== daily_supply sample (latest) ===")
r3 = requests.get(f'{url}/rest/v1/daily_supply?order=trade_date.desc&limit=3', headers=headers)
for row in r3.json():
    print(json.dumps(row, ensure_ascii=False)[:200])

print("\n=== daily_market sample (latest) ===")
r4 = requests.get(f'{url}/rest/v1/daily_market?order=trade_date.desc&limit=3', headers=headers)
for row in r4.json():
    print(json.dumps(row, ensure_ascii=False)[:200])

# 4. Check all tables
for table in ['daily_supply', 'daily_market', 'analysis_scores', 'sector_map', 'top3_history']:
    r = requests.get(f'{url}/rest/v1/{table}?select=*&limit=1', headers=headers)
    print(f"\n{table}: status={r.status_code}, rows={len(r.json())}")
    if r.json():
        print(f"  columns: {list(r.json()[0].keys())}")
