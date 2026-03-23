import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
headers = {'apikey': key, 'Authorization': f'Bearer {key}'}

# daily_supply 3/23
r1 = requests.get(f'{url}/rest/v1/daily_supply?trade_date=eq.2026-03-23&limit=2000', headers=headers)
with open('export_supply_0323.json', 'w', encoding='utf-8') as f:
    json.dump(r1.json(), f, ensure_ascii=False)
print(f'daily_supply: {len(r1.json())} rows saved')

# daily_market 3/23
r2 = requests.get(f'{url}/rest/v1/daily_market?trade_date=eq.2026-03-23&limit=2000', headers=headers)
with open('export_market_0323.json', 'w', encoding='utf-8') as f:
    json.dump(r2.json(), f, ensure_ascii=False)
print(f'daily_market: {len(r2.json())} rows saved')
