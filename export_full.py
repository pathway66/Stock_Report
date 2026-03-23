import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Prefer': 'count=exact'}

def fetch_all(table, query_params=""):
    all_rows = []
    offset = 0
    batch = 1000
    while True:
        r = requests.get(
            f'{url}/rest/v1/{table}?{query_params}&offset={offset}&limit={batch}',
            headers=headers
        )
        rows = r.json()
        if not rows:
            break
        all_rows.extend(rows)
        total = r.headers.get('content-range', '')
        print(f"  {table}: fetched {len(all_rows)} rows... ({total})")
        if len(rows) < batch:
            break
        offset += batch
    return all_rows

# 1. daily_supply 3/23
print("=== Fetching daily_supply 3/23 ===")
supply = fetch_all('daily_supply', 'date=eq.2026-03-23&order=id')
with open('export_supply_0323.json', 'w', encoding='utf-8') as f:
    json.dump(supply, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(supply)} rows saved\n")

# 2. daily_market 3/23
print("=== Fetching daily_market 3/23 ===")
market = fetch_all('daily_market', 'date=eq.2026-03-23&order=id')
with open('export_market_0323.json', 'w', encoding='utf-8') as f:
    json.dump(market, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(market)} rows saved\n")

# 3. analysis_scores 3/23
print("=== Fetching analysis_scores 3/23 ===")
scores = fetch_all('analysis_scores', 'date=eq.2026-03-23&order=final_score.desc')
with open('export_scores_0323.json', 'w', encoding='utf-8') as f:
    json.dump(scores, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(scores)} rows saved\n")

# 4. sector_map (full)
print("=== Fetching sector_map ===")
smap = fetch_all('sector_map', 'order=id')
with open('export_sector_map.json', 'w', encoding='utf-8') as f:
    json.dump(smap, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(smap)} rows saved\n")

# 5. top3_history (full)
print("=== Fetching top3_history ===")
top3 = fetch_all('top3_history', 'order=date.desc')
with open('export_top3_history.json', 'w', encoding='utf-8') as f:
    json.dump(top3, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(top3)} rows saved\n")

# 6. daily_supply ALL dates (for weight calculation)
print("=== Fetching ALL daily_supply ===")
supply_all = fetch_all('daily_supply', 'order=id')
with open('export_supply_all.json', 'w', encoding='utf-8') as f:
    json.dump(supply_all, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(supply_all)} rows saved\n")

# 7. daily_market ALL dates
print("=== Fetching ALL daily_market ===")
market_all = fetch_all('daily_market', 'order=id')
with open('export_market_all.json', 'w', encoding='utf-8') as f:
    json.dump(market_all, f, ensure_ascii=False, indent=2)
print(f"  TOTAL: {len(market_all)} rows saved\n")

print("="*50)
print("DONE! Upload these files to Claude:")
print("  1. export_supply_0323.json")
print("  2. export_market_0323.json")
print("  3. export_scores_0323.json")
print("  4. export_sector_map.json")
print("  5. export_top3_history.json")
print("  6. export_supply_all.json")
print("  7. export_market_all.json")
