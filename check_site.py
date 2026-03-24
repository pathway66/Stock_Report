import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
headers = {
    'apikey': key,
    'Authorization': f'Bearer {key}',
    'Content-Type': 'application/json'
}

# 1. Check analysis_scores for 3/23
r = requests.get(f'{url}/rest/v1/analysis_scores?date=eq.2026-03-23&order=final_score.desc&limit=5', headers=headers)
scores = r.json()
print(f"=== analysis_scores 3/23: {len(scores)} rows (showing top 5) ===")
for s in scores[:5]:
    print(f"  {s['stock_name']} ({s['sector']}) - {s['final_score']}점")

# 2. Check top3_history for 3/23
r2 = requests.get(f'{url}/rest/v1/top3_history?date=eq.2026-03-23&order=rank', headers=headers)
top3 = r2.json()
print(f"\n=== top3_history 3/23 ===")
for t in top3:
    print(f"  {t['rank']}위: {t['stock_name']} ({t['sector']}) 점수:{t['score']} 기준가:{t['base_price']}")

# 3. Check if website would show correct data
r3 = requests.get(f'{url}/rest/v1/analysis_scores?order=date.desc&limit=1', headers=headers)
latest = r3.json()
if latest:
    print(f"\n=== Latest date in analysis_scores: {latest[0]['date']} ===")

r4 = requests.get(f'{url}/rest/v1/top3_history?order=date.desc&limit=3', headers=headers)
latest_top3 = r4.json()
if latest_top3:
    print(f"=== Latest date in top3_history: {latest_top3[0]['date']} ===")

print("\n[OK] If both show 2026-03-23, the website should already display the latest data!")
print("Check: https://ai-pathway-web.vercel.app/")
