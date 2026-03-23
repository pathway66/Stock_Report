"""
섹터맵 fullversion을 Supabase sector_map 테이블에 업로드
사용법: python upload_sectormap.py
"""
import csv
import requests
import os
import time
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=merge-duplicates"
}

# 섹터맵 로드
rows = []
with open('./섹터맵_종목별2.csv', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        code = row['종목코드'].strip().zfill(6)
        name = row['종목명'].strip()
        sector = row['섹터'].strip()
        if code and name and sector:
            rows.append({
                "stock_code": code,
                "stock_name": name,
                "sector": sector
            })

print(f"섹터맵: {len(rows)}종목 업로드 시작")

# 배치 업로드
batch_size = 500
success = 0
for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    url = f"{SUPABASE_URL}/rest/v1/sector_map?on_conflict=stock_code"
    resp = requests.post(url, headers=headers, json=batch)
    if resp.status_code in [200, 201, 204]:
        success += len(batch)
        print(f"  배치 {i//batch_size+1}: {len(batch)}건 성공 ({success}/{len(rows)})")
    else:
        print(f"  배치 {i//batch_size+1}: 오류 {resp.status_code} {resp.text[:200]}")
    time.sleep(0.5)

print(f"\n✅ 업로드 완료: {success}/{len(rows)}건")
