"""Supabase 테이블 구조 빠른 확인 스크립트"""
import os
from supabase import create_client

url = os.environ.get("SUPABASE_URL", "https://ofclchxfrjldmrzswgwi.supabase.co")
key = os.environ["SUPABASE_KEY"]
s = create_client(url, key)

# 1. daily_supply: subject, direction 종류
print("=== daily_supply subject 종류 ===")
r = s.table("daily_supply").select("subject, direction").limit(5000).execute()
subjects = set()
directions = set()
for d in r.data:
    subjects.add(d.get("subject"))
    directions.add(d.get("direction"))
print(f"subject: {subjects}")
print(f"direction: {directions}")
print(f"샘플 수: {len(r.data)}")

# 2. 전체 테이블 존재 확인
print("\n=== 테이블 존재 확인 ===")
tables = [
    "daily_supply", "daily_market", "daily_ohlcv", "daily_index",
    "sr_supply_grades", "sr_supply_data", "top3_history",
    "sector_map", "rs_leaders", "force_buy_sell_lines",
    "market_indicators", "supply_ewma_cache",
]
for t in tables:
    try:
        r = s.table(t).select("id").limit(1).execute()
        if r.data:
            print(f"  {t}: OK (데이터 있음)")
        else:
            print(f"  {t}: EMPTY (테이블 있으나 비어있음)")
    except Exception as e:
        msg = str(e)
        if "PGRST205" in msg or "not find" in msg:
            print(f"  {t}: NOT FOUND")
        else:
            print(f"  {t}: ERROR - {msg[:80]}")

# 3. daily_supply 최신 데이터 상세
print("\n=== daily_supply 최신 1건 상세 ===")
r = s.table("daily_supply").select("*").order("date", desc=True).limit(1).execute()
if r.data:
    for k, v in r.data[0].items():
        print(f"  {k}: {v} ({type(v).__name__})")

# 4. daily_market 최신 데이터 상세
print("\n=== daily_market 최신 1건 상세 ===")
r = s.table("daily_market").select("*").order("date", desc=True).limit(1).execute()
if r.data:
    for k, v in r.data[0].items():
        print(f"  {k}: {v} ({type(v).__name__})")
