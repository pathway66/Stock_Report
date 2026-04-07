"""전체 테이블 날짜 분포 확인"""
import os
from supabase import create_client

url = os.environ.get("SUPABASE_URL", "https://ofclchxfrjldmrzswgwi.supabase.co")
key = os.environ["SUPABASE_KEY"]
s = create_client(url, key)

def get_all_dates(table, date_col="date"):
    """페이징으로 전체 날짜 수집"""
    dates = set()
    offset = 0
    step = 1000
    while True:
        r = s.table(table).select(date_col).order("id").range(offset, offset + step - 1).execute()
        if not r.data:
            break
        dates.update(d[date_col] for d in r.data)
        offset += step
    return sorted(dates, reverse=True)

tables = ["daily_supply", "daily_market", "daily_ohlcv", "daily_index", "force_buy_sell_lines"]

for t in tables:
    print(f"\n=== {t} ===")
    try:
        dates = get_all_dates(t)
        print(f"  총 {len(dates)}일")
        print(f"  최신: {dates[0] if dates else 'N/A'}")
        print(f"  최초: {dates[-1] if dates else 'N/A'}")
        # 최근 10일 출력
        for d in dates[:10]:
            print(f"    {d}")
    except Exception as e:
        print(f"  ERROR: {e}")
