"""
[*] daily_supply_v2.trade_value 백필
=====================================
kiwoom_data/data_auto_*.csv (~233개) 의 거래대금을
daily_supply_v2.trade_value 컬럼으로 백필.

CSV '거래대금' 단위: 억원 (예: "+112.74" = 112.74억)
DB trade_value 단위: 원 (bigint)
변환: float * 100_000_000

흐름:
  1) CSV 모두 읽어서 (date, stock_code, trade_value) 추출
  2) trade_value_stage 테이블에 batch INSERT (on_conflict 무시)
  3) UPDATE daily_supply_v2 FROM stage (모든 13개 주체 행에 동일 값)
  4) stage 테이블 DROP

이후 collect_daily_all.py 통합 시 일일 자동 수집됨.
"""

import os
import csv
import sys
import time
import requests
from dotenv import load_dotenv

load_dotenv(override=True)
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=ignore-duplicates",
}

CSV_DIR = r"C:/_Stock_Report/kiwoom_data"


def parse_trade_value(s: str):
    """CSV 거래대금 ('+112.74' = 112.74억원) → 원 (bigint)"""
    s = (s or '').strip().lstrip('+')
    if s.startswith('-'):
        s = s[1:]
    if not s:
        return None
    try:
        return int(float(s) * 100_000_000)
    except Exception:
        return None


def parse_date_from_filename(name: str):
    """data_auto_YYMMDD.csv → 20YY-MM-DD"""
    base = os.path.basename(name).replace('data_auto_', '').replace('.csv', '')
    if len(base) != 6:
        return None
    return f"20{base[:2]}-{base[2:4]}-{base[4:6]}"


def parse_csv(path: str):
    date = parse_date_from_filename(path)
    if not date:
        return []
    rows = []
    with open(path, 'r', encoding='utf-8-sig', newline='') as fp:
        reader = csv.DictReader(fp)
        for r in reader:
            code = (r.get('종목코드') or '').strip()
            tv = parse_trade_value(r.get('거래대금', ''))
            if code and tv and tv > 0:
                rows.append({'date': date, 'stock_code': code, 'trade_value': tv})
    return rows


def insert_batch(rows, batch=1000):
    url = f"{SUPABASE_URL}/rest/v1/trade_value_stage?on_conflict=date,stock_code"
    total = 0
    failed = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        try:
            resp = requests.post(url, headers=HEADERS, json=chunk, timeout=60)
            if resp.status_code in (200, 201, 204):
                total += len(chunk)
            else:
                failed += len(chunk)
                print(f"  [W] {resp.status_code} batch {i}: {resp.text[:200]}")
        except Exception as e:
            failed += len(chunk)
            print(f"  [X] batch {i} 오류: {e}")
        if (i // batch) % 50 == 0:
            print(f"    [progress] {total:,} inserted...", flush=True)
    return total, failed


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] env 미설정")
        return 1

    csvs = sorted(
        f for f in os.listdir(CSV_DIR)
        if f.startswith('data_auto_') and f.endswith('.csv')
    )
    print(f"[*] CSV {len(csvs)}개 파싱 중...")

    all_rows = []
    skipped_files = 0
    for f in csvs:
        path = os.path.join(CSV_DIR, f)
        try:
            rows = parse_csv(path)
            all_rows.extend(rows)
        except Exception as e:
            print(f"  [W] {f}: {e}")
            skipped_files += 1

    print(f"[OK] 파싱 완료 - 총 {len(all_rows):,} 행 (스킵 파일 {skipped_files}개)")
    if not all_rows:
        return 1

    print(f"\n[*] trade_value_stage INSERT 시작 ({len(all_rows):,} 행, 1000행 batch)")
    start = time.time()
    inserted, failed = insert_batch(all_rows)
    elapsed = time.time() - start
    print(f"\n[OK] INSERT 완료: {inserted:,} 성공 / {failed:,} 실패 ({elapsed:.0f}초)")
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
