"""
일일 수급 수집 (ka10059) → daily_supply_v2
=============================================
당일 1일치, 전종목(ETF 포함) 수집

사용법:
  python collect_supply_v2.py              -> 오늘 날짜
  python collect_supply_v2.py 20260413     -> 특정 날짜
"""

import requests
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

APP_KEY = os.getenv("KIWOOM_APP_KEY", "")
SECRET_KEY = os.getenv("KIWOOM_SECRET_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BASE_URL = "https://api.kiwoom.com"

INVESTOR_FIELDS = {
    "ind_invsr": "개인",
    "frgnr_invsr": "외국인",
    "orgn": "기관계",
    "fnnc_invt": "금융투자",
    "insrnc": "보험",
    "invtrt": "투신",
    "etc_fnnc": "기타금융",
    "bank": "은행",
    "penfnd_etc": "연기금",
    "samo_fund": "사모펀드",
    "natn": "국가",
    "etc_corp": "기타법인",
    "natfor": "내외국인",
}

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=merge-duplicates"
}


def get_token():
    resp = requests.post(
        f"{BASE_URL}/oauth2/token",
        headers={"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"},
        json={"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": SECRET_KEY},
        timeout=10
    )
    data = resp.json()
    if data.get('return_code') == 0:
        return data['token']
    print(f"[X] token fail: {data.get('return_msg')}")
    return None


def get_stock_list(token):
    """키움 ka10099 API로 전종목(ETF 포함) 리스트 조회"""
    all_stocks = []

    for mrkt_tp, market_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}",
            "api-id": "ka10099",
        }
        body = {"mrkt_tp": mrkt_tp}
        try:
            resp = requests.post(f"{BASE_URL}/api/dostk/stkinfo",
                                 headers=headers, json=body, timeout=15)
            data = resp.json()
            if data.get('return_code') != 0:
                print(f"  [W] ka10099 {market_name} 실패: {data.get('return_msg')}")
                continue

            # 응답에서 리스트 추출
            items = []
            for key, val in data.items():
                if isinstance(val, list) and len(val) > 0:
                    items = val
                    break

            for item in items:
                code = item.get('code', '').strip()
                name = item.get('name', '').strip()
                if code and name and len(code) == 6:
                    all_stocks.append({
                        'stock_code': code,
                        'stock_name': name,
                        'market': market_name,
                    })

            print(f"  [i] {market_name}: {len(items)}종목")
            time.sleep(0.3)
        except Exception as e:
            print(f"  [W] ka10099 {market_name} 에러: {e}")

    print(f"  [i] 전종목 합계: {len(all_stocks)}개 (ETF 포함)")

    # stock_sectors에 없는 종목 자동 등록
    _sync_stock_sectors(all_stocks)

    return all_stocks


def _sync_stock_sectors(stocks):
    """stock_sectors 테이블에 없는 종목 자동 추가"""
    auth = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

    # 기존 종목코드 조회
    existing = set()
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/stock_sectors?select=stock_code&limit=5000&offset={offset}"
        resp = requests.get(url, headers=auth)
        if resp.status_code != 200 or not resp.json():
            break
        data = resp.json()
        for r in data:
            existing.add(r['stock_code'])
        if len(data) < 5000:
            break
        offset += 5000

    # 신규 종목 추가
    new_rows = []
    for s in stocks:
        if s['stock_code'] not in existing:
            new_rows.append({
                'stock_code': s['stock_code'],
                'stock_name': s['stock_name'],
                'market': s['market'],
                'sector': 'ETF' if _is_etf(s['stock_name']) else '기타',
            })

    if new_rows:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }
        for i in range(0, len(new_rows), 500):
            batch = new_rows[i:i+500]
            url = f"{SUPABASE_URL}/rest/v1/stock_sectors?on_conflict=stock_code"
            requests.post(url, headers=headers, json=batch)
        print(f"  [+] stock_sectors 신규 {len(new_rows)}종목 추가")


def _is_etf(name):
    etf_kw = ['KODEX','TIGER','ACE','RISE','PLUS','SOL','HANARO','KIWOOM',
              'KoAct','TIME','ETN','KOSEF']
    return any(kw in name for kw in etf_kw)


def call_ka10059(token, stk_cd, dt, amt_qty_tp, trde_tp):
    """ka10059 1회 호출 (당일 데이터만)"""
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10059",
    }
    body = {
        "dt": dt,
        "stk_cd": stk_cd,
        "amt_qty_tp": str(amt_qty_tp),
        "trde_tp": str(trde_tp),
        "unit_tp": "1",
    }
    try:
        resp = requests.post(f"{BASE_URL}/api/dostk/stkinfo",
                             headers=headers, json=body, timeout=15)
        data = resp.json()
        if data.get('return_code') != 0:
            return None
        items = data.get('stk_invsr_orgn', [])
        # 당일 데이터만 필터
        for item in items:
            if item.get('dt') == dt:
                return item
        return items[0] if items else None
    except Exception as e:
        print(f"    [W] API err: {e}")
        return None


def collect_stock(token, stock, target_dt, dt_fmt):
    """1종목 당일 데이터 수집 → DB rows"""
    code = stock['stock_code']
    name = stock['stock_name']
    market = stock['market']

    # 4조합: (amt_qty_tp, trde_tp, field_name)
    combos = [
        ("1", "1", "buy_amt"),
        ("1", "2", "sell_amt"),
        ("2", "1", "buy_qty"),
        ("2", "2", "sell_qty"),
    ]

    merged = {}  # subject -> {buy_amt, sell_amt, buy_qty, sell_qty}

    for amt_qty_tp, trde_tp, field_name in combos:
        item = call_ka10059(token, code, target_dt, amt_qty_tp, trde_tp)
        if not item:
            continue

        for field_key, subject_name in INVESTOR_FIELDS.items():
            val_str = item.get(field_key, '0')
            try:
                val = int(str(val_str).replace(',', '').strip())
            except:
                val = 0

            if subject_name not in merged:
                merged[subject_name] = {"buy_amt": 0, "sell_amt": 0, "buy_qty": 0, "sell_qty": 0}
            merged[subject_name][field_name] = abs(val)

        time.sleep(0.25)

    rows = []
    for subject, vals in merged.items():
        rows.append({
            "date": dt_fmt,
            "market": market,
            "stock_code": code,
            "stock_name": name,
            "subject": subject,
            "buy_amt": vals["buy_amt"],
            "sell_amt": vals["sell_amt"],
            "buy_qty": vals["buy_qty"],
            "sell_qty": vals["sell_qty"],
        })
    return rows


def upsert_rows(rows):
    if not rows:
        return 0
    count = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i + 500]
        url = f"{SUPABASE_URL}/rest/v1/daily_supply_v2?on_conflict=date,stock_code,subject"
        resp = requests.post(url, headers=SB_HEADERS, json=batch)
        if resp.status_code in [200, 201, 204]:
            count += len(batch)
        else:
            print(f"  [W] DB err: {resp.status_code} {resp.text[:200]}")
    return count


def main():
    if len(sys.argv) > 1:
        target_dt = sys.argv[1].replace('-', '')
    else:
        target_dt = datetime.now().strftime("%Y%m%d")

    dt_fmt = f"{target_dt[:4]}-{target_dt[4:6]}-{target_dt[6:8]}"

    print(f"[*] daily_supply_v2 일일수집 (ka10059)")
    print(f"    날짜: {dt_fmt}")

    # 1. 토큰
    token = get_token()
    if not token:
        return 1

    # 2. 종목 리스트 (전종목, ETF 포함)
    stocks = get_stock_list(token)
    if not stocks:
        print("[X] 종목 리스트 없음")
        return 1
    print(f"    종목: {len(stocks)}개 (전종목, ETF 포함)")

    # 3. 수집
    total_rows = 0
    api_calls = 0
    start = time.time()

    for i, stock in enumerate(stocks):
        code = stock['stock_code']
        name = stock['stock_name']

        if (i + 1) % 50 == 0 or i == 0:
            elapsed = time.time() - start
            remaining = (elapsed / max(i, 1)) * (len(stocks) - i)
            print(f"  [{i+1}/{len(stocks)}] {name}({code}) ~{remaining/60:.0f}min left")

        rows = collect_stock(token, stock, target_dt, dt_fmt)
        if rows:
            cnt = upsert_rows(rows)
            total_rows += cnt
        api_calls += 4

        # 토큰 갱신 (20분마다)
        if (i + 1) % 120 == 0:
            token = get_token() or token

    elapsed = time.time() - start
    print(f"  -> 완료: {total_rows}행, API {api_calls}회, {elapsed/60:.1f}분")
    return 0


if __name__ == "__main__":
    sys.exit(main())
