"""
전종목 일일 수집 통합 스크립트 v1.0
=====================================
수급(ka10059) + OHLCV(ka10081) 통합, 병렬 처리

- 일반주: 수급(4회) + OHLCV(1회) = 5 API 호출/종목
- ETF:   OHLCV(1회)만 = 1 API 호출/종목
- 2워커 병렬 처리 (API rate limit 준수)

사용법:
  python collect_daily_all.py              -> 오늘 날짜
  python collect_daily_all.py 20260414     -> 특정 날짜
"""

import os
import sys
import time
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

load_dotenv()

APP_KEY = os.getenv("KIWOOM_APP_KEY", "")
SECRET_KEY = os.getenv("KIWOOM_SECRET_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BASE_URL = "https://api.kiwoom.com"

NUM_WORKERS = 2
API_DELAY = 0.25  # 워커당 호출 간격 (초)

ETF_KW = ['KODEX','TIGER','ACE','RISE','PLUS','SOL','HANARO','KIWOOM',
          'KoAct','TIME','ETN','KOSEF']

INVESTOR_FIELDS = {
    "ind_invsr": "개인", "frgnr_invsr": "외국인", "orgn": "기관계",
    "fnnc_invt": "금융투자", "insrnc": "보험", "invtrt": "투신",
    "etc_fnnc": "기타금융", "bank": "은행", "penfnd_etc": "연기금",
    "samo_fund": "사모펀드", "natn": "국가", "etc_corp": "기타법인",
    "natfor": "내외국인",
}

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=merge-duplicates"
}

# Thread-safe API rate limiter
api_lock = threading.Lock()
last_api_call = [0.0]  # mutable for closure


def rate_limited_post(url, headers, json_body, timeout=15):
    """Thread-safe API call with global rate limiting"""
    with api_lock:
        now = time.time()
        elapsed = now - last_api_call[0]
        if elapsed < API_DELAY:
            time.sleep(API_DELAY - elapsed)
        last_api_call[0] = time.time()
    return requests.post(url, headers=headers, json=json_body, timeout=timeout)


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


def get_all_stocks(token):
    """ka10099로 전종목 조회, ETF 분류"""
    all_stocks = []
    for mrkt_tp, market_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}",
            "api-id": "ka10099",
        }
        try:
            resp = requests.post(f"{BASE_URL}/api/dostk/stkinfo",
                                 headers=headers, json={"mrkt_tp": mrkt_tp}, timeout=15)
            data = resp.json()
            if data.get('return_code') != 0:
                continue
            items = []
            for key, val in data.items():
                if isinstance(val, list) and len(val) > 0:
                    items = val
                    break
            for item in items:
                code = item.get('code', '').strip()
                name = item.get('name', '').strip()
                if code and name and len(code) == 6:
                    is_etf = any(kw in name for kw in ETF_KW)
                    all_stocks.append({
                        'stock_code': code,
                        'stock_name': name,
                        'market': market_name,
                        'is_etf': is_etf,
                    })
            print(f"  [i] {market_name}: {len(items)}종목")
            time.sleep(0.3)
        except Exception as e:
            print(f"  [W] ka10099 {market_name}: {e}")

    etf_count = sum(1 for s in all_stocks if s['is_etf'])
    stock_count = len(all_stocks) - etf_count
    print(f"  [i] 합계: {len(all_stocks)}종목 (일반 {stock_count} + ETF {etf_count})")
    return all_stocks


def collect_supply(token, stock, target_dt, dt_fmt):
    """1종목 수급 수집 (ka10059) -> 13 rows"""
    code = stock['stock_code']
    api_headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10059",
    }
    combos = [("1", "1", "buy_amt"), ("1", "2", "sell_amt"),
              ("2", "1", "buy_qty"), ("2", "2", "sell_qty")]
    merged = {}

    for amt_qty_tp, trde_tp, field_name in combos:
        try:
            resp = rate_limited_post(
                f"{BASE_URL}/api/dostk/stkinfo", api_headers,
                {"dt": target_dt, "stk_cd": code,
                 "amt_qty_tp": amt_qty_tp, "trde_tp": trde_tp, "unit_tp": "1"})
            data = resp.json()
            if data.get('return_code') != 0:
                continue
            items = data.get('stk_invsr_orgn', [])
            item = None
            for it in items:
                if it.get('dt') == target_dt:
                    item = it
                    break
            if not item and items:
                item = items[0]
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
        except Exception as e:
            pass

    rows = []
    for subject, vals in merged.items():
        rows.append({
            "date": dt_fmt, "market": stock['market'],
            "stock_code": code, "stock_name": stock['stock_name'],
            "subject": subject,
            "buy_amt": vals["buy_amt"], "sell_amt": vals["sell_amt"],
            "buy_qty": vals["buy_qty"], "sell_qty": vals["sell_qty"],
        })
    return rows


def collect_ohlcv(token, stock, target_dt, dt_fmt):
    """1종목 OHLCV + 시총 + 52주고저 수집 (ka10081 + ka10001) -> dict or None"""
    code = stock['stock_code']

    def to_int(v):
        try:
            return int(str(v).replace(',', '').replace('+', '').replace('-', '').strip())
        except:
            return 0

    # --- 1) OHLCV (ka10081) ---
    try:
        resp = rate_limited_post(
            f"{BASE_URL}/api/dostk/chart",
            {"Content-Type": "application/json;charset=UTF-8",
             "authorization": f"Bearer {token}", "api-id": "ka10081"},
            {"stk_cd": code, "base_dt": target_dt, "upd_stkpc_tp": "1"})
        data = resp.json()
        if data.get('return_code') != 0:
            return None

        chart_items = []
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0:
                chart_items = val
                break
        if not chart_items:
            return None

        item = chart_items[0]
        if not item.get('dt', ''):
            return None

        close_p = to_int(item.get('cur_prc', 0))
        if close_p == 0:
            return None

        pred_pre = int(str(item.get('pred_pre', '0')).replace(',', '').strip()) if item.get('pred_pre') else 0
        prev_close = close_p - pred_pre
        change_pct = round(pred_pre / prev_close * 100, 2) if prev_close > 0 else 0.0

        volume = abs(to_int(item.get('trde_qty', 0)))
        close_abs = abs(close_p)
        # trade_value: 키움 trde_prica(억원 단위)가 응답에 있으면 사용, 없으면 close*volume 근사
        # CSV(kiwoom_collector_v3) 와 동일 로직: trde_prica * 100M = 원
        trade_value = None
        tprica_raw = item.get('trde_prica') or item.get('trde_pre')
        if tprica_raw:
            try:
                trade_value = int(float(str(tprica_raw).replace(',', '').lstrip('+').lstrip('-')) * 100_000_000)
            except Exception:
                trade_value = None
        if trade_value is None or trade_value <= 0:
            trade_value = close_abs * volume  # 근사 (종가 × 거래량)

        result = {
            "open": abs(to_int(item.get('open_pric', 0))),
            "high": abs(to_int(item.get('high_pric', 0))),
            "low": abs(to_int(item.get('low_pric', 0))),
            "close": close_abs,
            "volume": volume,
            "trade_value": trade_value,
            "change_pct": change_pct,
        }
    except:
        return None

    # --- 2) 시총 + 52주고저 (ka10001) ---
    try:
        resp2 = rate_limited_post(
            f"{BASE_URL}/api/dostk/stkinfo",
            {"Content-Type": "application/json;charset=UTF-8",
             "authorization": f"Bearer {token}", "api-id": "ka10001"},
            {"stk_cd": code})
        d2 = resp2.json()
        if d2.get('return_code') == 0:
            mac = to_int(d2.get('mac', '0'))
            result["market_cap"] = mac * 100_000_000  # 억원 -> 원
            result["listed_shares"] = to_int(d2.get('flo_stk', '0')) * 1000
            result["high_52w"] = to_int(d2.get('250hgst', '0'))
            result["low_52w"] = to_int(d2.get('250lwst', '0'))
    except:
        pass  # 시총 실패해도 OHLCV는 반환

    return result


def upsert_rows(rows):
    """daily_supply_v2에 upsert"""
    if not rows:
        return 0
    count = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        url = f"{SUPABASE_URL}/rest/v1/daily_supply_v2?on_conflict=date,stock_code,subject"
        resp = requests.post(url, headers=SB_HEADERS, json=batch)
        if resp.status_code in [200, 201, 204]:
            count += len(batch)
    return count


def patch_ohlcv(stock_code, date_fmt, ohlcv):
    """daily_supply_v2의 해당 종목 행에 OHLCV PATCH"""
    if not ohlcv:
        return False
    patch_headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    url = f"{SUPABASE_URL}/rest/v1/daily_supply_v2?date=eq.{date_fmt}&stock_code=eq.{stock_code}"
    resp = requests.patch(url, headers=patch_headers, json=ohlcv)
    return resp.status_code in [200, 204]


def process_stock(token, stock, target_dt, dt_fmt):
    """1종목 통합 처리: 수급 + OHLCV"""
    code = stock['stock_code']
    name = stock['stock_name']
    is_etf = stock['is_etf']
    result = {"code": code, "name": name, "supply_rows": 0, "ohlcv": False}

    # 1. 수급 (ETF 제외)
    if not is_etf:
        rows = collect_supply(token, stock, target_dt, dt_fmt)
        if rows:
            result["supply_rows"] = upsert_rows(rows)

    # 2. OHLCV (전종목)
    ohlcv = collect_ohlcv(token, stock, target_dt, dt_fmt)
    if ohlcv:
        if not is_etf and result["supply_rows"] > 0:
            # 수급 행이 있으면 PATCH
            result["ohlcv"] = patch_ohlcv(code, dt_fmt, ohlcv)
        else:
            # ETF 또는 수급 없는 종목: OHLCV 전용 행 upsert
            ohlcv_row = {
                "date": dt_fmt, "market": stock['market'],
                "stock_code": code, "stock_name": name,
                "subject": "개인",  # 대표 행 1개
                **ohlcv,
            }
            url = f"{SUPABASE_URL}/rest/v1/daily_supply_v2?on_conflict=date,stock_code,subject"
            resp = requests.post(url, headers=SB_HEADERS, json=[ohlcv_row])
            result["ohlcv"] = resp.status_code in [200, 201, 204]

    return result


def main():
    if len(sys.argv) > 1:
        target_dt = sys.argv[1].replace('-', '')
    else:
        target_dt = datetime.now().strftime("%Y%m%d")

    dt_fmt = f"{target_dt[:4]}-{target_dt[4:6]}-{target_dt[6:8]}"

    print(f"{'='*60}")
    print(f"  Daily Collector v1.0 (Supply + OHLCV 통합)")
    print(f"  Date: {dt_fmt} | Workers: {NUM_WORKERS}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # 1. Token
    token = get_token()
    if not token:
        return 1
    print(f"[1] Token OK")

    # 2. 전종목 조회
    print(f"[2] 전종목 조회 (ka10099)...")
    stocks = get_all_stocks(token)
    if not stocks:
        print("[X] 종목 없음")
        return 1

    etf_list = [s for s in stocks if s['is_etf']]
    stock_list = [s for s in stocks if not s['is_etf']]
    print(f"    수급+OHLCV: {len(stock_list)}종목 (일반)")
    print(f"    OHLCV만:    {len(etf_list)}종목 (ETF)")

    # 3. 병렬 수집
    print(f"\n[3] 수집 시작 ({NUM_WORKERS}워커 병렬)...")
    start_time = time.time()
    total_supply = 0
    total_ohlcv = 0
    errors = 0
    processed = 0
    total = len(stocks)
    token_refresh_time = time.time()

    def worker_fn(stock):
        nonlocal token, token_refresh_time
        # 토큰 갱신 (20분마다, thread-safe)
        with api_lock:
            if time.time() - token_refresh_time > 1200:
                new_token = get_token()
                if new_token:
                    token = new_token
                    token_refresh_time = time.time()
        return process_stock(token, stock, target_dt, dt_fmt)

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = {executor.submit(worker_fn, s): s for s in stocks}

        for future in as_completed(futures):
            processed += 1
            try:
                result = future.result()
                total_supply += result["supply_rows"]
                if result["ohlcv"]:
                    total_ohlcv += 1
            except Exception as e:
                errors += 1

            if processed % 100 == 0 or processed == total:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = (total - processed) / rate if rate > 0 else 0
                print(f"  [{processed}/{total}] "
                      f"수급:{total_supply}행 OHLCV:{total_ohlcv}건 "
                      f"에러:{errors} ~{remaining/60:.0f}min")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"  [DONE] 수집 완료!")
    print(f"  수급: {total_supply}행 ({total_supply//13}종목)")
    print(f"  OHLCV: {total_ohlcv}건")
    print(f"  에러: {errors}건")
    print(f"  소요: {elapsed/60:.1f}분")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
