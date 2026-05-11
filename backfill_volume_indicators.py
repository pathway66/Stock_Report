"""
[*] 거래량/거래대금 3대 지표 백필 + 매일 갱신
=================================================
종목별 (date, stock_code)에 CMF/A-D/MFI 계산 → daily_supply_v2 UPDATE.

사용법:
  python backfill_volume_indicators.py                       # 1년치 백필
  python backfill_volume_indicators.py --backfill-from 2024-01-01
  python backfill_volume_indicators.py --date 20260508       # 특정일만 (run_daily 통합)

지표 정의:
  CMF20 = sum(MFM × volume, 20D) / sum(volume, 20D)
    MFM = ((close-low) - (high-close)) / (high-low)
  AD_LINE = 누적 sum(MFM × volume) [절대값보다 변화/다이버전스가 의미]
  MFI14 = 100 - 100/(1 + sum(PMF,14)/sum(NMF,14))
    TP = (high+low+close)/3, RMF = TP×volume
    PMF: TP↑ 일자의 RMF, NMF: TP↓ 일자의 RMF

워크플로:
  1) lookback (백필 시작일 - 250D)부터 OHLCV 로드 (split-retry)
  2) 종목별 시계열로 지표 계산
  3) stage 테이블에 (date, stock_code, cmf, ad, mfi) batch INSERT
  4) UPDATE daily_supply_v2 FROM stage
  5) stage DROP

ETF 등은 daily_supply_v2에 OHLCV 있으면 그대로 계산 (필터 X).
"""

import os
import sys
import time
import argparse
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
WRITE_HEADERS = {**HEADERS, "Content-Type": "application/json", "Prefer": "return=minimal"}


# ---------- Supabase REST ----------

def sb_get(table, params, page_size=1000):
    rows, offset = [], 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={page_size}&offset={offset}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code != 200:
                msg = r.text[:200]
                if '57014' in msg or 'timeout' in msg:
                    raise TimeoutError(f"PG timeout offset={offset}")
                print(f"  [E] {r.status_code} {msg}")
                break
            chunk = r.json()
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        except TimeoutError:
            raise
        except Exception as e:
            print(f"  [X] {e}")
            break
    return rows


def sb_get_split(table, base_params, start, end, init_days=30):
    def fetch(s, e, days):
        try:
            return sb_get(table, f"date=gte.{s}&date=lt.{e}&{base_params}")
        except TimeoutError:
            if days <= 3:
                return []
            mid_d = days // 2
            mid = (datetime.strptime(s, "%Y-%m-%d") + timedelta(days=mid_d)).strftime("%Y-%m-%d")
            print(f"  [retry] {s}~{e} → split @{mid}")
            return fetch(s, mid, mid_d) + fetch(mid, e, days - mid_d)

    rows = []
    cur = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur < end_dt:
        nxt_dt = min(cur + timedelta(days=init_days), end_dt + timedelta(days=1))
        s, e = cur.strftime("%Y-%m-%d"), nxt_dt.strftime("%Y-%m-%d")
        chunk = fetch(s, e, init_days)
        print(f"    {s}~{e}: {len(chunk):,} rows")
        rows.extend(chunk)
        cur = nxt_dt
    return rows


def sb_post(table, rows, batch=500, on_conflict=None):
    if not rows:
        return 0
    inserted = 0
    suffix = f"?on_conflict={on_conflict}" if on_conflict else ""
    url = f"{SUPABASE_URL}/rest/v1/{table}{suffix}"
    headers = {**WRITE_HEADERS, "Prefer": "return=minimal,resolution=ignore-duplicates"} if on_conflict else WRITE_HEADERS
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        try:
            r = requests.post(url, headers=headers, json=chunk, timeout=60)
            if r.status_code in (200, 201, 204):
                inserted += len(chunk)
            else:
                print(f"  [W] {r.status_code} batch {i}: {r.text[:150]}")
        except Exception as e:
            print(f"  [X] {e}")
        if (i // batch) % 50 == 0 and i > 0:
            print(f"    [progress] {inserted:,} inserted")
    return inserted


# ---------- 지표 계산 ----------

def calc_indicators(lst):
    """입력: 한 종목의 시계열 [{date, open, high, low, close, volume}, ...]
    출력: 각 인덱스의 cmf_20, ad_line, mfi_14 (None if 부족)
    """
    n = len(lst)
    if n < 20:
        return [(None, None, None)] * n

    # MFV = MFM × volume
    mfv = [0.0] * n
    for i in range(n):
        h, l, c, v = lst[i]['high'], lst[i]['low'], lst[i]['close'], lst[i]['volume']
        rng = h - l
        if rng <= 0 or v <= 0:
            mfv[i] = 0
        else:
            mfm = ((c - l) - (h - c)) / rng
            mfv[i] = mfm * v

    # CMF20
    cmf = [None] * n
    for i in range(19, n):
        sum_mfv = sum(mfv[i - 19:i + 1])
        sum_vol = sum(lst[j]['volume'] for j in range(i - 19, i + 1))
        cmf[i] = sum_mfv / sum_vol if sum_vol > 0 else 0

    # A/D Line (누적)
    ad = [0.0] * n
    cum = 0
    for i in range(n):
        cum += mfv[i]
        ad[i] = cum

    # MFI14
    mfi = [None] * n
    tp = [(lst[i]['high'] + lst[i]['low'] + lst[i]['close']) / 3 for i in range(n)]
    rmf = [tp[i] * lst[i]['volume'] for i in range(n)]
    for i in range(14, n):
        pmf = nmf = 0
        for j in range(i - 13, i + 1):
            if tp[j] > tp[j - 1]:
                pmf += rmf[j]
            elif tp[j] < tp[j - 1]:
                nmf += rmf[j]
        if nmf == 0:
            mfi[i] = 100
        else:
            mfi[i] = 100 - 100 / (1 + pmf / nmf)

    return [(cmf[i], ad[i], mfi[i]) for i in range(n)]


# ---------- 메인 ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='특정일만 처리 (YYYYMMDD)')
    ap.add_argument('--backfill-from', default='2025-05-08')
    ap.add_argument('--backfill-to', default=None)
    args = ap.parse_args()

    # ⚠ A/D Line은 누적값이라 lookback 시작점이 달라지면 값이 점프함.
    # 단일일 모드도 backfill_from (고정)부터 lookback하여 일관된 누적값 유지.
    AD_CUMULATIVE_START = args.backfill_from  # 기본 '2025-05-08'

    if args.date:
        target = datetime.strptime(args.date, "%Y%m%d").strftime("%Y-%m-%d")
        target_dates = {target}
        # A/D 누적 일관성: 항상 같은 시작점부터 lookback
        load_start = AD_CUMULATIVE_START
        load_end = target
        print(f"[*] 단일일 모드: {target} (누적 시작점 {load_start} 고정)")
    else:
        backfill_from = args.backfill_from
        load_end = args.backfill_to or datetime.now().strftime("%Y-%m-%d")
        load_start = backfill_from  # 누적 일관성 위해 정확한 시작점
        target_dates = None  # 모든 날짜 처리하되 백필 시작일 이후만 저장
        print(f"[*] 백필 모드: {backfill_from}~{load_end}")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] env 미설정"); return 1

    t0 = time.time()

    # 1) OHLCV 로드 (subject=개인 한 행만)
    print(f"[1] OHLCV 로딩 ({load_start}~{load_end})...")
    base = "subject=eq.개인&select=date,stock_code,open,high,low,close,volume"
    rows = sb_get_split('daily_supply_v2', base, load_start, load_end, init_days=30)
    print(f"    [OK] {len(rows):,} rows")

    # 종목별 그룹핑
    by_stock = defaultdict(list)
    for r in rows:
        by_stock[r['stock_code']].append({
            'date': r['date'],
            'open': r.get('open') or 0, 'high': r.get('high') or 0,
            'low': r.get('low') or 0, 'close': r.get('close') or 0,
            'volume': r.get('volume') or 0,
        })
    for sc in by_stock:
        by_stock[sc].sort(key=lambda x: x['date'])

    # 2) 지표 계산 + stage 행 생성
    print("[2] 지표 계산 + stage 데이터 준비...")
    stage_rows = []
    for sc, lst in by_stock.items():
        results = calc_indicators(lst)
        for i, (cmf, ad, mfi) in enumerate(results):
            d = lst[i]['date']
            # 단일일/백필 모드에 따라 필터
            if args.date and d not in target_dates:
                continue
            if not args.date and d < args.backfill_from:
                continue
            if cmf is None and mfi is None:
                continue  # 둘 다 None이면 의미 없음
            stage_rows.append({
                'date': d,
                'stock_code': sc,
                'cmf_20': round(cmf, 4) if cmf is not None else None,
                'ad_line': round(ad, 0) if ad is not None else None,
                'mfi_14': round(mfi, 2) if mfi is not None else None,
            })
    print(f"    [OK] stage {len(stage_rows):,} 행 준비")

    if not stage_rows:
        print("[!] 처리할 데이터 없음")
        return 0

    # 3) stage 테이블 생성
    print("[3] stage 테이블 생성...")
    stage_sql_url = f"{SUPABASE_URL}/rest/v1/rpc/exec_sql"
    # rpc/exec_sql 없을 수 있으니 직접 SQL execute는 별도. 여기선 supabase mgmt SQL 사용 X.
    # 대신 stage 테이블이 이미 만들어져있다고 가정 — 사전 마이그레이션 필요.
    # 또는 직접 INSERT만 함. 단일/백필 모두 대상 날짜 먼저 DELETE 후 INSERT.

    # 대상 날짜 기존 데이터의 cmf/ad/mfi 컬럼 NULL 처리는 UPDATE의 영역.
    # 여기선 stage 활용 X — daily_supply_v2 직접 UPDATE.
    # 그러나 REST API로 100만 행 UPDATE 너무 느림 → SQL UPDATE FROM이 필요.
    # 따라서 별도 stage 테이블 신설 + UPDATE FROM 패턴.

    # 단순화: stage 테이블 이미 마이그레이션으로 만들었거나, 매번 생성/삭제.
    # 본 스크립트에선 Supabase RPC 또는 mgmt API 없이는 stage 테이블 SQL 실행 어려움.
    # 차선: REST PATCH (행마다 UPDATE). 100만 행은 너무 느림.
    # 또는 daily_supply_v2_indicators_stage 테이블을 사전에 마이그레이션으로 만들어두기.

    # === 본 스크립트 전제: stage 테이블 daily_supply_v2_ind_stage가 이미 존재 ===
    print("[3] daily_supply_v2_ind_stage 비우기 (각 대상 날짜)...")
    if args.date:
        # 단일일 — 해당 날짜만 삭제
        del_url = f"{SUPABASE_URL}/rest/v1/daily_supply_v2_ind_stage?date=eq.{target}"
        requests.delete(del_url, headers=WRITE_HEADERS, timeout=60)
    else:
        # 백필 — 시작일 이후 모두 삭제
        del_url = f"{SUPABASE_URL}/rest/v1/daily_supply_v2_ind_stage?date=gte.{args.backfill_from}"
        requests.delete(del_url, headers=WRITE_HEADERS, timeout=60)

    print(f"[4] stage INSERT ({len(stage_rows):,} 행)...")
    inserted = sb_post('daily_supply_v2_ind_stage', stage_rows, batch=500)
    print(f"    [OK] {inserted:,} 행 INSERT 완료")

    # 5) RPC 호출 — stage → daily_supply_v2 UPDATE
    print(f"[5] RPC update_indicators_from_stage 호출...")
    rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/update_indicators_from_stage"
    if args.date:
        rpc_payload = {"target_date": target}
    else:
        rpc_payload = {}  # NULL → 전체
    try:
        r = requests.post(rpc_url, headers=WRITE_HEADERS, json=rpc_payload, timeout=300)
        if r.status_code == 200:
            print(f"    [OK] {r.text} rows updated")
        else:
            print(f"    [W] RPC {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"    [X] RPC 오류: {e}")

    print(f"\n[OK] {time.time() - t0:.0f}초")


if __name__ == '__main__':
    sys.exit(main() or 0)
