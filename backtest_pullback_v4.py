"""
=============================================================
  눌림목 반등 + 3가지 추가 필터 백테스트 v4
  backtest_pullback_v4.py  |  2026-04-15

  Base: RS 상위 + 눌림목 + 반등
  추가필터:
    1. 시장방향: 지수 > 지수의 20일MA (상승장)
    2. 거래량급증: 당일 거래량 > 20일평균 × 1.5
    3. 차트패턴(프록시): 종가 > 종목의 20일MA (상승추세)
=============================================================
"""

import os
import time
import requests
import numpy as np
from dotenv import load_dotenv
from collections import defaultdict

env_paths = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
    r"C:\Users\bebes\OneDrive\_Stock_Report\.env",
]
for p in env_paths:
    if os.path.exists(p):
        load_dotenv(p)
        break

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
FORWARD = 20


def fetch_all(table, select, filters="", limit=500000):
    all_rows, offset, ps = [], 0, 10000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
        if filters: url += f"&{filters}"
        url += f"&limit={ps}&offset={offset}"
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code not in [200, 206]:
            print(f"[E] {resp.status_code}")
            break
        data = resp.json()
        if not data: break
        all_rows.extend(data)
        if len(data) < ps or len(all_rows) >= limit: break
        offset += ps
    return all_rows


def load_data():
    print("=" * 70)
    print("  Pullback + Volume + Market + Pattern Backtest v4")
    print("=" * 70)

    # 종목 종가+거래량 (분기별)
    print("\n[1] Stock closes + volume (quarterly)...", flush=True)
    t0 = time.time()
    stock_closes = defaultdict(dict)
    stock_volumes = defaultdict(dict)
    total = 0
    quarters = [
        ("2024-05-01","2024-08-01"),("2024-08-01","2024-11-01"),
        ("2024-11-01","2025-02-01"),("2025-02-01","2025-05-01"),
        ("2025-05-01","2025-08-01"),("2025-08-01","2025-11-01"),
        ("2025-11-01","2026-02-01"),("2026-02-01","2026-05-01"),
    ]
    for d_from, d_to in quarters:
        print(f"    {d_from}~{d_to}...", end=" ", flush=True)
        rows = fetch_all("daily_supply_v2", "stock_code,date,close,volume",
                         f"subject=eq.개인&close=gt.0&date=gte.{d_from}&date=lt.{d_to}",
                         limit=200000)
        for r in rows:
            stock_closes[r['stock_code']][r['date']] = float(r['close'])
            if r.get('volume'):
                stock_volumes[r['stock_code']][r['date']] = float(r['volume'])
        total += len(rows)
        print(f"{len(rows)}")
        time.sleep(1)
    print(f"    Total: {total} rows, {len(stock_closes)} stocks ({time.time()-t0:.1f}s)")

    # 마켓맵
    print("[2] Market map...", end=" ", flush=True)
    rows = fetch_all("stock_sectors", "stock_code,market", limit=10000)
    market_map = {r['stock_code']: r['market'] for r in rows if r.get('market')}
    print(f"{len(market_map)}")

    # 지수 종가
    print("[3] Index closes...", end=" ", flush=True)
    rows = fetch_all("daily_index", "index_code,date,close", "index_code=in.(KOSPI,KOSDAQ)")
    index_closes = defaultdict(dict)
    for r in rows:
        index_closes[r['index_code']][r['date']] = float(r['close'])
    print(f"KOSPI {len(index_closes['KOSPI'])}d, KOSDAQ {len(index_closes['KOSDAQ'])}d")

    dates = sorted(set(d for sc in stock_closes.values() for d in sc.keys()))
    print(f"[4] Trading days: {dates[0]} ~ {dates[-1]} ({len(dates)}d)")

    return stock_closes, stock_volumes, market_map, index_closes, dates


def sma(values):
    """리스트의 단순평균"""
    return sum(values) / len(values) if values else 0


def collect_events(stock_closes, stock_volumes, market_map, index_closes, dates):
    print(f"\n[5] Collecting events with all filters...")
    start_idx = 120 + 1
    end_idx = len(dates) - FORWARD
    events = []

    # 지수 20일MA 사전 계산
    index_above_ma = {}  # {date: {KOSPI: bool, KOSDAQ: bool}}
    for mkt in ['KOSPI', 'KOSDAQ']:
        ic = index_closes[mkt]
        for i in range(20, len(dates)):
            d = dates[i]
            if d not in ic: continue
            ma20_vals = [ic.get(dates[j], 0) for j in range(i-19, i+1) if dates[j] in ic]
            if len(ma20_vals) >= 15:
                ma20 = sma(ma20_vals)
                if d not in index_above_ma:
                    index_above_ma[d] = {}
                index_above_ma[d][mkt] = ic[d] > ma20

    for date_idx in range(start_idx, end_idx):
        today = dates[date_idx]
        if date_idx % 50 == 0:
            print(f"  {today} ({date_idx-start_idx}/{end_idx-start_idx})", end="\r")

        d1 = dates[date_idx-1]
        d5 = dates[date_idx-5] if date_idx >= 5 else None
        d10 = dates[date_idx-10] if date_idx >= 10 else None
        d60 = dates[date_idx-60] if date_idx >= 60 else None
        fwd_date = dates[date_idx + FORWARD]

        # 해당일 모든 60D 초과수익 (백분위용)
        all_e60 = []
        stock_list = []
        for code in stock_closes:
            if code not in market_map: continue
            mkt = market_map[code]
            if mkt not in ('KOSPI','KOSDAQ'): continue
            sc = stock_closes[code]
            ic = index_closes[mkt]
            if not d60 or today not in sc or d60 not in sc or fwd_date not in sc: continue
            if today not in ic or d60 not in ic or fwd_date not in ic: continue
            e60 = (sc[today]/sc[d60]-1)*100 - (ic[today]/ic[d60]-1)*100
            all_e60.append(e60)
            stock_list.append((code, mkt, e60))

        if not all_e60: continue

        for code, mkt, e60 in stock_list:
            sc = stock_closes[code]
            ic = index_closes[mkt]
            sv = stock_volumes.get(code, {})

            # 백분위
            pctl = sum(1 for v in all_e60 if v < e60) / len(all_e60) * 100

            # 단기 초과수익
            e1d = None
            if d1 and d1 in sc and d1 in ic:
                e1d = (sc[today]/sc[d1]-1)*100 - (ic[today]/ic[d1]-1)*100
            e5d = None
            if d5 and d5 in sc and d5 in ic:
                e5d = (sc[today]/sc[d5]-1)*100 - (ic[today]/ic[d5]-1)*100
            e10d = None
            if d10 and d10 in sc and d10 in ic:
                e10d = (sc[today]/sc[d10]-1)*100 - (ic[today]/ic[d10]-1)*100

            # 1D 종목수익률
            s1d = (sc[today]/sc[d1]-1)*100 if d1 and d1 in sc else None

            # 고점대비 하락
            dd20 = None
            if date_idx >= 20:
                highs = [sc.get(dates[j],0) for j in range(date_idx-20, date_idx+1) if dates[j] in sc]
                if highs and max(highs) > 0:
                    dd20 = (sc[today]/max(highs)-1)*100

            # ── 추가 필터 계산 ──

            # [1] 시장방향: 지수 > 20일MA
            mkt_bullish = index_above_ma.get(today, {}).get(mkt, False)

            # [2] 거래량급증: 오늘 거래량 / 20일 평균 거래량
            vol_ratio = None
            if today in sv and date_idx >= 20:
                vol_list = [sv.get(dates[j], 0) for j in range(date_idx-20, date_idx) if dates[j] in sv]
                vol_list = [v for v in vol_list if v > 0]
                if len(vol_list) >= 10:
                    avg_vol = sma(vol_list)
                    if avg_vol > 0:
                        vol_ratio = sv[today] / avg_vol

            # [3] 차트패턴: 종가 > 20일MA
            above_ma20 = False
            if date_idx >= 20:
                ma_vals = [sc.get(dates[j], 0) for j in range(date_idx-19, date_idx+1) if dates[j] in sc]
                ma_vals = [v for v in ma_vals if v > 0]
                if len(ma_vals) >= 15:
                    above_ma20 = sc[today] > sma(ma_vals)

            # Forward
            fwd = (sc[fwd_date]/sc[today]-1)*100 - (ic[fwd_date]/ic[today]-1)*100

            events.append({
                'pctl': pctl, 'e60': e60,
                'e1d': e1d, 'e5d': e5d, 'e10d': e10d,
                's1d': s1d, 'dd20': dd20,
                'mkt_bullish': mkt_bullish,
                'vol_ratio': vol_ratio,
                'above_ma20': above_ma20,
                'fwd': fwd,
                'win': 1 if fwd > 0 else 0,
            })

    print(f"\n  Total events: {len(events):,}")
    return events


def h(events, fn, min_n=30):
    f = [e for e in events if fn(e)]
    n = len(f)
    if n < min_n: return n, 0, 0, 0
    w = sum(e['win'] for e in f)
    return n, w/n*100, np.mean([e['fwd'] for e in f]), np.median([e['fwd'] for e in f])


def run_analysis(events):
    print("\n" + "=" * 70)
    print("  Analysis: Pullback + Additional Filters")
    print("=" * 70)

    def p(label, fn):
        n, hit, avg, med = h(events, fn)
        flag = " ★★★" if hit >= 60 and n >= 30 else (" ★★" if hit >= 55 and n >= 30 else "")
        print(f"  {label:<55s} {n:>6,d} {hit:>5.1f}% {avg:>+6.2f}% {med:>+6.2f}%{flag}")

    # ── 베이스라인 ──
    print(f"\n  {'Condition':<55s} {'N':>6s} {'Hit%':>6s} {'Avg':>7s} {'Med':>7s}")
    print(f"  {'-'*85}")

    print("\n  [Base] RS상위 + 눌림목 + 반등")
    base = lambda e: e['pctl']>=90 and e['dd20'] is not None and e['dd20']<=-5 and e['e1d'] is not None and e['e1d']>0
    base_strong = lambda e: e['pctl']>=90 and e['dd20'] is not None and e['dd20']<=-5 and e['e1d'] is not None and e['e1d']>1
    p("pctl>=90 & dd<=-5% & e1d>0 (BASE)", base)
    p("pctl>=90 & dd<=-5% & e1d>1 (BASE_STRONG)", base_strong)

    # ── 1. 시장방향 추가 ──
    print("\n  [+Market] 상승장만")
    p("BASE + 상승장", lambda e: base(e) and e['mkt_bullish'])
    p("BASE_STRONG + 상승장", lambda e: base_strong(e) and e['mkt_bullish'])

    # ── 2. 거래량 추가 ──
    print("\n  [+Volume] 거래량 급증")
    for vt in [1.2, 1.5, 2.0, 2.5, 3.0]:
        p(f"BASE + vol>{vt}x",
          lambda e, vt=vt: base(e) and e['vol_ratio'] is not None and e['vol_ratio'] > vt)
    for vt in [1.2, 1.5, 2.0, 2.5]:
        p(f"BASE_STRONG + vol>{vt}x",
          lambda e, vt=vt: base_strong(e) and e['vol_ratio'] is not None and e['vol_ratio'] > vt)

    # ── 3. 차트패턴 추가 ──
    print("\n  [+Pattern] 20일MA 위")
    p("BASE + above_ma20", lambda e: base(e) and e['above_ma20'])
    p("BASE_STRONG + above_ma20", lambda e: base_strong(e) and e['above_ma20'])

    # ══════════════════════════════════════════
    # 복합: 2개 이상 추가필터 조합
    # ══════════════════════════════════════════
    print("\n  [COMBO] 복합 조건 (2개 이상 추가필터)")
    print(f"  {'-'*85}")

    # 상승장 + 거래량
    for vt in [1.2, 1.5, 2.0]:
        p(f"BASE + 상승장 + vol>{vt}x",
          lambda e, vt=vt: base(e) and e['mkt_bullish'] and e['vol_ratio'] is not None and e['vol_ratio']>vt)
    for vt in [1.2, 1.5, 2.0]:
        p(f"BASE_STRONG + 상승장 + vol>{vt}x",
          lambda e, vt=vt: base_strong(e) and e['mkt_bullish'] and e['vol_ratio'] is not None and e['vol_ratio']>vt)

    # 상승장 + MA위
    p("BASE + 상승장 + above_ma20",
      lambda e: base(e) and e['mkt_bullish'] and e['above_ma20'])
    p("BASE_STRONG + 상승장 + above_ma20",
      lambda e: base_strong(e) and e['mkt_bullish'] and e['above_ma20'])

    # 거래량 + MA위
    for vt in [1.2, 1.5, 2.0]:
        p(f"BASE + vol>{vt}x + above_ma20",
          lambda e, vt=vt: base(e) and e['vol_ratio'] is not None and e['vol_ratio']>vt and e['above_ma20'])

    # ── 3개 전부 ──
    print("\n  [ALL 3] 상승장 + 거래량 + MA위")
    for vt in [1.0, 1.2, 1.5, 2.0]:
        p(f"BASE + 상승장 + vol>{vt}x + above_ma20",
          lambda e, vt=vt: base(e) and e['mkt_bullish'] and e['vol_ratio'] is not None and e['vol_ratio']>vt and e['above_ma20'])
    for vt in [1.0, 1.2, 1.5, 2.0]:
        p(f"BASE_STRONG + 상승장 + vol>{vt}x + above_ma20",
          lambda e, vt=vt: base_strong(e) and e['mkt_bullish'] and e['vol_ratio'] is not None and e['vol_ratio']>vt and e['above_ma20'])

    # ── 백분위 변형 ──
    print("\n  [PCTL variants] 백분위 변형")
    for pctl_t in [80, 85, 90, 95]:
        for vt in [1.2, 1.5]:
            p(f"pctl>={pctl_t} & dd<=-5 & e1d>0 & 상승장 & vol>{vt}x & MA위",
              lambda e, pt=pctl_t, vt=vt: e['pctl']>=pt and e['dd20'] is not None and e['dd20']<=-5 and e['e1d'] is not None and e['e1d']>0 and e['mkt_bullish'] and e['vol_ratio'] is not None and e['vol_ratio']>vt and e['above_ma20'])

    # ── 눌림 깊이 변형 ──
    print("\n  [Drawdown variants] 눌림 깊이 변형")
    for dd in [-3, -5, -7, -10]:
        p(f"pctl>=90 & dd<={dd}% & e1d>0 & 상승장 & vol>1.2x & MA위",
          lambda e, dd=dd: e['pctl']>=90 and e['dd20'] is not None and e['dd20']<=dd and e['e1d'] is not None and e['e1d']>0 and e['mkt_bullish'] and e['vol_ratio'] is not None and e['vol_ratio']>1.2 and e['above_ma20'])


if __name__ == "__main__":
    sc, sv, mm, ic, dates = load_data()
    events = collect_events(sc, sv, mm, ic, dates)
    run_analysis(events)
