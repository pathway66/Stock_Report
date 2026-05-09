"""
[*] 그루 패턴 × 거래량 지표 결합 시그널 백테스트
====================================================
9개 그루 패턴(P1~P9) × 거래량 지표(CMF/MFI) 결합 조건 검증.

가설:
  H1. 그루 시그널 + CMF≥+0.25 (강한 매집) → 더 정밀한 매수
  H2. 그루 시그널 + CMF≥0 (매집 진행 중) → 약한 필터, 표본 크게 유지
  H3. 그루 시그널 + MFI<70 (과매수 아님) → 진입 여유 있는 시그널
  H4. 그루 시그널 + MFI<30 (과매도 + 매수) → 셀링클라이맥스 유사 강력
  H5. 그루 시그널 + CMF≥0 AND MFI<80 → 매집 중 + 과매수 회피

각 결합 조건이 단독 그루 대비 개선도 측정 (음수비 ↓, 평균수익 ↑).

데이터 소스:
- guru_signals: 9개 그루 패턴 시그널 (date, stock_code, pattern_id)
- daily_supply_v2 (subject=개인): close, cmf_20, mfi_14 + forward 시계열
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(override=True)
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}


def sb_get(table, params, page_size=1000):
    rows, offset = [], 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={page_size}&offset={offset}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code != 200:
                msg = r.text[:200]
                if '57014' in msg or 'timeout' in msg:
                    raise TimeoutError("PG timeout")
                print(f"  [E] {r.status_code} {msg}"); break
            chunk = r.json()
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        except TimeoutError:
            raise
        except Exception as e:
            print(f"  [X] {e}"); break
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
            return fetch(s, mid, mid_d) + fetch(mid, e, days - mid_d)
    rows = []
    cur = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur < end_dt:
        nxt_dt = min(cur + timedelta(days=init_days), end_dt + timedelta(days=1))
        s, e = cur.strftime("%Y-%m-%d"), nxt_dt.strftime("%Y-%m-%d")
        chunk = fetch(s, e, init_days)
        print(f"    {s}~{e}: {len(chunk):,}")
        rows.extend(chunk)
        cur = nxt_dt
    return rows


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] env"); return 1
    t0 = time.time()

    # 1. 그루 시그널 로드 (1년치)
    print("[1] guru_signals 로드...")
    sigs = sb_get('guru_signals',
        "select=date,stock_code,pattern_id&date=gte.2025-05-08&date=lte.2026-04-15&order=date.asc")
    print(f"    [OK] {len(sigs):,} 시그널")

    # 2. OHLCV + cmf + mfi 로드 (forward 20일 마진 포함)
    print("[2] OHLCV + 지표 로드 (2025-05-08 ~ 2026-05-09)...")
    base = "subject=eq.개인&select=date,stock_code,close,market,cmf_20,mfi_14"
    rows = sb_get_split('daily_supply_v2', base, '2025-05-08', '2026-05-09', init_days=30)
    print(f"    [OK] {len(rows):,} rows")

    # 종목별 시계열 정리
    by_stock = defaultdict(list)
    for r in rows:
        by_stock[r['stock_code']].append({
            'date': r['date'], 'close': r.get('close') or 0,
            'market': r.get('market'),
            'cmf': float(r['cmf_20']) if r.get('cmf_20') is not None else None,
            'mfi': float(r['mfi_14']) if r.get('mfi_14') is not None else None,
        })
    for sc in by_stock:
        by_stock[sc].sort(key=lambda x: x['date'])

    # 종목/일자별 인덱스 (빠른 조회용)
    idx = {}
    for sc, lst in by_stock.items():
        for i, x in enumerate(lst):
            idx[(sc, x['date'])] = i

    # 3. 시장 등락률
    print("[3] 시장 등락률...")
    rows_idx = sb_get('daily_index',
        "select=date,index_code,close&date=gte.2025-05-08&date=lte.2026-05-09")
    by_mkt = defaultdict(list)
    for r in rows_idx:
        by_mkt[r['index_code']].append({'date': r['date'], 'close': r['close'] or 0})
    for k in by_mkt:
        by_mkt[k].sort(key=lambda x: x['date'])
    idx_ret = defaultdict(dict)
    for code, lst in by_mkt.items():
        for i in range(1, len(lst)):
            prev = lst[i - 1]['close']; curr = lst[i]['close']
            if prev > 0:
                idx_ret[code][lst[i]['date']] = (curr / prev - 1) * 100

    # 4. 시그널별 forward 수익률 + cmf/mfi
    print("[4] 시그널 분석...")
    enriched = []  # [{pattern, date, sc, cmf, mfi, fwd5, fwd10, fwd20, rel20}]
    for s in sigs:
        sc, d, pid = s['stock_code'], s['date'], s['pattern_id']
        i = idx.get((sc, d))
        if i is None:
            continue
        lst = by_stock[sc]
        entry = lst[i]
        if entry['close'] <= 0:
            continue
        fwd5 = lst[i + 5]['close'] if i + 5 < len(lst) else None
        fwd10 = lst[i + 10]['close'] if i + 10 < len(lst) else None
        fwd20 = lst[i + 20]['close'] if i + 20 < len(lst) else None

        mkt = entry.get('market') or 'KOSPI'
        rel20 = None
        if fwd20 and fwd20 > 0:
            ret20 = (fwd20 / entry['close'] - 1) * 100
            mkt20 = 0
            for k in range(1, 21):
                if i + k < len(lst):
                    mkt20 += idx_ret.get(mkt, {}).get(lst[i + k]['date'], 0)
            rel20 = ret20 - mkt20

        enriched.append({
            'pattern': pid, 'date': d, 'sc': sc,
            'cmf': entry['cmf'], 'mfi': entry['mfi'],
            'ret5': (fwd5 / entry['close'] - 1) * 100 if fwd5 and fwd5 > 0 else None,
            'ret10': (fwd10 / entry['close'] - 1) * 100 if fwd10 and fwd10 > 0 else None,
            'ret20': (fwd20 / entry['close'] - 1) * 100 if fwd20 and fwd20 > 0 else None,
            'rel20': rel20,
        })

    print(f"    [OK] {len(enriched):,} 시그널 분석")

    # 5. 결합 조건별 통계
    def stat(arr, key):
        vals = [r[key] for r in arr if r.get(key) is not None]
        if not vals:
            return (0, 0, 0)
        avg = sum(vals) / len(vals)
        neg = sum(1 for v in vals if v < 0) * 100 / len(vals)
        return (len(vals), avg, neg)

    # 결합 조건 정의
    def cond_pass(r, c):
        if c == 'all': return True
        if c == 'cmf_pos' and r['cmf'] is not None: return r['cmf'] >= 0
        if c == 'cmf_strong' and r['cmf'] is not None: return r['cmf'] >= 0.25
        if c == 'mfi_lt70' and r['mfi'] is not None: return r['mfi'] < 70
        if c == 'mfi_lt30' and r['mfi'] is not None: return r['mfi'] < 30
        if c == 'cmf_pos_mfi_lt80' and r['cmf'] is not None and r['mfi'] is not None:
            return r['cmf'] >= 0 and r['mfi'] < 80
        return False

    pat_names = {
        'P1': "O'Neil Pivot Buy 20D", 'P2': 'Minervini VCP', 'P3': 'Wyckoff Re-acc',
        'P4': "O'Neil Volume Surge", 'P5': 'Wyckoff Smart Money', 'P6': "O'Neil Follow-Through",
        'P7': 'Darvas 52주', 'P8': 'Weinstein Stage 2', 'P9': 'Livermore 50D',
    }
    conds = [
        ('all', '단독 (그루만)'),
        ('cmf_pos', '+ CMF≥0 (매집 진행)'),
        ('cmf_strong', '+ CMF≥+0.25 (강한 매집)'),
        ('mfi_lt70', '+ MFI<70 (과매수 회피)'),
        ('mfi_lt30', '+ MFI<30 (과매도 매수)'),
        ('cmf_pos_mfi_lt80', '+ CMF≥0 & MFI<80'),
    ]

    print("\n" + "=" * 110)
    print("  그루 패턴 × 거래량 지표 결합 시그널 — forward 20일 (1년치 검증)")
    print("=" * 110)
    print(f"  {'패턴':<22} {'결합조건':<26} {'N':>6}  {'5d_avg':>8} {'20d_avg':>8} {'20d_neg%':>9} {'rel20':>8}")
    print("  " + "-" * 106)

    for pid, pname in pat_names.items():
        psigs = [r for r in enriched if r['pattern'] == pid]
        if not psigs:
            continue
        for c, cname in conds:
            sub = [r for r in psigs if cond_pass(r, c)]
            n5, a5, _ = stat(sub, 'ret5')
            n20, a20, neg20 = stat(sub, 'ret20')
            _, rel20_avg, _ = stat(sub, 'rel20')
            mark = ''
            # 단독 vs 결합 비교 표시
            if c != 'all' and n20 >= 30:
                base = [r for r in psigs if r.get('ret20') is not None]
                if base:
                    base_neg = sum(1 for r in base if r['ret20'] < 0) * 100 / len(base)
                    base_avg = sum(r['ret20'] for r in base) / len(base)
                    if neg20 < base_neg - 3 and a20 > base_avg + 1:
                        mark = ' ⭐ 개선'
                    elif neg20 < base_neg - 3:
                        mark = ' (안정성↑)'
                    elif a20 > base_avg + 1:
                        mark = ' (수익↑)'
            print(f"  {pname:<22} {cname:<26} {n20:>6}  "
                  f"{a5:+7.2f}% {a20:+7.2f}% {neg20:>8.0f}% {rel20_avg:+7.2f}%{mark}")
        print()

    print("=" * 110)
    print(f"\n[OK] {time.time() - t0:.0f}초")


if __name__ == '__main__':
    sys.exit(main() or 0)
