"""
[*] Minervini VCP 점검 — range_ratio 0.7 / 0.75 비교
=====================================================
backfill_guru_signals.py의 VCP 룰만 분리해서 두 임계값으로 동시 추출.

질문:
  - 현재 0.7로 시그널 종목이 0건? 며칠? 표본 분포?
  - 0.75로 완화 시 얼마나 추가 검출되는지?
  - forward 20D 평균 수익은 얼마나 변하는지? (필터 완화로 질이 떨어지는지)

룰:
  range_recent = 최근 10D 평균 (high-low)
  range_prior  = 그 전 10D 평균 (high-low)
  range_ratio  = range_recent / range_prior
  → ratio < THRESHOLD AND vr20 ≥ 2.0 AND ret_1d ≥ +5%

실행: python inspect_vcp.py
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
    # 1년치 + forward 20일 마진 = 시그널 1년 + 향후 수익 검증용
    LOAD_START = '2025-04-01'
    LOAD_END = '2026-05-09'

    print(f"[1] OHLCV 로딩 ({LOAD_START}~{LOAD_END})...")
    base = "subject=eq.개인&select=date,stock_code,stock_name,market,high,low,close,volume,market_cap"
    rows = sb_get_split('daily_supply_v2', base, LOAD_START, LOAD_END, init_days=30)
    print(f"    [OK] 총 {len(rows):,} rows")

    # 종목별 시계열 정리
    by_stock = defaultdict(list)
    for r in rows:
        by_stock[r['stock_code']].append({
            'date': r['date'], 'stock_name': r.get('stock_name'),
            'market': r.get('market'),
            'high': r.get('high') or 0, 'low': r.get('low') or 0,
            'close': r.get('close') or 0, 'volume': r.get('volume') or 0,
            'market_cap': r.get('market_cap') or 0,
        })
    for sc in by_stock:
        by_stock[sc].sort(key=lambda x: x['date'])
    print(f"    [OK] {len(by_stock):,} 종목")

    # 2. VCP 시그널 추출 (각 임계값별)
    THRESHOLDS = [0.65, 0.70, 0.75, 0.80]
    print(f"\n[2] VCP 룰 적용 — 임계값: {THRESHOLDS}")

    # 각 임계값별 시그널 리스트
    signals_by_thr = {thr: [] for thr in THRESHOLDS}
    # 모든 후보의 range_ratio 분포 (vr20≥2.0 + ret_1d≥+5% 충족하는 것만)
    ratio_dist = []

    for sc, lst in by_stock.items():
        if len(lst) < 60:
            continue
        for i in range(20, len(lst)):
            today = lst[i]
            prev = lst[i - 1]
            if today['close'] <= 0 or prev['close'] <= 0 or today['volume'] <= 0:
                continue
            ret_1d = (today['close'] / prev['close'] - 1) * 100
            if ret_1d < 5:  # VCP 등락률 조건
                continue
            vol_20 = sum(x['volume'] for x in lst[i - 20:i]) / 20
            vr20 = today['volume'] / vol_20 if vol_20 > 0 else 0
            if vr20 < 2.0:  # VCP 거래량 조건
                continue
            range_recent = sum(x['high'] - x['low'] for x in lst[i - 10:i]) / 10
            range_prior = sum(x['high'] - x['low'] for x in lst[i - 20:i - 10]) / 10
            if range_prior <= 0:
                continue
            range_ratio = range_recent / range_prior

            # forward 20D 수익률
            fwd20 = lst[i + 20]['close'] if i + 20 < len(lst) else None
            ret_20d = ((fwd20 / today['close'] - 1) * 100) if (fwd20 and fwd20 > 0) else None

            ratio_dist.append({
                'date': today['date'], 'sc': sc, 'name': today['stock_name'],
                'market': today['market'], 'ratio': range_ratio,
                'ret_1d': ret_1d, 'vr20': vr20, 'ret_20d': ret_20d,
            })
            for thr in THRESHOLDS:
                if range_ratio < thr:
                    signals_by_thr[thr].append(ratio_dist[-1])

    # 3. 임계값별 통계
    print("\n" + "=" * 88)
    print(f"  Minervini VCP — range_ratio 임계값별 통계 (1년 검증, vr20≥2.0 & ret_1d≥+5% 고정)")
    print("=" * 88)
    print(f"  {'임계값':>8} {'표본':>8} {'일평균':>8} {'20D평균':>10} {'음수비':>8} {'중위값ret':>10}")
    print("  " + "-" * 86)
    for thr in THRESHOLDS:
        sigs = signals_by_thr[thr]
        n = len(sigs)
        with_fwd = [s['ret_20d'] for s in sigs if s['ret_20d'] is not None]
        if with_fwd:
            avg_20d = sum(with_fwd) / len(with_fwd)
            neg_pct = sum(1 for v in with_fwd if v < 0) * 100 / len(with_fwd)
            sorted_ret = sorted(with_fwd)
            median_ret = sorted_ret[len(sorted_ret) // 2]
        else:
            avg_20d = neg_pct = median_ret = 0
        marker = '  ★ 현재 사용 중' if thr == 0.70 else ''
        print(f"  {thr:>8.2f} {n:>8,} {n/250:>8.1f} {avg_20d:>+9.2f}% {neg_pct:>7.0f}% {median_ret:>+9.2f}%{marker}")
    print("=" * 88)

    # 4. range_ratio 분포 (vr20≥2.0 + ret_1d≥+5% 충족 후보 기준)
    print(f"\n[3] range_ratio 분포 (vr20≥2.0 & ret_1d≥+5% 후보 {len(ratio_dist):,}건 기준)")
    bins = [0.0, 0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.9, 1.0, 1.2, 1.5, 99]
    bin_counts = [0] * (len(bins) - 1)
    for r in ratio_dist:
        for k in range(len(bins) - 1):
            if bins[k] <= r['ratio'] < bins[k + 1]:
                bin_counts[k] += 1
                break
    cum = 0
    print(f"  {'range_ratio':<14} {'건수':>7} {'비율':>6} {'누적':>6}")
    print("  " + "-" * 40)
    for k in range(len(bins) - 1):
        cum += bin_counts[k]
        cum_pct = cum * 100 / len(ratio_dist) if ratio_dist else 0
        pct = bin_counts[k] * 100 / len(ratio_dist) if ratio_dist else 0
        bar = '█' * int(pct / 2)
        print(f"  {bins[k]:>5.2f}~{bins[k+1]:<6.2f} {bin_counts[k]:>7,} {pct:>5.1f}% {cum_pct:>5.1f}%  {bar}")

    # 5. 최근 30일 시그널 종목 (0.7 vs 0.75 비교)
    print(f"\n[4] 최근 30일 시그널 비교 (0.7 vs 0.75)")
    cutoff = (datetime.strptime(LOAD_END, '%Y-%m-%d') - timedelta(days=45)).strftime('%Y-%m-%d')
    sigs_70 = sorted([s for s in signals_by_thr[0.70] if s['date'] >= cutoff], key=lambda x: x['date'], reverse=True)
    sigs_75 = sorted([s for s in signals_by_thr[0.75] if s['date'] >= cutoff], key=lambda x: x['date'], reverse=True)
    extra = [s for s in sigs_75 if s['ratio'] >= 0.70]  # 0.75에서만 추가 검출
    print(f"  0.70 임계값: 최근 30일 시그널 {len(sigs_70)}건")
    for s in sigs_70[:15]:
        ret20_str = f"+{s['ret_20d']:.2f}%" if s['ret_20d'] is not None else 'N/A'
        print(f"    {s['date']} {s['sc']} {s['name'][:14]:<14} ratio={s['ratio']:.3f} +{s['ret_1d']:.1f}% vr={s['vr20']:.1f}x [20D:{ret20_str}]")
    print(f"\n  0.75로 완화 시 추가 검출: {len(extra)}건 (0.70≤ratio<0.75)")
    for s in extra[:15]:
        ret20_str = f"+{s['ret_20d']:.2f}%" if s['ret_20d'] is not None else 'N/A'
        print(f"    {s['date']} {s['sc']} {s['name'][:14]:<14} ratio={s['ratio']:.3f} +{s['ret_1d']:.1f}% vr={s['vr20']:.1f}x [20D:{ret20_str}]")

    # 6. 최근 90일 일별 시그널 카운트 (0.7 / 0.75)
    print(f"\n[5] 최근 90일 일별 VCP 시그널 카운트")
    cutoff_90 = (datetime.strptime(LOAD_END, '%Y-%m-%d') - timedelta(days=120)).strftime('%Y-%m-%d')
    daily_70 = defaultdict(int)
    daily_75 = defaultdict(int)
    for s in signals_by_thr[0.70]:
        if s['date'] >= cutoff_90:
            daily_70[s['date']] += 1
    for s in signals_by_thr[0.75]:
        if s['date'] >= cutoff_90:
            daily_75[s['date']] += 1
    all_dates = sorted(set(list(daily_70.keys()) + list(daily_75.keys())), reverse=True)
    print(f"  {'날짜':<12} {'0.70':>6} {'0.75':>6} {'+추가':>6}")
    print("  " + "-" * 36)
    for d in all_dates[:30]:
        n70 = daily_70[d]
        n75 = daily_75[d]
        diff = n75 - n70
        print(f"  {d:<12} {n70:>6} {n75:>6} {('+' + str(diff)) if diff > 0 else str(diff):>6}")

    print(f"\n[OK] {time.time() - t0:.0f}초")


if __name__ == '__main__':
    sys.exit(main() or 0)
