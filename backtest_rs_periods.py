"""
=============================================================
  RS 기간 최적화 백테스트 v1.0
  backtest_rs_periods.py  |  2026-04-14

  목적:
    한국 시장에 최적화된 RS(상대강도) 기간과 가중치를 찾기 위한
    체계적 백테스트. 미국식(Minervini/O'Neil) 기간을 그대로 쓰면
    한국 시장의 빠른 순환/높은 변동성에 안 맞을 수 있음.

  분석 항목:
    1. 개별 기간 예측력 (IC, 히트율, 롱숏 스프레드)
    2. 최적 복합 가중치 (회귀 기반)
    3. 모멘텀 가속 시그널 (탭2: 돌파 감지)
    4. 모멘텀 붕괴 시그널 (탭3: 하락 감지)

  데이터:
    - 521종목(코칠닥삼) × 717거래일 (2023-05~2026-04)
    - KOSPI/KOSDAQ 지수 동일 기간

  사용법:
    python backtest_rs_periods.py
=============================================================
"""

import os
import sys
import json
import time
import requests
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

# ─── .env ───
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

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

# ============================================================
# 테스트할 RS 기간 (한국 시장 특성 반영한 후보군)
# ============================================================
# 기존 미국식: [1, 5, 10, 20, 40, 60, 120, 200]
# 한국 추가 후보: [3, 7, 15, 30, 50, 80, 100, 150]
TEST_PERIODS = [1, 3, 5, 7, 10, 15, 20, 30, 40, 50, 60, 80, 100, 120, 150, 200]

# Forward return 측정 기간 (이 기간 후 수익률로 예측력 평가)
FORWARD_PERIODS = [5, 10, 20, 40, 60]

# 테스트 간격 (매 N거래일마다 테스트 → 연산 효율)
TEST_INTERVAL = 5  # 5거래일마다 = ~143 테스트 포인트


# ============================================================
# 데이터 로드
# ============================================================
def fetch_all(table, select, filters="", limit=500000):
    """Supabase REST API 페이지네이션 fetch"""
    all_rows = []
    offset = 0
    page_size = 10000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
        if filters:
            url += f"&{filters}"
        url += f"&limit={page_size}&offset={offset}"
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code not in [200, 206]:
            print(f"  [E] {resp.status_code}: {resp.text[:200]}")
            break
        data = resp.json()
        if not data:
            break
        all_rows.extend(data)
        if len(data) < page_size or len(all_rows) >= limit:
            break
        offset += page_size
    return all_rows


def load_data():
    """전체 데이터 로드 → 메모리 내 dict 구조"""
    print("=" * 70)
    print("  RS 기간 최적화 백테스트 v1.0")
    print("  한국 시장 맞춤 RS 파라미터 탐색")
    print("=" * 70)

    # 1. 종목 종가 (subject=개인, close>0)
    print("\n[1] 종목 종가 로드...", end=" ", flush=True)
    t0 = time.time()
    rows = fetch_all(
        "daily_supply_v2",
        "stock_code,date,close",
        "subject=eq.개인&close=gt.0&date=lt.2026-04-14",  # 4/14 전종목 제외 (1일치)
    )
    # {stock_code: {date: close}}
    stock_closes = defaultdict(dict)
    for r in rows:
        stock_closes[r['stock_code']][r['date']] = float(r['close'])
    print(f"{len(rows)}건, {len(stock_closes)}종목 ({time.time()-t0:.1f}s)")

    # 2. 종목 마켓 매핑
    print("[2] 종목-마켓 매핑...", end=" ", flush=True)
    rows = fetch_all("stock_sectors", "stock_code,market", limit=10000)
    market_map = {}
    for r in rows:
        if r.get('market'):
            market_map[r['stock_code']] = r['market']
    print(f"{len(market_map)}종목")

    # 3. 지수 종가
    print("[3] 지수 종가 로드...", end=" ", flush=True)
    rows = fetch_all(
        "daily_index",
        "index_code,date,close",
        "index_code=in.(KOSPI,KOSDAQ)"
    )
    # {index_code: {date: close}}
    index_closes = defaultdict(dict)
    for r in rows:
        index_closes[r['index_code']][r['date']] = float(r['close'])
    print(f"KOSPI {len(index_closes['KOSPI'])}일, KOSDAQ {len(index_closes['KOSDAQ'])}일")

    # 4. 거래일 목록 (정렬)
    all_dates = sorted(set(d for sc in stock_closes.values() for d in sc.keys()))
    print(f"[4] 거래일: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)}일)")

    return stock_closes, market_map, index_closes, all_dates


# ============================================================
# Phase 1: 개별 기간 예측력 분석
# ============================================================
def calc_period_return(closes_dict, date_idx, dates, period):
    """특정 시점의 N일 수익률 계산"""
    if date_idx - period < 0:
        return None
    today = dates[date_idx]
    past = dates[date_idx - period]
    c_today = closes_dict.get(today)
    c_past = closes_dict.get(past)
    if c_today and c_past and c_past > 0:
        return (c_today / c_past - 1) * 100
    return None


def phase1_individual_periods(stock_closes, market_map, index_closes, dates):
    """
    각 lookback 기간별로:
    1. Information Coefficient (IC) = Spearman rank correlation(RS순위, 미래수익률)
    2. 히트율 = 상위 20% 종목 중 지수 대비 양의 초과수익 비율
    3. 롱숏 스프레드 = 상위20% 평균수익 - 하위20% 평균수익
    """
    print("\n" + "=" * 70)
    print("  Phase 1: 개별 기간 예측력 분석")
    print("  (각 lookback 기간이 미래 수익을 얼마나 잘 예측하는가)")
    print("=" * 70)

    # 200D lookback + 60D forward 필요 → 시작점 = 260일째부터
    max_lookback = max(TEST_PERIODS)
    max_forward = max(FORWARD_PERIODS)
    start_idx = max_lookback
    end_idx = len(dates) - max_forward

    # 테스트 포인트
    test_indices = list(range(start_idx, end_idx, TEST_INTERVAL))
    print(f"  테스트 포인트: {len(test_indices)}개 "
          f"({dates[test_indices[0]]} ~ {dates[test_indices[-1]]})")

    # 결과 저장: {(lookback_period, forward_period): {ic_list, hit_list, ls_list}}
    results = {}
    for lp in TEST_PERIODS:
        for fp in FORWARD_PERIODS:
            results[(lp, fp)] = {"ic": [], "hit": [], "ls": [], "top_ret": [], "bot_ret": []}

    total = len(test_indices)
    for ti, date_idx in enumerate(test_indices):
        if (ti + 1) % 20 == 0 or ti == 0:
            print(f"  [{ti+1}/{total}] {dates[date_idx]}...", flush=True)

        # 이 시점에 데이터가 있는 종목들
        today = dates[date_idx]
        active_stocks = [
            code for code in stock_closes
            if today in stock_closes[code] and code in market_map
        ]

        for lp in TEST_PERIODS:
            if date_idx - lp < 0:
                continue

            # 1) Lookback RS (초과수익률) 계산
            rs_scores = {}
            for code in active_stocks:
                mkt = market_map[code]
                idx_code = mkt  # KOSPI or KOSDAQ

                stock_ret = calc_period_return(stock_closes[code], date_idx, dates, lp)
                idx_ret = calc_period_return(index_closes[idx_code], date_idx, dates, lp)

                if stock_ret is not None and idx_ret is not None:
                    rs_scores[code] = stock_ret - idx_ret

            if len(rs_scores) < 50:  # 최소 50종목 필요
                continue

            # RS 순위 매기기
            sorted_stocks = sorted(rs_scores.keys(), key=lambda c: rs_scores[c], reverse=True)
            n = len(sorted_stocks)
            quintile = n // 5  # 20%씩

            top_codes = set(sorted_stocks[:quintile])        # 상위 20%
            bot_codes = set(sorted_stocks[-quintile:])       # 하위 20%

            # RS 순위 (1 = 최고)
            rs_ranks = {code: rank for rank, code in enumerate(sorted_stocks)}

            for fp in FORWARD_PERIODS:
                if date_idx + fp >= len(dates):
                    continue

                # 2) Forward return 계산
                fwd_returns = {}
                for code in rs_scores:
                    mkt = market_map[code]
                    idx_code = mkt

                    stock_fwd = calc_period_return(stock_closes[code], date_idx + fp, dates, fp)

                    # 미래 시점(date_idx+fp)에서 fp일 전(=date_idx)까지의 수익률
                    # = (close[date_idx+fp] / close[date_idx] - 1) * 100
                    today_close = stock_closes[code].get(dates[date_idx])
                    future_close = stock_closes[code].get(dates[date_idx + fp])
                    if today_close and future_close and today_close > 0:
                        fwd_ret = (future_close / today_close - 1) * 100
                    else:
                        fwd_ret = None

                    idx_today = index_closes[idx_code].get(dates[date_idx])
                    idx_future = index_closes[idx_code].get(dates[date_idx + fp])
                    if idx_today and idx_future and idx_today > 0:
                        idx_fwd = (idx_future / idx_today - 1) * 100
                    else:
                        idx_fwd = None

                    if fwd_ret is not None and idx_fwd is not None:
                        fwd_returns[code] = fwd_ret - idx_fwd  # 초과수익률

                if len(fwd_returns) < 50:
                    continue

                key = (lp, fp)

                # ── IC (Spearman rank correlation) ──
                common = [c for c in fwd_returns if c in rs_ranks]
                if len(common) >= 50:
                    x_ranks = np.array([rs_ranks[c] for c in common], dtype=float)
                    y_vals = np.array([fwd_returns[c] for c in common], dtype=float)
                    y_order = y_vals.argsort().argsort()  # rank

                    # Spearman = Pearson of ranks
                    # rs_ranks: 0=best → 높은 RS = 낮은 rank
                    # 예측력 있으면 IC < 0 (rank 낮을수록 = RS 높을수록, 미래수익 높음)
                    n_c = len(common)
                    d = x_ranks - y_order
                    rho = 1 - 6 * np.sum(d**2) / (n_c * (n_c**2 - 1))
                    # IC를 "RS 높을수록 수익 높은" 방향으로 뒤집기
                    results[key]["ic"].append(-rho)

                # ── 히트율 (상위20% 중 양의 초과수익 비율) ──
                top_fwd = [fwd_returns[c] for c in top_codes if c in fwd_returns]
                bot_fwd = [fwd_returns[c] for c in bot_codes if c in fwd_returns]

                if top_fwd:
                    hit_rate = sum(1 for x in top_fwd if x > 0) / len(top_fwd)
                    results[key]["hit"].append(hit_rate)
                    results[key]["top_ret"].append(np.mean(top_fwd))

                if bot_fwd:
                    results[key]["bot_ret"].append(np.mean(bot_fwd))

                # ── 롱숏 스프레드 ──
                if top_fwd and bot_fwd:
                    ls = np.mean(top_fwd) - np.mean(bot_fwd)
                    results[key]["ls"].append(ls)

    return results


def print_phase1_results(results):
    """Phase 1 결과 출력 — 포맷: Lookback × Forward 매트릭스"""
    print("\n" + "=" * 70)
    print("  Phase 1 결과: 개별 기간 예측력")
    print("=" * 70)

    # ── 1) IC (Information Coefficient) ──
    print("\n┌─────────────────────────────────────────────────────────┐")
    print("│  Information Coefficient (IC)                           │")
    print("│  = RS순위와 미래 초과수익의 순위상관                      │")
    print("│  높을수록 예측력 좋음. 0.05+ 양호, 0.10+ 우수            │")
    print("└─────────────────────────────────────────────────────────┘")

    header = f"{'Lookback':>10s}"
    for fp in FORWARD_PERIODS:
        header += f"  Fwd{fp:>3d}D"
    print(header)
    print("-" * (10 + 10 * len(FORWARD_PERIODS)))

    ic_summary = {}
    for lp in TEST_PERIODS:
        line = f"{lp:>8d}D"
        row_avg = []
        for fp in FORWARD_PERIODS:
            key = (lp, fp)
            vals = results[key]["ic"]
            if vals:
                avg_ic = np.mean(vals)
                ic_summary[(lp, fp)] = avg_ic
                # 색분: 0.10+ ★★★, 0.05+ ★★, 0.02+ ★
                marker = "***" if avg_ic >= 0.10 else "** " if avg_ic >= 0.05 else "*  " if avg_ic >= 0.02 else "   "
                line += f"  {avg_ic:+.3f}{marker}"
                row_avg.append(avg_ic)
            else:
                line += f"     N/A   "
        if row_avg:
            line += f"  avg={np.mean(row_avg):+.3f}"
        print(line)

    # ── 2) 히트율 ──
    print("\n┌─────────────────────────────────────────────────────────┐")
    print("│  히트율 (Hit Rate)                                      │")
    print("│  = RS 상위 20% 종목 중 지수를 이긴 비율                  │")
    print("│  50%=랜덤, 55%+ 양호, 60%+ 우수                         │")
    print("└─────────────────────────────────────────────────────────┘")

    header = f"{'Lookback':>10s}"
    for fp in FORWARD_PERIODS:
        header += f"  Fwd{fp:>3d}D"
    print(header)
    print("-" * (10 + 10 * len(FORWARD_PERIODS)))

    for lp in TEST_PERIODS:
        line = f"{lp:>8d}D"
        for fp in FORWARD_PERIODS:
            key = (lp, fp)
            vals = results[key]["hit"]
            if vals:
                avg_hit = np.mean(vals) * 100
                marker = "***" if avg_hit >= 60 else "** " if avg_hit >= 55 else "*  " if avg_hit >= 52 else "   "
                line += f"  {avg_hit:5.1f}%{marker}"
            else:
                line += f"     N/A   "
        print(line)

    # ── 3) 롱숏 스프레드 ──
    print("\n┌─────────────────────────────────────────────────────────┐")
    print("│  롱숏 스프레드 (Long-Short Spread, %)                    │")
    print("│  = RS 상위20% 평균초과수익 - 하위20% 평균초과수익         │")
    print("│  높을수록 RS 분별력 좋음                                  │")
    print("└─────────────────────────────────────────────────────────┘")

    header = f"{'Lookback':>10s}"
    for fp in FORWARD_PERIODS:
        header += f"  Fwd{fp:>3d}D"
    print(header)
    print("-" * (10 + 10 * len(FORWARD_PERIODS)))

    ls_summary = {}
    for lp in TEST_PERIODS:
        line = f"{lp:>8d}D"
        row_avg = []
        for fp in FORWARD_PERIODS:
            key = (lp, fp)
            vals = results[key]["ls"]
            if vals:
                avg_ls = np.mean(vals)
                ls_summary[(lp, fp)] = avg_ls
                marker = "***" if avg_ls >= 5 else "** " if avg_ls >= 2 else "*  " if avg_ls >= 1 else "   "
                line += f"  {avg_ls:+6.2f}{marker}"
                row_avg.append(avg_ls)
            else:
                line += f"     N/A   "
        if row_avg:
            line += f"  avg={np.mean(row_avg):+.2f}"
        print(line)

    # ── 4) 상위20% 절대 초과수익률 ──
    print("\n┌─────────────────────────────────────────────────────────┐")
    print("│  상위 20% 평균 초과수익률 (%)                             │")
    print("│  = RS 탑 퀸틸 종목의 평균 지수대비 초과수익               │")
    print("└─────────────────────────────────────────────────────────┘")

    header = f"{'Lookback':>10s}"
    for fp in FORWARD_PERIODS:
        header += f"  Fwd{fp:>3d}D"
    print(header)
    print("-" * (10 + 10 * len(FORWARD_PERIODS)))

    for lp in TEST_PERIODS:
        line = f"{lp:>8d}D"
        for fp in FORWARD_PERIODS:
            key = (lp, fp)
            vals = results[key]["top_ret"]
            if vals:
                avg = np.mean(vals)
                line += f"  {avg:+6.2f}%  "
            else:
                line += f"     N/A   "
        print(line)

    return ic_summary, ls_summary


# ============================================================
# Phase 2: 최적 복합 가중치 탐색
# ============================================================
def phase2_optimal_weights(stock_closes, market_map, index_closes, dates, best_periods):
    """
    Phase 1에서 유망한 기간 조합으로 최적 가중치 탐색.
    Grid search: 가중치 조합별 복합 RS의 예측력 평가.
    """
    print("\n" + "=" * 70)
    print("  Phase 2: 최적 복합 가중치 탐색")
    print(f"  후보 기간: {best_periods}")
    print("=" * 70)

    max_lookback = max(best_periods)
    target_fwd = 20  # 20D forward return 기준 최적화

    start_idx = max_lookback
    end_idx = len(dates) - target_fwd
    test_indices = list(range(start_idx, end_idx, TEST_INTERVAL))

    # 미리 정의한 가중치 세트들
    weight_sets = {
        "미국식 (Minervini)": {1: 0.03, 5: 0.07, 10: 0.12, 20: 0.20, 40: 0.23, 60: 0.18, 120: 0.12, 200: 0.05},
        "단기 집중": {},
        "중기 집중": {},
        "장기 집중": {},
        "균등": {},
        "한국 최적화 A": {},
        "한국 최적화 B": {},
    }

    # best_periods 기반 가중치 생성
    n = len(best_periods)
    if n >= 3:
        # 단기 집중: 앞쪽 기간 강조
        w_short = {}
        for i, p in enumerate(best_periods):
            w_short[p] = max(0.05, 0.4 - i * 0.4 / n)
        total = sum(w_short.values())
        w_short = {k: round(v/total, 3) for k, v in w_short.items()}
        weight_sets["단기 집중"] = w_short

        # 중기 집중: 가운데 기간 강조
        w_mid = {}
        mid = n // 2
        for i, p in enumerate(best_periods):
            dist = abs(i - mid)
            w_mid[p] = max(0.05, 0.3 - dist * 0.05)
        total = sum(w_mid.values())
        w_mid = {k: round(v/total, 3) for k, v in w_mid.items()}
        weight_sets["중기 집중"] = w_mid

        # 장기 집중: 뒤쪽 기간 강조
        w_long = {}
        for i, p in enumerate(best_periods):
            w_long[p] = max(0.05, 0.05 + i * 0.4 / n)
        total = sum(w_long.values())
        w_long = {k: round(v/total, 3) for k, v in w_long.items()}
        weight_sets["장기 집중"] = w_long

        # 균등
        w_eq = {p: round(1/n, 3) for p in best_periods}
        weight_sets["균등"] = w_eq

        # 한국 최적화 A: 10~40D 무게중심
        w_kr_a = {}
        sweet_spot = {p: 1.0 for p in best_periods}
        for p in best_periods:
            if 10 <= p <= 40:
                sweet_spot[p] = 3.0
            elif 5 <= p <= 60:
                sweet_spot[p] = 2.0
        total = sum(sweet_spot.values())
        w_kr_a = {k: round(v/total, 3) for k, v in sweet_spot.items()}
        weight_sets["한국 최적화 A"] = w_kr_a

        # 한국 최적화 B: 5~20D 무게중심 (더 단기)
        w_kr_b = {}
        sweet_b = {p: 1.0 for p in best_periods}
        for p in best_periods:
            if 5 <= p <= 20:
                sweet_b[p] = 3.0
            elif 3 <= p <= 40:
                sweet_b[p] = 2.0
        total = sum(sweet_b.values())
        w_kr_b = {k: round(v/total, 3) for k, v in sweet_b.items()}
        weight_sets["한국 최적화 B"] = w_kr_b

    # 각 가중치 세트 평가
    ws_results = {}
    for ws_name, weights in weight_sets.items():
        if not weights:
            continue

        ic_list = []
        hit_list = []
        ls_list = []
        used_periods = [p for p in weights if p <= max(TEST_PERIODS)]

        for date_idx in test_indices:
            today = dates[date_idx]
            active_stocks = [
                code for code in stock_closes
                if today in stock_closes[code] and code in market_map
            ]

            # 복합 RS 계산
            composite_rs = {}
            for code in active_stocks:
                mkt = market_map[code]
                idx_code = mkt
                ws = 0
                wt = 0
                for p in used_periods:
                    if date_idx - p < 0:
                        continue
                    s_ret = calc_period_return(stock_closes[code], date_idx, dates, p)
                    i_ret = calc_period_return(index_closes[idx_code], date_idx, dates, p)
                    if s_ret is not None and i_ret is not None:
                        ws += (s_ret - i_ret) * weights.get(p, 0)
                        wt += weights.get(p, 0)
                if wt > 0:
                    composite_rs[code] = ws / wt

            if len(composite_rs) < 50:
                continue

            # Forward return
            if date_idx + target_fwd >= len(dates):
                continue

            fwd_returns = {}
            for code in composite_rs:
                mkt = market_map[code]
                idx_code = mkt
                tc = stock_closes[code].get(dates[date_idx])
                fc = stock_closes[code].get(dates[date_idx + target_fwd])
                ic_t = index_closes[idx_code].get(dates[date_idx])
                ic_f = index_closes[idx_code].get(dates[date_idx + target_fwd])
                if tc and fc and tc > 0 and ic_t and ic_f and ic_t > 0:
                    s_fwd = (fc / tc - 1) * 100
                    i_fwd = (ic_f / ic_t - 1) * 100
                    fwd_returns[code] = s_fwd - i_fwd

            if len(fwd_returns) < 50:
                continue

            # IC
            sorted_by_rs = sorted(composite_rs.keys(), key=lambda c: composite_rs[c], reverse=True)
            common = [c for c in sorted_by_rs if c in fwd_returns]
            if len(common) >= 50:
                rs_r = np.array([sorted_by_rs.index(c) for c in common], dtype=float)
                fwd_v = np.array([fwd_returns[c] for c in common], dtype=float)
                fwd_r = fwd_v.argsort().argsort()
                n_c = len(common)
                d = rs_r - fwd_r
                rho = 1 - 6 * np.sum(d**2) / (n_c * (n_c**2 - 1))
                ic_list.append(-rho)

            # Hit rate & LS
            n_stocks = len(sorted_by_rs)
            q = n_stocks // 5
            top = sorted_by_rs[:q]
            bot = sorted_by_rs[-q:]
            top_fwd = [fwd_returns[c] for c in top if c in fwd_returns]
            bot_fwd = [fwd_returns[c] for c in bot if c in fwd_returns]
            if top_fwd:
                hit_list.append(sum(1 for x in top_fwd if x > 0) / len(top_fwd))
            if top_fwd and bot_fwd:
                ls_list.append(np.mean(top_fwd) - np.mean(bot_fwd))

        ws_results[ws_name] = {
            "ic": np.mean(ic_list) if ic_list else 0,
            "hit": np.mean(hit_list) * 100 if hit_list else 0,
            "ls": np.mean(ls_list) if ls_list else 0,
            "weights": weights,
            "n_tests": len(ic_list),
        }

    # 결과 출력
    print(f"\n  복합 RS 가중치 세트 비교 (Forward = {target_fwd}D 기준)")
    print("-" * 70)
    print(f"{'가중치 세트':20s} {'IC':>8s} {'히트율':>8s} {'L/S스프레드':>12s} {'테스트수':>8s}")
    print("-" * 70)

    for name, r in sorted(ws_results.items(), key=lambda x: x[1]['ic'], reverse=True):
        print(f"{name:20s} {r['ic']:+8.4f} {r['hit']:7.1f}% {r['ls']:+11.2f}% {r['n_tests']:>7d}")

    # 최적 가중치 출력
    best_name = max(ws_results, key=lambda k: ws_results[k]['ic'])
    best = ws_results[best_name]
    print(f"\n  ★ 최적 가중치: {best_name}")
    print(f"    IC={best['ic']:+.4f}, 히트율={best['hit']:.1f}%, L/S={best['ls']:+.2f}%")
    print(f"    가중치: {best['weights']}")

    return ws_results


# ============================================================
# Phase 3: 모멘텀 가속/붕괴 시그널 분석
# ============================================================
def phase3_momentum_signals(stock_closes, market_map, index_closes, dates, best_periods):
    """
    탭2 (상승 돌파) / 탭3 (하락 붕괴) 시그널 최적화.
    단기 RS > 장기 RS → 모멘텀 가속 (돌파 후보)
    단기 RS < 장기 RS → 모멘텀 감속 (붕괴 후보)
    """
    print("\n" + "=" * 70)
    print("  Phase 3: 모멘텀 가속/붕괴 시그널 분석")
    print("  (탭2: 상승 돌파, 탭3: 하락 붕괴)")
    print("=" * 70)

    # 단기/장기 조합 테스트
    # 가속 = 단기RS - 장기RS > threshold
    short_candidates = [p for p in best_periods if p <= 20]
    long_candidates = [p for p in best_periods if p >= 20]

    if not short_candidates or not long_candidates:
        short_candidates = [5, 10, 20]
        long_candidates = [20, 40, 60, 120]

    pairs = []
    for sp in short_candidates:
        for lpp in long_candidates:
            if sp < lpp:
                pairs.append((sp, lpp))

    max_lookback = max(p for _, p in pairs)
    fwd = 20  # 20D forward로 평가
    start_idx = max_lookback
    end_idx = len(dates) - fwd
    test_indices = list(range(start_idx, end_idx, TEST_INTERVAL))

    print(f"  테스트 쌍: {len(pairs)}개")
    print(f"  테스트 포인트: {len(test_indices)}개")

    pair_results = {}

    for sp, lpp in pairs:
        accel_hits = []  # 가속 시그널 히트율
        decel_hits = []  # 감속 시그널 히트율
        accel_rets = []  # 가속 시그널 평균 초과수익
        decel_rets = []  # 감속 시그널 평균 초과수익

        for date_idx in test_indices:
            today = dates[date_idx]
            if date_idx + fwd >= len(dates):
                continue

            active_stocks = [
                code for code in stock_closes
                if today in stock_closes[code] and code in market_map
            ]

            accels = []  # (code, accel_score, fwd_excess_ret)
            decels = []

            for code in active_stocks:
                mkt = market_map[code]
                idx_code = mkt

                # 단기/장기 RS
                s_short = calc_period_return(stock_closes[code], date_idx, dates, sp)
                i_short = calc_period_return(index_closes[idx_code], date_idx, dates, sp)
                s_long = calc_period_return(stock_closes[code], date_idx, dates, lpp)
                i_long = calc_period_return(index_closes[idx_code], date_idx, dates, lpp)

                if None in (s_short, i_short, s_long, i_long):
                    continue

                rs_short = s_short - i_short
                rs_long = s_long - i_long
                accel = rs_short - rs_long  # 양수 = 가속, 음수 = 감속

                # Forward return
                tc = stock_closes[code].get(dates[date_idx])
                fc = stock_closes[code].get(dates[date_idx + fwd])
                ic_t = index_closes[idx_code].get(dates[date_idx])
                ic_f = index_closes[idx_code].get(dates[date_idx + fwd])
                if tc and fc and tc > 0 and ic_t and ic_f and ic_t > 0:
                    fwd_excess = (fc/tc - 1)*100 - (ic_f/ic_t - 1)*100
                else:
                    continue

                if accel > 5:  # 가속 (단기 RS가 장기보다 5%p+ 높음)
                    accels.append(fwd_excess)
                elif accel < -5:  # 감속
                    decels.append(fwd_excess)

            if accels:
                accel_hits.append(sum(1 for x in accels if x > 0) / len(accels))
                accel_rets.append(np.mean(accels))
            if decels:
                decel_hits.append(sum(1 for x in decels if x < 0) / len(decels))
                decel_rets.append(np.mean(decels))

        pair_results[(sp, lpp)] = {
            "accel_hit": np.mean(accel_hits) * 100 if accel_hits else 0,
            "accel_ret": np.mean(accel_rets) if accel_rets else 0,
            "decel_hit": np.mean(decel_hits) * 100 if decel_hits else 0,
            "decel_ret": np.mean(decel_rets) if decel_rets else 0,
            "n_tests": len(accel_hits),
        }

    # 결과
    print(f"\n  모멘텀 가속 시그널 (탭2: 상승 돌파 후보)")
    print(f"  조건: 단기RS - 장기RS > 5%p → {fwd}D후 초과수익 측정")
    print("-" * 65)
    print(f"{'Short':>7s} {'Long':>7s} {'히트율':>8s} {'평균초과수익':>12s} {'테스트수':>8s}")
    print("-" * 65)

    for (sp, lpp), r in sorted(pair_results.items(), key=lambda x: x[1]['accel_hit'], reverse=True):
        if r['n_tests'] > 0:
            print(f"{sp:>5d}D {lpp:>5d}D {r['accel_hit']:7.1f}% {r['accel_ret']:+11.2f}% {r['n_tests']:>7d}")

    print(f"\n  모멘텀 감속 시그널 (탭3: 하락 붕괴 후보)")
    print(f"  조건: 단기RS - 장기RS < -5%p → {fwd}D후 음의 초과수익 측정")
    print("-" * 65)
    print(f"{'Short':>7s} {'Long':>7s} {'히트율':>8s} {'평균초과수익':>12s} {'테스트수':>8s}")
    print("-" * 65)

    for (sp, lpp), r in sorted(pair_results.items(), key=lambda x: x[1]['decel_hit'], reverse=True):
        if r['n_tests'] > 0:
            print(f"{sp:>5d}D {lpp:>5d}D {r['decel_hit']:7.1f}% {r['decel_ret']:+11.2f}% {r['n_tests']:>7d}")

    return pair_results


# ============================================================
# 최종 결론 & 추천
# ============================================================
def final_recommendation(ic_summary, ls_summary, ws_results, pair_results, test_periods):
    """분석 결과 종합 → 한국 시장 최적 RS 설정 추천"""
    print("\n" + "=" * 70)
    print("  ★ 최종 분석 결론: 한국 시장 RS 최적 설정 ★")
    print("=" * 70)

    # 1. 가장 예측력 높은 개별 기간 (20D forward 기준)
    print("\n[1] 개별 기간 예측력 순위 (Forward 20D 기준 IC)")
    ic_20d = [(lp, ic_summary.get((lp, 20), 0)) for lp in test_periods]
    ic_20d.sort(key=lambda x: x[1], reverse=True)
    for rank, (lp, ic) in enumerate(ic_20d[:8], 1):
        ls = ls_summary.get((lp, 20), 0)
        print(f"  {rank}. {lp:>3d}D  IC={ic:+.4f}  L/S={ls:+.2f}%")

    # 2. 복합 가중치 최적
    print("\n[2] 복합 가중치 최적 설정")
    if ws_results:
        best_name = max(ws_results, key=lambda k: ws_results[k]['ic'])
        best = ws_results[best_name]
        print(f"  최적: {best_name}")
        print(f"  IC={best['ic']:+.4f}, 히트율={best['hit']:.1f}%, L/S={best['ls']:+.2f}%")
        print(f"  가중치:")
        for p, w in sorted(best['weights'].items()):
            bar = "█" * int(w * 50)
            print(f"    {p:>4d}D: {w:.3f} {bar}")

    # 3. 모멘텀 시그널 최적 쌍
    print("\n[3] 탭2 (상승 돌파) 최적 시그널")
    if pair_results:
        best_accel = max(pair_results.items(), key=lambda x: x[1]['accel_hit'])
        sp, lpp = best_accel[0]
        r = best_accel[1]
        print(f"  최적 쌍: {sp}D vs {lpp}D")
        print(f"  히트율={r['accel_hit']:.1f}%, 평균초과수익={r['accel_ret']:+.2f}%")

    print("\n[4] 탭3 (하락 붕괴) 최적 시그널")
    if pair_results:
        best_decel = max(pair_results.items(), key=lambda x: x[1]['decel_hit'])
        sp, lpp = best_decel[0]
        r = best_decel[1]
        print(f"  최적 쌍: {sp}D vs {lpp}D")
        print(f"  히트율={r['decel_hit']:.1f}%, 평균초과수익={r['decel_ret']:+.2f}%")

    print("\n" + "=" * 70)
    print("  → 이 결과를 기반으로 calc_rs_leaders.py 가중치 업데이트")
    print("  → 스캐너 탭1/2/3 파라미터 확정")
    print("=" * 70)


# ============================================================
# 메인
# ============================================================
def main():
    t_start = time.time()

    # 데이터 로드
    stock_closes, market_map, index_closes, dates = load_data()

    # Phase 1: 개별 기간 예측력
    results = phase1_individual_periods(stock_closes, market_map, index_closes, dates)
    ic_summary, ls_summary = print_phase1_results(results)

    # Phase 1에서 IC 상위 기간 선택 (20D forward 기준, 상위 6~8개)
    ic_20d = [(lp, ic_summary.get((lp, 20), 0)) for lp in TEST_PERIODS]
    ic_20d.sort(key=lambda x: x[1], reverse=True)
    best_periods = sorted([lp for lp, ic in ic_20d[:8] if ic > 0])
    if len(best_periods) < 4:
        best_periods = sorted([lp for lp, _ in ic_20d[:6]])
    print(f"\n  → Phase 2 진입 기간: {best_periods}")

    # Phase 2: 최적 복합 가중치
    ws_results = phase2_optimal_weights(stock_closes, market_map, index_closes, dates, best_periods)

    # Phase 3: 모멘텀 가속/붕괴 시그널
    pair_results = phase3_momentum_signals(stock_closes, market_map, index_closes, dates, best_periods)

    # 최종 결론
    final_recommendation(ic_summary, ls_summary, ws_results, pair_results, TEST_PERIODS)

    elapsed = time.time() - t_start
    print(f"\n  총 소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")


if __name__ == "__main__":
    main()
