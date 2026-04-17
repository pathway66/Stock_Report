"""
[*] AI+패스웨이 RS Leaders 엔진 v1.0
==================================================
RS(상대강도) 퍼센타일 기반 리더주 분석 엔진
"RS First, 수급 Alpha" 전략의 핵심 계산 모듈

처리 흐름:
  ① 유니버스 필터링 (시총 기준)
  ② 종목별 기간별 수익률 계산
  ③ 지수 대비 초과수익률 산출
  ④ 시장별 퍼센타일 순위
  ⑤ RS 유형 판별 (공격형/방어형)
  ⑥ 수급 콤보 크로스 + 슈퍼리더 판별
  ⑦ rs_leaders 테이블 UPSERT

사용법:
  python calculate_rs_leaders.py                -> 오늘 날짜로 계산
  python calculate_rs_leaders.py 20260404       -> 특정 날짜 계산

필요 라이브러리:
  pip install requests python-dotenv scipy numpy
"""

import requests
import os
import sys
import time
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from scipy import stats

# .env 파일 로드
load_dotenv()

# ============================================================
# 설정
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# RS 측정 기간 (영업일 기준)
PERIODS = [1, 5, 10, 20, 40, 60, 120, 200]
PERIOD_COLS = {p: f"{p}d" for p in PERIODS}

# 유니버스 시총 기준 (원)
MKTCAP_KOSPI = 800_000_000_000    # 8,000억원
MKTCAP_KOSDAQ = 400_000_000_000   # 4,000억원

# 퍼센타일 등급
PCTL_TIERS = {
    'top1': 99,
    'top3': 97,
    'top5': 95,
    'top10': 90,
}

# 슈퍼리더 수급 등급 기준
SUPER_COMBO_GRADES = ['S', 'A1', 'A2']


# ============================================================
# Supabase 클라이언트
# ============================================================
class SupabaseDB:
    def __init__(self, url, key):
        self.url = url.rstrip('/')
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }
        self.insert_count = 0

    def upsert(self, table, rows, on_conflict=None):
        if not rows:
            return True
        if on_conflict is None:
            conflict_map = {
                "rs_leaders": "date,stock_code",
                "daily_index_returns": "date,index_code",
            }
            on_conflict = conflict_map.get(table, "")
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            url = f"{self.url}/rest/v1/{table}"
            if on_conflict:
                url += f"?on_conflict={on_conflict}"
            resp = requests.post(url, headers=self.headers, json=batch)
            if resp.status_code in [200, 201, 204]:
                self.insert_count += len(batch)
            else:
                print(f"    [W] DB 오류 ({table}): {resp.status_code} {resp.text[:200]}")
                return False
        return True

    def query(self, table, params=""):
        """Supabase REST 조회 (페이지네이션 포함)"""
        all_rows = []
        offset = 0
        limit = 1000  # Supabase 최대 1000행

        while True:
            paged_params = f"{params}&limit={limit}&offset={offset}" if params else f"limit={limit}&offset={offset}"
            url = f"{self.url}/rest/v1/{table}?{paged_params}"
            resp = requests.get(url, headers={
                "apikey": self.headers["apikey"],
                "Authorization": self.headers["Authorization"]
            })
            if resp.status_code != 200:
                print(f"    [W] 조회 오류 ({table}): {resp.status_code}")
                break
            rows = resp.json()
            all_rows.extend(rows)
            if len(rows) < limit:
                break
            offset += limit

        return all_rows

    def query_single(self, table, params=""):
        """단일 페이지 조회 (limit 기반)"""
        url = f"{self.url}/rest/v1/{table}?{params}" if params else f"{self.url}/rest/v1/{table}"
        resp = requests.get(url, headers={
            "apikey": self.headers["apikey"],
            "Authorization": self.headers["Authorization"]
        })
        return resp.json() if resp.status_code == 200 else []


# ============================================================
# STEP 1: 유니버스 필터링
# ============================================================
def get_universe(db, target_date):
    """
    시총 기준 충족 종목 추출 (daily_market → fallback daily_supply_v2)
    KOSPI: 시총 8,000억+  /  KOSDAQ: 시총 4,000억+
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"  [>] 유니버스 필터링 ({date_obj})...")

    # 1차: daily_market 조회
    params = (
        f"date=eq.{date_obj}"
        f"&select=stock_code,stock_name,market,market_cap"
        f"&order=market_cap.desc"
    )
    rows = db.query("daily_market", params)

    # 2차: fallback → daily_supply_v2에서 시총 추출 (중복 제거)
    if not rows:
        print(f"    [i] daily_market 없음 -> daily_supply_v2 fallback")
        params2 = (
            f"date=eq.{date_obj}"
            f"&select=stock_code,stock_name,market,market_cap"
            f"&market_cap=gt.0"
            f"&order=market_cap.desc"
        )
        raw = db.query("daily_supply_v2", params2)
        seen = set()
        rows = []
        for r in (raw or []):
            code = r.get('stock_code', '')
            if code not in seen:
                seen.add(code)
                rows.append(r)

    if not rows:
        print(f"    [W] {date_obj} 시총 데이터 없음")
        return {}

    universe = {}
    kospi_count = 0
    kosdaq_count = 0

    for r in rows:
        code = r.get('stock_code', '')
        market = r.get('market', '')
        mktcap = r.get('market_cap', 0) or 0

        if market == 'KOSPI' and mktcap >= MKTCAP_KOSPI:
            universe[code] = {
                'name': r.get('stock_name', ''),
                'market': 'KOSPI',
                'market_cap': mktcap,
            }
            kospi_count += 1
        elif market == 'KOSDAQ' and mktcap >= MKTCAP_KOSDAQ:
            universe[code] = {
                'name': r.get('stock_name', ''),
                'market': 'KOSDAQ',
                'market_cap': mktcap,
            }
            kosdaq_count += 1

    print(f"    KOSPI: {kospi_count}종목 / KOSDAQ: {kosdaq_count}종목 / 합계: {len(universe)}종목")
    return universe


# ============================================================
# STEP 2: 종목별 수익률 계산
# ============================================================
def get_stock_returns(db, target_date, universe):
    """
    daily_ohlcv에서 유니버스 종목의 기간별 수익률 계산
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"  [>] 종목 수익률 계산 중...")

    # 최근 거래일 목록 조회 (daily_index: 페이지네이션으로 전체 조회)
    date_params = (
        f"date=lte.{date_obj}"
        f"&select=date"
        f"&order=date.desc"
    )
    date_rows = db.query("daily_index", date_params)
    if not date_rows:
        print(f"    [X] OHLCV 데이터 없음")
        return {}

    # 고유 거래일 목록 (최신→과거 순)
    trade_dates = sorted(set(r['date'] for r in date_rows), reverse=True)
    if not trade_dates:
        print(f"    [X] 거래일 없음")
        return {}

    today_date = trade_dates[0]
    print(f"    최신 거래일: {today_date} (총 {len(trade_dates)}일 확보)")

    # 기간별 필요한 과거 날짜 매핑
    period_dates = {}
    for p in PERIODS:
        if len(trade_dates) > p:
            period_dates[p] = trade_dates[p]
        else:
            period_dates[p] = None
            print(f"    [W] {p}D 데이터 부족")

    # 필요한 날짜 목록
    needed_dates = [today_date]
    for p, d in period_dates.items():
        if d and d not in needed_dates:
            needed_dates.append(d)

    # 종목 코드 리스트
    stock_codes = list(universe.keys())

    # 각 날짜의 종가 데이터 수집
    close_by_date = {}  # {date: {stock_code: close_price}}
    for d in needed_dates:
        print(f"    [{d}] 종가 조회...", end=" ", flush=True)
        params = (
            f"date=eq.{d}"
            f"&select=stock_code,close"
        )
        rows = db.query("daily_ohlcv", params)
        prices = {}
        for r in rows:
            code = r.get('stock_code', '')
            close = r.get('close', 0)
            if code in universe and close and close > 0:
                prices[code] = float(close)
        close_by_date[d] = prices
        print(f"{len(prices)}종목")

    # 종목별 기간별 수익률 계산
    stock_returns = {}  # {stock_code: {1: return_1d, 5: return_5d, ...}}

    today_prices = close_by_date.get(today_date, {})

    for code in stock_codes:
        if code not in today_prices:
            continue

        today_close = today_prices[code]
        returns = {}

        for p in PERIODS:
            past_date = period_dates.get(p)
            if not past_date:
                returns[p] = None
                continue
            past_prices = close_by_date.get(past_date, {})
            past_close = past_prices.get(code)
            if past_close and past_close > 0:
                returns[p] = (today_close / past_close - 1) * 100
            else:
                returns[p] = None

        stock_returns[code] = returns

    print(f"    수익률 계산 완료: {len(stock_returns)}종목")
    return stock_returns


# ============================================================
# STEP 2-B: 눌림목 지표 계산 (drawdown_20d, vol_ratio)
# ============================================================
def get_pullback_indicators(db, target_date, universe):
    """
    눌림목 반등 스캐너용 지표 계산:
    - drawdown_20d: 최근 20거래일 고가 대비 오늘 종가 하락률 (%)
    - vol_ratio: 오늘 거래량 / 최근 20일 평균 거래량
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"  [>] 눌림목 지표 계산 (drawdown_20d, vol_ratio)...")

    # 최근 21거래일 목록 (오늘 포함)
    date_params = (
        f"date=lte.{date_obj}"
        f"&select=date"
        f"&order=date.desc"
        f"&limit=21"
    )
    date_rows = db.query_single("daily_index", date_params)
    trade_dates = sorted(set(r['date'] for r in date_rows), reverse=True)

    if len(trade_dates) < 2:
        print(f"    [W] 거래일 부족")
        return {}

    today_date = trade_dates[0]
    past_20_dates = trade_dates[:21]  # 오늘 포함 21일 (오늘 + 과거 20일)

    # 최근 21일 OHLCV 데이터 조회
    date_list = ",".join(f'"{d}"' for d in past_20_dates)
    # Supabase에서 날짜 목록으로 필터링
    print(f"    최근 {len(past_20_dates)}거래일 OHLCV 조회...", end=" ", flush=True)

    all_ohlcv = []
    for d in past_20_dates:
        params = f"date=eq.{d}&select=stock_code,date,high,close,volume"
        rows = db.query("daily_ohlcv", params)
        all_ohlcv.extend(rows)

    print(f"{len(all_ohlcv)}건")

    # 종목별로 정리
    from collections import defaultdict
    stock_data = defaultdict(list)
    for r in all_ohlcv:
        code = r.get('stock_code', '')
        if code in universe:
            stock_data[code].append({
                'date': r['date'],
                'high': float(r.get('high', 0) or 0),
                'close': float(r.get('close', 0) or 0),
                'volume': int(r.get('volume', 0) or 0),
            })

    # 계산
    pullback = {}  # {stock_code: {'drawdown_20d': ..., 'vol_ratio': ...}}

    for code, days in stock_data.items():
        # 날짜 정렬 (최신→과거)
        days.sort(key=lambda x: x['date'], reverse=True)

        if len(days) < 2:
            continue

        today = days[0]
        today_close = today['close']
        today_vol = today['volume']

        if today_close <= 0:
            continue

        # drawdown_20d: 최근 20일 고가 중 최고 대비 오늘 종가 하락률
        highs = [d['high'] for d in days if d['high'] > 0]
        if highs:
            max_high = max(highs)
            drawdown = (today_close / max_high - 1) * 100 if max_high > 0 else 0
        else:
            drawdown = 0

        # vol_ratio: 오늘 거래량 / 과거 20일 평균 거래량
        past_vols = [d['volume'] for d in days[1:] if d['volume'] > 0]
        if past_vols and today_vol > 0:
            avg_vol = sum(past_vols) / len(past_vols)
            vol_ratio = today_vol / avg_vol if avg_vol > 0 else 0
        else:
            vol_ratio = 0

        pullback[code] = {
            'drawdown_20d': round(drawdown, 2),
            'vol_ratio': round(vol_ratio, 2),
        }

    print(f"    눌림목 지표 계산 완료: {len(pullback)}종목")
    return pullback


# STEP 3: 지수 수익률 가져오기
# ============================================================
def get_index_returns(db, target_date):
    """
    daily_index_returns에서 지수 기간별 수익률 조회
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"  [>] 지수 수익률 조회...")

    # 해당 날짜 또는 가장 최근 데이터
    params = (
        f"date=lte.{date_obj}"
        f"&order=date.desc"
        f"&limit=2"
    )
    rows = db.query_single("daily_index_returns", params)

    index_returns = {}  # {'KOSPI': {1: return_1d, ...}, 'KOSDAQ': {...}}

    for r in rows:
        idx = r['index_code']
        returns = {}
        for p in PERIODS:
            col = f"return_{p}d"
            val = r.get(col)
            returns[p] = float(val) if val is not None else None
        index_returns[idx] = returns
        r1 = returns.get(1)
        r20 = returns.get(20)
        print(f"    {idx}: 1D={r1 if r1 is not None else 'N/A'}% | "
              f"20D={r20 if r20 is not None else 'N/A'}%")

    return index_returns


# ============================================================
# STEP 4: 초과수익률 + 퍼센타일 + RS유형 계산
# ============================================================
def calculate_rs(universe, stock_returns, index_returns, target_date):
    """
    모든 종목에 대해:
    1) 초과수익률 = stock_return - index_return
    2) 시장별 퍼센타일 순위
    3) RS 유형 판별 (공격형/방어형)
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"  [>] 초과수익률 & 퍼센타일 계산...")

    # 시장별로 분리
    market_stocks = {'KOSPI': [], 'KOSDAQ': []}
    for code, info in universe.items():
        if code in stock_returns:
            market_stocks[info['market']].append(code)

    # 결과 저장
    results = {}  # {stock_code: {전체 RS 데이터}}

    for market in ['KOSPI', 'KOSDAQ']:
        codes = market_stocks.get(market, [])
        idx_returns = index_returns.get(market, {})

        if not codes or not idx_returns:
            print(f"    [W] {market}: 데이터 부족 (종목: {len(codes)}, 지수: {bool(idx_returns)})")
            continue

        print(f"    {market}: {len(codes)}종목 처리...", end=" ", flush=True)

        for period in PERIODS:
            idx_ret = idx_returns.get(period)
            if idx_ret is None:
                continue

            # 모든 종목의 초과수익률 배열
            excess_list = []
            code_excess_map = {}

            for code in codes:
                stk_ret = stock_returns.get(code, {}).get(period)
                if stk_ret is None:
                    continue
                excess = stk_ret - idx_ret
                excess_list.append(excess)
                code_excess_map[code] = (stk_ret, excess)

            if not excess_list:
                continue

            excess_array = np.array(excess_list)

            # RS 유형 판별 (기간별)
            rs_type = 'offensive' if idx_ret >= 0 else 'defensive'

            # 각 종목의 퍼센타일 계산
            for code, (stk_ret, excess) in code_excess_map.items():
                pctl = stats.percentileofscore(excess_array, excess, kind='rank')

                if code not in results:
                    info = universe[code]
                    results[code] = {
                        'date': date_obj,
                        'stock_code': code,
                        'stock_name': info['name'],
                        'market': info['market'],
                        'market_cap': info['market_cap'],
                    }

                results[code][f'return_{period}d'] = round(stk_ret, 4)
                results[code][f'excess_{period}d'] = round(excess, 4)
                results[code][f'pctl_{period}d'] = round(pctl, 2)
                results[code][f'rs_type_{period}d'] = rs_type

        print(f"[OK]")

    print(f"    RS 계산 완료: {len(results)}종목")
    return results


# ============================================================
# STEP 5: 수급 콤보 크로스 + 슈퍼리더 판별
# ============================================================
def apply_combo_and_super(db, results, target_date):
    """
    sr_supply_grades에서 수급 콤보 등급을 JOIN하여
    슈퍼리더(RS Top 3% + S/A 콤보) 판별
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"  [>] 수급 콤보 크로스 + 슈퍼리더 판별...")

    # 수급 콤보 데이터 조회 (실제 컬럼: grade, combo)
    params = (
        f"select=stock_code,grade,combo"
        f"&order=stock_code"
    )
    combo_rows = db.query("sr_supply_grades", params)

    # stock_code별 최고 등급 매핑
    combo_map = {}
    grade_priority = {'S': 0, 'A1': 1, 'A2': 2, 'B1': 3, 'B2': 4, 'C1': 5, 'C2': 6}

    for r in combo_rows:
        code = r.get('stock_code', '')
        grade = r.get('grade', '')
        label = r.get('combo', '')

        if code not in combo_map:
            combo_map[code] = {'grade': grade, 'label': label}
        else:
            # 더 높은 등급으로 업데이트
            current_priority = grade_priority.get(combo_map[code]['grade'], 99)
            new_priority = grade_priority.get(grade, 99)
            if new_priority < current_priority:
                combo_map[code] = {'grade': grade, 'label': label}

    combo_matched = 0
    super_count = 0

    for code, data in results.items():
        combo = combo_map.get(code)
        if combo:
            data['combo_grade'] = combo['grade']
            data['combo_label'] = combo['label']
            combo_matched += 1
        else:
            data['combo_grade'] = None
            data['combo_label'] = None

        # 슈퍼리더 판별
        is_super = False
        super_type = None
        has_offensive_super = False
        has_defensive_super = False

        combo_grade = data.get('combo_grade', '')
        is_good_combo = combo_grade in SUPER_COMBO_GRADES

        if is_good_combo:
            for period in PERIODS:
                pctl = data.get(f'pctl_{period}d')
                rs_type = data.get(f'rs_type_{period}d')

                if pctl is not None and pctl >= 97:  # Top 3%
                    if rs_type == 'offensive':
                        has_offensive_super = True
                    elif rs_type == 'defensive':
                        has_defensive_super = True

        if has_offensive_super and has_defensive_super:
            is_super = True
            super_type = 'dual_super'
        elif has_offensive_super:
            is_super = True
            super_type = 'offensive_super'
        elif has_defensive_super:
            is_super = True
            super_type = 'defensive_super'

        data['is_super_leader'] = is_super
        data['super_type'] = super_type

        if is_super:
            super_count += 1

    print(f"    수급 콤보 매칭: {combo_matched}종목")
    print(f"    [*] 슈퍼리더 발견: {super_count}종목")

    # 슈퍼리더 상세 출력
    if super_count > 0:
        print(f"\n    {'='*55}")
        print(f"    [*] 슈퍼리더 목록")
        print(f"    {'='*55}")
        for code, data in results.items():
            if data.get('is_super_leader'):
                name = data.get('stock_name', '')
                market = data.get('market', '')
                grade = data.get('combo_grade', '')
                label = data.get('combo_label', '')
                stype = data.get('super_type', '')
                pctl_20 = data.get('pctl_20d', '')
                print(f"    [{market}] {code} {name} | "
                      f"콤보: {grade}({label}) | "
                      f"유형: {stype} | "
                      f"20D 퍼센타일: {pctl_20}")
        print(f"    {'='*55}")

    return results


# ============================================================
# STEP 6: 섹터 매핑
# ============================================================
def apply_sector(db, results):
    """sector_map 테이블에서 섹터 정보 매핑"""
    print(f"  [>] 섹터 매핑...", end=" ", flush=True)

    params = "select=stock_code,sector"
    sector_rows = db.query("sector_map", params)

    sector_map = {r['stock_code']: r.get('sector', '') for r in sector_rows}

    matched = 0
    for code, data in results.items():
        sector = sector_map.get(code)
        if sector:
            data['sector'] = sector
            matched += 1
        else:
            data['sector'] = None

    print(f"[OK] {matched}종목 매핑")
    return results


# ============================================================
# STEP 7: rs_leaders 테이블에 UPSERT
# ============================================================
def save_rs_leaders(db, results):
    """결과를 rs_leaders 테이블에 저장"""
    print(f"  [>] rs_leaders 테이블 저장...")

    db_rows = []
    for code, data in results.items():
        row = {
            'date': data['date'],
            'stock_code': data['stock_code'],
            'stock_name': data.get('stock_name'),
            'market': data['market'],
            'sector': data.get('sector'),
            'market_cap': data.get('market_cap'),

            # 수급 콤보
            'combo_grade': data.get('combo_grade'),
            'combo_label': data.get('combo_label'),

            # 슈퍼리더
            'is_super_leader': data.get('is_super_leader', False),
            'super_type': data.get('super_type'),

            # 눌림목 지표
            'drawdown_20d': data.get('drawdown_20d'),
            'vol_ratio': data.get('vol_ratio'),
        }

        # 기간별 데이터
        for p in PERIODS:
            suffix = f"{p}d"
            row[f'return_{suffix}'] = data.get(f'return_{suffix}')
            row[f'excess_{suffix}'] = data.get(f'excess_{suffix}')
            row[f'pctl_{suffix}'] = data.get(f'pctl_{suffix}')
            row[f'rs_type_{suffix}'] = data.get(f'rs_type_{suffix}')

        db_rows.append(row)

    if db_rows:
        success = db.upsert("rs_leaders", db_rows)
        if success:
            print(f"    [OK] {len(db_rows)}종목 저장 완료")
        else:
            print(f"    [X] 저장 실패")
    else:
        print(f"    [W] 저장할 데이터 없음")

    return len(db_rows)


# ============================================================
# 결과 요약 출력
# ============================================================
def print_summary(results):
    """상위 종목 요약"""
    print(f"\n  [>] RS Leaders 요약 (20D 기준)")

    for market in ['KOSPI', 'KOSDAQ']:
        market_results = {k: v for k, v in results.items() if v['market'] == market}
        if not market_results:
            continue

        # 20D 퍼센타일 상위 10개
        sorted_by_pctl = sorted(
            market_results.items(),
            key=lambda x: x[1].get('pctl_20d', 0) or 0,
            reverse=True
        )[:10]

        print(f"\n    === {market} Top 10 (20D RS 퍼센타일) ===")
        print(f"    {'#':>2} {'종목코드':>8} {'종목명':<12} {'수익률':>8} {'초과':>8} {'퍼센타일':>8} {'유형':>10} {'콤보':>5}")
        print(f"    {'-'*75}")

        for i, (code, data) in enumerate(sorted_by_pctl, 1):
            name = (data.get('stock_name', '') or '')[:10]
            ret = data.get('return_20d')
            exc = data.get('excess_20d')
            pctl = data.get('pctl_20d')
            rs_type = data.get('rs_type_20d', '')
            combo = data.get('combo_grade', '') or '-'
            super_mark = '[*]' if data.get('is_super_leader') else '  '

            print(f"    {i:>2} {code:>8} {name:<12} "
                  f"{ret:>7.2f}% {exc:>7.2f}% {pctl:>7.1f}% "
                  f"{rs_type:>10} {combo:>5} {super_mark}")


# ============================================================
# 메인
# ============================================================
def main():
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime("%Y%m%d")

    print("=" * 60)
    print("[*] AI+패스웨이 RS Leaders 엔진 v1.0")
    print(f"   날짜: {target_date}")
    print(f"   전략: RS First, 수급 Alpha")
    print("=" * 60)

    # Supabase 연결
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] SUPABASE_URL, SUPABASE_KEY를 설정하세요.")
        return

    db = SupabaseDB(SUPABASE_URL, SUPABASE_KEY)

    start_total = time.time()

    # STEP 1: 유니버스 필터링
    print(f"\n{'-'*50}")
    print(f"[>] STEP 1: 유니버스 필터링")
    print(f"{'-'*50}")
    universe = get_universe(db, target_date)
    if not universe:
        print("[X] 유니버스 비어있음. 종료.")
        return

    # STEP 2: 종목별 수익률 계산
    print(f"\n{'-'*50}")
    print(f"[>] STEP 2: 종목별 수익률 계산")
    print(f"{'-'*50}")
    stock_returns = get_stock_returns(db, target_date, universe)
    if not stock_returns:
        print("[X] 종목 수익률 없음. 종료.")
        return

    # STEP 2-B: 눌림목 지표 계산
    print(f"\n{'-'*50}")
    print(f"[>] STEP 2-B: 눌림목 지표 (drawdown_20d, vol_ratio)")
    print(f"{'-'*50}")
    pullback_indicators = get_pullback_indicators(db, target_date, universe)

    # STEP 3: 지수 수익률 가져오기
    print(f"\n{'-'*50}")
    print(f"[>] STEP 3: 지수 수익률 조회")
    print(f"{'-'*50}")
    index_returns = get_index_returns(db, target_date)
    if not index_returns:
        print("[X] 지수 수익률 없음. collect_index_data.py를 먼저 실행하세요.")
        return

    # STEP 4: 초과수익률 + 퍼센타일 + RS유형 계산
    print(f"\n{'-'*50}")
    print(f"[>] STEP 4: 초과수익률 & 퍼센타일 & RS유형")
    print(f"{'-'*50}")
    results = calculate_rs(universe, stock_returns, index_returns, target_date)
    if not results:
        print("[X] RS 계산 결과 없음. 종료.")
        return

    # STEP 5: 섹터 매핑
    print(f"\n{'-'*50}")
    print(f"[>] STEP 5: 섹터 매핑")
    print(f"{'-'*50}")
    results = apply_sector(db, results)

    # STEP 6: 수급 콤보 크로스 + 슈퍼리더 판별
    print(f"\n{'-'*50}")
    print(f"[>] STEP 6: 수급 콤보 크로스 + 슈퍼리더")
    print(f"{'-'*50}")
    results = apply_combo_and_super(db, results, target_date)

    # STEP 6-B: 눌림목 지표 병합
    print(f"\n{'-'*50}")
    print(f"[>] STEP 6-B: 눌림목 지표 병합")
    print(f"{'-'*50}")
    merged = 0
    for code, data in results.items():
        pb = pullback_indicators.get(code)
        if pb:
            data['drawdown_20d'] = pb['drawdown_20d']
            data['vol_ratio'] = pb['vol_ratio']
            merged += 1
        else:
            data['drawdown_20d'] = None
            data['vol_ratio'] = None
    print(f"    눌림목 지표 병합: {merged}종목")

    # STEP 7: DB 저장
    print(f"\n{'-'*50}")
    print(f"[>] STEP 7: rs_leaders 저장")
    print(f"{'-'*50}")
    saved_count = save_rs_leaders(db, results)

    # 요약 출력
    print_summary(results)

    # 완료
    total_time = time.time() - start_total
    print(f"\n{'='*60}")
    print(f"[OK] RS Leaders 계산 완료!")
    print(f"   총 소요시간: {total_time:.1f}초")
    print(f"   분석 종목: {len(results)}종목")
    print(f"   DB 저장: {db.insert_count}건")

    # 슈퍼리더 카운트
    super_count = sum(1 for v in results.values() if v.get('is_super_leader'))
    if super_count:
        print(f"   [*] 슈퍼리더: {super_count}종목")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
