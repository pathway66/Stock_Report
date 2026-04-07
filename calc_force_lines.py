"""
[*] AI+패스웨이 주도세력 분할 매수선/매도선 계산기 v1.0
=====================================================
종목별 5대 주체 수급 분석 → 주도/보조 세력 판별 → SB/SS 라인 산출

핵심 원리:
  - 종목마다 주도세력이 다르다 — 모든 수급분석의 출발점
  - 주도세력의 매집원가(SB)가 진짜 지지선, 매도원가(SS)가 진짜 저항선

산출 라인:
  SB5/SB20/SB60 — 세력매수선 (단기/중기/장기 지지)
  SS5/SS20/SS60 — 세력매도선 (단기/중기/장기 저항)

대상: KOSPI 시총 8천억+, KOSDAQ 시총 4천억+

사용법:
  python calc_force_lines.py               -> 오늘 날짜
  python calc_force_lines.py 20260402      -> 특정 날짜
  python calc_force_lines.py --backfill 30 -> 최근 30일 백필
"""

import requests
import json
import os
import sys
import time
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

SUBJECTS = ['외국인', '연기금', '투신', '사모펀드', '기타법인']
SHORT = {'외국인': '외', '연기금': '연', '투신': '투', '사모펀드': '사', '기타법인': '기'}


# ============================================================
# Supabase 헬퍼
# ============================================================
def sb_headers_read():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

def sb_headers_write():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates"
    }

def sb_read(table, params=""):
    """Supabase REST API 페이징 읽기 (1000건 제한 대응)"""
    all_data = []
    offset = 0
    while True:
        sep = "&" if params else ""
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}{sep}limit=1000&offset={offset}"
        r = requests.get(url, headers=sb_headers_read())
        if r.status_code != 200:
            print(f"  [!] DB 읽기 오류 ({table}): {r.status_code} {r.text[:100]}")
            break
        data = r.json()
        if not data:
            break
        all_data.extend(data)
        if len(data) < 1000:
            break
        offset += 1000
    return all_data

def sb_upsert(table, rows, on_conflict="date,stock_code"):
    """Supabase upsert (50건 배치)"""
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), 50):
        batch = rows[i:i+50]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}",
            headers=sb_headers_write(),
            json=batch
        )
        if r.status_code in [200, 201]:
            total += len(batch)
        else:
            print(f"  [!] upsert 실패: {r.status_code} {r.text[:200]}")
    return total


# ============================================================
# 대상 종목 로드 (코팔/닥사)
# ============================================================
def get_target_stocks(date):
    """KOSPI 8천억+, KOSDAQ 4천억+ 종목 리스트"""
    rows = sb_read("daily_market", f"date=eq.{date}&select=stock_code,stock_name,market,market_cap&order=market_cap.desc")
    stocks = []
    for s in rows:
        mkt = s.get('market', '')
        cap = s.get('market_cap', 0) or 0
        if mkt == 'KOSPI' and cap >= 800000000000:
            stocks.append((s['stock_code'], s['stock_name'], mkt))
        elif mkt == 'KOSDAQ' and cap >= 400000000000:
            stocks.append((s['stock_code'], s['stock_name'], mkt))
    return stocks


# ============================================================
# 주도세력 판별 + SB/SS 라인 계산 (핵심 엔진)
# ============================================================
def analyze_stock(stock_code, supply_data, ohlcv_data=None):
    """
    종목 1개에 대해 주도세력 판별 + SB/SS 라인 계산

    Args:
        stock_code: 종목코드
        supply_data: daily_supply에서 가져온 해당 종목의 수급 데이터 (최근 60일+)
        ohlcv_data: daily_ohlcv에서 가져온 해당 종목의 일봉 데이터 (시그널용, optional)

    Returns:
        dict: 계산 결과 (주도세력, SB/SS 라인, 시그널)
    """

    # ─── 1. 일별 순매수금액 + 추정평균가 정리 ───
    # key: (date, subject) → {net_amount, buy_amount, sell_amount, buy_avg_price, sell_avg_price}
    daily = defaultdict(lambda: defaultdict(lambda: {
        'buy_amount': 0, 'sell_amount': 0,
        'buy_avg_price': 0, 'sell_avg_price': 0,
        'net_amount': 0
    }))

    for row in supply_data:
        d = row['date']
        subj = row['subject']
        direction = row['direction']
        amount = abs(row.get('amount', 0) or 0)
        avg_price = row.get('avg_price', 0) or 0

        if direction == '매수':
            daily[d][subj]['buy_amount'] = amount
            daily[d][subj]['buy_avg_price'] = avg_price
        elif direction == '매도':
            daily[d][subj]['sell_amount'] = amount
            daily[d][subj]['sell_avg_price'] = avg_price

    # 순매수금액 계산
    for d in daily:
        for subj in daily[d]:
            info = daily[d][subj]
            info['net_amount'] = info['buy_amount'] - info['sell_amount']

    dates_sorted = sorted(daily.keys())
    if len(dates_sorted) < 5:
        return None  # 데이터 부족

    # ─── 2. 주도세력 판별 (최근 20일 기준) ───
    recent_20 = dates_sorted[-20:] if len(dates_sorted) >= 20 else dates_sorted

    # 주체별 20일 누적 순매수
    cum_net = {subj: 0 for subj in SUBJECTS}
    cum_buy = {subj: 0 for subj in SUBJECTS}
    cum_sell = {subj: 0 for subj in SUBJECTS}
    consecutive_buy = {subj: 0 for subj in SUBJECTS}  # 연속 매수일수

    for d in recent_20:
        for subj in SUBJECTS:
            info = daily[d].get(subj, {})
            net = info.get('net_amount', 0)
            cum_net[subj] += net
            if net > 0:
                cum_buy[subj] += net
                consecutive_buy[subj] += 1
            elif net < 0:
                cum_sell[subj] += abs(net)
                consecutive_buy[subj] = 0  # 리셋 (마지막 연속만 카운트)

    # 마지막 연속 매수일수 재계산 (역순)
    for subj in SUBJECTS:
        count = 0
        for d in reversed(recent_20):
            info = daily[d].get(subj, {})
            if info.get('net_amount', 0) > 0:
                count += 1
            else:
                break
        consecutive_buy[subj] = count

    # 순매수 비중 기반 주도/보조 판별
    # 매수 주체: 순매수 > 0인 주체만
    buyers = {s: cum_net[s] for s in SUBJECTS if cum_net[s] > 0}
    sellers = {s: abs(cum_net[s]) for s in SUBJECTS if cum_net[s] < 0}

    total_buy = sum(buyers.values()) if buyers else 1
    total_sell = sum(sellers.values()) if sellers else 1

    # 매수 측 랭킹
    buy_ranked = sorted(buyers.items(), key=lambda x: x[1], reverse=True)
    dominant_buyer = buy_ranked[0][0] if buy_ranked else None
    dominant_buyer_pct = round(buy_ranked[0][1] / total_buy * 100, 1) if buy_ranked else 0
    sub_buyer = buy_ranked[1][0] if len(buy_ranked) > 1 else None
    sub_buyer_pct = round(buy_ranked[1][1] / total_buy * 100, 1) if len(buy_ranked) > 1 else 0

    # 매도 측 랭킹
    sell_ranked = sorted(sellers.items(), key=lambda x: x[1], reverse=True)
    dominant_seller = sell_ranked[0][0] if sell_ranked else None
    dominant_seller_pct = round(sell_ranked[0][1] / total_sell * 100, 1) if sell_ranked else 0

    # ─── 3. SB 라인 계산 (세력매수선) ───
    def calc_sb(n_days):
        """최근 N일간 주도+보조 매수주체의 가중평균 매수가"""
        target_dates = dates_sorted[-n_days:] if len(dates_sorted) >= n_days else dates_sorted
        twp = 0  # total weighted price
        tw = 0   # total weight

        for d in target_dates:
            for subj in [dominant_buyer, sub_buyer]:
                if subj is None:
                    continue
                info = daily[d].get(subj, {})
                net = info.get('net_amount', 0)
                avg_p = info.get('buy_avg_price', 0)

                # 순매수일에만 해당 주체의 매수 평균가를 사용
                if net > 0 and avg_p > 0:
                    weight = net  # 순매수금액을 가중치로 사용
                    twp += avg_p * weight
                    tw += weight

        return round(twp / tw) if tw > 0 else 0

    sb5 = calc_sb(5)
    sb20 = calc_sb(20)
    sb60 = calc_sb(60)

    # ─── 4. SS 라인 계산 (세력매도선) ───
    def calc_ss(n_days):
        """최근 N일간 주도 매도주체의 가중평균 매도가"""
        target_dates = dates_sorted[-n_days:] if len(dates_sorted) >= n_days else dates_sorted
        twp = 0
        tw = 0

        for d in target_dates:
            if dominant_seller is None:
                continue
            info = daily[d].get(dominant_seller, {})
            net = info.get('net_amount', 0)
            avg_p = info.get('sell_avg_price', 0)

            # 순매도일에만 매도 평균가 사용
            if net < 0 and avg_p > 0:
                weight = abs(net)
                twp += avg_p * weight
                tw += weight

        return round(twp / tw) if tw > 0 else 0

    ss5 = calc_ss(5)
    ss20 = calc_ss(20)
    ss60 = calc_ss(60)

    # ─── 5. 매매 시그널 생성 ───
    signal_status = 'neutral'
    signal_detail = {}

    if ohlcv_data and len(ohlcv_data) > 0:
        # 가장 최근 OHLCV
        latest = ohlcv_data[-1] if isinstance(ohlcv_data, list) else None
        if latest:
            close = latest.get('close', 0) or 0
            volume = latest.get('volume', 0) or 0

            if close > 0 and sb5 > 0:
                # ── [A] 상승 분할매도 시그널 ──
                sb5_gap = round((close - sb5) / sb5 * 100, 2) if sb5 > 0 else 0
                sb20_gap = round((close - sb20) / sb20 * 100, 2) if sb20 > 0 else 0
                sb60_gap = round((close - sb60) / sb60 * 100, 2) if sb60 > 0 else 0

                # 1차: 세력과열 (SB5 이격 >= +15%)
                overheated = sb5_gap >= 15

                # 2차: 세력모멘텀 약화 (최근 5일 주도매수 < 20일평균 × 50%)
                momentum_weak = False
                if dominant_buyer and len(dates_sorted) >= 20:
                    recent_5_buy = sum(
                        daily[d].get(dominant_buyer, {}).get('net_amount', 0)
                        for d in dates_sorted[-5:]
                        if daily[d].get(dominant_buyer, {}).get('net_amount', 0) > 0
                    )
                    avg_20_buy = sum(
                        daily[d].get(dominant_buyer, {}).get('net_amount', 0)
                        for d in dates_sorted[-20:]
                        if daily[d].get(dominant_buyer, {}).get('net_amount', 0) > 0
                    ) / 20
                    momentum_weak = recent_5_buy < (avg_20_buy * 50 / 100 * 5) if avg_20_buy > 0 else False

                # 3차: 세력전환 (주도매수주체 2일 연속 순매도)
                force_switch = False
                if dominant_buyer and len(dates_sorted) >= 2:
                    last2 = dates_sorted[-2:]
                    force_switch = all(
                        daily[d].get(dominant_buyer, {}).get('net_amount', 0) < 0
                        for d in last2
                    )

                # ── [B] 하락 분할매도 시그널 ──
                if close < sb60 and sb60 > 0:
                    signal_status = 'exit'        # 탈출 구간
                elif close < sb20 and sb20 > 0:
                    signal_status = 'danger'      # 위험 구간
                elif close < sb5 and sb5 > 0:
                    signal_status = 'warning'     # 경고 구간
                elif overheated:
                    signal_status = 'overheated'  # 세력과열
                elif force_switch:
                    signal_status = 'force_switch'  # 세력전환
                elif momentum_weak:
                    signal_status = 'momentum_weak' # 모멘텀 약화
                else:
                    signal_status = 'buy_zone'    # 매수 구간

                # 돌파 시그널 체크
                if close > ss20 and ss20 > 0 and volume > 0:
                    # 20일 평균 거래량 계산
                    if len(ohlcv_data) >= 20:
                        avg_vol = sum(d.get('volume', 0) or 0 for d in ohlcv_data[-20:]) / 20
                        if volume > avg_vol * 1.5:
                            signal_status = 'breakout'

                signal_detail = {
                    'close': close,
                    'sb5_gap_pct': sb5_gap,
                    'sb20_gap_pct': sb20_gap,
                    'sb60_gap_pct': sb60_gap,
                    'overheated': overheated,
                    'momentum_weak': momentum_weak,
                    'force_switch': force_switch,
                    'consecutive_buy_days': consecutive_buy.get(dominant_buyer, 0) if dominant_buyer else 0,
                }

    return {
        'dominant_buyer': dominant_buyer,
        'dominant_buyer_pct': dominant_buyer_pct,
        'dominant_buyer_short': SHORT.get(dominant_buyer, '') if dominant_buyer else '',
        'sub_buyer': sub_buyer,
        'sub_buyer_pct': sub_buyer_pct,
        'sub_buyer_short': SHORT.get(sub_buyer, '') if sub_buyer else '',
        'dominant_seller': dominant_seller,
        'dominant_seller_pct': dominant_seller_pct,
        'dominant_seller_short': SHORT.get(dominant_seller, '') if dominant_seller else '',
        'sb5': sb5, 'sb20': sb20, 'sb60': sb60,
        'ss5': ss5, 'ss20': ss20, 'ss60': ss60,
        'signal_status': signal_status,
        'signal_detail': json.dumps(signal_detail, ensure_ascii=False) if signal_detail else '{}',
        'data_days': len(dates_sorted),
        'consecutive_buy_days': consecutive_buy.get(dominant_buyer, 0) if dominant_buyer else 0,
    }


# ============================================================
# 메인 실행
# ============================================================
def main():
    # 날짜 파싱
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    target_date = args[0] if args else datetime.now().strftime('%Y%m%d')
    if len(target_date) == 8:
        target_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"

    # 백필 모드
    backfill_days = 0
    if '--backfill' in sys.argv:
        idx = sys.argv.index('--backfill')
        if idx + 1 < len(sys.argv):
            backfill_days = int(sys.argv[idx + 1])

    print("=" * 60)
    print("[*] AI+패스웨이 주도세력 매수선/매도선 계산기 v1.0")
    print(f"    대상일: {target_date}")
    print(f"    대상: KOSPI 8천억+, KOSDAQ 4천억+")
    print("=" * 60)

    start_time = time.time()

    # STEP 1: 대상 종목 로드
    print(f"\n[>] STEP 1: 대상 종목 로드 (daily_market)")
    stocks = get_target_stocks(target_date)
    if not stocks:
        print("[!] 대상 종목이 없습니다. daily_market 데이터 확인 필요.")
        return
    print(f"    -> {len(stocks)}종목 (KOSPI+KOSDAQ)")

    # STEP 2: 수급 데이터 로드 (최근 70일, 60일 계산 + 여유)
    # 전체 수급 데이터를 한 번에 로드 (종목별 개별 쿼리 대신 효율적)
    print(f"\n[>] STEP 2: 수급 데이터 로드 (daily_supply, 최근 70일)")

    from datetime import date as date_type
    dt = datetime.strptime(target_date, '%Y-%m-%d')
    start_date = (dt - timedelta(days=100)).strftime('%Y-%m-%d')  # 달력 100일 = 약 70거래일

    supply_data = sb_read(
        "daily_supply",
        f"date=gte.{start_date}&date=lte.{target_date}"
        f"&select=date,stock_code,subject,direction,amount,avg_price"
        f"&order=date.asc"
    )
    print(f"    -> {len(supply_data)}건 로드")

    # 종목별로 그룹핑
    supply_by_stock = defaultdict(list)
    for row in supply_data:
        supply_by_stock[row['stock_code']].append(row)

    # STEP 2.5: OHLCV 데이터 로드 (시그널 계산용)
    print(f"\n[>] STEP 2.5: OHLCV 데이터 로드 (daily_ohlcv, 최근 70일)")
    ohlcv_data = sb_read(
        "daily_ohlcv",
        f"date=gte.{start_date}&date=lte.{target_date}"
        f"&select=date,stock_code,close,volume"
        f"&order=date.asc"
    )
    print(f"    -> {len(ohlcv_data)}건 로드")

    ohlcv_by_stock = defaultdict(list)
    for row in ohlcv_data:
        ohlcv_by_stock[row['stock_code']].append(row)

    # STEP 3: 종목별 분석
    print(f"\n[>] STEP 3: 종목별 주도세력 분석 + SB/SS 라인 계산")
    print("-" * 60)

    results = []
    skipped = 0

    for i, (code, name, market) in enumerate(stocks, 1):
        sup = supply_by_stock.get(code, [])
        ohlcv = ohlcv_by_stock.get(code, [])

        if not sup:
            skipped += 1
            continue

        analysis = analyze_stock(code, sup, ohlcv)
        if analysis is None:
            skipped += 1
            continue

        row = {
            'date': target_date,
            'stock_code': code,
            'stock_name': name,
            'market': market,
            'dominant_buyer': analysis['dominant_buyer'],
            'dominant_buyer_pct': analysis['dominant_buyer_pct'],
            'sub_buyer': analysis['sub_buyer'],
            'sub_buyer_pct': analysis['sub_buyer_pct'],
            'dominant_seller': analysis['dominant_seller'],
            'dominant_seller_pct': analysis['dominant_seller_pct'],
            'sb5': analysis['sb5'] if analysis['sb5'] > 0 else None,
            'sb20': analysis['sb20'] if analysis['sb20'] > 0 else None,
            'sb60': analysis['sb60'] if analysis['sb60'] > 0 else None,
            'ss5': analysis['ss5'] if analysis['ss5'] > 0 else None,
            'ss20': analysis['ss20'] if analysis['ss20'] > 0 else None,
            'ss60': analysis['ss60'] if analysis['ss60'] > 0 else None,
            'signal_status': analysis['signal_status'],
            'signal_detail': analysis['signal_detail'],
            'data_days': analysis['data_days'],
            'consecutive_buy_days': analysis['consecutive_buy_days'],
        }
        results.append(row)

        # 진행 상황 출력 (50개마다)
        if i % 50 == 0 or i == len(stocks):
            print(f"    [{i}/{len(stocks)}] 분석 완료")

    print(f"\n    분석 완료: {len(results)}종목 (스킵: {skipped})")

    # STEP 4: 결과 출력 (TOP 10 미리보기)
    print(f"\n[>] 주도세력 분석 TOP 10 (매수세력 비중 순)")
    print("-" * 60)

    # 매수세력 비중 상위 + SB20 기준으로 정렬
    sorted_results = sorted(results, key=lambda x: x.get('dominant_buyer_pct', 0), reverse=True)

    for r in sorted_results[:10]:
        buyer_label = f"{SHORT.get(r['dominant_buyer'], '?')}({r['dominant_buyer_pct']}%)"
        sub_label = f"+{SHORT.get(r['sub_buyer'], '?')}({r['sub_buyer_pct']}%)" if r['sub_buyer'] else ""
        seller_label = f"{SHORT.get(r['dominant_seller'], '?')}({r['dominant_seller_pct']}%)" if r['dominant_seller'] else ""

        sb20_str = f"{r['sb20']:,}" if r['sb20'] else "N/A"
        ss20_str = f"{r['ss20']:,}" if r['ss20'] else "N/A"

        print(f"  {r['stock_name']:12s} | 매수: {buyer_label}{sub_label:12s} | 매도: {seller_label:12s} | SB20: {sb20_str:>10s} | SS20: {ss20_str:>10s} | {r['signal_status']}")

    # STEP 5: DB 저장
    print(f"\n[>] STEP 5: Supabase force_buy_sell_lines 저장")
    saved = sb_upsert("force_buy_sell_lines", results, on_conflict="date,stock_code")
    print(f"    -> {saved}건 저장 완료")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"[OK] 전체 완료! {len(results)}종목, 소요시간: {elapsed:.1f}초")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
