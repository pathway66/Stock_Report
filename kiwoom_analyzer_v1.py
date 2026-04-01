"""
[*] AI+패스웨이 분석 엔진 v1.1.0
================================
Supabase DB에서 수급+시총 데이터를 읽어 v3 점수 계산 후 DB에 저장
v1.1.0: 코팔/닥사 시총 필터 추가 (KOSPI 8천억+, KOSDAQ 4천억+)
v1.1.1: 섹터맵 CSV+Supabase 보충 방식으로 변경 (깨진 섹터명 해결)
v1.0.1: 페이징 수정 + upsert on_conflict 수정

사용법:
  python kiwoom_analyzer_v1.py          -> 오늘 날짜 분석
  python kiwoom_analyzer_v1.py 20260319 -> 특정 날짜 분석

필요: pip install requests python-dotenv
"""

import requests
import json
import csv
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# v3 가중치
WEIGHTS = {'외국인': 33.8, '사모펀드': 25.0, '투신': 19.1, '연기금': 15.3, '기타법인': 12.7}
SUBJECTS = ['외국인', '연기금', '투신', '사모펀드', '기타법인']
SHORT = {'외국인': '외', '연기금': '연', '투신': '투', '사모펀드': '사', '기타법인': '기'}
MULTIPLIER = 20
TUSHIN_BONUS = 10
CONFLICT_PENALTY = -15

ETF_KW = ['KODEX','TIGER','ACE','RISE','PLUS','SOL','HANARO','KIWOOM',
           'KoAct','TIME','ETN','KOSEF','메리츠','삼성증권']

SECTOR_MAP_PATH = "./섹터맵_종목별2.csv"


class SupabaseDB:
    def __init__(self):
        self.headers_read = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
        self.headers_write = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }

    def read(self, table, params=""):
        """DB에서 데이터 읽기 (페이징 지원, 전체 로드)"""
        all_data = []
        offset = 0
        limit = 1000
        while True:
            if params:
                url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={limit}&offset={offset}"
            else:
                url = f"{SUPABASE_URL}/rest/v1/{table}?limit={limit}&offset={offset}"
            resp = requests.get(url, headers=self.headers_read)
            if resp.status_code != 200:
                print(f"  [W] DB 읽기 오류 ({table}): {resp.status_code}")
                break
            data = resp.json()
            if not data:
                break
            all_data.extend(data)
            if len(data) < limit:
                break
            offset += limit
            print(f"    페이징: {len(all_data)}건 로드 중...")
        return all_data

    def upsert(self, table, rows, on_conflict="date,stock_code"):
        """DB에 데이터 삽입/업데이트 (중복 시 업데이트)"""
        if not rows:
            return 0
        count = 0
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
            resp = requests.post(url, headers=self.headers_write, json=batch)
            if resp.status_code in [200, 201, 204]:
                count += len(batch)
            else:
                print(f"  [W] DB 저장 오류 ({table}): {resp.status_code} {resp.text[:200]}")
        return count


def is_etf(name):
    return any(kw in name for kw in ETF_KW)


def load_sector_map():
    """섹터맵 로드 (CSV 우선 + Supabase 보충)"""
    smap = {}
    for path in [SECTOR_MAP_PATH, "./kiwoom_data/섹터맵_종목별2.csv"]:
        if os.path.exists(path):
            with open(path, encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    code = row['종목코드'].strip().zfill(6)
                    smap[code] = row['섹터'].strip()
            print(f"  섹터맵 로드: {path} ({len(smap)}종목)")
            break
    # Supabase sector_map에서 빠진 종목 보충
    db = SupabaseDB()
    data = db.read("sector_map")
    added = 0
    for row in data:
        code = row['stock_code'].zfill(6)
        if True:
            smap[code] = row['sector']
            added += 1
    if added:
        print(f"  섹터맵 보충: Supabase ({added}종목 추가)")
    smap['079550'] = '방산'
    return smap


def analyze(target_date):
    """메인 분석 로직"""
    date_str = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")

    print(f"\n{'-'*50}")
    print(f"[>] STEP 1: Supabase에서 데이터 읽기 ({date_str})")
    print(f"{'-'*50}")

    db = SupabaseDB()

    supply_data = db.read("daily_supply", f"date=eq.{date_str}")
    print(f"  수급 데이터: {len(supply_data)}건")

    if not supply_data:
        print("  [X] 수급 데이터가 없습니다. 먼저 kiwoom_collector_v3.py를 실행하세요.")
        return

    market_data = db.read("daily_market", f"date=eq.{date_str}")
    print(f"  시총 데이터: {len(market_data)}건")

    smap = load_sector_map()

    print(f"\n{'-'*50}")
    print(f"[>] STEP 2: v3 점수 계산")
    print(f"{'-'*50}")

    # 시총 딕셔너리
    mkt = {}
    for row in market_data:
        code = row['stock_code'].zfill(6)
        mkt[code] = {
            'name': row.get('stock_name', ''),
            'close': row.get('close_price', 0) or 0,
            'change_pct': row.get('change_pct', 0) or 0,
            'mktcap': row.get('market_cap', 0) or 0,
            'market': row.get('market', ''),
        }

    # 수급 데이터 -> 매수/매도 딕셔너리
    buys = {}
    sells = {}
    stock_names = {}

    for row in supply_data:
        code = row['stock_code'].zfill(6)
        name = row.get('stock_name', '')
        subject = row.get('subject', '')
        direction = row.get('direction', '')
        amount = row.get('amount', 0) or 0

        if is_etf(name):
            continue
        # 코팔/닥사 필터 (KOSPI 8천억+, KOSDAQ 4천억+)
        m = mkt.get(code, {})
        market = m.get('market', '')
        mktcap = m.get('mktcap', 0)
        if market == 'KOSPI' and mktcap < 800000000000:
            continue
        if market == 'KOSDAQ' and mktcap < 400000000000:
            continue
        if not market:
            continue
        stock_names[code] = name

        if direction == '매수' and amount > 0:
            if code not in buys:
                buys[code] = {}
            buys[code][subject] = amount
        elif direction == '매도' and abs(amount) > 0:
            if code not in sells:
                sells[code] = {}
            sells[code][subject] = abs(amount)

    print(f"  매수 종목: {len(buys)} / 매도 종목: {len(sells)}")

    # 점수 계산
    results = []
    for code in buys:
        buy_subjs = list(buys[code].keys())
        sell_subjs = list(sells.get(code, {}).keys())

        if len(buy_subjs) < 2:
            continue

        name = stock_names.get(code, '')
        base_score = sum(WEIGHTS[s] for s in buy_subjs if s in WEIGHTS)

        total_buy = sum(buys[code].values())
        total_sell = sum(sells.get(code, {}).values())
        net_amount = total_buy - total_sell

        mktcap_mil = mkt.get(code, {}).get('mktcap', 0) / 1_000_000
        net_ratio = net_amount / mktcap_mil if mktcap_mil > 0 else 0
        ratio_score = net_ratio * 100 * MULTIPLIER

        conflicts = [s for s in SUBJECTS if s not in buy_subjs and s in sell_subjs]
        conflict_pen = len(conflicts) * CONFLICT_PENALTY

        tushin = TUSHIN_BONUS if '투신' in buy_subjs else 0
        final_score = base_score + ratio_score + conflict_pen + tushin

        combo = '+'.join(SHORT[s] for s in SUBJECTS if s in buy_subjs)
        sector = smap.get(code, '기타')
        change_pct = mkt.get(code, {}).get('change_pct', 0)

        results.append({
            'date': date_str,
            'stock_code': code,
            'stock_name': name,
            'sector': sector,
            'combo': combo,
            'n_buyers': len(buy_subjs),
            'base_score': round(base_score, 2),
            'ratio_score': round(ratio_score, 2),
            'tushin_bonus': tushin,
            'conflict_penalty': conflict_pen,
            'final_score': round(final_score, 2),
            'net_ratio': round(net_ratio * 100, 4),
            'conflicts': ','.join(SHORT[c] for c in conflicts) if conflicts else '',
            'change_pct': change_pct,
        })

    results.sort(key=lambda x: x['final_score'], reverse=True)

    # 결과 출력
    print(f"\n{'='*60}")
    print(f"[G] 수급분석 리포트 ({date_str})")
    print(f"{'='*60}")

    three_plus = [r for r in results if r['n_buyers'] >= 3]
    five_all = [r for r in results if r['n_buyers'] == 5]
    d_strategy = [r for r in results
                  if '외국인' in buys.get(r['stock_code'], {})
                  and '연기금' in buys.get(r['stock_code'], {})
                  and '사모펀드' in buys.get(r['stock_code'], {})]

    # -- 요약 --
    print(f"\n# 오늘 요약")
    print(f"  -------------------------------------")
    print(f"  2주체^: {len(results)}종목 | 3주체^: {len(three_plus)}종목 | 5주체: {len(five_all)}종목")
    print(f"  D전략(외+연+사): {len(d_strategy)}종목")

    # -- 5주체 전원매수 --
    if five_all:
        print(f"\n# 5주체 전원매수 ({len(five_all)}종목)")
        print(f"  -------------------------------------")
        for r in five_all:
            chg_color = "[R]" if r['change_pct'] > 0 else "[*]"
            print(f"  {chg_color} {r['stock_name']:<14} {r['sector']:<12} {r['final_score']:>6.1f}점 {r['change_pct']:>+6.2f}%")

    # -- 당일수급 TOP25 --
    print(f"\n# 당일수급 TOP25 (점수순)")
    print(f"  -----------------------------------------------------------------")
    print(f"  {'순위':>3} {'종목명':<14} {'섹터':<12} {'조합':<14} {'주체':>2} {'점수':>6} {'등락률':>7} {'충돌'}")
    print(f"  -----------------------------------------------------------------")
    for i, r in enumerate(results[:25]):
        conflict_str = r['conflicts'] if r['conflicts'] else '-'
        marker = "*" if r['n_buyers'] == 5 else " "
        print(f"  {marker}{i+1:>2}. {r['stock_name']:<14} {r['sector']:<12} {r['combo']:<14} {r['n_buyers']:>2} {r['final_score']:>6.1f} {r['change_pct']:>+6.2f}% {conflict_str}")

    # -- D전략 --
    if d_strategy:
        print(f"\n# D전략 (외+연+사 포함) {len(d_strategy)}종목")
        print(f"  -----------------------------------------------------")
        for r in d_strategy:
            print(f"  {r['stock_name']:<14} {r['sector']:<12} {r['combo']:<14} {r['final_score']:>6.1f}점")

    # -- 전일 TOP3 성과 --
    print(f"\n# 전일 TOP3 성과 (익일 등락률)")
    print(f"  -----------------------------------------------------")
    from datetime import timedelta
    curr_date = datetime.strptime(date_str, "%Y-%m-%d")
    prev_date = curr_date - timedelta(days=1)
    while prev_date.weekday() >= 5:
        prev_date -= timedelta(days=1)
    prev_date_str = prev_date.strftime("%Y-%m-%d")
    
    prev_scores = db.read("analysis_scores", f"date=eq.{prev_date_str}&order=final_score.desc&limit=10")
    if prev_scores:
        prev_top3 = [r for r in prev_scores if not r.get('conflicts', '')][:3]
        if not prev_top3:
            prev_top3 = prev_scores[:3]
        
        for r in prev_top3:
            code = r['stock_code']
            name = r.get('stock_name', '')
            sector = r.get('sector', '')
            score = r.get('final_score', 0)
            today_chg = mkt.get(code.zfill(6), {}).get('change_pct', 'N/A')
            if isinstance(today_chg, (int, float)):
                chg_str = f"{today_chg:>+6.2f}%"
            else:
                chg_str = "  N/A"
            print(f"  {name:<16} {sector:<12} 선정:{score:>6.1f} -> 익일:{chg_str}")
        
        chg_vals = []
        for r in prev_top3:
            code = r['stock_code']
            today_chg = mkt.get(code.zfill(6), {}).get('change_pct', None)
            if isinstance(today_chg, (int, float)):
                chg_vals.append(today_chg)
        if chg_vals:
            avg_chg = sum(chg_vals) / len(chg_vals)
            print(f"  {'-'*40}")
            print(f"  {'평균':>38} {avg_chg:>+6.2f}%")
    else:
        print(f"  전일({prev_date_str}) 분석 데이터 없음")

    # -- 지니 추천 TOP3 --
    no_conflict = [r for r in results if not r['conflicts']]
    top3_candidates = no_conflict[:3] if len(no_conflict) >= 3 else results[:3]

    print(f"\n* 지니 추천 TOP3")
    print(f"  -----------------------------------------------------")
    for i, r in enumerate(top3_candidates):
        print(f"  {i+1}위: {r['stock_name']:<14} ({r['sector']}) {r['final_score']:>6.1f}점 {r['combo']}")
    print(f"\n[!] Shawn이 최종 TOP과 순위를 결정해주세요!")

    # DB 저장
    print(f"\n{'-'*50}")
    print(f"[>] STEP 4: Supabase DB 저장")
    print(f"{'-'*50}")

    saved = db.upsert("analysis_scores", results)
    print(f"  analysis_scores: {saved}건 저장")

    return results, top3_candidates


def main():
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime("%Y%m%d")

    print("=" * 60)
    print("[*] AI+패스웨이 분석 엔진 v1.1.0")
    print(f"   날짜: {target_date}")
    print(f"   데이터소스: Supabase DB")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] .env 파일에 SUPABASE_URL, SUPABASE_KEY를 설정하세요.")
        return

    start = time.time()
    result = analyze(target_date)
    elapsed = time.time() - start

    if result:
        results, top3 = result
        print(f"\n{'='*60}")
        print(f"[OK] 분석 완료!")
        print(f"   총 분석 종목: {len(results)}")
        print(f"   소요시간: {elapsed:.1f}초")
        print(f"   TOP3: {', '.join(r['stock_name'] for r in top3)}")
        print(f"\n[DB] Supabase analysis_scores 테이블에 저장 완료!")
        print("=" * 60)


if __name__ == "__main__":
    main()
