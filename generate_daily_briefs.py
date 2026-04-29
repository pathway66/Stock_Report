"""
[*] 슈퍼시그널 종목별 일일 AI 리포트 생성기
============================================
매일 발견된 슈퍼시그널 종목들(E3 추격 + E4 풀백)에 대해
종목별 10줄 리포트를 자동 생성합니다.

리포트 구성:
- 제목: 종목명 + 핵심 모멘텀 한 줄
- 상승 모멘텀 분석 (4-5줄): RS 패턴 + 거래량 + 산업 동향
- 수급 분석 (4-5줄): 외국인/연기금/사모 흐름

저장: daily_stock_briefs 테이블

사용법:
  python generate_daily_briefs.py          # 오늘
  python generate_daily_briefs.py 20260428 # 특정 날짜
"""

import os
import sys
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=merge-duplicates",
}


def sb_get(table: str, params: str) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    resp = requests.get(url, headers=SB_HEADERS, timeout=30)
    return resp.json() if resp.status_code == 200 else []


def sb_upsert(table: str, rows: list, on_conflict: str) -> int:
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    resp = requests.post(url, headers=SB_HEADERS, json=rows, timeout=60)
    return len(rows) if resp.status_code in [200, 201, 204] else 0


def get_super_signal_stocks(date_iso: str) -> list:
    """슈퍼시그널 조건 충족 종목 조회"""
    rs_data = sb_get(
        "rs_leaders",
        f"date=eq.{date_iso}&excess_60d=gte.30&excess_1d=gt.0&vol_ratio=gte.1.5"
        f"&select=stock_code,stock_name,market,sector,market_cap,"
        f"excess_1d,excess_5d,excess_10d,excess_20d,excess_60d,excess_120d,"
        f"vol_ratio,drawdown_20d,combo_grade,is_super_leader"
        f"&limit=500"
    )

    stocks = []
    for r in rs_data:
        e1 = r.get('excess_1d') or 0
        e5 = r.get('excess_5d') or 0
        e20 = r.get('excess_20d') or 0
        e60 = r.get('excess_60d') or 0
        e120 = r.get('excess_120d') or 0
        vol = r.get('vol_ratio') or 0

        # 풀백 (74%) 우선
        is_pullback = e60 > 50 and e20 < 0 and e5 > 0 and e1 > 2 and vol >= 1.5
        # 추격 (70%)
        is_momentum = e120 > 50 and e60 > 30 and e5 > 3 and e1 > 2 and vol >= 1.5

        if is_pullback:
            r['signal_type'] = 'PULLBACK'
            r['win_rate'] = 74.0
        elif is_momentum:
            r['signal_type'] = 'MOMENTUM'
            r['win_rate'] = 70.1
        else:
            continue

        stocks.append(r)

    # 풀백 우선 정렬
    stocks.sort(key=lambda s: (s['signal_type'] != 'PULLBACK', -(s.get('excess_120d') or 0) - (s.get('excess_60d') or 0)))
    return stocks


def get_smart_money_5d(stock_code: str, date_iso: str) -> dict:
    """5일 누적 스마트머니 순매수"""
    data = sb_get(
        "daily_supply_v2",
        f"stock_code=eq.{stock_code}&date=lte.{date_iso}&order=date.desc&limit=20"
        f"&subject=in.(외국인,연기금,사모펀드)"
        f"&select=date,subject,buy_amt,sell_amt"
    )

    sm = {}
    for r in data:
        subj = r['subject']
        net = (r.get('buy_amt') or 0) - (r.get('sell_amt') or 0)
        sm[subj] = sm.get(subj, 0) + net
    return sm


def get_recent_research_titles(stock_code: str, date_iso: str) -> list:
    """최근 1개월 증권사 리포트 제목"""
    if not stock_code:
        return []
    data = sb_get(
        "research_reports",
        f"stock_code=eq.{stock_code}&date=lte.{date_iso}&order=date.desc&limit=5"
        f"&select=date,brokerage,opinion,target_price,target_change,title"
    )
    return data


def generate_brief(stock: dict, sm: dict, reports: list) -> dict:
    """Claude로 종목별 10줄 리포트 생성"""
    if not ANTHROPIC_KEY:
        return None

    sm_summary = []
    for subj in ['외국인', '연기금', '사모펀드']:
        v = sm.get(subj, 0)
        if v:
            millions = v / 1_000_000  # 천원 → 십억원 단위
            sm_summary.append(f"{subj}: {millions:+,.1f}억")

    reports_summary = []
    for r in reports[:3]:
        target = r.get('target_price')
        target_str = f" 목표가 {target:,}원 ({r.get('target_change') or '-'})" if target else ""
        reports_summary.append(
            f"{r['date']} {r['brokerage']} [{r.get('opinion') or '-'}]{target_str}: {r['title']}"
        )

    signal_label = "🎯 풀백반등 (74%)" if stock['signal_type'] == 'PULLBACK' else "🚀 추격매수 (70%)"

    prompt = f"""다음 종목에 대한 일일 리포트를 작성해주세요. 한국 주식시장 슈퍼시그널 종목입니다.

[종목 정보]
- 종목: {stock['stock_name']} ({stock['stock_code']}) | {stock.get('market', '')} | {stock.get('sector') or '섹터미상'}
- 슈퍼시그널: {signal_label}
- 시가총액: {(stock.get('market_cap') or 0) / 100_000_000_000:.1f}조원

[RS 지표 (지수 대비 초과수익률)]
- 1일: {stock.get('excess_1d', 0):+.1f}%
- 5일: {stock.get('excess_5d', 0):+.1f}%
- 20일: {stock.get('excess_20d', 0):+.1f}%
- 60일: {stock.get('excess_60d', 0):+.1f}%
- 120일: {stock.get('excess_120d', 0):+.1f}%
- 거래량: 평균의 {stock.get('vol_ratio', 0):.1f}배
- 20일 눌림폭: {stock.get('drawdown_20d', 0):.1f}%

[5일 누적 스마트머니 순매수]
{chr(10).join('- ' + s for s in sm_summary) if sm_summary else '- 데이터 없음'}

[최근 증권사 리포트 (참고용 메타데이터만, 본문 인용 금지)]
{chr(10).join('- ' + r for r in reports_summary) if reports_summary else '- 최근 발행 리포트 없음'}

[작성 요구사항]
다음 JSON 형식으로 출력하세요. 각 항목은 한국어 해요체로 작성:

{{
  "title": "한 줄 핵심 (예: '대우건설, 건설업 회복 + 외국인 연기금 동시 매수')",
  "momentum_brief": "상승 모멘텀 분석 (4-5문장). RS 패턴(단기 강세, 장기 슈퍼리더 유지 등) + 거래량 동반 + 산업 흐름. 미너비니/오닐 관점 1번 활용 가능.",
  "supply_brief": "수급 분석 (4-5문장). 외국인/연기금/사모펀드 5일 누적 흐름 + 어떤 주체가 주도인지 + 의미 해석."
}}

JSON만 출력하세요. 다른 설명 없이.
"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=45,
        )
        if resp.status_code != 200:
            print(f"    [W] API: {resp.status_code}")
            return None
        text = resp.json()['content'][0]['text'].strip()
        # ```json 제거
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        text = text.strip()
        import json
        return json.loads(text)
    except Exception as e:
        print(f"    [W] Claude 오류: {e}")
        return None


def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
    date_iso = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"

    print("=" * 60)
    print(f"[*] 슈퍼시그널 종목별 일일 AI 리포트 생성")
    print(f"   기준일: {date_iso}")
    print("=" * 60)

    if not ANTHROPIC_KEY:
        print("[X] ANTHROPIC_API_KEY 없음")
        return 1

    # 슈퍼시그널 종목 조회
    print(f"\n[>] 슈퍼시그널 종목 조회...")
    stocks = get_super_signal_stocks(date_iso)
    pullback_n = sum(1 for s in stocks if s['signal_type'] == 'PULLBACK')
    momentum_n = sum(1 for s in stocks if s['signal_type'] == 'MOMENTUM')
    print(f"   풀백 {pullback_n}개 + 추격 {momentum_n}개 = 총 {len(stocks)}종목")

    if not stocks:
        print("[i] 슈퍼시그널 종목 없음")
        return 0

    saved_count = 0
    rows_to_save = []
    for i, stock in enumerate(stocks):
        print(f"\n  [{i+1}/{len(stocks)}] {stock['stock_name']} ({stock['stock_code']})")

        # 컨텍스트 수집
        sm = get_smart_money_5d(stock['stock_code'], date_iso)
        reports = get_recent_research_titles(stock['stock_code'], date_iso)

        # AI 리포트 생성
        brief = generate_brief(stock, sm, reports)
        if not brief:
            print(f"    [X] 생성 실패")
            continue

        # full_brief 합치기 (markdown)
        signal_emoji = "🎯" if stock['signal_type'] == 'PULLBACK' else "🚀"
        full = (
            f"## {signal_emoji} {brief.get('title', '')}\n\n"
            f"### 📈 상승 모멘텀\n{brief.get('momentum_brief', '')}\n\n"
            f"### 💰 수급 분석\n{brief.get('supply_brief', '')}"
        )

        rows_to_save.append({
            'date': date_iso,
            'stock_code': stock['stock_code'],
            'stock_name': stock['stock_name'],
            'market': stock.get('market'),
            'sector': stock.get('sector'),
            'signal_type': stock['signal_type'],
            'win_rate': stock['win_rate'],
            'market_cap': stock.get('market_cap'),
            'excess_1d': stock.get('excess_1d'),
            'excess_5d': stock.get('excess_5d'),
            'excess_20d': stock.get('excess_20d'),
            'excess_60d': stock.get('excess_60d'),
            'excess_120d': stock.get('excess_120d'),
            'vol_ratio': stock.get('vol_ratio'),
            'title': brief.get('title', ''),
            'momentum_brief': brief.get('momentum_brief', ''),
            'supply_brief': brief.get('supply_brief', ''),
            'full_brief': full,
        })
        print(f"    [OK] {brief.get('title', '')[:40]}")
        time.sleep(0.5)

    # 저장
    if rows_to_save:
        saved_count = sb_upsert("daily_stock_briefs", rows_to_save, "date,stock_code")

    print(f"\n{'=' * 60}")
    print(f"[OK] {saved_count}/{len(stocks)}건 저장 완료")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
