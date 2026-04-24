"""
[*] AI 일일 리포트 자동 생성
================================
매일 수집 완료 후 실행:
  - 오늘의 주도주 TOP 10
  - 주도섹터 TOP 5
  - 스마트머니 플로우 (외국인+연기금+사모 순매수 합산)
  - 매수/매도 시그널 요약
  - Claude API로 AI 내러티브 생성
  - daily_reports 테이블에 저장

사용법:
  python generate_daily_report.py          -> 오늘
  python generate_daily_report.py 20260424 -> 특정 날짜
"""

import os
import sys
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation,resolution=merge-duplicates",
}


def sb_query(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}" if params else f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.get(url, headers=SB_HEADERS)
    return resp.json() if resp.status_code == 200 else []


def sb_upsert(table, rows, on_conflict="date"):
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    resp = requests.post(url, headers=SB_HEADERS, json=rows)
    return resp.status_code in [200, 201, 204]


def get_top_stocks(date_str, limit=10):
    """주도주 TOP N (pctl_20d 상위)"""
    params = (
        f"date=eq.{date_str}"
        f"&select=stock_code,stock_name,market,sector,pctl_20d,excess_20d,return_20d,combo_grade,is_super_leader"
        f"&order=pctl_20d.desc"
        f"&limit={limit}"
    )
    return sb_query("rs_leaders", params)


def get_leading_sectors(date_str, limit=5):
    """주도섹터 TOP N (섹터별 RS 상위 10% 종목 수)"""
    params = f"date=eq.{date_str}&select=sector,pctl_20d&pctl_20d=gte.90"
    rows = sb_query("rs_leaders", params)

    sectors = {}
    for r in rows:
        sec = r.get('sector', '기타') or '기타'
        if sec not in sectors:
            sectors[sec] = {"sector": sec, "count": 0, "avg_pctl": 0, "sum_pctl": 0}
        sectors[sec]["count"] += 1
        sectors[sec]["sum_pctl"] += r.get('pctl_20d', 0) or 0

    for s in sectors.values():
        s["avg_pctl"] = round(s["sum_pctl"] / s["count"], 1) if s["count"] > 0 else 0
        del s["sum_pctl"]

    sorted_sectors = sorted(sectors.values(), key=lambda x: (-x["count"], -x["avg_pctl"]))
    return sorted_sectors[:limit]


def get_smart_money(date_str):
    """스마트머니 플로우 (외국인+연기금+사모펀드 합산)"""
    smart_subjects = ["외국인", "연기금", "사모펀드"]
    params = (
        f"date=eq.{date_str}"
        f"&subject=in.({','.join(smart_subjects)})"
        f"&select=stock_code,stock_name,subject,buy_amt,sell_amt"
    )
    rows = sb_query("daily_supply_v2", params)

    # 종목별 스마트머니 순매수 합산
    stocks = {}
    for r in rows:
        code = r['stock_code']
        net = (r.get('buy_amt', 0) or 0) - (r.get('sell_amt', 0) or 0)
        if code not in stocks:
            stocks[code] = {
                "stock_code": code,
                "stock_name": r['stock_name'],
                "net_amount": 0,
                "subjects": [],
            }
        stocks[code]["net_amount"] += net
        if net > 0:
            stocks[code]["subjects"].append(r['subject'])

    # 3주체 모두 순매수한 종목 우선
    triple_buy = [s for s in stocks.values() if len(s["subjects"]) == 3]
    triple_buy.sort(key=lambda x: -x["net_amount"])

    return {
        "triple_buy_count": len(triple_buy),
        "top_triple_buy": triple_buy[:10],
    }


def get_signals(date_str):
    """매수 시그널 (눌림목 반등) / 매도 시그널 (손실관리)"""
    # Breakout: pctl>=90 + drawdown_20d<=-5 + excess_1d>0
    params = (
        f"date=eq.{date_str}"
        f"&pctl_20d=gte.90&drawdown_20d=lte.-5&excess_1d=gt.0"
        f"&select=stock_code,stock_name,sector,pctl_20d,excess_1d,vol_ratio,drawdown_20d"
        f"&order=vol_ratio.desc"
        f"&limit=10"
    )
    buy = sb_query("rs_leaders", params)

    # Breakdown: pctl>=80 + excess_1d<0
    params2 = (
        f"date=eq.{date_str}"
        f"&pctl_20d=gte.80&excess_1d=lt.0"
        f"&select=stock_code,stock_name,sector,pctl_20d,excess_1d,vol_ratio"
        f"&order=excess_1d.asc"
        f"&limit=10"
    )
    sell = sb_query("rs_leaders", params2)

    strong_buy = [s for s in buy if (s.get('vol_ratio', 0) or 0) >= 2.5]
    distribution = [s for s in sell if (s.get('vol_ratio', 0) or 0) >= 2.0]

    return {
        "buy_signals": buy,
        "strong_buy_count": len(strong_buy),
        "sell_signals": sell,
        "distribution_count": len(distribution),
    }


def generate_ai_narrative(date_str, top_stocks, sectors, smart_money, signals):
    """Claude API로 AI 내러티브 생성"""
    if not ANTHROPIC_KEY:
        return None, "ANTHROPIC_API_KEY 미설정"

    # 데이터 요약
    top_5_names = [s['stock_name'] for s in top_stocks[:5]]
    sector_names = [s['sector'] for s in sectors[:3]]
    triple_buy_names = [s['stock_name'] for s in smart_money['top_triple_buy'][:5]]

    prompt = f"""한국 주식시장 일일 리포트를 작성해주세요.

날짜: {date_str}

데이터:
- 주도주 TOP5 (RS 20일 백분위 상위): {', '.join(top_5_names)}
- 주도섹터 TOP3: {', '.join(sector_names)}
- 스마트머니(외국인+연기금+사모) 3주체 동시 순매수 종목: {smart_money['triple_buy_count']}개, 상위 5개: {', '.join(triple_buy_names)}
- 눌림목 반등 매수 시그널: {len(signals['buy_signals'])}개 (강력시그널 ⚡: {signals['strong_buy_count']}개)
- 손실관리 매도 시그널: {len(signals['sell_signals'])}개 (분배일 🔻: {signals['distribution_count']}개)

요구사항:
1. 제목: 시장 흐름을 한 문장으로 압축 (예: "건설/AI 섹터 주도, 스마트머니 15종목 동시 유입")
2. 요약: 3~5문단 (총 300자 내외). 다음 내용 포함:
   - 오늘의 시장 특징 (어느 섹터가 강했는지)
   - 스마트머니 동향 (외국인/연기금/사모 선호 종목)
   - 매수/매도 시그널 해석
   - 내일 관전 포인트
3. 해요체 사용, 투자 조언 아닌 데이터 기반 분석으로 서술
4. 출력 형식 (JSON):
{{"title": "...", "summary": "..."}}

JSON만 출력하세요."""

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
            timeout=30,
        )
        if resp.status_code != 200:
            return None, f"Claude API 오류: {resp.status_code}"

        data = resp.json()
        text = data['content'][0]['text'].strip()

        # JSON 파싱 (```json 블록 제거)
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        parsed = json.loads(text)
        return parsed, None
    except Exception as e:
        return None, f"AI 생성 실패: {e}"


def main():
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
    date_str = f"{date_arg[:4]}-{date_arg[4:6]}-{date_arg[6:8]}"

    print(f"{'='*60}")
    print(f"[*] AI 일일 리포트 생성: {date_str}")
    print(f"{'='*60}")

    # 1. 데이터 수집
    print("\n[>] STEP 1: 데이터 집계")
    top_stocks = get_top_stocks(date_str, limit=10)
    sectors = get_leading_sectors(date_str, limit=5)
    smart_money = get_smart_money(date_str)
    signals = get_signals(date_str)

    print(f"  - 주도주 TOP10: {len(top_stocks)}개")
    print(f"  - 주도섹터 TOP5: {len(sectors)}개")
    print(f"  - 스마트머니 3주체 동시 매수: {smart_money['triple_buy_count']}개")
    print(f"  - 매수 시그널: {len(signals['buy_signals'])}개 (강력: {signals['strong_buy_count']})")
    print(f"  - 매도 시그널: {len(signals['sell_signals'])}개 (분배일: {signals['distribution_count']})")

    if len(top_stocks) == 0:
        print(f"[X] {date_str} 데이터 없음")
        return 1

    # 2. AI 내러티브 생성
    print("\n[>] STEP 2: AI 내러티브 생성 (Claude Sonnet 4)")
    narrative, err = generate_ai_narrative(date_str, top_stocks, sectors, smart_money, signals)
    if not narrative:
        print(f"  [W] {err} - 기본 제목 사용")
        narrative = {
            "title": f"{date_str} 주도주 리포트",
            "summary": f"오늘의 주도주 TOP: {', '.join([s['stock_name'] for s in top_stocks[:3]])}",
        }

    print(f"  제목: {narrative['title']}")

    # 3. DB 저장
    print("\n[>] STEP 3: daily_reports 테이블 저장")
    row = {
        "date": date_str,
        "title": narrative["title"],
        "summary": narrative["summary"],
        "top_stocks": top_stocks,
        "leading_sectors": sectors,
        "smart_money": smart_money,
        "signals": signals,
    }
    if sb_upsert("daily_reports", [row]):
        print(f"  [OK] 저장 완료")
    else:
        print(f"  [X] 저장 실패")
        return 1

    print(f"\n{'='*60}")
    print(f"[OK] 리포트 생성 완료: {date_str}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
