"""
[*] AI 리포트 요약 생성기
==========================
research_reports 테이블의 ai_summary가 비어있는 리포트들에 대해
우리 자체 데이터(RS, 수급, 슈퍼시그널)를 기반으로 4-6줄 요약을 생성합니다.

저작권 안전:
- 한경 본문은 사용 안 함
- 우리가 가진 RS, 수급, 거래량, 슈퍼시그널 데이터로 자체 분석
- 리포트 제목은 참고용으로만 활용

사용법:
  python generate_research_ai_summary.py          # 미생성 리포트 모두
  python generate_research_ai_summary.py 20260429 # 특정 날짜만
"""

import os
import sys
import json
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
    "Prefer": "return=minimal",
}


def sb_get(table: str, params: str = "") -> list:
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}" if params else f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.get(url, headers=SB_HEADERS, timeout=30)
    return resp.json() if resp.status_code == 200 else []


def sb_patch(table: str, filter_str: str, body: dict) -> bool:
    url = f"{SUPABASE_URL}/rest/v1/{table}?{filter_str}"
    resp = requests.patch(url, headers=SB_HEADERS, json=body, timeout=30)
    return resp.status_code in [200, 204]


def get_stock_context(stock_code: str, date: str) -> dict:
    """종목의 최근 RS + 수급 데이터 조회"""
    if not stock_code:
        return {}

    # RS Leaders (최신)
    rs_data = sb_get(
        "rs_leaders",
        f"stock_code=eq.{stock_code}&date=lte.{date}&order=date.desc&limit=1"
        f"&select=date,excess_1d,excess_5d,excess_20d,excess_60d,excess_120d,"
        f"pctl_20d,pctl_60d,combo_grade,is_super_leader,vol_ratio,drawdown_20d,sector"
    )

    # 스마트머니 수급 (최근 5일)
    supply_data = sb_get(
        "daily_supply_v2",
        f"stock_code=eq.{stock_code}&date=lte.{date}&order=date.desc&limit=20"
        f"&subject=in.(외국인,연기금,사모펀드)"
        f"&select=date,subject,buy_amt,sell_amt"
    )

    # 외국인+연기금+사모 5일 누적 순매수
    smart_money_5d = {}
    for r in supply_data[:20]:  # 최근 5일 × 3주체 = 15건
        subj = r.get('subject')
        net = (r.get('buy_amt', 0) or 0) - (r.get('sell_amt', 0) or 0)
        smart_money_5d[subj] = smart_money_5d.get(subj, 0) + net

    return {
        'rs': rs_data[0] if rs_data else {},
        'smart_money_5d': smart_money_5d,
    }


def generate_summary(report: dict, context: dict) -> str:
    """Claude로 4-6줄 요약 생성"""
    if not ANTHROPIC_KEY:
        return None

    rs = context.get('rs', {})
    sm = context.get('smart_money_5d', {})

    # 핵심 지표
    indicators = []
    if rs.get('excess_60d') is not None:
        indicators.append(f"60일 RS {rs['excess_60d']:+.1f}%")
    if rs.get('excess_20d') is not None:
        indicators.append(f"20일 RS {rs['excess_20d']:+.1f}%")
    if rs.get('excess_1d') is not None:
        indicators.append(f"당일 RS {rs['excess_1d']:+.1f}%")
    if rs.get('vol_ratio') and rs['vol_ratio'] > 0:
        indicators.append(f"거래량 {rs['vol_ratio']:.1f}x")
    if rs.get('combo_grade'):
        indicators.append(f"등급 {rs['combo_grade']}")
    if rs.get('is_super_leader'):
        indicators.append("슈퍼리더")

    # 스마트머니 (백만원 단위)
    sm_lines = []
    for subj in ['외국인', '연기금', '사모펀드']:
        val = sm.get(subj, 0)
        if val:
            millions = val / 1000  # 천원 → 백만원
            sm_lines.append(f"{subj} {millions:+,.0f}M")

    prompt = f"""한국 주식 종목 분석 요약을 4~6줄로 작성해주세요.

종목: {report.get('stock_name')} ({report.get('stock_code', '')})
증권사: {report.get('brokerage')}
리포트 제목: {report.get('title')}
투자의견: {report.get('opinion', '-')}
목표가: {(report.get('target_price') or 0):,}원 ({report.get('target_change') or '-'})

[우리 자체 분석 데이터]
- 섹터: {rs.get('sector', '-')}
- RS 지표: {' / '.join(indicators) if indicators else '데이터 없음'}
- 5일 스마트머니 누적 순매수: {' / '.join(sm_lines) if sm_lines else '데이터 없음'}
- 20일 눌림폭: {(rs.get('drawdown_20d') or 0):.1f}%

요구사항:
1. 4~6줄, 각 줄은 명확한 포인트 (불릿 형태로 시작)
2. 리포트 제목의 핵심 키워드 + 우리 데이터의 RS/수급 강점을 결합
3. 한경 본문 인용 금지 (제목만 참고)
4. 매수 시점/조건이 있다면 마지막 줄에 한 줄 추가
5. 해요체, 투자조언 아닌 데이터 기반 서술

출력: 줄바꿈으로 구분된 4~6개 줄만 (불릿 • 시작). 다른 설명 없이.
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
                "max_tokens": 512,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data['content'][0]['text'].strip()
    except Exception as e:
        print(f"  [W] Claude API 오류: {e}")
        return None


def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    today = datetime.now().strftime('%Y-%m-%d')

    print("=" * 60)
    print("[*] AI 리포트 요약 생성기")
    print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if target_date:
        date_iso = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
        print(f"   대상: {date_iso}")
    print("=" * 60)

    if not ANTHROPIC_KEY:
        print("[X] ANTHROPIC_API_KEY 없음")
        return 1

    # ai_summary가 NULL인 리포트 조회 (target_change 있는 것 우선)
    if target_date:
        date_iso = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
        params = f"date=eq.{date_iso}&ai_summary=is.null&order=id.desc"
    else:
        params = f"date=gte.{today}&ai_summary=is.null&order=id.desc"

    reports = sb_get("research_reports", params + "&limit=50")
    print(f"\n[>] 요약 미생성 리포트: {len(reports)}건")

    if not reports:
        print("  [i] 처리할 리포트 없음")
        return 0

    success = 0
    for i, r in enumerate(reports):
        print(f"\n  [{i+1}/{len(reports)}] {r.get('stock_name')} - {r.get('brokerage')}")

        # 종목 컨텍스트 조회
        ctx = get_stock_context(r.get('stock_code'), r.get('date'))

        # AI 요약 생성
        summary = generate_summary(r, ctx)

        if summary:
            ok = sb_patch(
                "research_reports",
                f"id=eq.{r['id']}",
                {"ai_summary": summary}
            )
            if ok:
                success += 1
                print(f"    [OK] 요약 저장")
            else:
                print(f"    [X] DB 저장 실패")
        else:
            print(f"    [W] AI 생성 실패")

        time.sleep(0.5)  # API rate limit

    print(f"\n{'=' * 60}")
    print(f"[OK] 총 {success}/{len(reports)}건 요약 생성")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
