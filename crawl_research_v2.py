"""
[*] 한경 컨센서스 리포트 크롤러 v2 (Playwright + Bearer 토큰)
================================================================
1. Playwright 헤드리스 브라우저로 한경 컨센서스 페이지 로드
2. axios 자동 호출 시 첨부되는 Bearer 토큰 자동 캡처
3. 캡처된 토큰으로 v2 API 직접 호출 → 페이지네이션 + 카테고리별 수집
4. 일 100~200건 + 산업 30~50건 + 시장 5~10건 가능

저작권 안전:
- 메타데이터만 수집 (제목/종목/증권사/의견/목표가/원문URL)
- 본문 다운로드 안 함
- 일 1회 실행, 정상 브라우저 UA

사용법:
  python crawl_research_v2.py          # 모든 카테고리
  python crawl_research_v2.py company  # 기업만
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=merge-duplicates",
}

CONSENSUS_URL = "https://markets.hankyung.com/consensus"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# 한경 reportType 약어 → 우리 카테고리
TYPE_TO_CATEGORY = {
    'CO': 'company', 'IN': 'industry', 'MK': 'market',
    'EC': 'economy', 'DE': 'derivative',
}
CATEGORY_TO_TYPE = {v: k for k, v in TYPE_TO_CATEGORY.items()}


def normalize_opinion(grade):
    if not grade: return None
    g = str(grade).strip().lower()
    if any(k in g for k in ['buy', '매수', '강력', 'strong']): return 'Buy'
    if any(k in g for k in ['hold', '보유', 'neutral', '중립']): return 'Hold'
    if any(k in g for k in ['sell', '매도']): return 'Sell'
    return grade


def determine_target_change(target, old_target):
    if not old_target or str(old_target).strip() in ('0', '', 'None'):
        return '신규'
    try:
        new_v = float(target) if target else 0
        old_v = float(old_target) if old_target else 0
        if abs(new_v - old_v) < 0.01: return '유지'
        return '상향' if new_v > old_v else '하향'
    except:
        return None


def fetch_reports(page, token: str, category: str, from_date: str, to_date: str, max_pages: int = 20) -> list:
    """v2 API 호출로 카테고리별 리포트 수집"""
    rt = CATEGORY_TO_TYPE.get(category, 'CO')
    print(f"  [>] {category} (reportType={rt}) {from_date} ~ {to_date}")

    all_items = []
    for page_no in range(1, max_pages + 1):
        params = (
            f"page={page_no}&reportType={rt}"
            f"&fromDate={from_date}&toDate={to_date}"
            f"&gradeCode=ALL&changePrices=ALL&searchType=ALL&reportRange=50"
        )

        try:
            result = page.evaluate(
                """async ({token, params}) => {
                    const r = await fetch('/api/v2/consensus/search/report?' + params, {
                        headers: {
                            'Authorization': token,
                            'Accept': 'application/json, text/plain, */*',
                            'Referer': 'https://markets.hankyung.com/consensus'
                        }
                    });
                    if (!r.ok) return { error: r.status };
                    return await r.json();
                }""",
                {'token': token, 'params': params}
            )
        except Exception as e:
            print(f"    [W] page {page_no} 호출 오류: {e}")
            break

        if not result or result.get('error'):
            print(f"    [i] page {page_no}: status={result.get('error') if result else 'none'}")
            break

        raw_data = result.get('data', []) if isinstance(result, dict) else []
        # data가 dict인 경우 (페이지 2+에서 발생) values만 추출
        if isinstance(raw_data, dict):
            items = list(raw_data.values())
        elif isinstance(raw_data, list):
            items = raw_data
        else:
            items = []

        # dict 타입만 유지 (string은 제외)
        items = [x for x in items if isinstance(x, dict)]

        if not items:
            break

        all_items.extend(items)
        last_page = result.get('last_page', 1)
        total = result.get('total', 0)
        print(f"    [{page_no}/{last_page}] {len(items)}건 (누적 {len(all_items)} / 전체 {total})")

        if page_no >= last_page:
            break
        time.sleep(0.3)

    return all_items


def parse_date(raw) -> str:
    """다양한 한경 날짜 형식을 ISO로 변환"""
    if not raw:
        return datetime.now().strftime('%Y-%m-%d')
    rd = str(raw).strip()
    # 1) "20260429" (8자리)
    if len(rd) == 8 and rd.isdigit():
        return f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}"
    # 2) "2026-04-29" (이미 ISO)
    if len(rd) == 10 and rd[4] == '-' and rd[7] == '-':
        return rd
    # 3) "2026-04-29 00:00:00" (datetime)
    if len(rd) >= 10 and rd[4] == '-':
        return rd[:10]
    # 4) "20260429083000" (14자리 datetime)
    if len(rd) >= 8 and rd[:8].isdigit():
        return f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}"
    return datetime.now().strftime('%Y-%m-%d')


def normalize(item: dict) -> dict:
    rt = str(item.get('REPORT_TYPE') or '').strip().upper()
    category = TYPE_TO_CATEGORY.get(rt, 'company')

    # REPORT_DATE 우선, 없으면 REGISTER_DATE
    date_iso = parse_date(item.get('REPORT_DATE') or item.get('REGISTER_DATE'))

    try:
        target_p = int(float(item.get('TARGET_STOCK_PRICES') or 0))
    except:
        target_p = 0

    business_code = str(item.get('BUSINESS_CODE') or '').strip()
    business_name = item.get('BUSINESS_NAME') or item.get('INDUSTRY_NAME') or '-'

    pdf_url = item.get('REPORT_FILEPATH') or ''
    if pdf_url and not pdf_url.startswith('http'):
        pdf_url = f"https://markets.hankyung.com{pdf_url}"

    return {
        'date': date_iso,
        'stock_code': business_code.zfill(6) if business_code else None,
        'stock_name': business_name,
        'brokerage': item.get('OFFICE_NAME', ''),
        'analyst': item.get('REPORT_WRITER'),
        'opinion': normalize_opinion(item.get('GRADE_VALUE')),
        'target_price': target_p if target_p > 0 else None,
        'target_change': determine_target_change(
            item.get('TARGET_STOCK_PRICES'),
            item.get('OLD_TARGET_STOCK_PRICES')
        ),
        'title': str(item.get('REPORT_TITLE', '')).strip(),
        'original_url': pdf_url if pdf_url else None,
        'source': 'hankyung',
        'report_category': category,
    }


def save_to_supabase(rows: list) -> int:
    if not rows:
        return 0

    # Batch 내 UNIQUE 키 기준 중복 제거 (같은 종목+증권사+제목+날짜)
    seen = set()
    deduped = []
    for r in rows:
        key = (r.get('date'), r.get('stock_name'), r.get('brokerage'), r.get('title'))
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    saved = 0
    for i in range(0, len(deduped), 100):
        batch = deduped[i:i+100]
        url = f"{SUPABASE_URL}/rest/v1/research_reports?on_conflict=date,stock_name,brokerage,title"
        try:
            resp = requests.post(url, headers=SB_HEADERS, json=batch, timeout=60)
            if resp.status_code in [200, 201, 204]:
                saved += len(batch)
            else:
                print(f"    [W] DB: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"    [X] DB 오류: {e}")
    return saved


def main():
    target_categories = sys.argv[1:] if len(sys.argv) > 1 else ['company', 'industry', 'market']

    # 날짜 범위: 최근 1개월
    today = datetime.now()
    from_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    to_date = today.strftime('%Y-%m-%d')

    print("=" * 60)
    print("[*] 한경 컨센서스 크롤러 v2 (Playwright + 토큰)")
    print(f"   시간: {today.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   카테고리: {', '.join(target_categories)}")
    print(f"   기간: {from_date} ~ {to_date}")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] SUPABASE 환경변수 없음")
        return 1

    total_extracted = 0
    total_saved = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA)
        page = context.new_page()

        # 1) Bearer 토큰 캡처
        print(f"\n[>] Bearer 토큰 캡처 중...")
        captured_token = None

        def on_request(req):
            nonlocal captured_token
            if '/api/v2/consensus' in req.url and 'authorization' in req.headers:
                if not captured_token:
                    captured_token = req.headers['authorization']
        page.on('request', on_request)

        try:
            page.goto(CONSENSUS_URL, timeout=60000, wait_until='load')
        except Exception as e:
            print(f"  [W] goto 경고: {e}")

        # 페이지 클릭으로 axios 활성화 + 토큰 캡처 (최대 60초)
        for i in range(60):
            time.sleep(1)
            if captured_token:
                break
            # 5초마다 페이지 스크롤로 axios 호출 유도
            if i > 0 and i % 5 == 0:
                try:
                    page.evaluate("window.scrollBy(0, 500)")
                except:
                    pass

        if not captured_token:
            print(f"[X] Bearer 토큰 캡처 실패 (60초 대기)")
            print(f"   페이지 URL: {page.url}")
            browser.close()
            return 1

        print(f"  [OK] 토큰 캡처: {captured_token[:30]}...")

        # 2) 카테고리별 수집
        for category in target_categories:
            print(f"\n[>] {category.upper()} 리포트 수집...")
            try:
                items = fetch_reports(page, captured_token, category, from_date, to_date, max_pages=20)
            except Exception as e:
                print(f"  [X] 수집 실패: {e}")
                continue

            if not items:
                print(f"  [W] 데이터 없음")
                continue

            rows = []
            errors = 0
            error_samples = []
            empty_name = 0
            empty_title = 0
            for idx, it in enumerate(items):
                try:
                    norm = normalize(it)
                    if not norm.get('stock_name') or norm.get('stock_name') == '-':
                        empty_name += 1
                        continue
                    if not norm.get('title'):
                        empty_title += 1
                        continue
                    rows.append(norm)
                except Exception as e:
                    errors += 1
                    if len(error_samples) < 3:
                        error_samples.append(f"idx {idx}: {type(e).__name__}: {e}")
            if error_samples:
                for s in error_samples:
                    print(f"     ERR: {s}")

            saved = save_to_supabase(rows)
            print(f"  [OK] 추출 {len(items)} / 정규화 {len(rows)} / 저장 {saved}")
            if empty_name + empty_title + errors > 0:
                print(f"     skip: name미상 {empty_name}, title없음 {empty_title}, 오류 {errors}")
            total_extracted += len(items)
            total_saved += saved

        browser.close()

    print(f"\n{'=' * 60}")
    print(f"[OK] 총 추출 {total_extracted}건 / 저장 {total_saved}건")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
