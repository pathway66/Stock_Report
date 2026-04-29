"""
[*] 한경 컨센서스 기업 리포트 크롤러
======================================
한경 컨센서스 페이지의 SSR 데이터(window.__NUXT__)를 파싱하여
메타데이터를 research_reports 테이블에 저장합니다.

저작권 안전:
- 메타데이터(제목, 종목, 증권사, 의견, 목표가, 작성일)만 저장
- 본문은 한경 원문 링크로 외부 이동
- AI 요약은 우리 자체 데이터(RS, 수급)로 별도 생성

사용법:
  python crawl_research_reports.py          # 최신 리포트 수집
  python crawl_research_reports.py 20260429 # 특정 날짜
"""

import os
import re
import sys
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal,resolution=merge-duplicates",
}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def fetch_consensus_page(report_type: str = "company") -> str:
    """
    한경 컨센서스 페이지 HTML 가져오기
    report_type: company (기업), industry (산업), market (시장)
    """
    type_map = {"company": "07020100", "industry": "07020200", "market": "07020300"}
    code = type_map.get(report_type, "07020100")
    url = f"https://markets.hankyung.com/consensus?sortOrder=DESC&reportType={code}"
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_nuxt_args(html: str) -> dict:
    """
    window.__NUXT__=(function(a,b,...,aH){...}(args)) 에서 변수 매핑 추출
    """
    # function(a,b,c,..,aH) 시그니처 매칭
    sig_match = re.search(r'window\.__NUXT__=\(function\(([^)]+)\)', html)
    if not sig_match:
        return {}
    var_names = [v.strip() for v in sig_match.group(1).split(',')]

    # 마지막 호출 인자 추출 (function 끝 })(...) 의 ... 부분)
    # NUXT 패턴: 마지막에 }(arg1,arg2,...))
    nuxt_section_match = re.search(r'window\.__NUXT__=\(function\([^)]+\){.*}\((.*?)\)\);?</script>', html, re.DOTALL)
    if not nuxt_section_match:
        # fallback: 마지막 ) 직전까지
        last_close = html.rfind('))')
        if last_close > 0:
            args_section = html[max(0, last_close - 5000):last_close]
            # 마지막 } 다음의 ( 찾기
            paren_idx = args_section.rfind('}(')
            if paren_idx >= 0:
                args_str = args_section[paren_idx + 2:]
            else:
                return {}
        else:
            return {}
    else:
        args_str = nuxt_section_match.group(1)

    # 인자 파싱 (간단한 JSON 배열로 [args_str] 처리)
    # 인자가 따옴표 문자열, 숫자, void 0, null 등이 콤마로 구분
    # 안전한 파싱을 위해 토큰별로 분리
    args = parse_args_list(args_str)

    # 변수명 → 값 dict
    return dict(zip(var_names, args))


def parse_args_list(s: str) -> list:
    """
    NUXT args 문자열을 파싱 — "string", number, null, void 0 등을 토큰화
    """
    args = []
    i = 0
    n = len(s)
    while i < n:
        # whitespace skip
        while i < n and s[i] in ' \t\n':
            i += 1
        if i >= n:
            break

        if s[i] == ',':
            i += 1
            continue

        if s[i] == '"':
            # 문자열
            end = i + 1
            while end < n and s[end] != '"':
                if s[end] == '\\':
                    end += 2
                else:
                    end += 1
            try:
                args.append(json.loads(s[i:end + 1]))
            except:
                args.append(s[i + 1:end])
            i = end + 1
        elif s[i:i + 6] == 'void 0':
            args.append(None)
            i += 6
        elif s[i:i + 4] == 'null':
            args.append(None)
            i += 4
        elif s[i:i + 4] == 'true':
            args.append(True)
            i += 4
        elif s[i:i + 5] == 'false':
            args.append(False)
            i += 5
        elif s[i] == '-' or s[i].isdigit():
            # 숫자
            end = i
            if s[end] == '-':
                end += 1
            while end < n and (s[end].isdigit() or s[end] == '.' or s[end].lower() == 'e'):
                end += 1
            try:
                num_str = s[i:end]
                args.append(float(num_str) if '.' in num_str else int(num_str))
            except:
                args.append(s[i:end])
            i = end
        else:
            # 알 수 없는 토큰 - skip
            i += 1
    return args


def resolve_value(val, var_dict: dict):
    """변수면 dict에서 lookup, 아니면 그대로"""
    if isinstance(val, str) and val in var_dict:
        return var_dict[val]
    return val


def parse_report_objects(html: str, var_dict: dict) -> list:
    """
    HTML에서 리포트 객체들을 정규식으로 추출
    {REPORT_IDX:NNN,...,BUSINESS_NAME:"...",...,TARGET_STOCK_PRICES:"...",...}
    """
    reports = []

    # 각 리포트 객체 찾기 (REPORT_IDX로 시작)
    obj_pattern = r'\{REPORT_IDX:(\d+)[^}]*?\}'
    for match in re.finditer(obj_pattern, html):
        obj_str = match.group(0)

        def extract_field(field, raw=False):
            """필드값 추출 (변수면 lookup)"""
            # "FIELD:value," or "FIELD:value}"
            m = re.search(rf'{field}:([^,}}]+)', obj_str)
            if not m:
                return None
            v = m.group(1).strip()
            # 문자열 ("..." or 변수)
            if v.startswith('"'):
                # 문자열 리터럴
                str_match = re.search(rf'{field}:"([^"]*)"', obj_str)
                return str_match.group(1) if str_match else None
            elif v in var_dict:
                return var_dict[v]
            else:
                # 숫자나 raw 값
                if raw:
                    return v
                try:
                    return float(v) if '.' in v else int(v)
                except:
                    return v

        try:
            report = {
                'report_idx': extract_field('REPORT_IDX'),
                'business_code': extract_field('BUSINESS_CODE'),
                'business_name': extract_field('BUSINESS_NAME'),
                'office_name': extract_field('OFFICE_NAME'),
                'report_title': extract_field('REPORT_TITLE'),
                'report_writer': extract_field('REPORT_WRITER'),
                'report_content': extract_field('REPORT_CONTENT'),
                'report_filepath': extract_field('REPORT_FILEPATH'),
                'report_date': extract_field('REPORT_DATE'),
                'grade_value': extract_field('GRADE_VALUE'),
                'old_grade_value': extract_field('OLD_GRADE_VALUE'),
                'target_stock_prices': extract_field('TARGET_STOCK_PRICES'),
                'old_target_stock_prices': extract_field('OLD_TARGET_STOCK_PRICES'),
                'change_stock_prices_rate': extract_field('CHANGE_STOCK_PRICES_RATE'),
                'report_type': extract_field('REPORT_TYPE'),  # 한경 카테고리 코드
                'industry_name': extract_field('INDUSTRY_NAME'),
            }
            if report['report_title']:  # 산업/시장 리포트는 business_name 없을 수 있음
                reports.append(report)
        except Exception as e:
            print(f"  [W] 리포트 파싱 오류: {e}")
            continue

    return reports


def normalize_opinion(grade: str) -> str:
    """한경 의견 코드 → 표준화"""
    if not grade:
        return None
    g = str(grade).strip().lower()
    if any(k in g for k in ['buy', '매수', '강력', 'strong']):
        return 'Buy'
    if any(k in g for k in ['hold', '보유', 'neutral', '중립']):
        return 'Hold'
    if any(k in g for k in ['sell', '매도']):
        return 'Sell'
    return grade


def determine_target_change(target: str, old_target: str, rate: str) -> str:
    """목표가 변동 판단"""
    if not old_target or old_target in ('0', '', None):
        return '신규'
    try:
        new_v = float(target) if target else 0
        old_v = float(old_target) if old_target else 0
        if abs(new_v - old_v) < 0.01:
            return '유지'
        return '상향' if new_v > old_v else '하향'
    except:
        return None


def determine_category(report_type: str, business_code: str, business_name: str) -> str:
    """리포트 카테고리 분류"""
    rt = str(report_type or '').strip()
    # 한경 REPORT_TYPE 코드: 07020100=기업, 07020200=산업, 07020300=시장, 07020400=경제, 07020500=파생
    if rt.startswith('0702'):
        if rt == '07020100':
            return 'company'
        elif rt == '07020200':
            return 'industry'
        elif rt == '07020300':
            return 'market'
        elif rt == '07020400':
            return 'economy'
        elif rt == '07020500':
            return 'derivative'
    # 코드 없으면 종목코드로 판단 (있으면 기업, 없으면 산업)
    if business_code and str(business_code).strip():
        return 'company'
    return 'industry'


def save_to_supabase(reports: list, report_date: str = None) -> int:
    """research_reports 테이블에 저장"""
    if not reports:
        return 0

    rows = []
    for r in reports:
        opinion = normalize_opinion(r.get('grade_value'))
        target_change = determine_target_change(
            str(r.get('target_stock_prices', '')),
            str(r.get('old_target_stock_prices', '')),
            str(r.get('change_stock_prices_rate', ''))
        )

        # 날짜 변환 (YYYYMMDD → YYYY-MM-DD)
        rd = str(r.get('report_date', ''))
        if rd and len(rd) == 8 and rd.isdigit():
            date_iso = f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}"
        elif report_date:
            date_iso = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}"
        else:
            date_iso = datetime.now().strftime('%Y-%m-%d')

        try:
            target_p = int(float(r.get('target_stock_prices') or 0))
        except:
            target_p = 0

        # PDF 링크
        original_url = r.get('report_filepath', '') or ''
        if original_url and not original_url.startswith('http'):
            original_url = f"https://markets.hankyung.com{original_url}"

        category = determine_category(
            r.get('report_type'),
            r.get('business_code'),
            r.get('business_name')
        )

        # 산업 리포트는 stock_name 자리에 INDUSTRY_NAME 사용
        display_name = r.get('business_name') or r.get('industry_name') or '-'

        rows.append({
            'date': date_iso,
            'stock_code': str(r.get('business_code', '')).zfill(6) if r.get('business_code') else None,
            'stock_name': display_name,
            'brokerage': r.get('office_name', ''),
            'analyst': r.get('report_writer'),
            'opinion': opinion,
            'target_price': target_p if target_p > 0 else None,
            'target_change': target_change,
            'title': r.get('report_title', '').strip(),
            'original_url': original_url if original_url else None,
            'source': 'hankyung',
            'report_category': category,
        })

    # Supabase upsert
    saved = 0
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        url = f"{SUPABASE_URL}/rest/v1/research_reports?on_conflict=date,stock_name,brokerage,title"
        try:
            resp = requests.post(url, headers=SB_HEADERS, json=batch, timeout=30)
            if resp.status_code in [200, 201, 204]:
                saved += len(batch)
            else:
                print(f"  [W] DB 오류: {resp.status_code} {resp.text[:300]}")
        except Exception as e:
            print(f"  [X] 저장 실패: {e}")

    return saved


def main():
    print("=" * 60)
    print("[*] 한경 컨센서스 리포트 크롤러 v2")
    print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   카테고리: 기업 + 산업 + 시장 (각 SSR 페이지 ~20건/카테고리)")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] SUPABASE 환경변수 없음")
        return 1

    # 메인 페이지 SSR로 일 20건 수집 (한경 첫 화면 topReports + todayReports)
    # Phase 2에서 Playwright 도입 시 일 100건+ 가능
    print(f"\n[>] 한경 컨센서스 메인 페이지 fetch...")
    try:
        html = fetch_consensus_page('company')
    except Exception as e:
        print(f"[X] fetch 실패: {e}")
        return 1

    var_dict = parse_nuxt_args(html)
    reports = parse_report_objects(html, var_dict)
    # 중복 제거 (REPORT_IDX 기준)
    seen = set()
    unique_reports = []
    for r in reports:
        idx = r.get('report_idx')
        if idx and idx not in seen:
            seen.add(idx)
            unique_reports.append(r)
    reports = unique_reports

    print(f"      추출: {len(reports)}건 (변수 {len(var_dict)}개)")

    if not reports:
        print(f"[W] 추출된 리포트 없음")
        return 0

    sample = reports[0]
    print(f"  [i] 샘플: {sample.get('business_name')} | {sample.get('office_name')} | {(sample.get('report_title') or '')[:40]}")

    saved = save_to_supabase(reports)
    print(f"\n{'=' * 60}")
    print(f"[OK] {saved}건 저장 (UNIQUE 제약으로 중복 자동 스킵)")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
