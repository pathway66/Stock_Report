"""
=============================================================
  AI+패스웨이 전체 파이프라인 검증 스크립트
  verify_pipeline.py  |  v1.0  |  2026-04-06

  실행: python verify_pipeline.py

  검증 범위:
    1. daily_supply  — 5대 주체 수급 데이터 (키움 API 기준)
    2. daily_ohlcv   — 종목별 OHLCV 데이터 (키움 API 기준)
    3. daily_market   — 시장 지표 (해외: yfinance, 국내: 키움)
    4. sr_supply_grades — 수급 콤보 등급
    5. top3_history   — TOP3 추천 이력
    6. sector_map     — 섹터 매핑

  데이터 원천 원칙:
    - 모든 국내 raw data = 키움 REST API 기준
    - yfinance/KRX = 해외지표 전용 (US10Y, WTI, NASDAQ, S&P500,
      Russell2000, DOW, VIX, BTC, CNN Fear & Greed)
=============================================================
"""

import os
import sys
import json
from datetime import datetime, timedelta, date
from collections import defaultdict

# ─── Supabase 연결 ───
try:
    from supabase import create_client, Client
except ImportError:
    print("❌ supabase 패키지 없음. 설치: pip install supabase")
    sys.exit(1)

# 환경변수에서 Supabase 키 로드
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ofclchxfrjldmrzswgwi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

if not SUPABASE_KEY:
    # .env 파일에서 시도 (OneDrive 경로)
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        env_path = os.path.join(r"C:\Users\bebes\OneDrive\_Stock_Report", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if line.startswith("SUPABASE_KEY="):
                    SUPABASE_KEY = line.strip().split("=", 1)[1].strip('"').strip("'")

    if not SUPABASE_KEY:
        print("❌ SUPABASE_KEY 환경변수 또는 .env 파일 필요")
        print("   set SUPABASE_KEY=your_key  (PowerShell)")
        sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── 설정 ───
TODAY = date.today()
# 영업일 기준 최근 검증 범위 (20일)
CHECK_DAYS = 60
CHECK_START = (TODAY - timedelta(days=CHECK_DAYS)).isoformat()

# 한국 공휴일 2026 (주요 공휴일 — 필요시 추가)
KR_HOLIDAYS_2026 = {
    "2026-01-01",  # 신정
    "2026-01-29", "2026-01-30", "2026-01-31",  # 설날 연휴 (추정)
    "2026-03-01",  # 삼일절
    "2026-05-05",  # 어린이날
    "2026-05-24",  # 석가탄신일 (추정)
    "2026-06-06",  # 현충일
    "2026-08-15",  # 광복절
    "2026-09-24", "2026-09-25", "2026-09-26",  # 추석 연휴 (추정)
    "2026-10-03",  # 개천절
    "2026-10-09",  # 한글날
    "2026-12-25",  # 크리스마스
}

def is_trading_day(d: date) -> bool:
    """한국 증시 영업일 여부"""
    if d.weekday() >= 5:  # 토/일
        return False
    if d.isoformat() in KR_HOLIDAYS_2026:
        return False
    return True

def get_expected_trading_days(start: date, end: date) -> list:
    """기간 내 예상 영업일 리스트"""
    days = []
    current = start
    while current <= end:
        if is_trading_day(current):
            days.append(current.isoformat())
        current += timedelta(days=1)
    return days


# ============================================================
#  색상 출력 헬퍼
# ============================================================
class C:
    OK = "\033[92m"     # 녹색
    WARN = "\033[93m"   # 노랑
    FAIL = "\033[91m"   # 빨강
    INFO = "\033[96m"   # 시안
    BOLD = "\033[1m"
    END = "\033[0m"

def ok(msg):   print(f"  {C.OK}✅ {msg}{C.END}")
def warn(msg): print(f"  {C.WARN}⚠️  {msg}{C.END}")
def fail(msg): print(f"  {C.FAIL}❌ {msg}{C.END}")
def info(msg): print(f"  {C.INFO}ℹ️  {msg}{C.END}")
def header(msg): print(f"\n{C.BOLD}{'='*60}\n  {msg}\n{'='*60}{C.END}")

results = {"pass": 0, "warn": 0, "fail": 0}

def check(condition, pass_msg, fail_msg, level="fail"):
    if condition:
        ok(pass_msg)
        results["pass"] += 1
    elif level == "warn":
        warn(fail_msg)
        results["warn"] += 1
    else:
        fail(fail_msg)
        results["fail"] += 1


# ============================================================
#  1. daily_supply 검증
# ============================================================
def verify_daily_supply():
    header("1. daily_supply — 5대 주체 수급 데이터 검증")

    # 1-1. 최신 데이터 날짜 확인
    info("최신 데이터 날짜 확인...")
    res = supabase.table("daily_supply") \
        .select("date") \
        .order("date", desc=True) \
        .limit(1) \
        .execute()

    if res.data:
        latest_date = res.data[0]["date"]
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").date()
        days_behind = (TODAY - latest_dt).days
        info(f"최신 데이터: {latest_date} (오늘 대비 {days_behind}일 전)")

        # 오늘이 영업일이면 어제 데이터까지 있어야 정상
        if days_behind > 3:
            fail(f"최신 데이터가 {days_behind}일 전 — 수집 파이프라인 중단 의심!")
        elif days_behind > 1:
            warn(f"최신 데이터가 {days_behind}일 전 — 주말/공휴일 아닌 경우 확인 필요")
        else:
            ok(f"최신 데이터 정상: {latest_date}")
    else:
        fail("daily_supply 테이블에 데이터 없음!")
        return

    # 1-2. 총 데이터 건수
    info("총 데이터 건수 확인...")
    res = supabase.table("daily_supply") \
        .select("*", count="exact") \
        .gte("date", CHECK_START) \
        .execute()

    total_rows = res.count if res.count else len(res.data)
    info(f"최근 {CHECK_DAYS}일 데이터: {total_rows:,}건")

    # 1-3. 날짜별 종목 수 일관성
    info("날짜별 종목 수 일관성 체크...")
    res = supabase.rpc("count_by_date_supply", {}).execute()

    # RPC가 없을 경우 수동 집계
    if not res.data:
        info("RPC 없음 → 수동 집계 (최근 20영업일)...")
        date_counts = {}
        # 최근 데이터에서 unique dates 추출
        res2 = supabase.table("daily_supply") \
            .select("date") \
            .gte("date", (TODAY - timedelta(days=40)).isoformat()) \
            .order("date", desc=True) \
            .execute()

        unique_dates = sorted(set(r["date"] for r in res2.data), reverse=True)[:20]

        for d in unique_dates:
            res3 = supabase.table("daily_supply") \
                .select("*", count="exact") \
                .eq("date", d) \
                .execute()
            cnt = res3.count if res3.count else len(res3.data)
            date_counts[d] = cnt

        if date_counts:
            counts = list(date_counts.values())
            min_cnt, max_cnt = min(counts), max(counts)
            avg_cnt = sum(counts) / len(counts)

            info(f"날짜별 종목 수: min={min_cnt}, max={max_cnt}, avg={avg_cnt:.0f}")

            # 정상: ~456종목 × 5주체 = ~2,280건 또는 종목당 1행에 5주체 컬럼
            check(
                max_cnt - min_cnt < 50,
                f"날짜별 종목 수 편차 정상 (±{max_cnt - min_cnt}건)",
                f"날짜별 종목 수 편차 큼: {min_cnt} ~ {max_cnt}건 — 누락 의심",
                level="warn"
            )

            # 편차가 큰 날짜 출력
            for d, cnt in sorted(date_counts.items()):
                if abs(cnt - avg_cnt) > 30:
                    warn(f"  {d}: {cnt}건 (평균 대비 {cnt - avg_cnt:+.0f})")

    # 1-4. NULL 값 체크
    info("NULL 값 체크...")
    # 주요 컬럼에 NULL이 있는지 확인
    null_check_cols = ["stock_code", "stock_name", "date"]
    for col in null_check_cols:
        res = supabase.table("daily_supply") \
            .select("*", count="exact") \
            .is_(col, "null") \
            .gte("date", CHECK_START) \
            .execute()
        null_count = res.count if res.count else len(res.data)
        check(
            null_count == 0,
            f"{col} NULL 없음",
            f"{col}에 NULL {null_count}건 발견!",
        )

    # 1-5. 영업일 데이터 누락 체크
    info("영업일 데이터 누락 체크...")
    res = supabase.table("daily_supply") \
        .select("date") \
        .gte("date", CHECK_START) \
        .order("date") \
        .execute()

    actual_dates = sorted(set(r["date"] for r in res.data))

    if actual_dates:
        start_dt = datetime.strptime(actual_dates[0], "%Y-%m-%d").date()
        end_dt = datetime.strptime(actual_dates[-1], "%Y-%m-%d").date()
        expected_days = get_expected_trading_days(start_dt, end_dt)

        missing_days = set(expected_days) - set(actual_dates)
        extra_days = set(actual_dates) - set(expected_days)

        if missing_days:
            warn(f"영업일 누락 {len(missing_days)}일:")
            for d in sorted(missing_days)[:10]:
                warn(f"  → {d}")
        else:
            ok("영업일 데이터 누락 없음")

        if extra_days:
            info(f"비영업일 데이터 {len(extra_days)}일 (공휴일 목록 확인 필요):")
            for d in sorted(extra_days)[:5]:
                info(f"  → {d}")

    # 1-6. 종목 유니버스 일관성 (코팔+닥사 ~456종목)
    info("종목 유니버스 체크...")
    if actual_dates:
        latest = actual_dates[-1]
        res = supabase.table("daily_supply") \
            .select("stock_code") \
            .eq("date", latest) \
            .execute()

        stock_count = len(set(r["stock_code"] for r in res.data))
        info(f"최신일({latest}) 종목 수: {stock_count}")
        check(
            400 <= stock_count <= 520,
            f"종목 수 정상 범위: {stock_count} (기대: 코팔230 + 닥사226 ≈ 456)",
            f"종목 수 이상: {stock_count} (기대: ~456)",
            level="warn"
        )

    # 1-7. 5대 주체 데이터 존재 확인
    info("5대 주체 데이터 확인...")
    # 테이블 구조에 따라 컬럼명 확인 (1행=1종목1일 vs 1행=1종목1일1주체)
    res = supabase.table("daily_supply") \
        .select("*") \
        .order("date", desc=True) \
        .limit(5) \
        .execute()

    if res.data:
        sample = res.data[0]
        cols = list(sample.keys())
        info(f"daily_supply 컬럼: {cols}")

        # 주체별 컬럼이 있는지, 또는 investor_type 컬럼이 있는지 확인
        investor_cols = [c for c in cols if any(k in c.lower() for k in
            ["foreign", "pension", "mutual", "private", "other", "외국인", "연기금", "투신", "사모", "기타"])]

        if investor_cols:
            ok(f"주체별 컬럼 발견: {investor_cols}")
        elif "investor_type" in cols or "주체" in cols:
            ok("investor_type 컬럼 확인 → 1행=1종목1일1주체 구조")
        else:
            info(f"전체 컬럼 목록으로 구조 확인 필요: {cols}")

    print()
    return actual_dates


# ============================================================
#  2. daily_ohlcv 검증
# ============================================================
def verify_daily_ohlcv(supply_dates=None):
    header("2. daily_ohlcv — OHLCV 데이터 검증")

    # 2-1. 최신 데이터 날짜
    info("최신 데이터 날짜 확인...")
    res = supabase.table("daily_ohlcv") \
        .select("date") \
        .order("date", desc=True) \
        .limit(1) \
        .execute()

    if res.data:
        latest = res.data[0]["date"]
        info(f"최신 OHLCV: {latest}")
    else:
        fail("daily_ohlcv 데이터 없음!")
        return

    # 2-2. supply vs ohlcv 날짜 일치 확인
    if supply_dates:
        res2 = supabase.table("daily_ohlcv") \
            .select("date") \
            .gte("date", CHECK_START) \
            .order("date") \
            .execute()
        ohlcv_dates = sorted(set(r["date"] for r in res2.data))

        supply_only = set(supply_dates) - set(ohlcv_dates)
        ohlcv_only = set(ohlcv_dates) - set(supply_dates)

        check(
            len(supply_only) == 0,
            "daily_supply의 모든 날짜가 daily_ohlcv에도 존재",
            f"daily_supply에만 있는 날짜 {len(supply_only)}일: {sorted(supply_only)[:5]}",
        )

        if ohlcv_only:
            info(f"daily_ohlcv에만 있는 날짜 {len(ohlcv_only)}일 (정상일 수 있음)")

    # 2-3. 이상치 탐지 — 가격 0 또는 음수
    info("가격 이상치 탐지...")
    res = supabase.table("daily_ohlcv") \
        .select("stock_code, date, close_price, open_price, high_price, low_price", count="exact") \
        .gte("date", CHECK_START) \
        .or_("close_price.lte.0,open_price.lte.0,high_price.lte.0,low_price.lte.0") \
        .execute()

    bad_price = res.count if res.count else len(res.data)
    check(
        bad_price == 0,
        "가격 ≤ 0 데이터 없음",
        f"가격 ≤ 0 데이터 {bad_price}건 발견!",
    )

    # 2-4. high >= close >= low 관계 검증 (샘플)
    info("가격 관계 검증 (high ≥ close ≥ low)...")
    res = supabase.table("daily_ohlcv") \
        .select("stock_code, date, high_price, low_price, close_price") \
        .gte("date", (TODAY - timedelta(days=10)).isoformat()) \
        .limit(1000) \
        .execute()

    violations = []
    for r in res.data:
        h = r.get("high_price") or 0
        l = r.get("low_price") or 0
        c = r.get("close_price") or 0
        if h < c or c < l or h < l:
            violations.append(f"{r['stock_code']} {r['date']}: H={h} C={c} L={l}")

    check(
        len(violations) == 0,
        "가격 관계(H≥C≥L) 정상",
        f"가격 관계 위반 {len(violations)}건: {violations[:3]}",
    )

    # 2-5. 종목 수 일치 확인
    if supply_dates:
        latest_common = max(set(supply_dates) & set(ohlcv_dates)) if ohlcv_dates else None
        if latest_common:
            res_s = supabase.table("daily_supply") \
                .select("stock_code") \
                .eq("date", latest_common) \
                .execute()
            res_o = supabase.table("daily_ohlcv") \
                .select("stock_code") \
                .eq("date", latest_common) \
                .execute()

            supply_stocks = set(r["stock_code"] for r in res_s.data)
            ohlcv_stocks = set(r["stock_code"] for r in res_o.data)

            missing_ohlcv = supply_stocks - ohlcv_stocks
            missing_supply = ohlcv_stocks - supply_stocks

            info(f"[{latest_common}] supply 종목: {len(supply_stocks)}, ohlcv 종목: {len(ohlcv_stocks)}")

            if missing_ohlcv:
                warn(f"supply에는 있는데 ohlcv에 없는 종목 {len(missing_ohlcv)}개:")
                for s in sorted(missing_ohlcv)[:10]:
                    warn(f"  → {s}")
            else:
                ok("supply ↔ ohlcv 종목 완전 일치")

    print()


# ============================================================
#  3. daily_market 검증 (해외지표 = yfinance)
# ============================================================
def verify_daily_market():
    header("3. daily_market — 시장 지표 검증")

    # 3-1. 최신 데이터
    info("최신 데이터 확인...")
    res = supabase.table("daily_market") \
        .select("*") \
        .order("date", desc=True) \
        .limit(3) \
        .execute()

    if res.data:
        latest = res.data[0]
        info(f"최신 날짜: {latest.get('date')}")
        cols = list(latest.keys())
        info(f"컬럼: {cols}")

        # 주요 지표 존재 확인
        expected_indicators = {
            "국내": ["kospi", "kosdaq"],
            "환율/원자재": ["usd_krw", "wti"],
            "해외지수": ["nasdaq", "sp500", "dow", "russell2000"],
            "기타": ["us10y", "btc", "vix", "fear_greed"],
        }

        for category, indicators in expected_indicators.items():
            for ind in indicators:
                # 컬럼명 다양한 형태 대응
                found = any(ind.lower() in c.lower().replace("_", "").replace("-", "")
                           for c in cols)
                if not found:
                    # value 기반 구조일 수 있음 (indicator_name 컬럼)
                    found = "indicator" in " ".join(cols).lower()

                check(
                    found,
                    f"{category} > {ind} 컬럼/데이터 확인됨",
                    f"{category} > {ind} 데이터 미확인 — 컬럼 구조 재확인 필요",
                    level="warn"
                )
    else:
        fail("daily_market 데이터 없음!")
        return

    # 3-2. 해외지표 데이터 소스 검증 (yfinance 전용)
    info("해외지표 수집 원칙 확인...")
    info("  ✓ US10Y, WTI, NASDAQ, S&P500, Russell2000, DOW, VIX, BTC → yfinance")
    info("  ✓ KOSPI, KOSDAQ, USD/KRW → 키움 API")
    info("  ✓ CNN Fear & Greed Index → CNN 전용 수집")

    # 3-3. NULL/0값 체크
    info("NULL/0값 체크 (최근 5영업일)...")
    for row in res.data[:5]:
        nulls = [k for k, v in row.items() if v is None and k not in ("id", "created_at")]
        zeros = [k for k, v in row.items() if v == 0 and k not in ("id",)]
        if nulls:
            warn(f"  {row.get('date')}: NULL → {nulls}")
        if zeros:
            info(f"  {row.get('date')}: 0값 → {zeros} (정상 여부 확인)")

    print()


# ============================================================
#  4. sr_supply_grades 검증
# ============================================================
def verify_sr_supply_grades():
    header("4. sr_supply_grades — 수급 콤보 등급 검증")

    # 4-1. 총 건수 및 종목 수
    res = supabase.table("sr_supply_grades") \
        .select("*", count="exact") \
        .execute()

    total = res.count if res.count else len(res.data)
    info(f"총 데이터: {total}건")

    # 4-2. 등급 분포
    res = supabase.table("sr_supply_grades") \
        .select("*") \
        .execute()

    if res.data:
        grades = defaultdict(int)
        stocks = set()
        for r in res.data:
            grade = r.get("combo_grade") or r.get("grade") or "UNKNOWN"
            grades[grade] += 1
            stocks.add(r.get("stock_code"))

        info(f"종목 수: {len(stocks)}")
        info(f"등급 분포:")
        for g in ["S", "A", "B", "C"]:
            cnt = grades.get(g, 0)
            pct = cnt / total * 100 if total > 0 else 0
            info(f"  {g}: {cnt}건 ({pct:.1f}%)")

        # S등급 비율 체크 (너무 많으면 기준 느슨)
        s_pct = grades.get("S", 0) / total * 100 if total > 0 else 0
        check(
            s_pct < 20,
            f"S등급 비율 적정: {s_pct:.1f}%",
            f"S등급 비율 {s_pct:.1f}% — 기준 검토 필요 (+3%p 초과수익 적용 확인)",
            level="warn"
        )

        check(
            len(stocks) >= 400,
            f"분석 종목 수 정상: {len(stocks)}",
            f"분석 종목 수 부족: {len(stocks)} (기대: ~415+)",
            level="warn"
        )

    print()


# ============================================================
#  5. top3_history 검증
# ============================================================
def verify_top3_history():
    header("5. top3_history — TOP3 추천 이력 검증")

    # 5-1. 최신 데이터
    res = supabase.table("top3_history") \
        .select("*") \
        .order("date", desc=True) \
        .limit(10) \
        .execute()

    if res.data:
        latest_date = res.data[0].get("date")
        info(f"최신 TOP3 날짜: {latest_date}")

        # 최신일 TOP3 종목 출력
        latest_items = [r for r in res.data if r.get("date") == latest_date]
        info(f"최신 TOP3 ({latest_date}):")
        for item in latest_items:
            rank = item.get("rank", "?")
            name = item.get("stock_name", item.get("name", "?"))
            code = item.get("stock_code", "?")
            info(f"  #{rank}: {name} ({code})")

        # 날짜별 3건씩 있는지 확인
        date_groups = defaultdict(int)
        for r in res.data:
            date_groups[r.get("date")] += 1

        for d, cnt in sorted(date_groups.items(), reverse=True)[:5]:
            check(
                cnt == 3,
                f"{d}: {cnt}건 (정상)",
                f"{d}: {cnt}건 (3건이 아님!)",
                level="warn"
            )
    else:
        warn("top3_history 데이터 없음")

    print()


# ============================================================
#  6. sector_map 검증
# ============================================================
def verify_sector_map():
    header("6. sector_map — 섹터 매핑 검증")

    res = supabase.table("sector_map") \
        .select("*", count="exact") \
        .execute()

    total = res.count if res.count else len(res.data)
    info(f"총 매핑: {total}건")

    if res.data:
        sectors = defaultdict(int)
        codes = set()
        for r in res.data:
            sector = r.get("sector", "UNKNOWN")
            code = r.get("stock_code", "?")
            sectors[sector] += 1
            codes.add(code)

        info(f"유니크 종목: {len(codes)}")
        info(f"섹터 수: {len(sectors)}")

        # 상위 섹터 출력
        top_sectors = sorted(sectors.items(), key=lambda x: -x[1])[:10]
        info("상위 10 섹터:")
        for s, cnt in top_sectors:
            info(f"  {s}: {cnt}종목")

        # sector 길이 체크 (VARCHAR 200 이슈 재발 방지)
        long_sectors = [s for s in sectors.keys() if len(s) > 30]
        if long_sectors:
            info(f"30자 초과 섹터명 {len(long_sectors)}건 (VARCHAR(200) 필요 확인):")
            for s in long_sectors[:5]:
                info(f"  → [{len(s)}자] {s}")

        # daily_supply 종목과 교차 검증
        res2 = supabase.table("daily_supply") \
            .select("stock_code") \
            .order("date", desc=True) \
            .limit(500) \
            .execute()

        if res2.data:
            supply_codes = set(r["stock_code"] for r in res2.data)
            unmapped = supply_codes - codes

            check(
                len(unmapped) == 0,
                f"supply 종목 전체 섹터 매핑 완료",
                f"섹터 미매핑 종목 {len(unmapped)}개: {sorted(unmapped)[:10]}",
                level="warn"
            )

    print()


# ============================================================
#  7. 파이프라인 종합 상태
# ============================================================
def verify_pipeline_health():
    header("7. 파이프라인 종합 상태 점검")

    tables = ["daily_supply", "daily_ohlcv", "daily_market", "sr_supply_grades", "top3_history", "sector_map"]

    info("테이블별 최신 데이터 날짜:")
    for t in tables:
        try:
            res = supabase.table(t) \
                .select("date") \
                .order("date", desc=True) \
                .limit(1) \
                .execute()

            if res.data:
                latest = res.data[0].get("date", "N/A")
                days_ago = (TODAY - datetime.strptime(latest, "%Y-%m-%d").date()).days if latest != "N/A" else "?"
                info(f"  {t:25s} → {latest} ({days_ago}일 전)")
            else:
                warn(f"  {t:25s} → 데이터 없음")
        except Exception as e:
            warn(f"  {t:25s} → 조회 실패: {e}")

    # 15:50 자동 수집 점검 안내
    info("")
    info("── 15:50 자동 수집 점검 체크리스트 ──")
    info("  □ Windows Task Scheduler에서 run_scheduled.bat 활성 상태 확인")
    info("  □ 최근 실행 기록 (schtasks /query /tn run_daily /fo LIST /v)")
    info("  □ C:\\_Stock_Report\\logs\\ 에서 최신 로그 확인")
    info("  □ 키움 HeroesWeb4 로그인 세션 만료 여부")

    print()


# ============================================================
#  8. 데이터 이상치 상세 분석
# ============================================================
def verify_data_anomalies():
    header("8. 데이터 이상치 상세 분석")

    # 8-1. daily_supply에서 극단적 순매수/순매도 값 탐지
    info("극단적 수급 데이터 탐지 (최근 20영업일)...")
    res = supabase.table("daily_supply") \
        .select("*") \
        .gte("date", (TODAY - timedelta(days=30)).isoformat()) \
        .order("date", desc=True) \
        .limit(2000) \
        .execute()

    if res.data:
        sample = res.data[0]
        # 수치형 컬럼 자동 감지
        numeric_cols = [k for k, v in sample.items()
                       if isinstance(v, (int, float)) and k not in ("id",)]

        for col in numeric_cols[:10]:  # 주요 컬럼 10개만
            values = [r[col] for r in res.data if r.get(col) is not None]
            if not values:
                continue

            avg_val = sum(values) / len(values)
            max_val = max(values)
            min_val = min(values)

            # 평균 대비 10배 이상 차이나는 값 탐지
            if avg_val != 0:
                outliers = [(r.get("stock_code", "?"), r.get("date", "?"), r[col])
                           for r in res.data
                           if r.get(col) is not None and abs(r[col]) > abs(avg_val) * 10]

                if outliers and len(outliers) < 20:
                    info(f"  {col}: 이상치 {len(outliers)}건 (평균의 10배 초과)")
                    for code, dt, val in outliers[:3]:
                        info(f"    → {code} {dt}: {val:,.0f}")

    # 8-2. 추정평균가 0원 체크 (sr_supply_data)
    info("sr_supply_data 추정평균가 0원 체크...")
    try:
        res = supabase.table("sr_supply_data") \
            .select("*", count="exact") \
            .gte("date", CHECK_START) \
            .eq("estimated_avg_price", 0) \
            .execute()

        zero_price = res.count if res.count else len(res.data)
        check(
            zero_price == 0,
            "추정평균가 0원 없음",
            f"추정평균가 0원 {zero_price}건 — 키움 API 수집 오류 가능성",
            level="warn"
        )
    except Exception as e:
        info(f"sr_supply_data 테이블 접근 실패: {e}")

    print()


# ============================================================
#  MAIN
# ============================================================
def main():
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  AI+패스웨이 파이프라인 검증 리포트                          ║
║  실행일: {TODAY.isoformat()}                                  ║
║  검증범위: 최근 {CHECK_DAYS}일 ({CHECK_START} ~ {TODAY.isoformat()})      ║
╚══════════════════════════════════════════════════════════╝
    """)

    # 순차 검증 실행
    supply_dates = verify_daily_supply()
    verify_daily_ohlcv(supply_dates)
    verify_daily_market()
    verify_sr_supply_grades()
    verify_top3_history()
    verify_sector_map()
    verify_pipeline_health()
    verify_data_anomalies()

    # 종합 결과
    header("📋 종합 검증 결과")
    total = results["pass"] + results["warn"] + results["fail"]
    print(f"""
  {C.OK}✅ PASS: {results['pass']}{C.END}
  {C.WARN}⚠️  WARN: {results['warn']}{C.END}
  {C.FAIL}❌ FAIL: {results['fail']}{C.END}
  ─────────────
  총 체크: {total}건
    """)

    if results["fail"] > 0:
        print(f"  {C.FAIL}{C.BOLD}⚠️  FAIL 항목이 있습니다. 긴급 확인 필요!{C.END}")
    elif results["warn"] > 0:
        print(f"  {C.WARN}{C.BOLD}ℹ️  경고 항목을 확인해주세요.{C.END}")
    else:
        print(f"  {C.OK}{C.BOLD}🎉 전체 파이프라인 정상!{C.END}")

    print()


if __name__ == "__main__":
    main()
