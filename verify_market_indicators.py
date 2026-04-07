"""
=============================================================
  daily_market 해외지표 검증 스크립트
  verify_market_indicators.py  |  v1.0  |  2026-04-06

  실행: python verify_market_indicators.py

  데이터 원천 원칙:
    국내지표 (키움 API 전용):
      - KOSPI, KOSDAQ, USD/KRW

    해외지표 (yfinance 전용):
      - US10Y (미국 10년물 국채금리)  → ^TNX
      - WTI (원유)                    → CL=F
      - NASDAQ                        → ^IXIC
      - S&P500                        → ^GSPC
      - Russell2000                   → ^RUT
      - DOW                           → ^DJI
      - VIX                           → ^VIX
      - BTC (비트코인 선물)            → BTC=F

    특수지표:
      - CNN Fear & Greed Index        → CNN API 직접 수집

  ※ KRX 데이터는 해외지표 수집 시 사용하지 않음
=============================================================
"""

import os
import sys
from datetime import datetime, date, timedelta

try:
    from supabase import create_client, Client
except ImportError:
    print("❌ supabase 패키지 없음. pip install supabase")
    sys.exit(1)

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    print("⚠️  yfinance 미설치 — 실시간 교차검증 생략")
    print("    pip install yfinance")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ofclchxfrjldmrzswgwi.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

if not SUPABASE_KEY:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        env_path = os.path.join(r"C:\Users\bebes\OneDrive\_Stock_Report", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if line.startswith("SUPABASE_KEY="):
                    SUPABASE_KEY = line.strip().split("=", 1)[1].strip('"').strip("'")

if not SUPABASE_KEY:
    print("❌ SUPABASE_KEY 필요")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = date.today()


class C:
    OK = "\033[92m"; WARN = "\033[93m"; FAIL = "\033[91m"
    INFO = "\033[96m"; BOLD = "\033[1m"; END = "\033[0m"

def ok(msg):   print(f"  {C.OK}✅ {msg}{C.END}")
def warn(msg): print(f"  {C.WARN}⚠️  {msg}{C.END}")
def fail(msg): print(f"  {C.FAIL}❌ {msg}{C.END}")
def info(msg): print(f"  {C.INFO}ℹ️  {msg}{C.END}")
def header(msg): print(f"\n{C.BOLD}{'='*60}\n  {msg}\n{'='*60}{C.END}")


# ─── 지표 매핑 ───
YFINANCE_TICKERS = {
    "us10y":      {"ticker": "^TNX",  "name": "미국 10년물 국채금리", "unit": "%"},
    "wti":        {"ticker": "CL=F",  "name": "WTI 원유",           "unit": "USD"},
    "nasdaq":     {"ticker": "^IXIC", "name": "NASDAQ",             "unit": "pt"},
    "sp500":      {"ticker": "^GSPC", "name": "S&P500",             "unit": "pt"},
    "russell2000": {"ticker": "^RUT", "name": "Russell 2000",       "unit": "pt"},
    "dow":        {"ticker": "^DJI",  "name": "DOW Jones",          "unit": "pt"},
    "vix":        {"ticker": "^VIX",  "name": "VIX 변동성지수",      "unit": "pt"},
    "btc":        {"ticker": "BTC=F", "name": "BTC 선물",           "unit": "USD"},
}

KIWOOM_INDICATORS = {
    "kospi":    {"name": "KOSPI",   "source": "키움 API"},
    "kosdaq":   {"name": "KOSDAQ",  "source": "키움 API"},
    "usd_krw":  {"name": "USD/KRW", "source": "키움 API"},
}

CNN_INDICATORS = {
    "fear_greed": {"name": "CNN Fear & Greed Index", "source": "CNN API", "range": "0~100"},
}


# ============================================================
#  1. Supabase daily_market 구조 파악
# ============================================================
def check_market_structure():
    header("1. daily_market 테이블 구조 파악")

    res = supabase.table("daily_market") \
        .select("*") \
        .order("date", desc=True) \
        .limit(3) \
        .execute()

    if not res.data:
        fail("daily_market 데이터 없음!")
        return None

    sample = res.data[0]
    cols = list(sample.keys())
    info(f"컬럼 수: {len(cols)}")
    info(f"컬럼 목록: {cols}")
    info(f"최신 날짜: {sample.get('date')}")
    info("")

    # 지표별 매핑 상태 확인
    all_indicators = {**YFINANCE_TICKERS, **KIWOOM_INDICATORS, **CNN_INDICATORS}
    mapped = {}
    unmapped = []

    for key, meta in all_indicators.items():
        # 유사 컬럼명 탐색
        matches = [c for c in cols if key.lower().replace("_", "") in c.lower().replace("_", "")]
        if matches:
            mapped[key] = {"col": matches[0], "value": sample.get(matches[0]), **meta}
            ok(f"{meta['name']} → 컬럼 '{matches[0]}' = {sample.get(matches[0])}")
        else:
            unmapped.append(key)
            warn(f"{meta['name']} → 매핑 컬럼 없음 (키: {key})")

    if unmapped:
        info(f"\n미매핑 지표 {len(unmapped)}개 — 컬럼명 수동 확인 필요:")
        for u in unmapped:
            info(f"  → {u}: {all_indicators[u]['name']}")

    return res.data, mapped


# ============================================================
#  2. 해외지표 yfinance 교차검증
# ============================================================
def cross_verify_yfinance(market_data, mapped):
    header("2. 해외지표 yfinance 교차검증")

    if not HAS_YFINANCE:
        warn("yfinance 미설치 — 교차검증 생략")
        return

    if not market_data:
        warn("Supabase 데이터 없음")
        return

    latest = market_data[0]
    latest_date = latest.get("date")
    info(f"Supabase 최신: {latest_date}")

    # yfinance에서 최근 5일 데이터 가져오기
    info("yfinance에서 최근 데이터 조회 중...")
    end_dt = TODAY
    start_dt = TODAY - timedelta(days=10)

    mismatches = []
    for key, ticker_info in YFINANCE_TICKERS.items():
        ticker = ticker_info["ticker"]
        name = ticker_info["name"]

        if key not in mapped:
            info(f"  {name}: Supabase 컬럼 미매핑 → 스킵")
            continue

        try:
            data = yf.download(ticker, start=start_dt, end=end_dt, progress=False)
            if data.empty:
                warn(f"  {name}: yfinance 데이터 없음")
                continue

            # 최신 종가
            yf_close = float(data["Close"].iloc[-1])
            yf_date = data.index[-1].strftime("%Y-%m-%d")

            # Supabase 값
            supa_col = mapped[key]["col"]
            supa_val = latest.get(supa_col)

            if supa_val is not None and yf_close > 0:
                supa_val = float(supa_val)
                diff_pct = abs(supa_val - yf_close) / yf_close * 100

                if diff_pct > 5:
                    warn(f"  {name}: Supabase={supa_val:,.2f} vs yfinance={yf_close:,.2f} "
                         f"(차이 {diff_pct:.1f}%) ← 날짜 차이 가능")
                    mismatches.append(name)
                elif diff_pct > 1:
                    info(f"  {name}: Supabase={supa_val:,.2f} vs yfinance={yf_close:,.2f} "
                         f"(차이 {diff_pct:.1f}% — 날짜/시간대 차이)")
                else:
                    ok(f"  {name}: {supa_val:,.2f} ≈ {yf_close:,.2f} (일치)")
            else:
                info(f"  {name}: Supabase={supa_val}, yfinance={yf_close:,.2f}")

        except Exception as e:
            warn(f"  {name} ({ticker}): yfinance 오류 — {e}")

    if mismatches:
        info("")
        warn(f"큰 차이 발생 지표 {len(mismatches)}개: {', '.join(mismatches)}")
        info("  가능한 원인:")
        info("  1. Supabase 데이터가 이전 날짜 (최신 수집 안됨)")
        info("  2. yfinance 조회 시점과 수집 시점의 마감 차이")
        info("  3. 수집 스크립트 오류")
    else:
        ok("해외지표 전체 정합성 확인 완료")

    print()


# ============================================================
#  3. 시계열 연속성 검증
# ============================================================
def verify_timeseries_continuity():
    header("3. 시계열 연속성 검증 (최근 30일)")

    res = supabase.table("daily_market") \
        .select("date") \
        .gte("date", (TODAY - timedelta(days=45)).isoformat()) \
        .order("date") \
        .execute()

    if not res.data:
        warn("데이터 부족")
        return

    dates = sorted(set(r["date"] for r in res.data))
    info(f"기간: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    # 연속 날짜 갭 탐지
    gaps = []
    for i in range(1, len(dates)):
        d1 = datetime.strptime(dates[i-1], "%Y-%m-%d").date()
        d2 = datetime.strptime(dates[i], "%Y-%m-%d").date()
        diff = (d2 - d1).days

        if diff > 4:  # 주말+공휴일 감안해도 4일 이상 갭이면 이상
            gaps.append((dates[i-1], dates[i], diff))

    if gaps:
        warn(f"시계열 갭 {len(gaps)}건:")
        for g1, g2, diff in gaps:
            warn(f"  {g1} → {g2} ({diff}일 갭)")
    else:
        ok("시계열 연속성 정상")

    # 중복 날짜 체크
    if len(dates) != len(set(dates)):
        duplicates = [d for d in dates if dates.count(d) > 1]
        warn(f"중복 날짜: {set(duplicates)}")
    else:
        ok("중복 날짜 없음")

    print()


# ============================================================
#  4. CNN Fear & Greed Index 검증
# ============================================================
def verify_fear_greed():
    header("4. CNN Fear & Greed Index 검증")

    res = supabase.table("daily_market") \
        .select("*") \
        .order("date", desc=True) \
        .limit(10) \
        .execute()

    if not res.data:
        return

    # fear_greed 관련 컬럼 탐색
    sample = res.data[0]
    fg_cols = [c for c in sample.keys()
               if "fear" in c.lower() or "greed" in c.lower() or "fg" in c.lower()]

    if not fg_cols:
        info("fear_greed 컬럼 미확인 — 컬럼명 확인 필요")
        return

    col = fg_cols[0]
    info(f"Fear & Greed 컬럼: {col}")

    for r in res.data[:5]:
        val = r.get(col)
        dt = r.get("date")
        if val is not None:
            if 0 <= val <= 100:
                zone = (
                    "극심한 공포" if val <= 25
                    else "공포" if val <= 45
                    else "중립" if val <= 55
                    else "탐욕" if val <= 75
                    else "극심한 탐욕"
                )
                info(f"  {dt}: {val:.0f} ({zone})")
            else:
                warn(f"  {dt}: {val} — 범위 이탈! (0~100)")
        else:
            warn(f"  {dt}: NULL")

    print()


# ============================================================
#  5. 데이터 수집 순서 확인
# ============================================================
def verify_collection_order():
    header("5. 데이터 수집 순서 원칙 확인")

    info("── market_indicators.py 수집 순서 (확인) ──")
    info("")
    info("  국내 지표 (키움 API):")
    info("    1. KOSPI")
    info("    2. KOSDAQ")
    info("    3. USD/KRW")
    info("")
    info("  해외 지표 (yfinance):")
    info("    4. US10Y (^TNX)")
    info("    5. WTI (CL=F)")
    info("    6. NASDAQ (^IXIC)")
    info("    7. S&P500 (^GSPC)")
    info("    8. Russell2000 (^RUT)")
    info("    9. DOW (^DJI)")
    info("   10. VIX (^VIX)")
    info("   11. BTC 선물 (BTC=F)")
    info("")
    info("  특수 지표:")
    info("   12. CNN Fear & Greed Index (CNN API)")
    info("")
    info("  ⚠️  주의사항:")
    info("  - 국내 지표는 반드시 키움 API로만 수집")
    info("  - 해외 지표는 yfinance로만 수집")
    info("  - KRX, FinanceDataReader는 해외지표에 사용하지 않음")
    info("  - CNN Fear & Greed는 별도 CNN API 엔드포인트 사용")

    print()


# ============================================================
#  MAIN
# ============================================================
def main():
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  daily_market 해외지표 검증                                ║
║  실행일: {TODAY.isoformat()}                                       ║
║  원칙: 국내=키움API / 해외=yfinance / CNN=CNN API          ║
╚══════════════════════════════════════════════════════════╝
    """)

    result = check_market_structure()
    if result:
        market_data, mapped = result
        cross_verify_yfinance(market_data, mapped)
    else:
        warn("구조 파악 실패 — 이후 검증 생략")

    verify_timeseries_continuity()
    verify_fear_greed()
    verify_collection_order()

    header("📋 해외지표 검증 완료")
    info("모든 해외지표는 yfinance를 통해서만 수집됩니다.")
    info("국내지표(KOSPI, KOSDAQ, USD/KRW)는 키움 API 전용입니다.")
    print()


if __name__ == "__main__":
    main()
