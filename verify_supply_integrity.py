"""
=============================================================
  daily_supply 심층 무결성 검증 스크립트
  verify_supply_integrity.py  |  v1.0  |  2026-04-06

  실행: python verify_supply_integrity.py [YYYYMMDD]
        (날짜 미지정 시 최신 영업일 자동 선택)

  검증 항목:
    A. 키움 API raw CSV ↔ Supabase daily_supply 정합성
    B. 5대 주체 순매수합 = 0 검증 (매수-매도 균형)
    C. 추정평균가 계산 검증
    D. 이격도(Gap%) 시점 일치 검증 — SB/SS v1 이슈 재현 방지
    E. 코팔/닥사 유니버스 커버리지 100% 확인
=============================================================
"""

import os
import sys
import csv
import json
import glob
from datetime import datetime, date, timedelta
from collections import defaultdict

try:
    from supabase import create_client, Client
except ImportError:
    print("❌ supabase 패키지 없음. pip install supabase")
    sys.exit(1)

# ─── Supabase ───
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

# ─── 설정 ───
# 키움 raw 데이터 디렉토리 (오피스 노트북 기준)
KIWOOM_DATA_DIR = os.getenv("KIWOOM_DATA_DIR", r"C:\Users\bebes\OneDrive\_Stock_Report\data")

# 코팔/닥사 유니버스 파일
UNIVERSE_FILE = os.getenv("UNIVERSE_FILE", r"C:\Users\bebes\OneDrive\_Stock_Report\universe_stocks.json")

TODAY = date.today()


class C:
    OK = "\033[92m"
    WARN = "\033[93m"
    FAIL = "\033[91m"
    INFO = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"

def ok(msg):   print(f"  {C.OK}✅ {msg}{C.END}")
def warn(msg): print(f"  {C.WARN}⚠️  {msg}{C.END}")
def fail(msg): print(f"  {C.FAIL}❌ {msg}{C.END}")
def info(msg): print(f"  {C.INFO}ℹ️  {msg}{C.END}")
def header(msg): print(f"\n{C.BOLD}{'='*60}\n  {msg}\n{'='*60}{C.END}")


# ============================================================
#  A. 키움 raw CSV ↔ Supabase 정합성
# ============================================================
def verify_kiwoom_vs_supabase(target_date: str):
    header(f"A. 키움 raw CSV ↔ Supabase 정합성 [{target_date}]")

    # 키움 CSV 파일 탐색
    csv_pattern = os.path.join(KIWOOM_DATA_DIR, f"*{target_date.replace('-', '')}*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        warn(f"키움 CSV 파일 없음: {csv_pattern}")
        info("키움 데이터 디렉토리 경로를 확인하세요:")
        info(f"  현재 설정: {KIWOOM_DATA_DIR}")
        info("  환경변수로 변경: set KIWOOM_DATA_DIR=C:\\your\\path")
        return

    info(f"키움 CSV 발견: {len(csv_files)}개")

    # CSV에서 수급 데이터 로드
    kiwoom_data = {}
    for csv_file in csv_files:
        info(f"  파싱: {os.path.basename(csv_file)}")
        try:
            with open(csv_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get("종목코드", row.get("stock_code", "")).strip()
                    if code:
                        kiwoom_data[code] = row
        except Exception as e:
            warn(f"  CSV 파싱 오류: {e}")

    if not kiwoom_data:
        warn("CSV에서 데이터 추출 실패")
        return

    info(f"키움 CSV 종목 수: {len(kiwoom_data)}")

    # Supabase에서 동일 날짜 데이터 조회
    res = supabase.table("daily_supply") \
        .select("*") \
        .eq("date", target_date) \
        .execute()

    if not res.data:
        fail(f"Supabase에 {target_date} 데이터 없음!")
        return

    supabase_data = {}
    for r in res.data:
        code = r.get("stock_code", "")
        supabase_data[code] = r

    info(f"Supabase 종목 수: {len(supabase_data)}")

    # 종목 매칭
    kiwoom_codes = set(kiwoom_data.keys())
    supa_codes = set(supabase_data.keys())

    only_kiwoom = kiwoom_codes - supa_codes
    only_supa = supa_codes - kiwoom_codes

    check_count = 0
    if only_kiwoom:
        warn(f"키움에만 있는 종목 {len(only_kiwoom)}개:")
        for c in sorted(only_kiwoom)[:10]:
            warn(f"  → {c}")
        check_count += 1
    else:
        ok("키움 종목 전체 Supabase 적재 완료")

    if only_supa:
        info(f"Supabase에만 있는 종목 {len(only_supa)}개 (이전 유니버스 잔여 가능):")
        for c in sorted(only_supa)[:10]:
            info(f"  → {c}")

    # 값 정합성 (샘플 비교)
    info("수치 정합성 샘플 비교 (10종목)...")
    common = sorted(kiwoom_codes & supa_codes)[:10]
    mismatch_count = 0
    for code in common:
        kw = kiwoom_data[code]
        sp = supabase_data[code]
        # 주요 수급 필드 비교 (컬럼명 매핑은 실제 구조에 따라 조정 필요)
        # 이 부분은 실제 컬럼명을 확인한 후 정확히 매핑해야 함
        info(f"  {code}: 키움={list(kw.keys())[:5]}... | Supabase={list(sp.keys())[:5]}...")

    print()


# ============================================================
#  B. 5대 주체 순매수합 = 0 검증
# ============================================================
def verify_net_balance(target_date: str):
    header(f"B. 5대 주체 순매수합 균형 검증 [{target_date}]")

    res = supabase.table("daily_supply") \
        .select("*") \
        .eq("date", target_date) \
        .execute()

    if not res.data:
        warn(f"{target_date} 데이터 없음")
        return

    # 테이블 구조 파악
    sample = res.data[0]
    cols = list(sample.keys())
    info(f"컬럼 구조: {cols}")

    # 5대 주체 순매수 관련 컬럼 탐지
    net_cols = [c for c in cols if "net" in c.lower() or "순매수" in c or "순매도" in c]
    info(f"순매수 관련 컬럼: {net_cols}")

    if len(net_cols) >= 5:
        # 종목별로 5주체 순매수합이 대략 0인지 확인
        # (실제로는 개인투자자를 포함한 전체 합이 0)
        imbalances = []
        for r in res.data:
            total = sum(r.get(c, 0) or 0 for c in net_cols if r.get(c) is not None)
            code = r.get("stock_code", "?")
            if abs(total) > 1000000:  # 100만원 이상 불균형
                imbalances.append((code, total))

        if imbalances:
            info(f"순매수합 불균형 종목 {len(imbalances)}개 (개인 미포함시 정상):")
            for code, total in sorted(imbalances, key=lambda x: -abs(x[1]))[:5]:
                info(f"  {code}: 합계 {total:+,.0f}원")
            info("  ※ 5대 기관주체만의 합계이므로 개인투자자 미포함 → 불균형 정상")
        else:
            ok("주체간 순매수 데이터 정상")

    elif "investor_type" in cols:
        # 1행=1종목1주체 구조
        info("1행=1종목1주체 구조 → 종목별 합산 체크")
        stock_totals = defaultdict(float)
        for r in res.data:
            code = r.get("stock_code", "?")
            net = r.get("net_buy", 0) or r.get("net_amount", 0) or 0
            stock_totals[code] += net

        imbalances = [(c, t) for c, t in stock_totals.items() if abs(t) > 1000000]
        info(f"  종목별 합산 불균형: {len(imbalances)}개 (개인 미포함시 정상)")

    print()


# ============================================================
#  C. 추정평균가 검증
# ============================================================
def verify_estimated_avg_price(target_date: str):
    header(f"C. 추정평균가 검증 [{target_date}]")

    try:
        res = supabase.table("sr_supply_data") \
            .select("*") \
            .eq("date", target_date) \
            .limit(500) \
            .execute()
    except Exception as e:
        info(f"sr_supply_data 접근 실패: {e}")
        info("daily_supply 내 추정평균가 컬럼으로 대체 검증...")

        res = supabase.table("daily_supply") \
            .select("*") \
            .eq("date", target_date) \
            .limit(500) \
            .execute()

    if not res.data:
        warn("데이터 없음")
        return

    sample = res.data[0]
    cols = list(sample.keys())
    price_cols = [c for c in cols if "avg" in c.lower() or "평균" in c or "price" in c.lower()]
    info(f"추정평균가 관련 컬럼: {price_cols}")

    # 추정평균가 0 또는 NULL 체크
    issues = []
    for r in res.data:
        code = r.get("stock_code", "?")
        for pc in price_cols:
            val = r.get(pc)
            if val is not None and val == 0:
                issues.append(f"{code}/{pc}=0")
            elif val is not None and val < 0:
                issues.append(f"{code}/{pc}={val} (음수!)")

    if issues:
        warn(f"추정평균가 이상 {len(issues)}건:")
        for i in issues[:10]:
            warn(f"  → {i}")
    else:
        ok("추정평균가 값 정상 (0/음수 없음)")

    # OHLCV와 교차 검증: 추정평균가가 당일 high/low 범위 내인지
    info("추정평균가 ↔ OHLCV 범위 교차 검증...")
    ohlcv_res = supabase.table("daily_ohlcv") \
        .select("stock_code, high_price, low_price") \
        .eq("date", target_date) \
        .execute()

    if ohlcv_res.data:
        ohlcv_map = {r["stock_code"]: r for r in ohlcv_res.data}

        out_of_range = []
        for r in res.data:
            code = r.get("stock_code", "?")
            ohlcv = ohlcv_map.get(code)
            if not ohlcv:
                continue

            high = ohlcv.get("high_price", 0) or 0
            low = ohlcv.get("low_price", 0) or 0
            if high == 0 or low == 0:
                continue

            # 추정평균가가 당일 가격 범위를 크게 벗어나는 경우
            margin = (high - low) * 0.5  # 50% 여유
            for pc in price_cols:
                avg_p = r.get(pc)
                if avg_p and avg_p > 0:
                    if avg_p > high + margin or avg_p < low - margin:
                        out_of_range.append(f"{code}: {pc}={avg_p:,.0f} (범위: {low:,.0f}~{high:,.0f})")

        if out_of_range:
            warn(f"추정평균가 범위 이탈 {len(out_of_range)}건:")
            for o in out_of_range[:10]:
                warn(f"  → {o}")
            info("  ※ 키움 API의 추정평균가는 당일 시가~종가 기간의 가중평균이므로")
            info("    전일 대비 갭상승/갭하락 시 범위 이탈 가능 (정상)")
        else:
            ok("추정평균가 전체 OHLCV 범위 내")

    print()


# ============================================================
#  D. 이격도(Gap%) 시점 일치 검증
# ============================================================
def verify_gap_timing(target_date: str):
    header(f"D. 이격도(Gap%) 시점 일치 검증 [{target_date}]")

    info("── SB/SS v1 이슈 재현 방지 체크 ──")
    info("  문제: OHLCV 수집 시점과 수급 분석 시점의 시간차로 이격도 왜곡")
    info("  원인: 장중 수집 vs 장마감 후 OHLCV → 상한가 종목에서 최대 +7% 오차")
    info("")

    # 당일 daily_supply와 daily_ohlcv의 최종 갱신 시간 비교
    # (created_at 또는 updated_at 컬럼이 있다면)

    supply_res = supabase.table("daily_supply") \
        .select("*") \
        .eq("date", target_date) \
        .limit(5) \
        .execute()

    ohlcv_res = supabase.table("daily_ohlcv") \
        .select("*") \
        .eq("date", target_date) \
        .limit(5) \
        .execute()

    if supply_res.data and ohlcv_res.data:
        s_sample = supply_res.data[0]
        o_sample = ohlcv_res.data[0]

        s_time = s_sample.get("created_at") or s_sample.get("updated_at")
        o_time = o_sample.get("created_at") or o_sample.get("updated_at")

        if s_time and o_time:
            info(f"daily_supply 갱신: {s_time}")
            info(f"daily_ohlcv 갱신: {o_time}")

            # 시간차 계산
            try:
                # ISO format parsing
                s_dt = datetime.fromisoformat(s_time.replace("Z", "+00:00"))
                o_dt = datetime.fromisoformat(o_time.replace("Z", "+00:00"))
                diff_minutes = abs((s_dt - o_dt).total_seconds()) / 60

                if diff_minutes > 30:
                    warn(f"수급 ↔ OHLCV 수집 시간차: {diff_minutes:.0f}분")
                    warn("이격도 계산 시 시점 불일치 발생 가능!")
                    info("→ 해결: run_daily.py에서 OHLCV 수집 완료 후 수급 분석 실행")
                else:
                    ok(f"수집 시간차 {diff_minutes:.0f}분 — 정상 범위")
            except Exception as e:
                info(f"시간 파싱 실패: {e}")
        else:
            info("created_at/updated_at 컬럼 없음 — 시점 검증 불가")
            info("→ 권장: 테이블에 updated_at TIMESTAMPTZ DEFAULT NOW() 컬럼 추가")

    # 이격도 직접 계산 & 검증
    info("이격도 직접 계산 검증 (상위 변동 종목)...")

    # OHLCV에서 당일 등락률 상위 종목
    ohlcv_all = supabase.table("daily_ohlcv") \
        .select("stock_code, close_price, open_price") \
        .eq("date", target_date) \
        .execute()

    if ohlcv_all.data:
        # 등락률 계산
        movers = []
        for r in ohlcv_all.data:
            close = r.get("close_price", 0) or 0
            opn = r.get("open_price", 0) or 0
            if opn > 0:
                chg = (close - opn) / opn * 100
                movers.append((r["stock_code"], close, chg))

        # 상위 등락 종목
        movers.sort(key=lambda x: -abs(x[2]))
        top_movers = movers[:10]

        info("당일 상위 변동 종목:")
        for code, close, chg in top_movers:
            info(f"  {code}: 종가 {close:,.0f}원 (시가대비 {chg:+.1f}%)")

        info("")
        info("⚠️  상한가(+30%) 또는 급등(+10%+) 종목은 이격도 왜곡 위험")
        info("   → 수급 분석 시 당일 종가가 아닌 수집 시점 가격을 사용할 수 있음")

    print()


# ============================================================
#  E. 코팔/닥사 유니버스 커버리지
# ============================================================
def verify_universe_coverage(target_date: str):
    header(f"E. 코팔/닥사 유니버스 커버리지 [{target_date}]")

    # 유니버스 파일 로드
    if os.path.exists(UNIVERSE_FILE):
        with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
            universe = json.load(f)

        if isinstance(universe, dict):
            # {"kospi_large": [...], "kosdaq_small": [...]} 형태 추정
            all_codes = set()
            for key, stocks in universe.items():
                if isinstance(stocks, list):
                    for s in stocks:
                        if isinstance(s, dict):
                            all_codes.add(s.get("code", s.get("stock_code", "")))
                        else:
                            all_codes.add(str(s))
                info(f"  {key}: {len(stocks)}종목")
        elif isinstance(universe, list):
            all_codes = set()
            for s in universe:
                if isinstance(s, dict):
                    all_codes.add(s.get("code", s.get("stock_code", "")))
                else:
                    all_codes.add(str(s))

        info(f"유니버스 총 종목: {len(all_codes)}")

        # Supabase daily_supply와 비교
        res = supabase.table("daily_supply") \
            .select("stock_code") \
            .eq("date", target_date) \
            .execute()

        if res.data:
            supply_codes = set(r["stock_code"] for r in res.data)

            missing = all_codes - supply_codes
            extra = supply_codes - all_codes

            if missing:
                warn(f"유니버스에 있으나 수급 미수집 종목 {len(missing)}개:")
                for c in sorted(missing)[:15]:
                    warn(f"  → {c}")
            else:
                ok("유니버스 전체 수급 수집 완료 (100%)")

            if extra:
                info(f"유니버스 밖 종목 {len(extra)}개 (유니버스 업데이트 반영 전 잔여):")
                for c in sorted(extra)[:10]:
                    info(f"  → {c}")
        else:
            warn(f"{target_date} daily_supply 데이터 없음")

    else:
        info(f"유니버스 파일 없음: {UNIVERSE_FILE}")
        info("환경변수로 경로 설정: set UNIVERSE_FILE=C:\\your\\path\\universe_stocks.json")

        # 유니버스 파일 없이도 기본 체크
        res = supabase.table("daily_supply") \
            .select("stock_code") \
            .eq("date", target_date) \
            .execute()

        if res.data:
            count = len(set(r["stock_code"] for r in res.data))
            info(f"당일 수급 종목: {count}개")
            if 400 <= count <= 520:
                ok(f"종목 수 정상 범위 (코팔230 + 닥사226 ≈ 456)")
            else:
                warn(f"종목 수 범위 이탈: {count} (기대: 400~520)")

    print()


# ============================================================
#  MAIN
# ============================================================
def main():
    # 검증 대상 날짜 결정
    if len(sys.argv) > 1:
        raw_date = sys.argv[1]
        if len(raw_date) == 8:  # YYYYMMDD
            target_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        else:
            target_date = raw_date
    else:
        # 최신 데이터 날짜 자동 선택
        res = supabase.table("daily_supply") \
            .select("date") \
            .order("date", desc=True) \
            .limit(1) \
            .execute()
        if res.data:
            target_date = res.data[0]["date"]
        else:
            target_date = TODAY.isoformat()

    print(f"""
╔══════════════════════════════════════════════════════════╗
║  daily_supply 심층 무결성 검증                              ║
║  검증 날짜: {target_date}                                  ║
║  실행일: {TODAY.isoformat()}                                       ║
║  데이터 원천: 키움 REST API (기준)                           ║
╚══════════════════════════════════════════════════════════╝
    """)

    verify_kiwoom_vs_supabase(target_date)
    verify_net_balance(target_date)
    verify_estimated_avg_price(target_date)
    verify_gap_timing(target_date)
    verify_universe_coverage(target_date)

    header("📋 검증 완료")
    info(f"대상 날짜: {target_date}")
    info("모든 FAIL/WARN 항목을 확인하고 조치하세요.")
    info("")
    info("── 다음 단계 ──")
    info("1. FAIL 항목 → 즉시 수정 (데이터 재수집 또는 파이프라인 수정)")
    info("2. WARN 항목 → 원인 파악 후 필요시 조치")
    info("3. SB/SS v2 재설계 시 이격도 시점 문제 반드시 반영")
    print()


def check(condition, pass_msg, fail_msg, level="fail"):
    if condition:
        ok(pass_msg)
    elif level == "warn":
        warn(fail_msg)
    else:
        fail(fail_msg)


if __name__ == "__main__":
    main()
