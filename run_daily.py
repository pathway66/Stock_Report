"""
[*] AI+패스웨이 일일 자동 실행기 v2.0
=====================================
매일 장 마감 후 이것 하나만 실행하면 모든 데이터 수집+분석 완료.

파이프라인:
  1) collect_daily_all.py   -> 전종목 수급+OHLCV+시총+52주고저 -> daily_supply_v2
  2) collect_index_data.py  -> 지수 OHLCV+리턴 -> index_supply + daily_index_returns
  3) calculate_rs_leaders.py -> RS 8기간 랭킹 -> rs_leaders
  4) calc_force_lines.py    -> 주도세력 SB/SS 라인 -> force_buy_sell_lines

사용법:
  python run_daily.py          -> 오늘 날짜
  python run_daily.py 20250415 -> 특정 날짜
"""

import subprocess
import sys
import os
import time
from datetime import datetime


# 한국 증시 휴장일 (공휴일 + 임시휴장일)
# 업데이트 필요 시: https://open.krx.co.kr/
KRX_HOLIDAYS = {
    # 2026
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-02-18",
    "2026-03-01", "2026-03-02", "2026-05-05", "2026-05-22",
    "2026-06-03", "2026-06-06", "2026-08-15", "2026-08-17",
    "2026-09-24", "2026-09-25", "2026-09-28", "2026-09-29",
    "2026-10-03", "2026-10-05", "2026-10-09", "2026-12-25",
    "2026-12-31",
    # 2027 (미리 일부만)
    "2027-01-01",
}


def is_trading_day(date_str):
    """YYYYMMDD → 한국 증시 영업일 여부"""
    try:
        d = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return False
    # 주말 제외
    if d.weekday() >= 5:  # 5=토, 6=일
        return False
    # 공휴일 제외
    iso = d.strftime("%Y-%m-%d")
    if iso in KRX_HOLIDAYS:
        return False
    return True


# 로그 파일 (스케줄러 실행 시 콘솔 없으므로 파일에 기록)
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)


class TeeLogger:
    """stdout을 파일과 콘솔 양쪽에 기록"""
    def __init__(self, log_path):
        self.log = open(log_path, 'w', encoding='utf-8')
        self.stdout = sys.stdout
    def write(self, msg):
        self.log.write(msg); self.log.flush()
        try: self.stdout.write(msg)
        except: pass
    def flush(self):
        self.log.flush()
        try: self.stdout.flush()
        except: pass


def run(script, args=[], critical=True):
    """스크립트 실행. critical=True면 실패 시 중단."""
    cmd = [sys.executable, script] + args
    print(f"  >> {script} {' '.join(args)}")
    result = subprocess.run(cmd, capture_output=False)
    ok = result.returncode == 0
    if not ok and critical:
        print(f"  [X] {script} 실패! (exit code: {result.returncode})")
    return ok


def revalidate_site():
    """Next.js 사이트 캐시 새로고침 (/api/revalidate 호출)"""
    try:
        import requests
        from dotenv import load_dotenv
        load_dotenv()
        site_url = os.getenv("SITE_URL", "https://ai-pathway-web.vercel.app")
        secret = os.getenv("REVALIDATE_SECRET", "")
        if not secret:
            print("  [W] REVALIDATE_SECRET 미설정 — 스킵")
            return False
        resp = requests.post(
            f"{site_url}/api/revalidate",
            headers={"Authorization": f"Bearer {secret}"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"  [OK] revalidate 완료: {len(data.get('revalidated', []))}개 경로")
            return True
        else:
            print(f"  [W] revalidate 실패: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"  [W] revalidate 오류: {e}")
        return False


def main():
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")

    # 로그 파일 설정 (스케줄러에서도 확인 가능)
    log_path = os.path.join(LOG_DIR, f"run_daily_{date_arg}.log")
    sys.stdout = TeeLogger(log_path)

    # 증시 영업일 체크 (주말/공휴일은 스킵)
    if not is_trading_day(date_arg):
        print("=" * 60)
        print(f"[SKIP] {date_arg}는 증시 휴장일입니다. 수집 건너뜀.")
        print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        return

    print("=" * 60)
    print("[*] AI+패스웨이 일일 자동 실행기 v2.0")
    print(f"   날짜: {date_arg}")
    print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start = time.time()
    errors = []

    # STEP 1: 전종목 수급+OHLCV+시총+52주고저 통합 수집
    print("\n[>] STEP 1: 전종목 통합 수집 (수급+OHLCV+시총+52주)")
    print("-" * 50)
    if not run("collect_daily_all.py", [date_arg]):
        errors.append("STEP 1: 전종목 수집 실패")
        print("[X] 핵심 데이터 수집 실패. 중단합니다.")
        return

    # STEP 2: 지수 데이터 수집 (KOSPI/KOSDAQ)
    print("\n[>] STEP 2: 지수 데이터 수집 (KOSPI/KOSDAQ)")
    print("-" * 50)
    if not run("collect_index_data.py", [date_arg]):
        errors.append("STEP 2: 지수 수집 실패")

    # STEP 3: RS Leaders 계산
    print("\n[>] STEP 3: RS Leaders 계산")
    print("-" * 50)
    if not run("calculate_rs_leaders.py", [date_arg]):
        errors.append("STEP 3: RS Leaders 실패")

    # STEP 4: 주도세력 SB/SS 라인 계산
    print("\n[>] STEP 4: 주도세력 SB/SS 라인 계산")
    print("-" * 50)
    if not run("calc_force_lines.py", [date_arg], critical=False):
        errors.append("STEP 4: SB/SS 라인 실패 (비핵심)")

    # STEP 5: AI 일일 리포트 생성 (Claude API)
    print("\n[>] STEP 5: AI 일일 리포트 생성")
    print("-" * 50)
    if not run("generate_daily_report.py", [date_arg], critical=False):
        errors.append("STEP 5: AI 리포트 생성 실패 (비핵심)")

    # STEP 6: 한경 컨센서스 기업 리포트 크롤링
    print("\n[>] STEP 6: 한경 컨센서스 리포트 크롤링")
    print("-" * 50)
    if not run("crawl_research_reports.py", [], critical=False):
        errors.append("STEP 6: 리포트 크롤링 실패 (비핵심)")

    # STEP 7: 리포트 AI 요약 생성 (오늘 발행분만)
    print("\n[>] STEP 7: 리포트 AI 요약 생성")
    print("-" * 50)
    if not run("generate_research_ai_summary.py", [date_arg], critical=False):
        errors.append("STEP 7: 리포트 AI 요약 실패 (비핵심)")

    # STEP 8: Next.js 사이트 캐시 revalidate
    print("\n[>] STEP 8: 웹사이트 캐시 새로고침")
    print("-" * 50)
    revalidate_site()

    elapsed = time.time() - start

    print("\n" + "=" * 60)
    if errors:
        print(f"[!] 완료 (경고 {len(errors)}건)")
        for e in errors:
            print(f"  [W] {e}")
    else:
        print(f"[OK] 전체 완료! 에러 없음")
    print(f"   소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    print(f"   DB: daily_supply_v2, daily_index, rs_leaders")
    print("=" * 60)


if __name__ == "__main__":
    main()
