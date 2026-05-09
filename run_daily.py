"""
[*] AI+패스웨이 일일 자동 실행기 v2.0
=====================================
매일 장 마감 후 이것 하나만 실행하면 모든 데이터 수집+분석 완료.

파이프라인:
  1) collect_daily_all.py        -> 전종목 수급+OHLCV+시총+52주고저 -> daily_supply_v2
  2) collect_index_data.py       -> 지수 OHLCV+리턴 -> index_supply + daily_index_returns
  3) calculate_rs_leaders.py     -> RS 8기간 랭킹 -> rs_leaders
  4) backfill_guru_signals.py    -> 9개 그루 패턴 시그널 -> guru_signals
  5) backfill_volume_indicators.py -> CMF/AD/MFI 3대 지표 -> daily_supply_v2
  6) generate_daily_report.py    -> AI 일일 시장 리포트
  7) crawl_research_v2.py        -> 한경 컨센서스 리포트 크롤링 -> research_reports
  8) generate_research_ai_summary.py -> 리포트 AI 요약
  9) (비활성) generate_daily_briefs.py
 10) /api/revalidate             -> 사이트 캐시 새로고침

사용법:
  python run_daily.py          -> 오늘 날짜
  python run_daily.py 20250415 -> 특정 날짜
"""

import subprocess
import sys
import os
import time
from datetime import datetime

# 텔레그램 알림 (선택적 — 환경변수 미설정 시 조용히 스킵)
try:
    from telegram_bot import send_telegram as _send_telegram
except Exception:
    _send_telegram = None


def notify(msg: str):
    """텔레그램 알림. 실패해도 메인 파이프라인 영향 없음."""
    if _send_telegram is None:
        return
    try:
        _send_telegram(msg)
    except Exception as e:
        print(f"  [W] 텔레그램 알림 실패: {e}")


def fmt_elapsed(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h > 0:
        return f"{h}시간 {m}분 {sec}초"
    if m > 0:
        return f"{m}분 {sec}초"
    return f"{sec}초"


# 한국 증시 휴장일 (공휴일 + 임시휴장일)
# 업데이트 필요 시: https://open.krx.co.kr/
KRX_HOLIDAYS = {
    # 2026
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-02-18",
    "2026-03-01", "2026-03-02", "2026-05-01", "2026-05-05", "2026-05-22",
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
    """스크립트 실행. 자식 stdout/stderr를 라인 단위로 부모 로그(TeeLogger)에 흘려
    실시간 진행률 + 실패 시 stderr까지 모두 logs/run_daily_*.log에 기록.

    자식을 -u(unbuffered) 플래그 + PYTHONUNBUFFERED=1 로 띄워야 자식 print()가
    pipe를 통해 즉시 흘러나옴 (없으면 자식 stdout이 fully-buffered가 됨).
    critical=True면 실패 시 중단."""
    cmd = [sys.executable, "-u", script] + args
    print(f"  >> {script} {' '.join(args)}", flush=True)
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding='utf-8', errors='replace', bufsize=1, env=env,
    )
    if proc.stdout is not None:
        for line in proc.stdout:
            print(line, end='', flush=True)
    proc.wait()
    ok = proc.returncode == 0
    if not ok and critical:
        print(f"  [X] {script} 실패! (exit code: {proc.returncode})")
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

    iso_date = f"{date_arg[:4]}-{date_arg[4:6]}-{date_arg[6:8]}"

    # 증시 영업일 체크 (주말/공휴일은 스킵)
    if not is_trading_day(date_arg):
        print("=" * 60)
        print(f"[SKIP] {date_arg}는 증시 휴장일입니다. 수집 건너뜀.")
        print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        notify(f"💤 {iso_date} 휴장일 — 수집 건너뜀")
        return

    print("=" * 60)
    print("[*] AI+패스웨이 일일 자동 실행기 v2.0")
    print(f"   날짜: {date_arg}")
    print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start = time.time()
    errors = []

    notify(f"🚀 AI패스웨이 일일 수집 시작\n📅 {iso_date} {datetime.now().strftime('%H:%M:%S')}")

    # STEP 1: 전종목 수급+OHLCV+시총+52주고저 통합 수집
    print("\n[>] STEP 1: 전종목 통합 수집 (수급+OHLCV+시총+52주)")
    print("-" * 50)
    if not run("collect_daily_all.py", [date_arg]):
        errors.append("STEP 1: 전종목 수집 실패")
        print("[X] 핵심 데이터 수집 실패. 중단합니다.")
        elapsed_fail = time.time() - start
        notify(
            f"❌ AI패스웨이 일일 수집 실패\n"
            f"📅 {iso_date} {datetime.now().strftime('%H:%M:%S')}\n"
            f"⛔ STEP 1: 전종목 수집 실패 — 파이프라인 중단됨\n"
            f"⏱ 소요 {fmt_elapsed(elapsed_fail)} (실패 시점)"
        )
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

    # STEP 4: 9개 그루 패턴 시그널 갱신 (Darvas/Minervini/O'Neil/Wyckoff/Livermore/Weinstein)
    print("\n[>] STEP 4: 9개 그루 패턴 시그널 갱신")
    print("-" * 50)
    if not run("backfill_guru_signals.py", ["--date", date_arg], critical=False):
        errors.append("STEP 4: 그루 시그널 갱신 실패 (비핵심)")

    # STEP 5: 거래량/거래대금 3대 지표 갱신 (CMF / A-D Line / MFI)
    print("\n[>] STEP 5: 거래량 지표 갱신 (CMF / A-D / MFI)")
    print("-" * 50)
    if not run("backfill_volume_indicators.py", ["--date", date_arg], critical=False):
        errors.append("STEP 5: 거래량 지표 갱신 실패 (비핵심)")

    # STEP 6: AI 일일 리포트 생성 (Claude API)
    print("\n[>] STEP 6: AI 일일 리포트 생성")
    print("-" * 50)
    if not run("generate_daily_report.py", [date_arg], critical=False):
        errors.append("STEP 6: AI 리포트 생성 실패 (비핵심)")

    # STEP 7: 한경 컨센서스 리포트 크롤링 (Playwright v2: 일 100~200건)
    print("\n[>] STEP 7: 한경 컨센서스 리포트 크롤링 (Playwright)")
    print("-" * 50)
    if not run("crawl_research_v2.py", [], critical=False):
        errors.append("STEP 7: 리포트 크롤링 실패 (비핵심)")

    # STEP 8: 리포트 AI 요약 생성 (오늘 발행분만)
    print("\n[>] STEP 8: 리포트 AI 요약 생성")
    print("-" * 50)
    if not run("generate_research_ai_summary.py", [date_arg], critical=False):
        errors.append("STEP 8: 리포트 AI 요약 실패 (비핵심)")

    # STEP 9: 슈퍼시그널 종목별 일일 AI 리포트 — [비활성화]
    print("\n[>] STEP 9: 슈퍼시그널 일일 리포트 [비활성화 — 메뉴 정리됨]")
    print("-" * 50)

    # STEP 10: Next.js 사이트 캐시 revalidate
    print("\n[>] STEP 10: 웹사이트 캐시 새로고침")
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

    # 텔레그램 종료 알림
    finish_ts = datetime.now().strftime('%H:%M:%S')
    if errors:
        warn_lines = "\n".join(f"❗ {e}" for e in errors)
        notify(
            f"⚠️ AI패스웨이 일일 수집 완료 (경고 {len(errors)}건)\n"
            f"📅 {iso_date} {finish_ts}\n"
            f"⏱ 소요 {fmt_elapsed(elapsed)}\n"
            f"{warn_lines}"
        )
    else:
        notify(
            f"✅ AI패스웨이 일일 수집 완료\n"
            f"📅 {iso_date} {finish_ts}\n"
            f"⏱ 소요 {fmt_elapsed(elapsed)}\n"
            f"📊 daily_supply_v2 / rs_leaders / research_reports 갱신"
        )


if __name__ == "__main__":
    main()
