"""
[*] AI+패스웨이 일일 자동 실행기
================================
매일 장 마감 후 이것 하나만 실행하면:
  1) 키움 REST API -> 수급+시총 수집 -> CSV + Supabase 저장
  2) Supabase에서 읽어 v3 분석 -> 결과 Supabase 저장

사용법:
  python run_daily.py          -> 오늘 날짜
  python run_daily.py 20260319 -> 특정 날짜
"""

import subprocess
import sys
import time
from datetime import datetime

def run(script, args=[]):
    """스크립트 실행"""
    cmd = [sys.executable, script] + args
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0

def main():
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")

    print("=" * 60)
    print("[*] AI+패스웨이 일일 자동 실행기")
    print(f"   날짜: {date_arg}")
    print(f"   시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start = time.time()

    # STEP 1: 데이터 수집
    print("\n[>] STEP 1: 키움 REST API 데이터 수집")
    print("-" * 50)
    if not run("kiwoom_collector_v3.py", [date_arg]):
        print("[X] 데이터 수집 실패. 중단합니다.")
        return

    # STEP 2: 분석 엔진
    print("\n[>] STEP 2: v3 분석 엔진 실행")
    print("-" * 50)
    if not run("kiwoom_analyzer_v1.py", [date_arg]):
        print("[X] 분석 실패.")
        return

    # STEP 2.5: OHLCV 수집 (코팔/닥사 456종목, 최근 10일)
    print("\n[>] STEP 2.5: OHLCV 일봉 수집 (코팔/닥사)")
    print("-" * 50)
    date_formatted = f"{date_arg[:4]}-{date_arg[4:6]}-{date_arg[6:8]}" if len(date_arg) == 8 else date_arg
    if not run("collect_ohlcv.py", [date_formatted]):
        print("[W] OHLCV 수집 실패. 계속 진행합니다.")

    # STEP 2.7: 주도세력 매수선/매도선 계산
    print("\n[>] STEP 2.7: 주도세력 SB/SS 라인 계산")
    print("-" * 50)
    if not run("calc_force_lines.py", [date_arg]):
        print("[W] SB/SS 라인 계산 실패. 계속 진행합니다.")

    # STEP 2.8: 지수 데이터 수집 (KOSPI/KOSDAQ 일봉)
    print("\n[>] STEP 2.8: 지수 데이터 수집 (KOSPI/KOSDAQ)")
    print("-" * 50)
    if not run("collect_index_data.py", [date_arg]):
        print("[W] 지수 데이터 수집 실패. 계속 진행합니다.")

    # STEP 2.9: RS Leaders 계산 (RS First, 수급 Alpha)
    print("\n[>] STEP 2.9: RS Leaders 계산")
    print("-" * 50)
    if not run("calculate_rs_leaders.py", [date_arg]):
        print("[W] RS Leaders 계산 실패. 계속 진행합니다.")

    # STEP 3: 텔레그램 봇 (TOP3 선정)
    print("\n[>] STEP 3: 텔레그램 봇 (TOP3 선정 대기)")
    print("-" * 50)
    run("telegram_bot.py", [date_arg])

    elapsed = time.time() - start

    print("\n" + "=" * 60)
    print(f"[!] 전체 완료! 총 소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    print(f"\n[F] CSV 파일: ./kiwoom_data/")
    print(f"[DB] Supabase DB: daily_supply + daily_market + analysis_scores")
    print(f"[DB] Supabase DB: daily_index + daily_index_returns + rs_leaders")
    print(f"\n[!] 이제 Supabase 대시보드에서 분석 결과를 확인하세요!")
    print("=" * 60)


if __name__ == "__main__":
    main()
