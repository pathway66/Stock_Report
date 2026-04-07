"""
[*] run_daily.py RS Leaders 연동 패치
==================================================
기존 run_daily.py에 추가할 RS Leaders 파이프라인 코드

사용법:
  기존 run_daily.py의 STEP 2 (시가총액) 이후에
  아래 코드를 삽입하면 됩니다.

또는 독립 실행:
  python run_daily_rs_patch.py 20260404
==================================================
"""

import sys
import time
import subprocess
from datetime import datetime


def run_rs_pipeline(target_date):
    """
    RS Leaders 파이프라인 실행
    기존 수집(STEP 1: 수급, STEP 2: 시총) 완료 후 호출
    """
    print(f"\n{'='*60}")
    print(f"[>] RS Leaders 파이프라인 시작")
    print(f"{'='*60}")

    start = time.time()

    # ────────────────────────────────────────────
    # STEP 3: 지수 데이터 수집
    # ────────────────────────────────────────────
    print(f"\n[>] STEP 3: 지수 데이터 수집 (KOSPI/KOSDAQ)")
    try:
        result = subprocess.run(
            [sys.executable, "collect_index_data.py", target_date],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
            print("[OK] 지수 수집 완료")
        else:
            print(f"[X] 지수 수집 실패:")
            print(result.stderr[-300:] if result.stderr else "에러 없음")
            # 지수 수집 실패해도 기존 데이터로 RS 계산 시도
            print("[W] 기존 지수 데이터로 RS 계산 시도...")
    except subprocess.TimeoutExpired:
        print("[X] 지수 수집 타임아웃 (120초)")
    except Exception as e:
        print(f"[X] 지수 수집 에러: {e}")

    # ────────────────────────────────────────────
    # STEP 4: RS Leaders 계산
    # ────────────────────────────────────────────
    print(f"\n[>] STEP 4: RS Leaders 계산")
    try:
        result = subprocess.run(
            [sys.executable, "calculate_rs_leaders.py", target_date],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            print(result.stdout[-1000:] if len(result.stdout) > 1000 else result.stdout)
            print("[OK] RS Leaders 계산 완료")
        else:
            print(f"[X] RS Leaders 계산 실패:")
            print(result.stderr[-300:] if result.stderr else "에러 없음")
    except subprocess.TimeoutExpired:
        print("[X] RS Leaders 계산 타임아웃 (300초)")
    except Exception as e:
        print(f"[X] RS Leaders 계산 에러: {e}")

    elapsed = time.time() - start
    print(f"\n[OK] RS 파이프라인 완료 ({elapsed:.1f}초)")
    print(f"{'='*60}")


# ============================================================
# run_daily.py에 삽입할 코드 (복붙용)
# ============================================================
PATCH_CODE = """
# ============================================================
# ★ RS Leaders 파이프라인 (STEP 2 시총 수집 이후에 추가)
# ============================================================
# STEP 3: 지수 데이터 수집
print(f"\\n[>] STEP 3: 지수 데이터 수집")
try:
    from collect_index_data import main as collect_index_main
    # sys.argv 임시 조작
    original_argv = sys.argv
    sys.argv = ['collect_index_data.py', target_date]
    collect_index_main()
    sys.argv = original_argv
    print("[OK] 지수 수집 완료")
except Exception as e:
    print(f"[W] 지수 수집 실패: {e} (기존 데이터로 RS 계산 시도)")

# STEP 4: RS Leaders 계산
print(f"\\n[>] STEP 4: RS Leaders 계산")
try:
    from calculate_rs_leaders import main as rs_main
    original_argv = sys.argv
    sys.argv = ['calculate_rs_leaders.py', target_date]
    rs_main()
    sys.argv = original_argv
    print("[OK] RS Leaders 완료")
except Exception as e:
    print(f"[X] RS Leaders 실패: {e}")
"""


def main():
    """독립 실행 모드"""
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime("%Y%m%d")

    run_rs_pipeline(target_date)

    # 통합 안내
    print(f"\n{'─'*60}")
    print("[INFO] run_daily.py 통합 방법:")
    print("  1. 위 PATCH_CODE를 run_daily.py의 STEP 2 이후에 삽입")
    print("  2. 또는 run_daily.py 끝에 다음 한 줄 추가:")
    print("     from run_daily_rs_patch import run_rs_pipeline")
    print("     run_rs_pipeline(target_date)")
    print(f"{'─'*60}")


if __name__ == "__main__":
    main()
