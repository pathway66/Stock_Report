"""
🔮 AI+패스웨이 과거 데이터 소급 수집
================================
6개월치 데이터를 한 번에 수집+분석+DB저장

사용법:
  python run_backfill.py              → 25/9/22 ~ 어제까지
  python run_backfill.py 20260101     → 26/1/2 ~ 어제까지
  python run_backfill.py 20250922 20260319  → 특정 기간

예상 소요: 영업일 1일당 약 3.5분
  3개월(~65일) → 약 3.8시간
  6개월(~120일) → 약 7시간
"""

import subprocess
import sys
import time
from datetime import datetime, timedelta

def get_business_days(start_str, end_str):
    """영업일 리스트 생성 (주말 제외, 공휴일은 미포함)"""
    start = datetime.strptime(start_str, "%Y%m%d")
    end = datetime.strptime(end_str, "%Y%m%d")
    
    # 한국 공휴일 (25년 하반기 ~ 26년 상반기)
    holidays = {
        '20250926',  # 추석연휴
        '20250929',  # 추석연휴
        '20250930',  # 추석연휴
        '20251001',  # 추석대체
        '20251003',  # 개천절
        '20251006',  # 대체공휴일
        '20251009',  # 한글날
        '20251225',  # 크리스마스
        '20251231',  # 연말
        '20260101',  # 신정
        '20260102',  # 신정대체(임시공휴일 가능)
        '20260126',  # 설연휴
        '20260127',  # 설연휴
        '20260128',  # 설연휴
        '20260129',  # 설대체
        '20260301',  # 삼일절
        '20260302',  # 삼일절대체
        '20260505',  # 어린이날
        '20260525',  # 부처님오신날(추정)
        '20260606',  # 현충일
    }
    
    days = []
    current = start
    while current <= end:
        ds = current.strftime("%Y%m%d")
        if current.weekday() < 5 and ds not in holidays:
            days.append(ds)
        current += timedelta(days=1)
    return days


def main():
    # 기본값: 25/9/22 ~ 어제
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    elif len(sys.argv) == 2:
        start_date = sys.argv[1]
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    else:
        start_date = "20250922"
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    days = get_business_days(start_date, end_date)

    print("=" * 60)
    print("🔮 AI+패스웨이 과거 데이터 소급 수집")
    print(f"   기간: {start_date} ~ {end_date}")
    print(f"   영업일: {len(days)}일")
    print(f"   예상 소요: {len(days) * 3.5 / 60:.1f}시간")
    print(f"   시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    success = 0
    fail = 0
    total_start = time.time()

    for i, day in enumerate(days):
        elapsed = time.time() - total_start
        remaining = (elapsed / max(i, 1)) * (len(days) - i)
        
        print(f"\n{'─'*50}")
        print(f"📅 [{i+1}/{len(days)}] {day} (남은 약 {remaining/60:.0f}분)")
        print(f"{'─'*50}")

        # 수집
        result = subprocess.run(
            [sys.executable, "kiwoom_collector_v3.py", day],
            capture_output=False
        )
        if result.returncode != 0:
            print(f"  ⚠️ 수집 실패: {day}")
            fail += 1
            continue

        # 분석
        result = subprocess.run(
            [sys.executable, "kiwoom_analyzer_v1.py", day],
            capture_output=False
        )
        if result.returncode != 0:
            print(f"  ⚠️ 분석 실패: {day}")
        
        success += 1

        # 진행률
        pct = (i + 1) / len(days) * 100
        print(f"\n  ✅ {day} 완료 ({pct:.0f}%, 성공:{success} 실패:{fail})")

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print(f"🎉 소급 수집 완료!")
    print(f"   총 소요: {total_elapsed/3600:.1f}시간 ({total_elapsed/60:.0f}분)")
    print(f"   성공: {success}일 / 실패: {fail}일")
    print(f"   종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
