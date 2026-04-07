"""
[*] AI+패스웨이 지수 데이터 수집기 v1.0
==================================================
키움 REST API로 KOSPI/KOSDAQ 일별 지수 데이터 수집 + Supabase 저장
RS Leaders 엔진의 기반 데이터

키움 API 스펙:
  - 업종일봉조회: api-id=ka20006, URL=/api/dostk/chart
  - 업종코드: 001=KOSPI, 101=KOSDAQ
  - 지수 값은 소수점 제거 후 100배 값으로 반환 (252127 → 2521.27)

사용법:
  python collect_index_data.py                    -> 오늘 날짜로 수집
  python collect_index_data.py 20260404           -> 특정 날짜 수집 (base_dt 기준 과거 데이터)
  python collect_index_data.py backfill           -> 1년치 백필 수집

필요 라이브러리:
  pip install requests python-dotenv
"""

import requests
import json
import os
import sys
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# ============================================================
# 설정 (.env 파일에서 읽기)
# ============================================================
APP_KEY = os.getenv("KIWOOM_APP_KEY", "")
SECRET_KEY = os.getenv("KIWOOM_SECRET_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BASE_URL = "https://api.kiwoom.com"

# 지수 코드 매핑 (키움 업종코드)
INDEX_MAP = {
    'KOSPI': '001',   # 코스피 종합
    'KOSDAQ': '101',  # 코스닥 종합
}


# ============================================================
# Supabase 클라이언트 (kiwoom_collector_v3.py와 동일 패턴)
# ============================================================
class SupabaseDB:
    def __init__(self, url, key):
        self.url = url.rstrip('/')
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates"
        }
        self.insert_count = 0

    def upsert(self, table, rows, on_conflict=None):
        """DB에 데이터 삽입/업데이트 (중복 시 업데이트)"""
        if not rows:
            return True
        if on_conflict is None:
            conflict_map = {
                "daily_index": "date,index_code",
                "daily_index_returns": "date,index_code",
            }
            on_conflict = conflict_map.get(table, "")
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            url = f"{self.url}/rest/v1/{table}"
            if on_conflict:
                url += f"?on_conflict={on_conflict}"
            resp = requests.post(url, headers=self.headers, json=batch)
            if resp.status_code in [200, 201, 204]:
                self.insert_count += len(batch)
            else:
                print(f"    [W] DB 오류 ({table}): {resp.status_code} {resp.text[:200]}")
                return False
        return True

    def query(self, table, params=""):
        """DB 조회"""
        url = f"{self.url}/rest/v1/{table}?{params}" if params else f"{self.url}/rest/v1/{table}"
        resp = requests.get(url, headers={
            "apikey": self.headers["apikey"],
            "Authorization": self.headers["Authorization"]
        })
        if resp.status_code == 200:
            return resp.json()
        return []

    def test_connection(self):
        """연결 테스트"""
        resp = requests.get(
            f"{self.url}/rest/v1/daily_index?limit=1",
            headers={"apikey": self.headers["apikey"],
                     "Authorization": self.headers["Authorization"]}
        )
        return resp.status_code == 200


# ============================================================
# 키움 API 클라이언트 (kiwoom_collector_v3.py와 동일 패턴)
# ============================================================
class KiwoomAPI:
    def __init__(self):
        self.token = None
        self.call_count = 0
        self.token_time = None

    def get_token(self):
        try:
            resp = requests.post(
                f"{BASE_URL}/oauth2/token",
                headers={"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"},
                json={"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": SECRET_KEY},
                timeout=10
            )
            if resp.status_code != 200 or not resp.text.strip():
                print(f"[X] 키움 토큰 실패: HTTP {resp.status_code}")
                return False
            data = resp.json()
            if data.get('return_code') == 0:
                self.token = data['token']
                self.token_time = time.time()
                return True
            print(f"[X] 키움 토큰 실패: {data.get('return_msg')}")
            return False
        except Exception as e:
            print(f"[X] 키움 토큰 에러: {e}")
            return False

    def refresh_token_if_needed(self):
        """토큰 발급 후 20분 경과 시 자동 갱신"""
        if self.token_time and (time.time() - self.token_time) > 1200:
            print("  [R] 토큰 갱신 중...", end=" ", flush=True)
            if self.get_token():
                print("[OK]")
                return True
            else:
                print("[X] 재시도...")
                time.sleep(5)
                return self.get_token()
        return True

    def call(self, api_id, url_path, body):
        self.refresh_token_if_needed()
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": api_id,
            "authorization": f"Bearer {self.token}"
        }
        try:
            resp = requests.post(f"{BASE_URL}{url_path}", headers=headers, json=body, timeout=15)
            self.call_count += 1
            return resp.json(), resp.headers
        except Exception as e:
            print(f"  [W] API 호출 에러: {e}")
            return {"return_code": -1}, {}

    def call_paged(self, api_id, url_path, body, max_pages=10):
        """페이지네이션 지원 호출 (cont-yn/next-key)"""
        all_items = []
        cont_yn, next_key = "", ""
        for page in range(max_pages):
            self.refresh_token_if_needed()
            headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "api-id": api_id,
                "authorization": f"Bearer {self.token}"
            }
            if cont_yn == "Y":
                headers["cont-yn"] = cont_yn
                headers["next-key"] = next_key
            resp = requests.post(f"{BASE_URL}{url_path}", headers=headers, json=body, timeout=15)
            self.call_count += 1
            data = resp.json()
            if data.get('return_code') != 0:
                break
            # 리스트 데이터 추출
            for key, val in data.items():
                if isinstance(val, list):
                    all_items.extend(val)
                    break
            cont_yn = resp.headers.get('cont-yn', 'N')
            next_key = resp.headers.get('next-key', '')
            if cont_yn != 'Y':
                break
            time.sleep(0.3)
        return all_items


# ============================================================
# 지수값 변환 (100배 보정)
# 키움 API는 지수를 소수점 제거 후 100배로 반환
# 예: 2521.27 → "252127"
# ============================================================
def index_to_real(val_str):
    """키움 지수 값(100배) → 실제 지수 값"""
    try:
        raw = float(str(val_str).replace(',', '').replace('+', '').strip())
        return round(raw / 100, 2)
    except:
        return 0.0

def to_int(s):
    try:
        return int(str(s).replace(',', '').replace('+', '').replace('-', '').strip())
    except:
        return 0


# ============================================================
# 지수 일봉 데이터 수집 (ka20006)
# ============================================================
def collect_index_daily(api, db, base_date):
    """
    키움 업종일봉조회 API (ka20006)로 KOSPI/KOSDAQ 지수 데이터 수집

    API 스펙:
      api-id: ka20006
      URL: /api/dostk/chart
      Body: {"inds_cd": "001", "base_dt": "20260404"}
      Response: inds_dt_pole_qry 리스트
        - cur_prc: 종가 (100배)
        - open_pric: 시가 (100배)
        - high_pric: 고가 (100배)
        - low_pric: 저가 (100배)
        - trde_qty: 거래량
        - trde_prica: 거래대금
        - dt: 일자 (YYYYMMDD)

    ※ base_dt 기준으로 과거 데이터를 내림차순으로 반환
    ※ 페이지네이션(cont-yn/next-key) 지원
    """
    db_rows = []

    print(f"\n  [>] 지수 일봉 수집 (base_dt: {base_date})")

    for index_name, inds_cd in INDEX_MAP.items():
        print(f"    {index_name} (업종코드: {inds_cd})...", end=" ", flush=True)

        # 키움 업종일봉 API 호출 (페이지네이션으로 충분한 데이터 확보)
        items = api.call_paged(
            "ka20006",           # api-id: 업종일봉조회요청
            "/api/dostk/chart",  # URL
            {
                "inds_cd": inds_cd,
                "base_dt": base_date,
            },
            max_pages=20  # 약 400~600일치 (24/10/18부터 커버)
        )

        if not items:
            print(f"[X] 데이터 없음")
            continue

        count = 0
        for item in items:
            raw_date = item.get('dt', '')
            if not raw_date or len(raw_date) != 8:
                continue

            # YYYYMMDD → YYYY-MM-DD
            fmt_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

            close_val = index_to_real(item.get('cur_prc', '0'))
            if close_val == 0:
                continue

            open_val = index_to_real(item.get('open_pric', '0'))
            high_val = index_to_real(item.get('high_pric', '0'))
            low_val = index_to_real(item.get('low_pric', '0'))
            volume_val = to_int(item.get('trde_qty', '0'))
            trade_value = to_int(item.get('trde_prica', '0'))

            row = {
                "date": fmt_date,
                "index_code": index_name,
                "open": open_val if open_val else None,
                "high": high_val if high_val else None,
                "low": low_val if low_val else None,
                "close": close_val,
                "volume": volume_val if volume_val else None,
                "trade_value": trade_value if trade_value else None,
                "change_pct": None,  # 전일 종가 대비 등락률은 수익률 계산 단계에서 채움
            }
            db_rows.append(row)
            count += 1

        # 가장 최근(첫 번째) 데이터 출력
        if items:
            latest_close = index_to_real(items[0].get('cur_prc', '0'))
            latest_date = items[0].get('dt', '')
            print(f"[OK] {count}건 | 최신: {latest_date} 종가: {latest_close:,.2f}")
        else:
            print(f"[OK] {count}건")

        time.sleep(0.5)

    # DB 저장
    if db and db_rows:
        db.upsert("daily_index", db_rows)
        print(f"  [DB] daily_index 저장: {len(db_rows)}건")

    return db_rows


# ============================================================
# 지수 기간별 수익률 계산 + daily_index_returns 저장
# ============================================================
PERIODS = [1, 5, 10, 20, 40, 60, 120, 200]
PERIOD_COLS = {p: f"return_{p}d" for p in PERIODS}

def calculate_index_returns(db, target_date):
    """
    daily_index 테이블에서 지수 종가를 읽어
    기간별 수익률을 계산 → daily_index_returns에 저장
    """
    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    result_rows = []

    for index_code in ['KOSPI', 'KOSDAQ']:
        print(f"  [>] {index_code} 수익률 계산...", end=" ", flush=True)

        # Supabase에서 해당 지수의 최근 250일 데이터 조회
        params = (
            f"index_code=eq.{index_code}"
            f"&date=lte.{date_obj}"
            f"&order=date.desc"
            f"&limit=250"
            f"&select=date,close"
        )
        rows = db.query("daily_index", params)

        if not rows or len(rows) == 0:
            print(f"[X] 데이터 없음")
            continue

        # 날짜순 정렬 (오래된 → 최신)
        rows.sort(key=lambda r: r['date'])

        latest = rows[-1]
        date_obj_actual = latest['date']
        today_close = float(latest['close'])

        return_row = {
            "date": date_obj_actual,
            "index_code": index_code,
        }

        for period in PERIODS:
            col = PERIOD_COLS[period]
            if len(rows) > period:
                past_close = float(rows[-(period + 1)]['close'])
                if past_close > 0:
                    ret = (today_close / past_close - 1) * 100
                    return_row[col] = round(ret, 4)
                else:
                    return_row[col] = None
            else:
                return_row[col] = None

        result_rows.append(return_row)

        # 결과 출력
        r1 = return_row.get('return_1d')
        r20 = return_row.get('return_20d')
        r60 = return_row.get('return_60d')
        r200 = return_row.get('return_200d')
        print(f"[OK] 1D:{r1 if r1 is not None else '-'}% | "
              f"20D:{r20 if r20 is not None else '-'}% | "
              f"60D:{r60 if r60 is not None else '-'}% | "
              f"200D:{r200 if r200 is not None else '-'}%")

    # DB 저장
    if db and result_rows:
        db.upsert("daily_index_returns", result_rows)
        print(f"  [DB] daily_index_returns 저장: {len(result_rows)}건")

    return result_rows


# ============================================================
# 메인
# ============================================================
def main():
    # 인자 파싱
    if len(sys.argv) == 2 and sys.argv[1] == "backfill":
        # 백필 모드: 오늘 기준으로 1년치+여유 수집
        target_date = datetime.now().strftime("%Y%m%d")
        mode = "backfill"
    elif len(sys.argv) == 2:
        # 단일 날짜 기준: python collect_index_data.py 20260404
        target_date = sys.argv[1]
        mode = "single"
    else:
        # 오늘 날짜
        target_date = datetime.now().strftime("%Y%m%d")
        mode = "single"

    print("=" * 60)
    print("[*] AI+패스웨이 지수 데이터 수집기 v1.0")
    print(f"   모드: {'백필 (1년+)' if mode == 'backfill' else '일일'}")
    print(f"   기준일: {target_date}")
    print(f"   지수: KOSPI(001), KOSDAQ(101)")
    print(f"   API: ka20006 (업종일봉조회) → /api/dostk/chart")
    print("=" * 60)

    # 설정 확인
    if not APP_KEY or not SECRET_KEY:
        print("[X] .env 파일에 KIWOOM_APP_KEY, KIWOOM_SECRET_KEY를 설정하세요.")
        return

    # Supabase 연결
    db = None
    if SUPABASE_URL and SUPABASE_KEY:
        db = SupabaseDB(SUPABASE_URL, SUPABASE_KEY)
        print("\n[>] Supabase 연결 테스트...", end=" ")
        if db.test_connection():
            print("[OK] 성공")
        else:
            print("[X] 연결 실패")
            return
    else:
        print("\n[X] SUPABASE_URL, SUPABASE_KEY를 설정하세요.")
        return

    # 키움 토큰
    api = KiwoomAPI()
    print("[>] 키움 토큰 발급...", end=" ")
    if not api.get_token():
        return
    print("[OK] 성공")

    start_total = time.time()

    # STEP 1: 지수 일봉 데이터 수집
    # ka20006은 base_dt 기준 과거 데이터를 한번에 반환
    # → single이든 backfill이든 같은 함수로 처리
    # backfill은 페이지를 더 많이 넘김
    print(f"\n{'-'*50}")
    print(f"[>] STEP 1: 지수 일봉 데이터 수집")
    print(f"{'-'*50}")

    index_rows = collect_index_daily(api, db, target_date)

    # STEP 2: 기간별 수익률 계산
    if index_rows:
        print(f"\n{'-'*50}")
        print(f"[>] STEP 2: 지수 기간별 수익률 계산")
        print(f"{'-'*50}")
        calculate_index_returns(db, target_date)

    # 완료
    total_time = time.time() - start_total
    print(f"\n{'='*60}")
    print(f"[OK] 지수 수집 완료!")
    print(f"   총 소요시간: {total_time:.1f}초")
    print(f"   키움 API 호출: {api.call_count}회")
    if db:
        print(f"   DB 저장: {db.insert_count}건")
    print("=" * 60)


if __name__ == "__main__":
    main()
