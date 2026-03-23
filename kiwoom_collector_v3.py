"""
🔮 AI+패스웨이 키움 REST API 자동수집기 v3.0
==================================================
v2.0 + Supabase DB 자동 저장
수급 10개 + 시총 1개 = 11개 CSV 파일 생성 + DB 동시 저장

사용법:
  python kiwoom_collector_v3.py          → 오늘 날짜로 수집
  python kiwoom_collector_v3.py 20260319 → 특정 날짜 수집

필요 라이브러리:
  pip install requests python-dotenv supabase
"""

import requests
import json
import csv
import os
import sys
import time
from datetime import datetime
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
MKTCAP_MODE = "supply"  # "supply": 수급종목만 / "full": 전종목

OUTPUT_DIR = "./kiwoom_data"

INVESTORS = {
    '외국인': '9000',
    '연기금': '6000',
    '투신': '3000',
    '사모펀드': '3100',
    '기타법인': '7100',
}

MARKETS = {'코스피': '001', '코스닥': '101'}

ETF_KW = ['KODEX','TIGER','ACE','RISE','PLUS','SOL','HANARO','KIWOOM',
           'KoAct','TIME','ETN','KOSEF','메리츠','삼성증권']


def is_etf(name):
    return any(kw in name for kw in ETF_KW)


# ============================================================
# Supabase 클라이언트
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
            return
        # 테이블별 기본 on_conflict 키
        if on_conflict is None:
            conflict_map = {
                "daily_supply": "date,stock_code,subject,direction",
                "daily_market": "date,stock_code",
                "analysis_scores": "date,stock_code",
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
                print(f"    ⚠️ DB 오류 ({table}): {resp.status_code} {resp.text[:200]}")

    def test_connection(self):
        """연결 테스트"""
        resp = requests.get(
            f"{self.url}/rest/v1/daily_supply?limit=1",
            headers={"apikey": self.headers["apikey"],
                     "Authorization": self.headers["Authorization"]}
        )
        return resp.status_code == 200


# ============================================================
# 키움 API 클라이언트
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
                print(f"❌ 키움 토큰 실패: HTTP {resp.status_code}")
                return False
            data = resp.json()
            if data.get('return_code') == 0:
                self.token = data['token']
                self.token_time = time.time()
                return True
            print(f"❌ 키움 토큰 실패: {data.get('return_msg')}")
            return False
        except Exception as e:
            print(f"❌ 키움 토큰 에러: {e}")
            return False

    def refresh_token_if_needed(self):
        """토큰 발급 후 20분 경과 시 자동 갱신"""
        if self.token_time and (time.time() - self.token_time) > 1200:
            print("  🔄 토큰 갱신 중...", end=" ", flush=True)
            if self.get_token():
                print("✅")
                return True
            else:
                print("❌ 재시도...")
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
            print(f"  ⚠️ API 호출 에러: {e}")
            return {"return_code": -1}, {}

    def call_paged(self, api_id, url_path, body, max_pages=5):
        all_items = []
        cont_yn, next_key = "", ""
        for _ in range(max_pages):
            headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "api-id": api_id,
                "authorization": f"Bearer {self.token}"
            }
            if cont_yn == "Y":
                headers["cont-yn"] = cont_yn
                headers["next-key"] = next_key
            resp = requests.post(f"{BASE_URL}{url_path}", headers=headers, json=body)
            self.call_count += 1
            data = resp.json()
            if data.get('return_code') != 0:
                break
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
# 수급 데이터 수집 + DB 저장
# ============================================================
def collect_supply(api, db, target_date, date_short):
    print(f"\n{'─'*50}")
    print(f"📌 STEP 1: 수급 데이터 수집 + DB 저장")
    print(f"{'─'*50}")

    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")
    results = []

    # 시장 구분용 종목 리스트 (코스피/코스닥)
    stock_markets = {}
    for mrkt_tp, mrkt_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
        data, _ = api.call("ka10099", "/api/dostk/stkinfo", {"mrkt_tp": mrkt_tp})
        for item in data.get('list', []):
            code = item.get('code', '').replace('_AL','').replace('_NX','')
            if code:
                stock_markets[code] = mrkt_name
        time.sleep(0.5)

    for inv_name, inv_code in INVESTORS.items():
        for trade_type, trade_name in [('2', '매수'), ('1', '매도')]:
            print(f"  ▶ {inv_name} 순{trade_name}...", end=" ", flush=True)

            all_db_rows = []

            for mkt_name, mkt_code in MARKETS.items():
                items = api.call_paged("ka10058", "/api/dostk/stkinfo", {
                    "strt_dt": target_date, "end_dt": target_date,
                    "trde_tp": trade_type, "mrkt_tp": mkt_code,
                    "invsr_tp": inv_code, "stex_tp": "3"
                })

                # CSV 저장 (코스피/코스닥 분리)
                mkt_label = "코스피" if mkt_code == "001" else "코스닥"
                filename = f"{inv_name}_순{trade_name}_{mkt_label}_{date_short}.csv"
                filepath = os.path.join(OUTPUT_DIR, filename)
                header = ['종목코드', '종목명', f'순{trade_name}수량(백주)',
                          f'순{trade_name}금액(백만)', '추정평균가', '현재가', '전일대비', '전일대비']

                csv_rows = []
                for item in items:
                    name = item.get('stk_nm', '')
                    if is_etf(name):
                        continue
                    code = item.get('stk_cd', '').replace('_AL','').replace('_NX','')
                    qty = item.get('netslmt_qty', '0')
                    amt = item.get('netslmt_amt', '0')
                    avg_prc = item.get('prsm_avg_pric', '0')
                    cur_prc = item.get('cur_prc', '0')
                    pre_rt = item.get('pre_rt', '0')

                    csv_rows.append([
                        f"'{code}", name, qty, amt, avg_prc, cur_prc,
                        '▲' if item.get('pre_sig') == '2' else ('▼' if item.get('pre_sig') in ['4','5'] else '-'),
                        item.get('pred_pre', '0')
                    ])

                    # DB용 데이터
                    def parse_int(s):
                        try: return int(str(s).replace(',','').replace('+','').replace('-','').strip())
                        except: return 0

                    def parse_signed(s):
                        try:
                            s = str(s).replace(',','').strip()
                            return int(s)
                        except: return 0

                    all_db_rows.append({
                        "date": date_obj,
                        "market": mkt_label.replace('코스피','KOSPI').replace('코스닥','KOSDAQ'),
                        "stock_code": code,
                        "stock_name": name,
                        "subject": inv_name,
                        "direction": trade_name,
                        "quantity": parse_signed(qty),
                        "amount": parse_signed(amt),
                        "avg_price": parse_int(avg_prc),
                        "current_price": parse_int(cur_prc.replace('+','').replace('-','')),
                        "change_pct": float(pre_rt) if pre_rt else 0
                    })

                # 금액 정렬
                def sort_amt(r):
                    try: return abs(int(r[3].replace(',','').replace('+','').replace('-','')))
                    except: return 0
                csv_rows.sort(key=sort_amt, reverse=True)

                with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    writer.writerow(header)
                    writer.writerows(csv_rows[:100])

                results.append(filename)
                time.sleep(0.3)

            # DB 저장 (전체 저장)
            if db and all_db_rows:
                db.upsert("daily_supply", all_db_rows)

            print(f"→ {inv_name}_순{trade_name} 코스피+코스닥 ({len(all_db_rows)}종목)")

    return results, stock_markets


# ============================================================
# 시가총액 수집 + DB 저장
# ============================================================
def collect_mktcap(api, db, target_date, date_short, stock_markets):
    print(f"\n{'─'*50}")
    print(f"📌 STEP 2: 시가총액 수집 + DB 저장")
    print(f"{'─'*50}")

    date_obj = datetime.strptime(target_date, "%Y%m%d").strftime("%Y-%m-%d")

    # 수급 CSV에서 종목 추출
    supply_codes = set()
    for f in os.listdir(OUTPUT_DIR):
        if f.endswith(f'_{date_short}.csv') and ('순매수' in f or '순매도' in f):
            try:
                with open(os.path.join(OUTPUT_DIR, f), encoding='utf-8-sig') as fp:
                    reader = csv.reader(fp)
                    next(reader)
                    for row in reader:
                        code = row[0].strip().replace("'","").zfill(6)
                        supply_codes.add(code)
            except:
                pass

    target_codes = {code: stock_markets.get(code, 'KOSPI') for code in supply_codes}
    total = len(target_codes)
    print(f"  수집 대상: {total}종목")

    csv_rows = []
    db_rows = []
    errors = 0
    start_time = time.time()

    for i, (code, market) in enumerate(target_codes.items()):
        try:
            data, _ = api.call("ka10001", "/api/dostk/stkinfo", {"stk_cd": code})
            if data.get('return_code') != 0:
                errors += 1
                continue

            name = data.get('stk_nm', '')
            if is_etf(name):
                continue

            mac = data.get('mac', '0')
            flo_stk = data.get('flo_stk', '0')
            cur_prc = data.get('cur_prc', '0').lstrip('+').lstrip('-')
            pred_pre = data.get('pred_pre', '0')
            flu_rt = data.get('flu_rt', '0')
            open_pric = data.get('open_pric', '0')
            high_pric = data.get('high_pric', '0')
            low_pric = data.get('low_pric', '0')
            trde_qty = data.get('trde_qty', '0')
            trde_prica = data.get('trde_pre', '0')

            def to_int(s):
                try: return int(str(s).replace(',','').replace('+','').replace('-','').strip())
                except: return 0

            mac_won = to_int(mac) * 100_000_000
            shares = to_int(flo_stk) * 1000

            csv_rows.append([code, name, market, '', cur_prc, pred_pre, flu_rt,
                            open_pric, high_pric, low_pric, trde_qty, trde_prica,
                            str(mac_won), str(shares)])

            high_52w = to_int(data.get('250hgst', '0'))
            high_52w_date = data.get('250hgst_pric_dt', '')
            low_52w = to_int(data.get('250lwst', '0'))

            db_rows.append({
                "date": date_obj,
                "stock_code": code,
                "stock_name": name,
                "market": market,
                "close_price": to_int(cur_prc),
                "change_amount": to_int(pred_pre),
                "change_pct": float(flu_rt) if flu_rt else 0,
                "open_price": to_int(open_pric),
                "high_price": to_int(high_pric),
                "low_price": to_int(low_pric),
                "volume": to_int(trde_qty),
                "trade_value": to_int(trde_prica),
                "market_cap": mac_won,
                "listed_shares": shares,
                "high_52w": high_52w,
                "high_52w_date": high_52w_date if high_52w_date else None,
                "low_52w": low_52w
            })



            if (i+1) % 100 == 0:
                elapsed = time.time() - start_time
                remaining = elapsed / (i+1) * (total - i - 1)
                print(f"  진행: {i+1}/{total} ({(i+1)/total*100:.0f}%) 남은: {remaining:.0f}초")

            time.sleep(0.2)
        except:
            errors += 1
            continue

    # CSV 저장
    filename = f"data_auto_{date_short}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    header = ['종목코드', '종목명', '시장구분', '소속부', '종가', '대비',
              '등락률', '시가', '고가', '저가', '거래량', '거래대금',
              '시가총액', '상장주식수']

    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(csv_rows)

    # DB 저장
    if db and db_rows:
        db.upsert("daily_market", db_rows)

    elapsed = time.time() - start_time
    print(f"  → {filename} ({len(csv_rows)}종목, {elapsed:.0f}초)")
    return filename, len(csv_rows), errors


# ============================================================
# 메인
# ============================================================
def main():
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime("%Y%m%d")
    date_short = target_date[2:]

    print("=" * 60)
    print("🔮 AI+패스웨이 키움 REST API 자동수집기 v3.0")
    print(f"   날짜: {target_date}  인코딩: UTF-8")
    print(f"   CSV + Supabase DB 동시 저장")
    print("=" * 60)

    # 설정 확인
    if not APP_KEY or not SECRET_KEY:
        print("❌ .env 파일에 KIWOOM_APP_KEY, KIWOOM_SECRET_KEY를 설정하세요.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Supabase 연결
    db = None
    if SUPABASE_URL and SUPABASE_KEY:
        db = SupabaseDB(SUPABASE_URL, SUPABASE_KEY)
        print("\n📌 Supabase 연결 테스트...", end=" ")
        if db.test_connection():
            print("✅ 성공")
        else:
            print("⚠️ 연결 실패 (CSV만 저장합니다)")
            db = None
    else:
        print("\n⚠️ Supabase 설정 없음 (CSV만 저장합니다)")

    # 키움 토큰
    api = KiwoomAPI()
    print("📌 키움 토큰 발급...", end=" ")
    if not api.get_token():
        return
    print("✅ 성공")

    start_total = time.time()

    # STEP 1: 수급
    supply_files, stock_markets = collect_supply(api, db, target_date, date_short)

    # STEP 2: 시총
    mkt_file, mkt_count, mkt_errors = collect_mktcap(api, db, target_date, date_short, stock_markets)

    # 완료
    total_time = time.time() - start_total
    print(f"\n{'='*60}")
    print(f"✅ 전체 수집 완료!")
    print(f"   총 소요시간: {total_time:.0f}초 ({total_time/60:.1f}분)")
    print(f"   키움 API 호출: {api.call_count}회")
    if db:
        print(f"   DB 저장: {db.insert_count}건")
    print(f"   출력 폴더: {os.path.abspath(OUTPUT_DIR)}")
    print(f"\n📁 생성된 파일 (총 {len(supply_files)+1}개):")
    for f in supply_files:
        print(f"   ✓ {f}")
    print(f"   ✓ {mkt_file} ({mkt_count}종목)")
    if db:
        print(f"\n💾 Supabase DB에도 동시 저장 완료!")
    print("=" * 60)


if __name__ == "__main__":
    main()
