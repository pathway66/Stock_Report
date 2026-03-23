"""
[*] AI+패스웨이 키움 REST API 자동 수급 수집기 v2.0
==================================================
수급 10개 + 시총 1개 = 11개 파일 자동 생성
인코딩: UTF-8 (Cursor/Claude/Excel 호환)
날짜: 자동 인식 (오늘 날짜) 또는 수동 지정

사용법:
  python kiwoom_collector_v2.py          → 오늘 날짜로 수집
  python kiwoom_collector_v2.py 20260318 → 특정 날짜 수집
"""

import requests
import json
import csv
import os
import sys
import time
from datetime import datetime

# ============================================================
# ★ 여기만 수정하세요 ★
# ============================================================
APP_KEY = "To4RH8MD7yxT5C4dtGn-zzuUNXxHXV_eg_EswRmKWZ4"
SECRET_KEY = "N9N548PPhiOxkxTkxJeo7fJAoAiHdZ9on1K84kNpDmI"
BASE_URL = "https://api.kiwoom.com"
MKTCAP_MODE = "supply"  # "supply": 수급종목만(3분) / "full": 전종목(15분)
# ============================================================

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


class KiwoomAPI:
    def __init__(self):
        self.token = None
        self.call_count = 0

    def get_token(self):
        resp = requests.post(
            f"{BASE_URL}/oauth2/token",
            headers={"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"},
            json={"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": SECRET_KEY}
        )
        data = resp.json()
        if data.get('return_code') == 0:
            self.token = data['token']
            return True
        print(f"[X] 토큰 실패: {data.get('return_msg')}")
        return False

    def call(self, api_id, url_path, body):
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": api_id,
            "authorization": f"Bearer {self.token}"
        }
        resp = requests.post(f"{BASE_URL}{url_path}", headers=headers, json=body)
        self.call_count += 1
        return resp.json(), resp.headers

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


def collect_supply(api, target_date, date_short):
    """수급 데이터 10개 파일 수집"""
    print(f"\n{'─'*50}")
    print(f"[>] STEP 1: 수급 데이터 수집")
    print(f"{'─'*50}")

    results = []
    for inv_name, inv_code in INVESTORS.items():
        for trade_type, trade_name in [('2', '매수'), ('1', '매도')]:
            print(f"  ▶ {inv_name} 순{trade_name}...", end=" ", flush=True)

            all_items = []
            for mkt_name, mkt_code in MARKETS.items():
                items = api.call_paged("ka10058", "/api/dostk/stkinfo", {
                    "strt_dt": target_date, "end_dt": target_date,
                    "trde_tp": trade_type, "mrkt_tp": mkt_code,
                    "invsr_tp": inv_code, "stex_tp": "3"
                })
                all_items.extend(items)
                time.sleep(0.3)

            # CSV 저장
            filename = f"{inv_name}_순{trade_name}_{date_short}.csv"
            filepath = os.path.join(OUTPUT_DIR, filename)
            header = ['종목코드', '종목명', f'순{trade_name}수량(백주)',
                      f'순{trade_name}금액(백만)', '추정평균가', '현재가', '전일대비', '전일대비']

            rows = []
            for item in all_items:
                name = item.get('stk_nm', '')
                if is_etf(name):
                    continue
                code = item.get('stk_cd', '').replace('_AL','').replace('_NX','')
                rows.append([
                    f"'{code}", name,
                    item.get('netslmt_qty', '0'), item.get('netslmt_amt', '0'),
                    item.get('prsm_avg_pric', '0'), item.get('cur_prc', '0'),
                    '▲' if item.get('pre_sig') == '2' else ('▼' if item.get('pre_sig') in ['4','5'] else '-'),
                    item.get('pred_pre', '0')
                ])

            def sort_amt(r):
                try: return abs(int(r[3].replace(',','').replace('+','').replace('-','')))
                except: return 0
            rows.sort(key=sort_amt, reverse=True)

            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows[:100])

            print(f"→ {filename} ({len(rows[:100])}종목)")
            results.append(filename)

    return results


def collect_mktcap(api, target_date, date_short):
    """시가총액 파일 1개 수집"""
    print(f"\n{'─'*50}")
    print(f"[>] STEP 2: 시가총액 수집 ({MKTCAP_MODE} 모드)")
    print(f"{'─'*50}")

    # 대상 종목 결정
    if MKTCAP_MODE == "full":
        target_codes = {}
        for mrkt_tp, mrkt_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
            data, _ = api.call("ka10099", "/api/dostk/stkinfo", {"mrkt_tp": mrkt_tp})
            for item in data.get('list', []):
                code = item.get('code', '').replace('_AL','').replace('_NX','')
                name = item.get('name', '')
                if code and not is_etf(name):
                    target_codes[code] = mrkt_name
            print(f"  {mrkt_name}: {len(data.get('list', []))}종목")
            time.sleep(0.5)
    else:
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
        print(f"  수급 종목: {len(supply_codes)}개")

        # 시장 구분
        all_stocks = {}
        for mrkt_tp, mrkt_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
            data, _ = api.call("ka10099", "/api/dostk/stkinfo", {"mrkt_tp": mrkt_tp})
            for item in data.get('list', []):
                code = item.get('code', '').replace('_AL','').replace('_NX','')
                if code:
                    all_stocks[code] = mrkt_name
            time.sleep(0.5)

        target_codes = {code: all_stocks.get(code, 'KOSPI') for code in supply_codes}

    # 종목별 시가총액 수집
    total = len(target_codes)
    print(f"  수집 대상: {total}종목")

    rows = []
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

            try: mac_won = int(str(mac).replace(',','')) * 100_000_000
            except: mac_won = 0

            try: shares = int(str(flo_stk).replace(',','')) * 1000
            except: shares = 0

            rows.append([code, name, market, '', cur_prc, pred_pre, flu_rt,
                        open_pric, high_pric, low_pric, trde_qty, trde_prica,
                        str(mac_won), str(shares)])

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
        writer.writerows(rows)

    elapsed = time.time() - start_time
    print(f"  → {filename} ({len(rows)}종목, {elapsed:.0f}초)")
    return filename, len(rows), errors


def main():
    # 날짜 결정
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = datetime.now().strftime("%Y%m%d")

    date_short = target_date[2:]  # YYMMDD

    print("=" * 60)
    print("[*] AI+패스웨이 키움 REST API 자동수집기 v2.0")
    print(f"   날짜: {target_date}  인코딩: UTF-8")
    print(f"   서버: {BASE_URL}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    api = KiwoomAPI()
    print("\n[>] 토큰 발급 중...", end=" ")
    if not api.get_token():
        return
    print("[OK] 성공")

    start_total = time.time()

    # STEP 1: 수급 데이터
    supply_files = collect_supply(api, target_date, date_short)

    # STEP 2: 시가총액
    mkt_file, mkt_count, mkt_errors = collect_mktcap(api, target_date, date_short)

    # 완료 요약
    total_time = time.time() - start_total
    print(f"\n{'='*60}")
    print(f"[OK] 전체 수집 완료!")
    print(f"   총 소요시간: {total_time:.0f}초 ({total_time/60:.1f}분)")
    print(f"   총 API 호출: {api.call_count}회")
    print(f"   출력 폴더: {os.path.abspath(OUTPUT_DIR)}")
    print(f"\n[F] 생성된 파일 (총 {len(supply_files)+1}개):")
    for f in supply_files:
        print(f"   ✓ {f}")
    print(f"   ✓ {mkt_file} ({mkt_count}종목)")
    print(f"\n[!] 이 파일들을 Claude에 업로드하면 수급분석 리포트를 생성합니다.")
    print(f"[!] 인코딩: UTF-8 (Cursor/Claude/Excel 모두 호환)")
    print("=" * 60)


if __name__ == "__main__":
    main()
