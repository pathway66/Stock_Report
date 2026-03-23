"""
키움 REST API - 시가총액 자동수집
=================================
전략: ka10099(전종목 리스트+상장주식수) + ka10001(시가총액+종가+등락률)
  - 수급 데이터에 나온 종목만 우선 수집 (약 300~500종목)
  - 전종목이 필요하면 full_mode=True로 변경 (약 15분)

사용법:
  1) APP_KEY, SECRET_KEY 수정
  2) python kiwoom_mktcap_test.py
"""

import requests
import json
import csv
import os
import time
from datetime import datetime

# ============================================================
# ★ 여기만 수정하세요 ★
# ============================================================
APP_KEY = "To4RH8MD7yxT5C4dtGn-zzuUNXxHXV_eg_EswRmKWZ4"
SECRET_KEY = "N9N548PPhiOxkxTkxJeo7fJAoAiHdZ9on1K84kNpDmI"
BASE_URL = "https://api.kiwoom.com"
TARGET_DATE = "20260319"
FULL_MODE = False  # True: 전종목(~2800종목, ~15분) / False: 수급종목만(~500종목, ~3분)
# ============================================================

OUTPUT_DIR = "./kiwoom_data"
DATE_SHORT = TARGET_DATE[2:]
DATE_FILE = TARGET_DATE.replace('2026','26')  # 260319

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
            print(f"✅ 토큰 발급 성공")
            return True
        print(f"❌ 토큰 실패: {data.get('return_msg')}")
        return False

    def call(self, api_id, url_path, body):
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": api_id,
            "authorization": f"Bearer {self.token}"
        }
        resp = requests.post(f"{BASE_URL}{url_path}", headers=headers, json=body)
        self.call_count += 1
        return resp.json()

    def get_stock_list(self, mrkt_tp):
        """ka10099 전종목 리스트"""
        return self.call("ka10099", "/api/dostk/stkinfo", {"mrkt_tp": mrkt_tp})

    def get_stock_info(self, stk_cd):
        """ka10001 주식기본정보 (시가총액 포함)"""
        return self.call("ka10001", "/api/dostk/stkinfo", {"stk_cd": stk_cd})

    def get_supply_codes(self):
        """수급 CSV에서 종목코드 수집"""
        codes = set()
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(f'_{DATE_SHORT}.csv') and ('순매수' in f or '순매도' in f):
                try:
                    with open(os.path.join(OUTPUT_DIR, f), encoding='euc-kr') as fp:
                        reader = csv.reader(fp)
                        next(reader)
                        for row in reader:
                            code = row[0].strip().replace("'","").zfill(6)
                            codes.add(code)
                except:
                    pass
        return codes


def main():
    print("=" * 60)
    print("🔮 키움 REST API 시가총액 자동수집")
    print(f"   날짜: {TARGET_DATE}  모드: {'전종목' if FULL_MODE else '수급종목 우선'}")
    print("=" * 60)

    api = KiwoomAPI()
    if not api.get_token():
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1: 대상 종목 결정
    if FULL_MODE:
        # 전종목 리스트 가져오기
        print("\n📌 전종목 리스트 수집 중...")
        target_codes = {}
        for mrkt_tp, mrkt_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
            data = api.get_stock_list(mrkt_tp)
            items = data.get('list', [])
            for item in items:
                code = item.get('code', '').replace('_AL','').replace('_NX','')
                name = item.get('name', '')
                if code and not is_etf(name):
                    list_count = item.get('listCount', '0')
                    target_codes[code] = {'name': name, 'market': mrkt_name, 'shares': list_count}
            print(f"  {mrkt_name}: {len(items)}종목")
            time.sleep(0.5)
    else:
        # 수급 CSV에서 종목만
        supply_codes = api.get_supply_codes()
        print(f"\n📌 수급 데이터 종목: {len(supply_codes)}개")

        # 코스피/코스닥 구분을 위해 리스트도 수집
        all_stocks = {}
        for mrkt_tp, mrkt_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
            data = api.get_stock_list(mrkt_tp)
            for item in data.get('list', []):
                code = item.get('code', '').replace('_AL','').replace('_NX','')
                if code:
                    all_stocks[code] = mrkt_name
            time.sleep(0.5)

        target_codes = {}
        for code in supply_codes:
            market = all_stocks.get(code, 'KOSPI')
            target_codes[code] = {'name': '', 'market': market, 'shares': '0'}

    # Step 2: 종목별 시가총액 수집
    total = len(target_codes)
    print(f"\n📌 시가총액 수집: {total}종목")

    rows = []
    errors = 0
    start_time = time.time()

    for i, (code, info) in enumerate(target_codes.items()):
        try:
            data = api.get_stock_info(code)
            if data.get('return_code') != 0:
                errors += 1
                continue

            name = data.get('stk_nm', info.get('name', ''))
            if is_etf(name):
                continue

            mac = data.get('mac', '0')          # 억원 단위
            flo_stk = data.get('flo_stk', '0')  # 천주 단위
            cur_prc = data.get('cur_prc', '0').lstrip('+').lstrip('-')
            pred_pre = data.get('pred_pre', '0')
            flu_rt = data.get('flu_rt', '0')
            open_pric = data.get('open_pric', '0')
            high_pric = data.get('high_pric', '0')
            low_pric = data.get('low_pric', '0')
            trde_qty = data.get('trde_qty', '0')
            trde_prica = data.get('trde_pre', '0')  # 거래대금

            # 시가총액: 억원 → 원 변환
            try:
                mac_won = int(str(mac).replace(',','')) * 100_000_000
            except:
                mac_won = 0

            # 상장주식수: 천주 → 주 변환
            try:
                shares = int(str(flo_stk).replace(',','')) * 1000
            except:
                shares = 0

            market = info.get('market', 'KOSPI')

            rows.append([
                code, name, market, '',
                cur_prc, pred_pre, flu_rt,
                open_pric, high_pric, low_pric,
                trde_qty, trde_prica,
                str(mac_won), str(shares)
            ])

            if (i+1) % 100 == 0:
                elapsed = time.time() - start_time
                remaining = elapsed / (i+1) * (total - i - 1)
                print(f"  진행: {i+1}/{total} ({(i+1)/total*100:.0f}%) "
                      f"남은시간: {remaining:.0f}초")

            time.sleep(0.2)  # Rate limit

        except Exception as e:
            errors += 1
            continue

    # Step 3: CSV 저장 (영웅문4 형식)
    filename = f"data_auto_{DATE_FILE}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    header = ['종목코드', '종목명', '시장구분', '소속부', '종가', '대비',
              '등락률', '시가', '고가', '저가', '거래량', '거래대금',
              '시가총액', '상장주식수']

    with open(filepath, 'w', newline='', encoding='euc-kr') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"✅ 시가총액 수집 완료!")
    print(f"   수집 종목: {len(rows)}")
    print(f"   오류: {errors}")
    print(f"   API 호출: {api.call_count}회")
    print(f"   소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    print(f"   📁 {filename}")
    print("=" * 60)


if __name__ == "__main__":
    main()
