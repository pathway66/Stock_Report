"""
키움 REST API - 수급 데이터 수집 테스트
5주체 × 순매수/순매도 × 코스피+코스닥 통합 = 10개 CSV 파일 생성
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
TARGET_DATE = "20260319"  # 수집할 날짜 (YYYYMMDD)
# ============================================================

OUTPUT_DIR = "./kiwoom_data"
DATE_SHORT = TARGET_DATE[2:]  # YYMMDD

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


def get_token():
    resp = requests.post(
        f"{BASE_URL}/oauth2/token",
        headers={"Content-Type": "application/json;charset=UTF-8", "api-id": "au10001"},
        json={"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": SECRET_KEY}
    )
    data = resp.json()
    if data.get('return_code') == 0:
        print(f"✅ 토큰 발급 성공")
        return data['token']
    else:
        print(f"❌ 토큰 실패: {data.get('return_msg')}")
        return None


def call_ka10058(token, inv_code, mkt_code, trade_type):
    """ka10058 호출 (연속조회 포함)"""
    all_items = []
    cont_yn, next_key = "", ""

    for _ in range(5):  # 최대 5페이지
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10058",
            "authorization": f"Bearer {token}"
        }
        if cont_yn == "Y":
            headers["cont-yn"] = cont_yn
            headers["next-key"] = next_key

        body = {
            "strt_dt": TARGET_DATE,
            "end_dt": TARGET_DATE,
            "trde_tp": trade_type,
            "mrkt_tp": mkt_code,
            "invsr_tp": inv_code,
            "stex_tp": "3"
        }

        resp = requests.post(f"{BASE_URL}/api/dostk/stkinfo", headers=headers, json=body)
        data = resp.json()

        if data.get('return_code') != 0:
            break

        items = data.get('invsr_daly_trde_stk', [])
        all_items.extend(items)

        cont_yn = resp.headers.get('cont-yn', 'N')
        next_key = resp.headers.get('next-key', '')
        if cont_yn != 'Y':
            break
        time.sleep(0.3)

    return all_items


def save_csv(items, inv_name, trade_name):
    """영웅문4 동일 형식 CSV 저장"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"{inv_name}_순{trade_name}_{DATE_SHORT}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    header = ['종목코드', '종목명', f'순{trade_name}수량(백주)',
              f'순{trade_name}금액(백만)', '추정평균가', '현재가', '전일대비', '전일대비']

    rows = []
    for item in items:
        name = item.get('stk_nm', '')
        if is_etf(name):
            continue

        code = item.get('stk_cd', '').replace('_AL', '').replace('_NX', '')
        qty = item.get('netslmt_qty', '0')
        amt = item.get('netslmt_amt', '0')
        avg = item.get('prsm_avg_pric', '0')
        cur = item.get('cur_prc', '0')
        pre_sig = item.get('pre_sig', '3')
        pred = item.get('pred_pre', '0')

        sig_str = '▲' if pre_sig == '2' else ('▼' if pre_sig in ['4','5'] else '-')

        rows.append([f"'{code}", name, qty, amt, avg, cur, sig_str, pred])

    # 금액 기준 정렬
    def sort_amt(r):
        try:
            return abs(int(r[3].replace(',','').replace('+','').replace('-','')))
        except:
            return 0
    rows.sort(key=sort_amt, reverse=True)

    with open(filepath, 'w', newline='', encoding='euc-kr') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows[:100])

    return filename, len(rows[:100])


def main():
    print("=" * 60)
    print("🔮 키움 REST API 수급 데이터 수집 테스트")
    print(f"   날짜: {TARGET_DATE}  서버: {BASE_URL}")
    print("=" * 60)

    token = get_token()
    if not token:
        return

    total_calls = 0
    results = []

    for inv_name, inv_code in INVESTORS.items():
        for trade_type, trade_name in [('2', '매수'), ('1', '매도')]:
            print(f"\n  ▶ {inv_name} 순{trade_name} 수집 중...", end=" ")

            # 코스피 + 코스닥 합산
            all_items = []
            for mkt_name, mkt_code in MARKETS.items():
                items = call_ka10058(token, inv_code, mkt_code, trade_type)
                all_items.extend(items)
                total_calls += 1
                time.sleep(0.3)

            filename, count = save_csv(all_items, inv_name, trade_name)
            print(f"→ {filename} ({count}종목)")
            results.append((filename, count))

    print(f"\n{'=' * 60}")
    print(f"✅ 수집 완료! API 호출: {total_calls}회")
    print(f"   출력 폴더: {os.path.abspath(OUTPUT_DIR)}")
    print(f"\n📁 생성된 파일:")
    for fname, cnt in results:
        print(f"   ✓ {fname} ({cnt}종목)")
    print(f"\n💡 이 파일들을 영웅문4 다운로드 파일과 비교해보세요!")
    print("=" * 60)


if __name__ == "__main__":
    main()
