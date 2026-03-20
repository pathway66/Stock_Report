"""
키움 REST API 자동 수급 데이터 수집기 v1.0
AI+패스웨이 주식투자 시스템

사용법:
1. 키움 REST API 사이트(openapi.kiwoom.com)에서 App Key/Secret 발급
2. 아래 설정에 입력
3. python kiwoom_auto_collector.py 실행
4. 생성된 CSV 파일을 Claude에 업로드

필요 라이브러리: pip install requests
"""

import requests
import json
import csv
import os
import time
from datetime import datetime, timedelta

# ============================================================
# 설정 (Shawn이 수정할 부분)
# ============================================================
APP_KEY = "YOUR_APP_KEY"          # 키움 REST API App Key
SECRET_KEY = "YOUR_SECRET_KEY"    # 키움 REST API Secret Key
BASE_URL = "https://api.kiwoom.com"  # 운영 도메인
# BASE_URL = "https://mockapi.kiwoom.com"  # 모의투자 테스트용

OUTPUT_DIR = "./kiwoom_data"      # 출력 폴더
TODAY = datetime.now().strftime("%y%m%d")  # 오늘 날짜 YYMMDD
TODAY_FULL = datetime.now().strftime("%Y%m%d")  # YYYYMMDD

# 투자자 구분 코드
INVESTOR_CODES = {
    '외국인': '9000',
    '연기금': '6000',
    '투신': '3000',
    '사모펀드': '3100',
    '기타법인': '7100',
}

# 시장 구분
MARKET_CODES = {
    '코스피': '001',
    '코스닥': '101',
}

# ETF 필터 키워드
ETF_KEYWORDS = ['KODEX','TIGER','ACE','RISE','PLUS','SOL','HANARO','KIWOOM',
                'KoAct','TIME','ETN','KOSEF','메리츠','삼성증권']


# ============================================================
# 1단계: 인증 (Access Token 발급)
# ============================================================
class KiwoomAPI:
    def __init__(self, app_key, secret_key, base_url):
        self.app_key = app_key
        self.secret_key = secret_key
        self.base_url = base_url
        self.token = None
        self.call_count = 0
        
    def get_token(self):
        """접근토큰 발급 (au10001)"""
        url = f"{self.base_url}/oauth2/token"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "au10001"
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.secret_key
        }
        
        resp = requests.post(url, headers=headers, json=body)
        data = resp.json()
        
        if data.get('return_code') == 0:
            self.token = data['token']
            print(f"✅ 토큰 발급 성공 (만료: {data.get('expires_dt', 'N/A')})")
            return True
        else:
            print(f"❌ 토큰 발급 실패: {data.get('return_msg', 'Unknown error')}")
            return False
    
    def _call_api(self, api_id, url_path, body, max_pages=10):
        """범용 API 호출 (연속조회 자동 처리)"""
        url = f"{self.base_url}{url_path}"
        all_data = []
        cont_yn = ""
        next_key = ""
        
        for page in range(max_pages):
            headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "api-id": api_id,
                "authorization": f"Bearer {self.token}",
            }
            if cont_yn == "Y":
                headers["cont-yn"] = cont_yn
                headers["next-key"] = next_key
            
            resp = requests.post(url, headers=headers, json=body)
            self.call_count += 1
            data = resp.json()
            
            if data.get('return_code') != 0:
                print(f"  ⚠️ API 오류: {data.get('return_msg', 'Unknown')}")
                break
            
            # 응답 데이터 추출 (LIST형 필드 자동 감지)
            for key, val in data.items():
                if isinstance(val, list):
                    all_data.extend(val)
                    break
            
            # 연속조회 처리
            resp_headers = resp.headers
            cont_yn = resp_headers.get('cont-yn', 'N')
            next_key = resp_headers.get('next-key', '')
            
            if cont_yn != 'Y':
                break
            
            time.sleep(0.5)  # API 호출 간격 (초과 방지)
        
        return all_data
    
    # ============================================================
    # 2단계: 수급 데이터 수집 (ka10058)
    # ============================================================
    def get_investor_trades(self, investor_code, market_code, trade_type, 
                           start_date, end_date):
        """
        투자자별일별매매종목요청 (ka10058)
        
        trade_type: '1'=순매도, '2'=순매수
        market_code: '001'=코스피, '101'=코스닥
        investor_code: '9000'=외국인, '6000'=연기금, etc.
        """
        body = {
            "strt_dt": start_date,    # YYYYMMDD
            "end_dt": end_date,       # YYYYMMDD
            "trde_tp": trade_type,    # 1:순매도, 2:순매수
            "mrkt_tp": market_code,   # 001:코스피, 101:코스닥
            "invsr_tp": investor_code,
            "stex_tp": "3"            # 3:통합(KRX+NXT)
        }
        
        return self._call_api("ka10058", "/api/dostk/stkinfo", body)
    
    # ============================================================
    # 3단계: 시가총액 데이터 수집 (ka10001)
    # ============================================================
    def get_stock_info(self, stock_code):
        """주식기본정보요청 (ka10001) - 시가총액 포함"""
        body = {"stk_cd": stock_code}
        url = f"{self.base_url}/api/dostk/stkinfo"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10001",
            "authorization": f"Bearer {self.token}",
        }
        resp = requests.post(url, headers=headers, json=body)
        self.call_count += 1
        return resp.json()
    
    def get_stock_list(self, market_type):
        """종목정보 리스트 (ka10099) - 전종목 코드/명"""
        body = {"mrkt_tp": market_type}  # "0":코스피, "10":코스닥
        return self._call_api("ka10099", "/api/dostk/stkinfo", body)


# ============================================================
# 4단계: CSV 변환 & 저장
# ============================================================
def is_etf(name):
    return any(kw in name for kw in ETF_KEYWORDS)

def save_supply_csv(data, investor_name, trade_type_name, date_str, output_dir):
    """수급 데이터를 영웅문4 CSV 형식으로 저장"""
    filename = f"{investor_name}_{trade_type_name}_{date_str}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # 영웅문4 동일 컬럼 형식
    headers = ['종목코드', '종목명', f'순{trade_type_name}수량(백주)', 
               f'순{trade_type_name}금액(백만)', '추정평균가', '현재가', '전일대비', '전일대비']
    
    rows = []
    for item in data:
        name = item.get('stk_nm', '')
        if is_etf(name):
            continue
        
        code = item.get('stk_cd', '')
        qty = item.get('netslmt_qty', '0')
        amt = item.get('netslmt_amt', '0')
        avg_prc = item.get('prsm_avg_pric', '0')
        cur_prc = item.get('cur_prc', '0').replace('+', '').replace('-', '')
        pre_sig = item.get('pre_sig', '3')
        pred_pre = item.get('pred_pre', '0')
        
        # 대비기호 → ▲/▼
        if pre_sig == '2':
            sig_str = '▲'
        elif pre_sig in ['4', '5']:
            sig_str = '▼'
        else:
            sig_str = '-'
        
        rows.append([f"'{code}", name, qty, amt, avg_prc, cur_prc, sig_str, pred_pre])
    
    with open(filepath, 'w', newline='', encoding='euc-kr') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows[:100])  # TOP 100
    
    print(f"  📁 {filename} ({len(rows[:100])}종목)")
    return filepath

def save_market_csv(api, stock_codes, date_str, output_dir):
    """시가총액 데이터를 영웅문4 형식으로 저장"""
    filename = f"data_auto_{date_str}.csv"
    filepath = os.path.join(output_dir, filename)
    
    headers = ['종목코드', '종목명', '시장구분', '소속부', '종가', '대비', 
               '등락률', '시가', '고가', '저가', '거래량', '거래대금', 
               '시가총액', '상장주식수']
    
    rows = []
    total = len(stock_codes)
    
    for i, (code, name, market) in enumerate(stock_codes):
        if is_etf(name):
            continue
        
        try:
            info = api.get_stock_info(code)
            if info.get('return_code') != 0:
                continue
            
            mac = info.get('mac', '0')
            flo_stk = info.get('flo_stk', '0')
            cur_prc = info.get('cur_prc', '0').replace('+','').replace('-','')
            pred_pre = info.get('pred_pre', '0').replace('+','').replace('-','')
            flu_rt = info.get('flu_rt', '0')
            
            # 시가총액 단위 변환 (억원 → 원): mac이 억원 단위로 보임
            # ka10001 예시: mac: "24352" (삼성전자 약 243조) 
            # → 실제로는 억원 단위. 원 단위 변환: ×100,000,000
            try:
                mac_won = int(mac.replace(',','')) * 100_000_000
            except:
                mac_won = 0
            
            try:
                flo_count = int(flo_stk.replace(',','')) * 1000  # 천주 단위
            except:
                flo_count = 0
            
            rows.append([code, name, market, '', cur_prc, pred_pre, flu_rt,
                        '', '', '', '', '', str(mac_won), str(flo_count)])
            
            if (i+1) % 50 == 0:
                print(f"    시총 수집 중... {i+1}/{total}")
            
            time.sleep(0.3)  # Rate limit
            
        except Exception as e:
            print(f"    ⚠️ {code} {name} 오류: {e}")
            continue
    
    with open(filepath, 'w', newline='', encoding='euc-kr') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    
    print(f"  📁 {filename} ({len(rows)}종목)")
    return filepath


# ============================================================
# 메인 실행
# ============================================================
def main():
    print("=" * 60)
    print("🔮 AI+패스웨이 키움 REST API 자동 수급 수집기 v1.0")
    print(f"   실행일: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    
    # 출력 폴더 생성
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # API 초기화
    api = KiwoomAPI(APP_KEY, SECRET_KEY, BASE_URL)
    
    # Step 1: 토큰 발급
    print("\n📌 Step 1: 접근토큰 발급")
    if not api.get_token():
        print("토큰 발급 실패. App Key/Secret Key를 확인하세요.")
        return
    
    # Step 2: 수급 데이터 수집
    print(f"\n📌 Step 2: 수급 데이터 수집 (날짜: {TODAY_FULL})")
    
    collected_codes = set()  # 시총 조회용 종목 코드 수집
    
    for inv_name, inv_code in INVESTOR_CODES.items():
        print(f"\n  ▶ {inv_name} ({inv_code})")
        
        for trade_type, trade_name in [('2', '순매수'), ('1', '순매도')]:
            # 코스피 + 코스닥 분리 수집 후 합산 (TOP100)
            all_items = []
            
            for mkt_name, mkt_code in MARKET_CODES.items():
                items = api.get_investor_trades(
                    inv_code, mkt_code, trade_type, TODAY_FULL, TODAY_FULL
                )
                all_items.extend(items)
                time.sleep(0.5)
            
            # 금액 기준 정렬 후 TOP100
            def sort_key(x):
                try:
                    amt = int(x.get('netslmt_amt', '0').replace(',','').replace('+','').replace('-',''))
                    return amt
                except:
                    return 0
            
            all_items.sort(key=sort_key, reverse=True)
            
            # CSV 저장 (영웅문4 형식)
            trade_suffix = '매수' if trade_type == '2' else '매도'
            save_supply_csv(all_items, inv_name, trade_suffix, TODAY, OUTPUT_DIR)
            
            # 종목 코드 수집 (시총 조회용)
            for item in all_items[:100]:
                code = item.get('stk_cd', '')
                name = item.get('stk_nm', '')
                if code and not is_etf(name):
                    # 시장 판별 (코스피 목록에 있으면 코스피)
                    collected_codes.add((code, name))
    
    # Step 3: 종목 리스트 & 시가총액
    print(f"\n📌 Step 3: 전종목 시가총액 수집")
    
    # 코스피/코스닥 전종목 리스트
    stock_list = []
    for mkt_type, mkt_name in [("0", "KOSPI"), ("10", "KOSDAQ")]:
        items = api.get_stock_list(mkt_type)
        for item in items:
            code = item.get('code', '')
            name = item.get('name', '')
            if code and not is_etf(name):
                stock_list.append((code, name, mkt_name))
        print(f"  {mkt_name}: {len(items)}종목")
        time.sleep(1)
    
    # 시가총액 수집 (전종목)
    save_market_csv(api, stock_list, TODAY_FULL.replace('20',''), OUTPUT_DIR)
    
    # 완료
    print(f"\n{'=' * 60}")
    print(f"✅ 수집 완료!")
    print(f"   총 API 호출: {api.call_count}회")
    print(f"   출력 폴더: {OUTPUT_DIR}/")
    print(f"\n📁 생성된 파일:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        print(f"   - {f}")
    print(f"\n💡 이 파일들을 Claude에 업로드하면 수급분석 리포트를 생성합니다.")
    print("=" * 60)


if __name__ == "__main__":
    main()
