"""
키움 REST API 연결 테스트 스크립트
=================================
이 스크립트로 3단계를 순서대로 테스트합니다:
  1단계: 토큰 발급
  2단계: 외국인 순매수 TOP 데이터 1건 호출
  3단계: 시가총액 데이터 1건 호출

사용법:
  1) 아래 APP_KEY, SECRET_KEY를 본인 것으로 변경
  2) cmd 또는 PowerShell에서: python kiwoom_test.py
  3) 결과를 Claude에 복사해서 보내주세요
"""

import requests
import json

# ============================================================
# ★ 여기만 수정하세요 ★
# ============================================================
APP_KEY = "To4RH8MD7yxT5C4dtGn-zzuUNXxHXV_eg_EswRmKWZ4"
SECRET_KEY = "N9N548PPhiOxkxTkxJeo7fJAoAiHdZ9on1K84kNpDmI"
# ============================================================

BASE_URL = "https://api.kiwoom.com"


def test_step1_token():
    """1단계: 토큰 발급 테스트"""
    print("=" * 50)
    print("1단계: 접근토큰 발급 테스트")
    print("=" * 50)
    
    url = f"{BASE_URL}/oauth2/token"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "au10001"
    }
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": SECRET_KEY
    }
    
    try:
        resp = requests.post(url, headers=headers, json=body)
        print(f"HTTP 상태코드: {resp.status_code}")
        data = resp.json()
        print(f"응답: {json.dumps(data, indent=2, ensure_ascii=False)}")
        
        if data.get('return_code') == 0:
            token = data.get('token', '')
            print(f"\n✅ 토큰 발급 성공!")
            print(f"   토큰 앞 20자: {token[:20]}...")
            print(f"   만료일: {data.get('expires_dt', 'N/A')}")
            return token
        else:
            print(f"\n❌ 토큰 발급 실패: {data.get('return_msg', 'Unknown')}")
            return None
    except Exception as e:
        print(f"\n❌ 연결 오류: {e}")
        return None


def test_step2_supply(token):
    """2단계: 외국인 순매수 데이터 테스트 (ka10058)"""
    print("\n" + "=" * 50)
    print("2단계: 외국인 코스피 순매수 테스트 (ka10058)")
    print("=" * 50)
    
    url = f"{BASE_URL}/api/dostk/stkinfo"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "ka10058",
        "authorization": f"Bearer {token}"
    }
    body = {
        "strt_dt": "20260319",   # 오늘 날짜 (YYYYMMDD)
        "end_dt": "20260319",
        "trde_tp": "2",          # 2 = 순매수
        "mrkt_tp": "001",        # 001 = 코스피
        "invsr_tp": "9000",      # 9000 = 외국인
        "stex_tp": "3"           # 3 = 통합(KRX+NXT)
    }
    
    print(f"요청 Body: {json.dumps(body, indent=2)}")
    
    try:
        resp = requests.post(url, headers=headers, json=body)
        print(f"HTTP 상태코드: {resp.status_code}")
        
        # 응답 헤더 확인 (연속조회 여부)
        print(f"응답 헤더 cont-yn: {resp.headers.get('cont-yn', 'N/A')}")
        print(f"응답 헤더 next-key: {resp.headers.get('next-key', 'N/A')}")
        
        data = resp.json()
        
        if data.get('return_code') == 0:
            items = data.get('invsr_daly_trde_stk', [])
            print(f"\n✅ 성공! 수신 종목 수: {len(items)}")
            
            # 상위 5개만 출력
            for i, item in enumerate(items[:5]):
                print(f"\n  [{i+1}] {item.get('stk_nm', 'N/A')} ({item.get('stk_cd', '')})")
                print(f"      순매수금액: {item.get('netslmt_amt', 'N/A')} 백만")
                print(f"      추정평균가: {item.get('prsm_avg_pric', 'N/A')}")
                print(f"      현재가: {item.get('cur_prc', 'N/A')}")
                print(f"      대비율: {item.get('pre_rt', 'N/A')}%")
            
            if len(items) == 0:
                print("\n⚠️ 데이터가 0건입니다!")
                print("   가능한 원인:")
                print("   1) 날짜가 휴장일이거나 아직 장 마감 전")
                print("   2) strt_dt/end_dt 형식 확인 (YYYYMMDD)")
                print("   3) 장 마감 후 데이터 반영까지 시간 소요")
        else:
            print(f"\n❌ 실패: {data.get('return_msg', 'Unknown')}")
            print(f"   전체 응답: {json.dumps(data, indent=2, ensure_ascii=False)}")
    except Exception as e:
        print(f"\n❌ 오류: {e}")


def test_step3_marketcap(token):
    """3단계: 시가총액 테스트 (ka10001 - 삼성전자)"""
    print("\n" + "=" * 50)
    print("3단계: 시가총액 테스트 - 삼성전자 (ka10001)")
    print("=" * 50)
    
    url = f"{BASE_URL}/api/dostk/stkinfo"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "ka10001",
        "authorization": f"Bearer {token}"
    }
    body = {"stk_cd": "005930"}
    
    try:
        resp = requests.post(url, headers=headers, json=body)
        print(f"HTTP 상태코드: {resp.status_code}")
        data = resp.json()
        
        if data.get('return_code') == 0:
            print(f"\n✅ 성공!")
            print(f"   종목: {data.get('stk_nm', 'N/A')} ({data.get('stk_cd', '')})")
            print(f"   시가총액(mac): {data.get('mac', 'N/A')}")
            print(f"   상장주식(flo_stk): {data.get('flo_stk', 'N/A')}")
            print(f"   현재가: {data.get('cur_prc', 'N/A')}")
            print(f"   등락률: {data.get('flu_rt', 'N/A')}")
            print(f"   PER: {data.get('per', 'N/A')}")
            print(f"   PBR: {data.get('pbr', 'N/A')}")
            
            # mac 단위 확인
            mac_val = data.get('mac', '0')
            print(f"\n   📌 mac 원본값: '{mac_val}'")
            print(f"   → 삼성전자 시총이 약 120조원이라면")
            print(f"     mac이 억원 단위면: {mac_val}억원")
            print(f"     mac이 백만원 단위면: {mac_val}백만원")
        else:
            print(f"\n❌ 실패: {json.dumps(data, indent=2, ensure_ascii=False)}")
    except Exception as e:
        print(f"\n❌ 오류: {e}")


def test_step4_stocklist(token):
    """4단계: 전종목 리스트 테스트 (ka10099)"""
    print("\n" + "=" * 50)
    print("4단계: 코스피 종목 리스트 테스트 (ka10099)")
    print("=" * 50)
    
    url = f"{BASE_URL}/api/dostk/stkinfo"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "api-id": "ka10099",
        "authorization": f"Bearer {token}"
    }
    body = {"mrkt_tp": "0"}  # 0 = 코스피
    
    try:
        resp = requests.post(url, headers=headers, json=body)
        print(f"HTTP 상태코드: {resp.status_code}")
        data = resp.json()
        
        items = data.get('list', [])
        print(f"\n✅ 코스피 종목 수: {len(items)}")
        for item in items[:3]:
            print(f"   {item.get('code', '')} {item.get('name', '')} (상장주식: {item.get('listCount', '')})")
        
        # 연속조회 확인
        print(f"\n   cont-yn: {resp.headers.get('cont-yn', 'N/A')}")
        if resp.headers.get('cont-yn') == 'Y':
            print(f"   → 연속조회 필요 (next-key: {resp.headers.get('next-key', '')[:30]}...)")
    except Exception as e:
        print(f"\n❌ 오류: {e}")


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    print("🔮 키움 REST API 연결 테스트")
    print(f"   대상 서버: {BASE_URL}")
    print()
    
    if "여기에" in APP_KEY:
        print("❌ APP_KEY와 SECRET_KEY를 먼저 입력하세요!")
        print("   스크립트 상단의 APP_KEY, SECRET_KEY를 수정 후 다시 실행하세요.")
        exit()
    
    # 1단계: 토큰
    token = test_step1_token()
    if not token:
        print("\n⛔ 토큰 발급 실패. 이후 테스트를 중단합니다.")
        print("   → App Key / Secret Key 확인")
        print("   → openapi.kiwoom.com에서 사용 등록 상태 확인")
        exit()
    
    # 2단계: 수급 데이터
    test_step2_supply(token)
    
    # 3단계: 시가총액
    test_step3_marketcap(token)
    
    # 4단계: 종목 리스트
    test_step4_stocklist(token)
    
    print("\n" + "=" * 50)
    print("테스트 완료! 위 결과를 Claude에 복사해서 보내주세요.")
    print("=" * 50)
