"""
일봉 OHLCV 데이터 수집기 v2
- 코팔/닥사 전체 종목의 일봉 데이터 수집 (KOSPI 8천억+, KOSDAQ 4천억+)
- FinanceDataReader 사용
- Supabase daily_ohlcv 테이블에 upsert
- run_daily.py에서 호출하거나 단독 실행 가능
"""
import FinanceDataReader as fdr
import requests, json, os, sys, time
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

sb_headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

def get_target_stocks(date):
    """코팔/닥사 기준 전체 종목 가져오기 (KOSPI 8천억+, KOSDAQ 4천억+)"""
    headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'}
    stocks = []
    offset = 0
    while True:
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/daily_market?date=eq.{date}'
            f'&select=stock_code,stock_name,market,market_cap'
            f'&order=market_cap.desc&limit=1000&offset={offset}',
            headers=headers
        )
        data = r.json()
        if not data:
            break
        stocks.extend(data)
        if len(data) < 1000:
            break
        offset += 1000

    filtered = []
    for s in stocks:
        market = s.get('market', '')
        mktcap = s.get('market_cap', 0) or 0
        if market == 'KOSPI' and mktcap >= 800000000000:
            filtered.append((s['stock_code'], s['stock_name']))
        elif market == 'KOSDAQ' and mktcap >= 400000000000:
            filtered.append((s['stock_code'], s['stock_name']))
    return filtered

def collect_ohlcv(stock_code, stock_name, days=200):
    """FinanceDataReader로 일봉 데이터 수집"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days + 30)).strftime('%Y-%m-%d')
    
    try:
        df = fdr.DataReader(stock_code, start_date, end_date)
        if df is None or len(df) == 0:
            return []
        
        # 중복 날짜 제거
        df = df[~df.index.duplicated(keep='last')]
        # 최근 200일만
        df = df.tail(days)
        
        records = []
        for idx, row in df.iterrows():
            date_str = idx.strftime('%Y-%m-%d')
            
            def safe_int(val):
                try:
                    import math
                    if val is None or (isinstance(val, float) and math.isnan(val)):
                        return 0
                    return int(val) if val > 0 else 0
                except:
                    return 0
            
            def safe_pct(val):
                try:
                    import math
                    if val is None or (isinstance(val, float) and math.isnan(val)):
                        return 0
                    return round(float(val) * 100, 2)
                except:
                    return 0
            
            records.append({
                'stock_code': stock_code,
                'date': date_str,
                'open': safe_int(row.get('Open', 0)),
                'high': safe_int(row.get('High', 0)),
                'low': safe_int(row.get('Low', 0)),
                'close': safe_int(row.get('Close', 0)),
                'volume': safe_int(row.get('Volume', 0)),
                'change_pct': safe_pct(row.get('Change', 0))
            })
        return records
    except Exception as e:
        print(f'  [!] {stock_name}({stock_code}) 수집 실패: {e}')
        return []

def upsert_ohlcv(records):
    """Supabase에 upsert (stock_code + date 기준)"""
    if not records:
        return 0
    
    # 50개씩 배치 upsert
    total = 0
    for i in range(0, len(records), 50):
        batch = records[i:i+50]
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/daily_ohlcv?on_conflict=stock_code,date',
            headers={**sb_headers, 'Prefer': 'resolution=merge-duplicates,return=minimal'},
            json=batch
        )
        if r.status_code in [200, 201]:
            total += len(batch)
        else:
            print(f'  [!] upsert 실패: {r.status_code} {r.text[:100]}')
    return total

def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    date = args[0] if args else None
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')
    if len(date) == 8:
        date = f'{date[:4]}-{date[4:6]}-{date[6:8]}'
    
    print(f'[*] 일봉 OHLCV 수집기 ({date})')
    print('=' * 50)
    
    # 코팔/닥사 전체 종목 가져오기
    stocks = get_target_stocks(date)
    if not stocks:
        print('[!] 코팔/닥사 종목이 없습니다. daily_market 데이터를 먼저 확인하세요.')
        return
    
    # 일일 수집: 최근 10일만 (빠른 실행), 전체 수집: --full 옵션
    days = 10
    if '--full' in sys.argv:
        days = 500
        print(f'  [!] 전체 수집 모드: {days}일치')
    
    print(f'  수집 대상: {len(stocks)}종목, {days}일치 일봉')
    print('-' * 50)
    
    total_records = 0
    for i, (code, name) in enumerate(stocks, 1):
        records = collect_ohlcv(code, name, days=days)
        if records:
            saved = upsert_ohlcv(records)
            total_records += saved
            if i % 50 == 0 or i == len(stocks):
                print(f'  [{i}/{len(stocks)}] {name}({code}): {len(records)}일 -> {saved}건 저장')
        time.sleep(0.3)  # API 부하 방지
    
    print('=' * 50)
    print(f'[OK] 완료! 총 {total_records}건 저장')

if __name__ == '__main__':
    main()
