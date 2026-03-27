"""
일봉 OHLCV 데이터 수집기
- FinanceDataReader로 TOP25 종목의 200일치 일봉 데이터 수집
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

def get_top25_codes(date):
    """Supabase에서 해당 날짜 TOP25 종목코드 가져오기"""
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/analysis_scores?date=eq.{date}&order=final_score.desc&limit=25',
        headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'}
    )
    scores = r.json()
    if not scores:
        return []
    return [(s['stock_code'], s['stock_name']) for s in scores]

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
            f'{SUPABASE_URL}/rest/v1/daily_ohlcv',
            headers={**sb_headers, 'Prefer': 'resolution=merge-duplicates,return=minimal'},
            json=batch
        )
        if r.status_code in [200, 201]:
            total += len(batch)
        else:
            print(f'  [!] upsert 실패: {r.status_code} {r.text[:100]}')
    return total

def main():
    date = sys.argv[1] if len(sys.argv) > 1 else None
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')
    if len(date) == 8:
        date = f'{date[:4]}-{date[4:6]}-{date[6:8]}'
    
    print(f'[*] 일봉 OHLCV 수집기 ({date})')
    print('=' * 50)
    
    # TOP25 종목 가져오기
    stocks = get_top25_codes(date)
    if not stocks:
        print('[!] TOP25 종목이 없습니다. 분석 데이터를 먼저 실행하세요.')
        return
    
    print(f'  수집 대상: {len(stocks)}종목, 200일치 일봉')
    print('-' * 50)
    
    total_records = 0
    for i, (code, name) in enumerate(stocks, 1):
        records = collect_ohlcv(code, name)
        if records:
            saved = upsert_ohlcv(records)
            total_records += saved
            print(f'  [{i}/{len(stocks)}] {name}({code}): {len(records)}일 -> {saved}건 저장')
        else:
            print(f'  [{i}/{len(stocks)}] {name}({code}): 데이터 없음')
        time.sleep(0.3)  # API 부하 방지
    
    print('=' * 50)
    print(f'[OK] 완료! 총 {total_records}건 저장')

if __name__ == '__main__':
    main()
