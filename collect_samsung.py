import FinanceDataReader as fdr
import requests, os, math
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
h = {'apikey': key, 'Authorization': f'Bearer {key}', 'Content-Type': 'application/json', 'Prefer': 'resolution=merge-duplicates,return=minimal'}

start = (datetime.now() - timedelta(days=230)).strftime('%Y-%m-%d')
end = datetime.now().strftime('%Y-%m-%d')
df = fdr.DataReader('005930', start, end)
df = df[~df.index.duplicated(keep='last')].tail(200)

def si(v):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return 0
        return int(v) if v > 0 else 0
    except: return 0

def sp(v):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return 0
        return round(float(v) * 100, 2)
    except: return 0

records = []
for idx, row in df.iterrows():
    records.append({
        'stock_code': '005930',
        'date': idx.strftime('%Y-%m-%d'),
        'open': si(row.get('Open', 0)),
        'high': si(row.get('High', 0)),
        'low': si(row.get('Low', 0)),
        'close': si(row.get('Close', 0)),
        'volume': si(row.get('Volume', 0)),
        'change_pct': sp(row.get('Change', 0))
    })

total = 0
for i in range(0, len(records), 50):
    batch = records[i:i+50]
    r = requests.post(f'{url}/rest/v1/daily_ohlcv', headers=h, json=batch)
    if r.status_code in [200, 201]:
        total += len(batch)

last = records[-1]
print(f'삼성전자: {len(records)}일 -> {total}건 저장')
print(f'최근 종가: {last["close"]:,} ({last["date"]})')
