import FinanceDataReader as fdr
from datetime import datetime, timedelta
import requests

past = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y-%m-%d')

# US 10Y Treasury (FRED)
try:
    df = fdr.DataReader('FRED:DGS10', past, today)
    if len(df) > 0:
        val = df.iloc[-1].values[0]
        print(f'US10Y: {val:.2f}%')
except Exception as e:
    print(f'US10Y: ERROR - {e}')

# CNN Fear and Greed Index
try:
    r = requests.get('https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
                     headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    if r.status_code == 200:
        data = r.json()
        fg = data.get('fear_and_greed', {})
        score = fg.get('score', 0)
        rating = fg.get('rating', '?')
        print(f'FnG Index: {score:.0f} ({rating})')
    else:
        print(f'FnG: Status {r.status_code}')
except Exception as e:
    print(f'FnG: ERROR - {e}')

# WTI Oil (FRED)
try:
    df = fdr.DataReader('FRED:DCOILWTICO', past, today)
    if len(df) > 0:
        val = df.iloc[-1].values[0]
        print(f'WTI: ${val:.2f}')
except Exception as e:
    print(f'WTI: ERROR - {e}')
