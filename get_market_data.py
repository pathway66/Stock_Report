"""야후 파이낸스에서 시장 지표 가져오기"""
import yfinance as yf

tickers = {
    'S&P500': '^GSPC',
    'NASDAQ': '^IXIC',
    'DOW': '^DJI',
    'Russell2000': '^RUT',
    'VIX': '^VIX',
    'US10Y': '^TNX',
    'WTI': 'CL=F',
    'Gold': 'GC=F',
    'BTC': 'BTC-USD',
    'USD/KRW': 'KRW=X'
}

for name, symbol in tickers.items():
    try:
        t = yf.Ticker(symbol)
        h = t.history(period='5d')
        if len(h) >= 2:
            last = h.iloc[-1]['Close']
            prev = h.iloc[-2]['Close']
            chg = (last - prev) / prev * 100
            print(f'{name}: {last:,.2f} ({chg:+.2f}%)')
        elif len(h) == 1:
            last = h.iloc[-1]['Close']
            print(f'{name}: {last:,.2f}')
        else:
            print(f'{name}: no data')
    except Exception as e:
        print(f'{name}: error - {e}')
