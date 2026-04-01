"""
시장 주요지표 수집기 v2
- FinanceDataReader를 이용한 주요 지표 수집
- BTC는 yfinance에서 수집 (더 정확)
- 전일 대비 등락률 정확 계산
- blog_posts의 content에 market_indicators 섹션으로 저장
"""
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime, timedelta

def get_market_indicators(date_str=None):
    """주요 시장 지표 수집. date_str: 'YYYY-MM-DD' 형식"""
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    # 충분한 과거 데이터를 가져와서 전일 대비 등락률 계산
    past = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=10)).strftime('%Y-%m-%d')
    
    indicators_config = [
        ('KOSPI', 'KS11', 'index'),
        ('KOSDAQ', 'KQ11', 'index'),
        ('USD/KRW', 'USD/KRW', 'fx'),
        ('US10Y', 'FRED:DGS10', 'bond'),
        ('WTI', 'FRED:DCOILWTICO', 'commodity'),
    ]
    
    results = []
    
    for name, code, category in indicators_config:
        try:
            df = fdr.DataReader(code, past, date_str)
            # 중복 날짜 제거 (마지막 값 유지)
            df = df[~df.index.duplicated(keep='last')]
            if len(df) >= 2:
                # Close 컬럼이 있는 경우와 없는 경우(FRED) 분기
                if 'Close' in df.columns:
                    close = float(df.iloc[-1]['Close'])
                    prev_close = float(df.iloc[-2]['Close'])
                else:
                    close = float(df.iloc[-1].values[0])
                    prev_close = float(df.iloc[-2].values[0])
                
                change_pct = ((close - prev_close) / prev_close * 100) if prev_close != 0 else 0
                
                # 포맷팅
                if category == 'bond':
                    close_str = f'{close:.2f}%'
                elif category == 'fx':
                    close_str = f'{close:,.1f}'
                elif category == 'commodity':
                    close_str = f'${close:,.2f}'
                elif category == 'volatility':
                    close_str = f'{close:.2f}'
                elif category == 'crypto':
                    close_str = f'${close:,.0f}'
                else:
                    close_str = f'{close:,.2f}'
                
                results.append({
                    'name': name,
                    'close': close_str,
                    'close_raw': round(close, 2),
                    'change_pct': round(change_pct, 2),
                    'category': category,
                    'note': ''
                })
            elif len(df) == 1:
                if 'Close' in df.columns:
                    close = float(df.iloc[-1]['Close'])
                else:
                    close = float(df.iloc[-1].values[0])
                
                if category == 'bond':
                    close_str = f'{close:.2f}%'
                elif category == 'fx':
                    close_str = f'{close:,.1f}'
                elif category == 'commodity':
                    close_str = f'${close:,.2f}'
                elif category == 'volatility':
                    close_str = f'{close:.2f}'
                elif category == 'crypto':
                    close_str = f'${close:,.0f}'
                else:
                    close_str = f'{close:,.2f}'
                
                results.append({
                    'name': name,
                    'close': close_str,
                    'close_raw': round(close, 2),
                    'change_pct': 0,
                    'category': category,
                    'note': ''
                })
        except Exception as e:
            print(f'  [!] {name} 수집 실패: {e}')
            results.append({
                'name': name,
                'close': '-',
                'close_raw': 0,
                'change_pct': 0,
                'category': category,
                'note': '수집실패'
            })
    
    # BTC - yfinance에서 수집 (더 정확)
    try:
        btc_df = yf.download('BTC-USD', period='5d', progress=False)
        if len(btc_df) >= 2:
            btc_close = float(btc_df['Close'].iloc[-1].iloc[0])
            btc_prev = float(btc_df['Close'].iloc[-2].iloc[0])
            btc_chg = ((btc_close - btc_prev) / btc_prev * 100) if btc_prev != 0 else 0
            results.append({
                'name': 'BTC',
                'close': f'${btc_close:,.0f}',
                'close_raw': round(btc_close, 2),
                'change_pct': round(btc_chg, 2),
                'category': 'crypto',
                'note': ''
            })
    except Exception as e:
        print(f'  [!] BTC 수집 실패: {e}')
        results.append({
            'name': 'BTC',
            'close': '-',
            'close_raw': 0,
            'change_pct': 0,
            'category': 'crypto',
            'note': '수집실패'
        })

    # 미국 지표 - yfinance (순서: NASDAQ, S&P500, Russell2000, DOW, VIX)
    us_tickers = [
        ('NASDAQ', '^IXIC', 'index'),
        ('S&P500', '^GSPC', 'index'),
        ('Russell2000', '^RUT', 'index'),
        ('DOW', '^DJI', 'index'),
        ('VIX', '^VIX', 'volatility'),
    ]
    for name, symbol, category in us_tickers:
        try:
            t = yf.Ticker(symbol)
            h = t.history(period='5d')
            if len(h) >= 2:
                close = float(h.iloc[-1]['Close'])
                prev = float(h.iloc[-2]['Close'])
                chg = ((close - prev) / prev * 100) if prev != 0 else 0
                if category == 'volatility':
                    close_str = f'{close:.2f}'
                else:
                    close_str = f'{close:,.2f}'
                results.append({
                    'name': name,
                    'close': close_str,
                    'close_raw': round(close, 2),
                    'change_pct': round(chg, 2),
                    'category': category,
                    'note': ''
                })
            else:
                results.append({'name': name, 'close': '-', 'close_raw': 0, 'change_pct': 0, 'category': category, 'note': ''})
        except Exception as e:
            print(f'  [!] {name} 수집 실패: {e}')
            results.append({'name': name, 'close': '-', 'close_raw': 0, 'change_pct': 0, 'category': category, 'note': '수집실패'})

    # CNN Fear & Greed Index - 비고란에 CNN 링크
    results.append({
        'name': 'CNN Fear & Greed Index',
        'close': '-',
        'close_raw': 0,
        'change_pct': 0,
        'category': 'sentiment',
        'note': 'https://edition.cnn.com/markets/fear-and-greed'
    })
    
    return results


if __name__ == '__main__':
    print('[*] 주요 시장 지표 수집 테스트 v2')
    print('=' * 60)
    indicators = get_market_indicators()
    
    print(f'{"지표":<25} {"종가":<15} {"등락률":<10} {"비고"}')
    print('-' * 60)
    for ind in indicators:
        chg = ind['change_pct']
        chg_str = f'+{chg:.2f}%' if chg >= 0 else f'{chg:.2f}%'
        note = ind['note'] if ind['note'] else ''
        print(f'{ind["name"]:<25} {ind["close"]:<15} {chg_str:<10} {note}')
