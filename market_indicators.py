"""
시장 주요지표 수집기
- FinanceDataReader를 이용한 9개 지표 수집
- blog_posts의 content에 market_indicators 섹션으로 저장
"""
import FinanceDataReader as fdr
from datetime import datetime, timedelta

def get_market_indicators(date_str=None):
    """주요 시장 지표 수집. date_str: 'YYYY-MM-DD' 형식"""
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    # 최근 5일치 데이터를 가져와서 마지막 2일로 등락률 계산
    past = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
    
    indicators_config = [
        ('KOSPI', 'KS11', 'index'),
        ('KOSDAQ', 'KQ11', 'index'),
        ('USD/KRW', 'USD/KRW', 'fx'),
        ('WTI', 'FRED:DCOILWTICO', 'commodity'),
        ('S&P500', 'S&P500', 'index'),
        ('NASDAQ', 'IXIC', 'index'),
        ('DOW', 'DJI', 'index'),
        ('VIX', 'VIX', 'volatility'),
        ('US10Y', 'FRED:DGS10', 'bond'),
    ]
    
    results = []
    
    for name, code, category in indicators_config:
        try:
            df = fdr.DataReader(code, past, date_str)
            if len(df) >= 2:
                # 마지막 값과 전일 값
                if 'Close' in df.columns:
                    close = float(df.iloc[-1]['Close'])
                    prev_close = float(df.iloc[-2]['Close'])
                else:
                    # FRED 데이터는 컬럼명이 다를 수 있음
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
    
    # F&G Index는 비고란에 링크
    results.append({
        'name': 'F&G Index',
        'close': '-',
        'close_raw': 0,
        'change_pct': 0,
        'category': 'sentiment',
        'note': 'https://edition.cnn.com/markets/fear-and-greed'
    })
    
    return results


def format_indicators_telegram(indicators):
    """텔레그램 메시지용 포맷"""
    msg = '<b>[주요지표]</b>\n'
    for ind in indicators:
        if ind['name'] == 'F&G Index':
            continue
        chg = ind['change_pct']
        chg_str = f'+{chg:.2f}%' if chg >= 0 else f'{chg:.2f}%'
        msg += f'{ind["name"]}: {ind["close"]} ({chg_str})\n'
    return msg


if __name__ == '__main__':
    print('[*] 주요 시장 지표 수집 테스트')
    print('=' * 50)
    indicators = get_market_indicators()
    
    print(f'{"지표":<10} {"종가":<15} {"등락률":<10} {"비고"}')
    print('-' * 50)
    for ind in indicators:
        chg = ind['change_pct']
        chg_str = f'+{chg:.2f}%' if chg >= 0 else f'{chg:.2f}%'
        note = ind['note'] if ind['note'] else ''
        print(f'{ind["name"]:<10} {ind["close"]:<15} {chg_str:<10} {note}')
    
    print('\n[텔레그램 메시지 미리보기]')
    print(format_indicators_telegram(indicators))
