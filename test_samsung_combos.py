"""삼성전자(005930) combo별 상세 분석 — 테스트용"""
import os, json, urllib.request
from collections import defaultdict

def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ[k.strip()] = v.strip()
load_env()

URL = os.environ.get('SUPABASE_URL', '')
KEY = os.environ.get('SUPABASE_KEY', '')
SUBJECTS = ['외국인', '연기금', '투신', '사모펀드', '기타법인']
SHORT = {'외국인':'외','연기금':'연','투신':'투','사모펀드':'사','기타법인':'기'}
CODE = '005930'

def get(table, params):
    all_rows = []
    offset = 0
    while True:
        url = f"{URL}/rest/v1/{table}?{params}&limit=1000&offset={offset}&order=id.asc"
        req = urllib.request.Request(url, headers={
            'apikey': KEY, 'Authorization': f'Bearer {KEY}', 'Content-Type': 'application/json'
        })
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            if not data: break
            all_rows.extend(data)
            if len(data) < 1000: break
            offset += 1000
    return all_rows

print(f"삼성전자({CODE}) combo 상세 분석")
print("=" * 70)

# 1) OHLCV
print("OHLCV 로드...")
ohlcv = get('daily_ohlcv', f'select=date,close&stock_code=eq.{CODE}')
price_map = {r['date']: float(r['close']) for r in ohlcv if r.get('close') and float(r['close']) > 0}
all_dates = sorted(price_map.keys())
date_idx = {d: i for i, d in enumerate(all_dates)}
print(f"  {len(ohlcv)}건, {all_dates[0]} ~ {all_dates[-1]}")

# 2) Supply
print("Supply 로드...")
supply = get('daily_supply', f'select=date,subject,direction,amount&stock_code=eq.{CODE}')
print(f"  {len(supply)}건")

# 3) Market (지수 대용)
print("Market 로드...")
market = get('daily_market', f'select=date,market,change_pct&market=eq.KOSPI')
idx_map = defaultdict(float)
idx_count = defaultdict(int)
for r in market:
    try:
        idx_map[r['date']] += float(r['change_pct'])
        idx_count[r['date']] += 1
    except: pass
idx_avg = {d: idx_map[d] / idx_count[d] for d in idx_map if idx_count[d] > 0}

# 4) 매수 주체 맵
buy_map = defaultdict(set)  # {date: set(subjects)}
for r in supply:
    if r.get('direction') == '매수' and r['subject'] in SUBJECTS:
        buy_map[r['date']].add(r['subject'])

# 5) Combo별 수익률 분석
combo_data = defaultdict(lambda: {'d1':[], 'd3':[], 'd5':[], 'd10':[], 'idx_d5':[], 'count': 0})

for dt, buyers in buy_map.items():
    if len(buyers) < 2 or dt not in date_idx:
        continue
    idx = date_idx[dt]
    base = price_map.get(dt)
    if not base or base <= 0:
        continue
    
    returns = {}
    for label, offset in [('d1',1),('d3',3),('d5',5),('d10',10)]:
        ti = idx + offset
        if ti < len(all_dates):
            tp = price_map.get(all_dates[ti])
            if tp and tp > 0:
                returns[label] = (tp - base) / base * 100
    
    if not returns:
        continue
    
    # 지수 D+5 누적
    idx_d5 = 0
    for i in range(1, 6):
        ti = idx + i
        if ti < len(all_dates):
            idx_d5 += idx_avg.get(all_dates[ti], 0)
    
    combo_key = '+'.join(sorted([SHORT[s] for s in buyers]))
    combo_data[combo_key]['count'] += 1
    for label, ret in returns.items():
        combo_data[combo_key][label].append(ret)
    if 'd5' in returns:
        combo_data[combo_key]['idx_d5'].append(idx_d5)

# 6) 결과 출력 (3회 이상만, 초과수익률 기준 정렬)
print(f"\n{'='*70}")
print(f"삼성전자 유효 combo (3회 이상 발생)")
print(f"{'='*70}")

valid = []
for combo, d in combo_data.items():
    if d['count'] < 3:
        continue
    avg_d5 = sum(d['d5']) / len(d['d5']) if d['d5'] else 0
    avg_d1 = sum(d['d1']) / len(d['d1']) if d['d1'] else 0
    avg_d10 = sum(d['d10']) / len(d['d10']) if d['d10'] else 0
    avg_idx = sum(d['idx_d5']) / len(d['idx_d5']) if d['idx_d5'] else 0
    excess = avg_d5 - avg_idx
    win5 = sum(1 for r in d['d5'] if r > 0) / len(d['d5']) * 100 if d['d5'] else 0
    
    grade = 'S' if excess > 0 else 'A' if excess >= -2 else 'B' if excess >= -7 else 'C'
    valid.append({
        'combo': combo, 'count': d['count'], 'avg_d1': avg_d1, 'avg_d5': avg_d5,
        'avg_d10': avg_d10, 'avg_idx': avg_idx, 'excess': excess, 'win5': win5, 'grade': grade
    })

valid.sort(key=lambda x: -x['excess'])

print(f"\n총 유효 combo: {len(valid)}개")
print(f"\n{'순위':>3} {'combo':<20} {'등급':>3} {'횟수':>4} {'D+1':>8} {'D+5':>8} {'D+10':>8} {'지수D+5':>8} {'초과':>8} {'승률D5':>6}")
print("-" * 90)
for i, v in enumerate(valid):
    marker = " ★" if i < 5 else ""
    print(f"{i+1:>3}. {v['combo']:<20} {v['grade']:>3} {v['count']:>4}회 "
          f"{v['avg_d1']:>+7.2f}% {v['avg_d5']:>+7.2f}% {v['avg_d10']:>+7.2f}% "
          f"{v['avg_idx']:>+7.2f}% {v['excess']:>+7.2f}%p {v['win5']:>5.0f}%{marker}")

print(f"\n상위 5개 combo가 차트에 기준선으로 그려집니다 (★ 표시)")
