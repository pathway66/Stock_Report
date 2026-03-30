"""
🔮 AI+패스웨이 — 종목별 Best/Dominant Combo 분석기 v2
=====================================================
v2 변경사항:
  - 시총 필터: 코스피 8000억+, 코스닥 4000억+
  - 등급: 지수 초과수익률 기반 (S/A/B/C)
  - daily_market에서 시총 + 지수 대용 수익률 산출

사용법: python analyze_combo_grades.py
"""

import os
import json
import urllib.request
from datetime import datetime
from collections import defaultdict

# ============================================================
# Supabase 연결
# ============================================================
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ[key.strip()] = val.strip()

load_env()

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ SUPABASE_URL 또는 SUPABASE_KEY가 .env에 없습니다.")
    exit(1)

def supabase_get(table, params='', page_size=1000):
    all_rows = []
    offset = 0
    while True:
        sep = '&' if params else ''
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}{sep}limit={page_size}&offset={offset}&order=id.asc"
        req = urllib.request.Request(url, headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type': 'application/json',
        })
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())
                if not data:
                    break
                all_rows.extend(data)
                if len(all_rows) % 10000 == 0 or len(data) < page_size:
                    print(f"    ... {len(all_rows):,}건 로드")
                if len(data) < page_size:
                    break
                offset += page_size
        except Exception as e:
            print(f"❌ API 오류 (offset={offset}): {e}")
            break
    return all_rows

def supabase_insert(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(rows, ensure_ascii=False, default=str).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST', headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"❌ Insert 오류: {e.code} — {body[:200]}")
        return None
    except Exception as e:
        print(f"❌ Insert 오류: {e}")
        return None

def supabase_delete(table, params=''):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    req = urllib.request.Request(url, method='DELETE', headers={
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except Exception as e:
        print(f"❌ Delete 오류: {e}")
        return None

# ============================================================
# 상수
# ============================================================
SUBJECTS = ['외국인', '연기금', '투신', '사모펀드', '기타법인']
SHORT = {'외국인': '외', '연기금': '연', '투신': '투', '사모펀드': '사', '기타법인': '기'}

COMBO_GRADES = {
    frozenset(['외국인', '연기금', '사모펀드']): 'S',
    frozenset(['외국인', '연기금']): 'A1',
    frozenset(['외국인', '사모펀드']): 'A2',
    frozenset(['외국인', '투신']): 'B1',
    frozenset(['연기금', '사모펀드']): 'B2',
    frozenset(['외국인', '기타법인']): 'C1',
    frozenset(['연기금', '투신']): 'C2',
}

# 시총 필터 (원 단위)
KOSPI_MIN_CAP = 800_000_000_000    # 8,000억
KOSDAQ_MIN_CAP = 400_000_000_000   # 4,000억

def combo_label(subjects_set):
    sorted_subs = sorted(subjects_set, key=lambda s: SUBJECTS.index(s))
    short_name = '+'.join(SHORT[s] for s in sorted_subs)
    grade = COMBO_GRADES.get(frozenset(subjects_set), '')
    if grade:
        return f"{grade}({short_name})"
    return short_name

# ============================================================
# 데이터 로드
# ============================================================
def load_data():
    print("=" * 60)
    print("🔮 종목별 Best/Dominant Combo 분석기 v2")
    print(f"   시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   시총 필터: 코스피 8,000억+ / 코스닥 4,000억+")
    print("   등급 기준: 지수 초과수익률 기반")
    print("=" * 60)

    # 1) daily_supply
    print("\n📊 STEP 1: daily_supply 로드...")
    supply_raw = supabase_get('daily_supply', 'select=stock_code,stock_name,date,subject,direction,amount')
    print(f"   → {len(supply_raw):,}건")

    # 2) daily_ohlcv
    print("\n📊 STEP 2: daily_ohlcv 로드...")
    ohlcv_raw = supabase_get('daily_ohlcv', 'select=stock_code,date,close')
    print(f"   → {len(ohlcv_raw):,}건")

    # 3) daily_market (시총 + 시장구분 + 등락률)
    print("\n📊 STEP 3: daily_market 로드 (시총 + 지수 대용)...")
    market_raw = supabase_get('daily_market', 'select=stock_code,stock_name,date,market,market_cap,change_pct')
    print(f"   → {len(market_raw):,}건")

    return supply_raw, ohlcv_raw, market_raw

# ============================================================
# 분석
# ============================================================
def analyze(supply_raw, ohlcv_raw, market_raw):
    
    # ----- 시총 필터 대상 종목 추출 (최신 날짜 기준) -----
    print("\n🔍 STEP 4: 시총 필터 적용...")
    
    # 최신 날짜 찾기
    market_dates = set(r['date'] for r in market_raw if r.get('date'))
    latest_date = max(market_dates) if market_dates else None
    print(f"   시총 기준일: {latest_date}")
    
    # 시총 필터 적용
    target_stocks = {}  # {stock_code: (stock_name, market)}
    for r in market_raw:
        if r['date'] != latest_date:
            continue
        code = r['stock_code']
        mkt = r.get('market', '')
        cap = r.get('market_cap') or 0
        if isinstance(cap, str):
            cap = float(cap)
        
        if mkt == 'KOSPI' and cap >= KOSPI_MIN_CAP:
            target_stocks[code] = (r.get('stock_name', code), mkt)
        elif mkt == 'KOSDAQ' and cap >= KOSDAQ_MIN_CAP:
            target_stocks[code] = (r.get('stock_name', code), mkt)
    
    print(f"   코스피 8,000억+: {sum(1 for v in target_stocks.values() if v[1]=='KOSPI')}종목")
    print(f"   코스닥 4,000억+: {sum(1 for v in target_stocks.values() if v[1]=='KOSDAQ')}종목")
    print(f"   분석 대상 합계: {len(target_stocks)}종목")
    
    # ----- 지수 대용 수익률 계산 (코스피/코스닥 일별 평균 등락률) -----
    print("\n🔍 STEP 5: 지수 대용 수익률 계산...")
    
    # {date: {market: avg_change_pct}}
    market_daily = defaultdict(lambda: defaultdict(list))
    for r in market_raw:
        mkt = r.get('market', '')
        pct = r.get('change_pct')
        if mkt and pct is not None:
            try:
                market_daily[r['date']][mkt].append(float(pct))
            except:
                pass
    
    # 일별 시장 평균 수익률
    index_returns = {}  # {date: {KOSPI: avg, KOSDAQ: avg}}
    for dt, mkts in market_daily.items():
        index_returns[dt] = {}
        for mkt, pcts in mkts.items():
            index_returns[dt][mkt] = sum(pcts) / len(pcts) if pcts else 0
    
    # ----- OHLCV → 가격맵 -----
    print("\n🔍 STEP 6: 가격 데이터 정리...")
    price_map = defaultdict(dict)
    for row in ohlcv_raw:
        code = row['stock_code']
        if code not in target_stocks:
            continue
        cl = row.get('close')
        if cl and float(cl) > 0:
            price_map[code][row['date']] = float(cl)
    
    all_dates = sorted(set(d for code_dates in price_map.values() for d in code_dates))
    date_index = {d: i for i, d in enumerate(all_dates)}
    print(f"   거래일: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)}일)")
    print(f"   가격 데이터 보유 종목: {len(price_map):,}개")
    
    # ----- Supply → 매수 주체 맵 -----
    supply_map = defaultdict(lambda: defaultdict(set))
    stock_names = {}
    for row in supply_raw:
        code = row['stock_code']
        if code not in target_stocks:
            continue
        dt = row['date']
        subj = row['subject']
        direction = row.get('direction', '')
        if direction == '매수' and subj in SUBJECTS:
            supply_map[code][dt].add(subj)
        if code not in stock_names:
            stock_names[code] = row.get('stock_name', code)
    
    print(f"   수급 데이터 보유 종목: {len(supply_map):,}개")
    
    # ----- 지수 누적 수익률 계산 (D+N) -----
    # 각 날짜에서 D+5 지수 수익률 미리 계산
    def calc_index_cumulative(dt, market, offset):
        """dt부터 offset 거래일 후까지의 지수 누적 수익률"""
        idx = date_index.get(dt)
        if idx is None:
            return None
        total = 0
        for i in range(1, offset + 1):
            target_idx = idx + i
            if target_idx < len(all_dates):
                target_dt = all_dates[target_idx]
                if target_dt in index_returns and market in index_returns[target_dt]:
                    total += index_returns[target_dt][market]
        return total
    
    # ----- 종목별 분석 -----
    print("\n🔍 STEP 7: 종목별 combo 수익률 분석...")
    
    results = []
    analyzed = 0
    skipped = 0
    
    for code in target_stocks:
        if code not in supply_map or code not in price_map:
            skipped += 1
            continue
        
        stock_name = target_stocks[code][0]
        stock_market = target_stocks[code][1]
        stock_dates = supply_map[code]
        stock_prices = price_map[code]
        
        combo_returns = defaultdict(lambda: {'d1': [], 'd3': [], 'd5': [], 'd10': []})
        combo_index_returns = defaultdict(lambda: {'d5': []})  # 지수 수익률도 저장
        combo_counts = defaultdict(int)
        
        for dt, buyers in stock_dates.items():
            if len(buyers) < 2:
                continue
            if dt not in date_index:
                continue
            
            idx = date_index[dt]
            base_price = stock_prices.get(dt)
            if not base_price or base_price <= 0:
                continue
            
            returns = {}
            for d_label, d_offset in [('d1', 1), ('d3', 3), ('d5', 5), ('d10', 10)]:
                target_idx = idx + d_offset
                if target_idx < len(all_dates):
                    target_date = all_dates[target_idx]
                    target_price = stock_prices.get(target_date)
                    if target_price and target_price > 0:
                        returns[d_label] = (target_price - base_price) / base_price * 100
            
            if not returns:
                continue
            
            # 지수 D+5 수익률
            idx_d5 = calc_index_cumulative(dt, stock_market, 5)
            
            combo_key = frozenset(buyers)
            combo_counts[combo_key] += 1
            for d_label, ret in returns.items():
                combo_returns[combo_key][d_label].append(ret)
            if idx_d5 is not None and 'd5' in returns:
                combo_index_returns[combo_key]['d5'].append(idx_d5)
        
        # 유효 combo (3회 이상)
        valid_combos = []
        for combo_key, counts in combo_counts.items():
            if counts < 3:
                continue
            rets = combo_returns[combo_key]
            idx_rets = combo_index_returns[combo_key]
            
            avg_d1 = sum(rets['d1']) / len(rets['d1']) if rets['d1'] else 0
            avg_d3 = sum(rets['d3']) / len(rets['d3']) if rets['d3'] else 0
            avg_d5 = sum(rets['d5']) / len(rets['d5']) if rets['d5'] else 0
            avg_d10 = sum(rets['d10']) / len(rets['d10']) if rets['d10'] else 0
            win_d1 = sum(1 for r in rets['d1'] if r > 0) / len(rets['d1']) * 100 if rets['d1'] else 0
            win_d5 = sum(1 for r in rets['d5'] if r > 0) / len(rets['d5']) * 100 if rets['d5'] else 0
            
            # 지수 D+5 평균
            avg_idx_d5 = sum(idx_rets['d5']) / len(idx_rets['d5']) if idx_rets['d5'] else 0
            # 초과수익률
            excess_d5 = avg_d5 - avg_idx_d5
            
            valid_combos.append({
                'combo_key': combo_key,
                'label': combo_label(combo_key),
                'count': counts,
                'avg_d1': round(avg_d1, 4),
                'avg_d3': round(avg_d3, 4),
                'avg_d5': round(avg_d5, 4),
                'avg_d10': round(avg_d10, 4),
                'win_rate_d1': round(win_d1, 4),
                'win_rate_d5': round(win_d5, 4),
                'avg_idx_d5': round(avg_idx_d5, 4),
                'excess_d5': round(excess_d5, 4),
            })
        
        if not valid_combos:
            skipped += 1
            continue
        
        # 초과수익률 기준 정렬 → 상위 5개
        valid_combos.sort(key=lambda x: -x['excess_d5'])
        top5 = valid_combos[:5]
        total_valid = len(valid_combos)
        
        for rank, vc in enumerate(top5, 1):
            ex = vc['excess_d5']
            grade = 'S' if ex > 0 else 'A' if ex >= -2 else 'B' if ex >= -7 else 'C'
            
            results.append({
                'stock_code': code,
                'stock_name': stock_name,
                'market': stock_market,
                'combo': vc['label'],
                'combo_rank': rank,
                'grade': grade,
                'avg_d1': vc['avg_d1'],
                'avg_d3': vc['avg_d3'],
                'avg_d5': vc['avg_d5'],
                'avg_d10': vc['avg_d10'],
                'index_d5': vc['avg_idx_d5'],
                'excess_d5': vc['excess_d5'],
                'win_rate_d5': vc['win_rate_d5'],
                'combo_count': vc['count'],
                'is_best': rank == 1,
                'total_combos': total_valid,
                'analyzed_days': len(all_dates),
                'period_start': all_dates[0] if all_dates else None,
                'period_end': all_dates[-1] if all_dates else None,
                'backtest_date': datetime.now().strftime('%Y-%m-%d'),
            })
        
        analyzed += 1
        analyzed += 1
    
    print(f"   분석 완료: {analyzed}종목 / 스킵: {skipped}종목")
    return results

# ============================================================
# 출력
# ============================================================
def print_summary(results):
    print("\n" + "=" * 70)
    print("📊 종목별 Best Combo 분석 결과 (v3 — 종목당 상위 5개 combo)")
    print("=" * 70)
    
    # best combo만 추출 (combo_rank == 1)
    best_only = [r for r in results if r['is_best']]
    total_stocks = len(best_only)
    total_rows = len(results)
    
    print(f"\n분석 종목: {total_stocks}개 / 총 저장 행: {total_rows}개 (종목당 최대 5개 combo)")
    
    # 등급 분포 (combo별)
    grade_dist = defaultdict(int)
    for r in results:
        grade_dist[r['grade']] += 1
    
    print(f"\n전체 combo 등급 분포:")
    for g in ['S', 'A', 'B', 'C']:
        cnt = grade_dist.get(g, 0)
        pct = cnt / total_rows * 100 if total_rows else 0
        bar = '█' * (cnt // 5)
        print(f"  {g}등급: {cnt:>4}건 ({pct:.0f}%) {bar}")
    
    # Best combo(rank1) 분포
    print(f"\nBest combo TOP10 (rank=1 종목 기준):")
    combo_dist = defaultdict(int)
    for r in best_only:
        combo_dist[r['combo']] += 1
    for combo, cnt in sorted(combo_dist.items(), key=lambda x: -x[1])[:10]:
        print(f"  {combo:<25s} {cnt:>4}종목")
    
    # S등급 best combo TOP20
    s_best = sorted([r for r in best_only if r['grade'] == 'S'], key=lambda x: -x['excess_d5'])
    if s_best:
        print(f"\n🏆 S등급 종목 TOP20 ({len(s_best)}개 중):")
        for r in s_best[:20]:
            stars = '★★★'
            print(f"  {stars} {r['stock_name']:<16s} [{r['market']}] {r['combo']:<18s} "
                  f"D+5={r['avg_d5']:>+7.2f}% {r['market']}대비={r['excess_d5']:>+6.2f}%p "
                  f"승률={r['win_rate_d5']:.0f}% ({r['combo_count']}회)")
    
    # 종목별 combo 수 분포
    combo_counts = defaultdict(int)
    for r in results:
        combo_counts[r['stock_code']] += 1
    avg_combos = sum(combo_counts.values()) / len(combo_counts) if combo_counts else 0
    print(f"\n종목당 combo 수: 평균 {avg_combos:.1f}개, 최대 {max(combo_counts.values()) if combo_counts else 0}개")
    
    # 샘플 출력 (삼성전자가 있으면)
    samsung = [r for r in results if r['stock_code'] == '005930']
    if samsung:
        print(f"\n📋 삼성전자 상위 combo:")
        for r in samsung:
            stars = '★★★' if r['grade'] == 'S' else '★★☆' if r['grade'] == 'A' else '★☆☆' if r['grade'] == 'B' else '☆☆☆'
            print(f"  #{r['combo_rank']} {stars} {r['combo']:<20s} "
                  f"D+5={r['avg_d5']:>+7.2f}% 코스피대비={r['excess_d5']:>+6.2f}%p 승률={r['win_rate_d5']:.0f}% ({r['combo_count']}회)")

def save_to_supabase(results):
    print(f"\n💾 STEP 8: Supabase sr_supply_grades 저장...")
    
    # 기존 데이터 삭제
    print("   기존 데이터 삭제...")
    supabase_delete('sr_supply_grades', 'id=gt.0')
    
    # 50개씩 배치 insert
    batch_size = 50
    total = 0
    for i in range(0, len(results), batch_size):
        batch = results[i:i+batch_size]
        status = supabase_insert('sr_supply_grades', batch)
        if status:
            total += len(batch)
            if total % 100 == 0 or i + batch_size >= len(results):
                print(f"    ... {total}/{len(results)}건 저장")
        else:
            print(f"    ⚠️ 배치 실패, 1건씩 재시도...")
            for row in batch:
                s = supabase_insert('sr_supply_grades', [row])
                if s:
                    total += 1
            break
    
    print(f"   → 총 {total}건 저장 완료!")

# ============================================================
# 메인
# ============================================================
def main():
    supply_raw, ohlcv_raw, market_raw = load_data()
    results = analyze(supply_raw, ohlcv_raw, market_raw)
    print_summary(results)
    save_to_supabase(results)
    
    print(f"\n{'='*60}")
    print(f"🎉 분석 완료! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   총 {len(results)}종목 분석 → sr_supply_grades 저장")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
