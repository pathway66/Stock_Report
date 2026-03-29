"""
🔮 AI+패스웨이 — 종목별 Best/Dominant Combo 분석기
===================================================
Supabase daily_supply + daily_ohlcv 데이터를 사용하여
종목별로 어떤 주체 조합이 매수했을 때 수익률이 가장 좋았는지 분석

사용법:
  python analyze_combo_grades.py

필요 환경변수 (.env):
  SUPABASE_URL, SUPABASE_KEY
"""

import os
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import combinations

# ============================================================
# Supabase 연결
# ============================================================
def load_env():
    """Load .env file"""
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
    """Supabase REST API GET — 1000건씩 페이지네이션"""
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
                raw = resp.read().decode()
                data = json.loads(raw)
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
    """Supabase REST API POST (insert)"""
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

# ============================================================
# 데이터 로드
# ============================================================
def load_data():
    print("=" * 60)
    print("🔮 종목별 Best/Dominant Combo 분석기")
    print(f"   시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1) daily_supply 로드
    print("\n📊 STEP 1: daily_supply 로드...")
    supply_raw = supabase_get('daily_supply', 'select=stock_code,stock_name,date,subject,direction,amount')
    print(f"   → {len(supply_raw):,}건")

    # 2) daily_ohlcv 로드
    print("\n📊 STEP 2: daily_ohlcv 로드...")
    ohlcv_raw = supabase_get('daily_ohlcv', 'select=stock_code,date,close')
    print(f"   → {len(ohlcv_raw):,}건")

    return supply_raw, ohlcv_raw

# ============================================================
# 분석 로직
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

def combo_label(subjects_set):
    """조합을 레이블로 변환: 외+연+사 → S(외+연+사)"""
    sorted_subs = sorted(subjects_set, key=lambda s: SUBJECTS.index(s))
    short_name = '+'.join(SHORT[s] for s in sorted_subs)
    grade = COMBO_GRADES.get(frozenset(subjects_set), '')
    if grade:
        return f"{grade}({short_name})"
    return short_name

def analyze(supply_raw, ohlcv_raw):
    print("\n🔍 STEP 3: 데이터 정리...")

    # OHLCV → {stock_code: {date: close}}
    price_map = defaultdict(dict)
    for row in ohlcv_raw:
        code = row['stock_code']
        dt = row['date']
        cl = row.get('close')
        if cl and float(cl) > 0:
            price_map[code][dt] = float(cl)

    # 거래일 목록 (정렬)
    all_dates = sorted(set(d for code_dates in price_map.values() for d in code_dates))
    date_index = {d: i for i, d in enumerate(all_dates)}
    print(f"   거래일: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)}일)")

    # Supply → {stock_code: {date: set(순매수 주체들)}}
    # direction이 '순매수'인 주체만 "매수"로 간주
    supply_map = defaultdict(lambda: defaultdict(set))
    stock_names = {}
    for row in supply_raw:
        code = row['stock_code']
        dt = row['date']
        subj = row['subject']
        direction = row.get('direction', '')
        if direction == '매수' and subj in SUBJECTS:
            supply_map[code][dt].add(subj)
        if code not in stock_names:
            stock_names[code] = row.get('stock_name', code)

    print(f"   수급 데이터 종목: {len(supply_map):,}개")

    # ============================================================
    # 종목별 분석
    # ============================================================
    print("\n🔍 STEP 4: 종목별 combo 수익률 분석...")

    results = []
    analyzed = 0
    skipped = 0

    for code in supply_map:
        if code not in price_map:
            skipped += 1
            continue

        stock_dates = supply_map[code]
        stock_prices = price_map[code]
        stock_name = stock_names.get(code, code)

        # 이 종목에서 발생한 모든 combo 이벤트 수집
        combo_returns = defaultdict(lambda: {'d1': [], 'd3': [], 'd5': [], 'd10': []})
        combo_counts = defaultdict(int)

        for dt, buyers in stock_dates.items():
            if len(buyers) < 2:
                continue  # 2주체 이상만 분석

            if dt not in date_index:
                continue

            idx = date_index[dt]
            base_price = stock_prices.get(dt)
            if not base_price or base_price <= 0:
                continue

            # D+1, D+3, D+5, D+10 수익률 계산
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

            # 이 날의 매수 조합
            combo_key = frozenset(buyers)
            combo_counts[combo_key] += 1
            for d_label, ret in returns.items():
                combo_returns[combo_key][d_label].append(ret)

        # 유효한 combo 분석 (최소 3회 이상 발생)
        valid_combos = []
        for combo_key, counts in combo_counts.items():
            if counts < 3:
                continue
            rets = combo_returns[combo_key]
            avg_d1 = sum(rets['d1']) / len(rets['d1']) if rets['d1'] else 0
            avg_d3 = sum(rets['d3']) / len(rets['d3']) if rets['d3'] else 0
            avg_d5 = sum(rets['d5']) / len(rets['d5']) if rets['d5'] else 0
            avg_d10 = sum(rets['d10']) / len(rets['d10']) if rets['d10'] else 0
            win_d1 = sum(1 for r in rets['d1'] if r > 0) / len(rets['d1']) * 100 if rets['d1'] else 0
            win_d5 = sum(1 for r in rets['d5'] if r > 0) / len(rets['d5']) * 100 if rets['d5'] else 0

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
            })

        if not valid_combos:
            skipped += 1
            continue

        # Best combo (D+5 수익률 기준)
        best = max(valid_combos, key=lambda x: x['avg_d5'])
        # Worst combo
        worst = min(valid_combos, key=lambda x: x['avg_d5'])
        # Dominant combo (가장 많이 출현)
        dominant = max(valid_combos, key=lambda x: x['count'])

        # 종목 전체 평균 (모든 combo 통합)
        all_d5 = []
        for vc in valid_combos:
            all_d5.extend(combo_returns[vc['combo_key']]['d5'])
        overall_avg = sum(all_d5) / len(all_d5) if all_d5 else 0
        overall_win = sum(1 for r in all_d5 if r > 0) / len(all_d5) * 100 if all_d5 else 0

        # Grade 결정 (D+5 기준)
        if best['avg_d5'] >= 3.0 and best['win_rate_d5'] >= 60:
            grade = 'S'
        elif best['avg_d5'] >= 1.5 and best['win_rate_d5'] >= 50:
            grade = 'A'
        elif best['avg_d5'] >= 0.5 and best['win_rate_d5'] >= 40:
            grade = 'B'
        elif best['avg_d5'] >= 0:
            grade = 'C'
        else:
            grade = 'D'

        total_events = sum(vc['count'] for vc in valid_combos)

        results.append({
            'stock_code': code,
            'stock_name': stock_name,
            'stock_index': '',
            'grade': grade,
            'success_rate': round(overall_win, 1),
            'avg_mdd': 0,
            'avg_return': round(overall_avg, 2),
            'best_combo': best['label'],
            'dominant_combo': dominant['label'],
            'worst_combo': worst['label'],
            'total_events': total_events,
            'backtest_date': datetime.now().strftime('%Y-%m-%d'),
            'avg_d1': best['avg_d1'],
            'avg_d3': best['avg_d3'],
            'avg_d5': best['avg_d5'],
            'avg_d10': best['avg_d10'],
            'win_rate_d1': best['win_rate_d1'],
            'win_rate_d5': best['win_rate_d5'],
            'analyzed_days': len(all_dates),
            'period_start': all_dates[0],
            'period_end': all_dates[-1],
        })
        analyzed += 1

    print(f"   분석 완료: {analyzed}종목 / 스킵: {skipped}종목")
    return results

# ============================================================
# 결과 출력 & 저장
# ============================================================
def print_summary(results):
    print("\n" + "=" * 70)
    print("📊 종목별 Best Combo 분석 결과")
    print("=" * 70)

    # Grade 분포
    grade_dist = defaultdict(int)
    for r in results:
        grade_dist[r['grade']] += 1

    print(f"\n등급 분포:")
    for g in ['S', 'A', 'B', 'C', 'D']:
        cnt = grade_dist.get(g, 0)
        bar = '█' * (cnt // 5)
        print(f"  {g}등급: {cnt:>4}종목 {bar}")

    # Best combo 분포
    print(f"\nBest combo TOP10 (가장 많은 종목에서 best인 조합):")
    combo_dist = defaultdict(int)
    for r in results:
        combo_dist[r['best_combo']] += 1
    for combo, cnt in sorted(combo_dist.items(), key=lambda x: -x[1])[:10]:
        print(f"  {combo:<25s} {cnt:>4}종목")

    # Dominant combo 분포
    print(f"\nDominant combo TOP10 (가장 빈번한 조합):")
    dom_dist = defaultdict(int)
    for r in results:
        dom_dist[r['dominant_combo']] += 1
    for combo, cnt in sorted(dom_dist.items(), key=lambda x: -x[1])[:10]:
        print(f"  {combo:<25s} {cnt:>4}종목")

    # S등급 종목 리스트
    s_grade = [r for r in results if r['grade'] == 'S']
    if s_grade:
        print(f"\n🏆 S등급 종목 ({len(s_grade)}개):")
        s_grade.sort(key=lambda x: -x['avg_d5'])
        for r in s_grade[:20]:
            print(f"  {r['stock_name']:<20s} best={r['best_combo']:<20s} "
                  f"D+5={r['avg_d5']:>+7.2f}% 승률={r['win_rate_d5']:.0f}% "
                  f"이벤트={r['total_events']}회")

    # A등급 TOP10
    a_grade = sorted([r for r in results if r['grade'] == 'A'], key=lambda x: -x['avg_d5'])
    if a_grade:
        print(f"\n⭐ A등급 TOP10:")
        for r in a_grade[:10]:
            print(f"  {r['stock_name']:<20s} best={r['best_combo']:<20s} "
                  f"D+5={r['avg_d5']:>+7.2f}% 승률={r['win_rate_d5']:.0f}%")

def save_to_supabase(results):
    print(f"\n💾 STEP 5: Supabase sr_supply_grades에 저장...")
    
    # 50개씩 배치 insert
    batch_size = 50
    total = 0
    for i in range(0, len(results), batch_size):
        batch = results[i:i+batch_size]
        status = supabase_insert('sr_supply_grades', batch)
        if status:
            total += len(batch)
            if total % 200 == 0 or i + batch_size >= len(results):
                print(f"    ... {total}/{len(results)}건 저장")
        else:
            # 첫 실패시 1건씩 시도해서 문제 행 찾기
            print(f"    ⚠️ 배치 실패, 1건씩 재시도...")
            for row in batch:
                s = supabase_insert('sr_supply_grades', [row])
                if s:
                    total += 1
            break  # 에러 원인 확인 후 계속

    print(f"   → 총 {total}건 저장 완료!")

# ============================================================
# 메인
# ============================================================
def main():
    supply_raw, ohlcv_raw = load_data()
    results = analyze(supply_raw, ohlcv_raw)
    print_summary(results)
    save_to_supabase(results)

    elapsed = datetime.now()
    print(f"\n{'='*60}")
    print(f"🎉 분석 완료! {elapsed.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   총 {len(results)}종목 분석 → sr_supply_grades 저장")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
