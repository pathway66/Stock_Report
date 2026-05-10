"""
[*] guru_signals 백필 + 매일 갱신
=====================================
9개 투자 그루 패턴 시그널을 추출해 guru_signals 테이블에 저장.

사용법:
  python backfill_guru_signals.py                   # 1년치(25/5/8~26/5/8) 백필
  python backfill_guru_signals.py --backfill-from 2024-01-01
  python backfill_guru_signals.py --date 20260508   # 특정일만 (run_daily 통합용)

패턴:
  P1: O'Neil Pivot Buy (20D 신고가 돌파)
  P2: Minervini VCP (변동성 수축 후 폭발)
  P3: Wyckoff Re-accumulation (깊은 조정 후 재상승)
  P4: O'Neil Volume Surge (강력 거래량 양봉, 광범위)
  P5: Smart Money Triple Buy (사모+연기금+외인 동시 매수)
  P6: Follow-Through Day (Shawn 매수법)
  P7: Darvas Box Theory (52주 신고가 돌파)
  P8: Weinstein Stage 2 (30주 MA 거래량 동반 돌파)
  P9: Livermore Pivotal Points (50일 직전 고점 돌파)
"""

import os
import sys
import time
import json
import argparse
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
WRITE_HEADERS = {**HEADERS, "Content-Type": "application/json",
                 "Prefer": "return=minimal,resolution=ignore-duplicates"}

PATTERN_NAMES = {
    'P1': "O'Neil Pivot Buy",
    'P2': "Minervini VCP",
    'P3': "Wyckoff Re-accumulation",
    'P4': "O'Neil Volume Surge",
    'P5': "Smart Money Triple Buy",
    'P6': "Follow-Through Day",
    'P7': "Darvas Box Theory",
    'P8': "Weinstein Stage 2",
    'P9': "Livermore Pivotal Points",
}


def sb_get(table, params, page_size=1000, raise_on_timeout=False):
    """페이지네이션 + timeout 감지. raise_on_timeout=True면 호출자가 split-retry 가능."""
    rows, offset = [], 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={page_size}&offset={offset}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code != 200:
                msg = r.text[:200]
                if '57014' in msg or 'statement timeout' in msg:
                    if raise_on_timeout:
                        raise TimeoutError(f"PG timeout at offset={offset}")
                    print(f"  [E] timeout offset={offset}")
                else:
                    print(f"  [E] {r.status_code} {msg}")
                break
            chunk = r.json()
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        except TimeoutError:
            raise
        except Exception as e:
            print(f"  [X] {e}"); break
    return rows


def sb_get_split(table, base_params_no_date, start_date, end_date, init_days=30, min_days=3):
    """날짜 윈도우 split-retry로 timeout 회피.
    timeout 발생 시 윈도우 절반으로 줄여 재시도. 재귀 split."""
    def fetch(s_iso, e_iso, days):
        params = f"date=gte.{s_iso}&date=lt.{e_iso}&{base_params_no_date}"
        try:
            return sb_get(table, params, raise_on_timeout=True)
        except TimeoutError:
            if days <= min_days:
                print(f"  [W] {s_iso}~{e_iso} timeout (min {min_days}d 미달, 일부 누락)")
                # 최소 윈도우에서도 timeout이면 단일 일자로 시도
                results = []
                cur = datetime.strptime(s_iso, "%Y-%m-%d")
                end_dt = datetime.strptime(e_iso, "%Y-%m-%d")
                while cur < end_dt:
                    nxt = cur + timedelta(days=1)
                    p = f"date=eq.{cur.strftime('%Y-%m-%d')}&{base_params_no_date}"
                    try:
                        results.extend(sb_get(table, p))
                    except Exception:
                        pass
                    cur = nxt
                return results
            # 절반으로 split
            mid_days = days // 2
            mid = (datetime.strptime(s_iso, "%Y-%m-%d") + timedelta(days=mid_days)).strftime("%Y-%m-%d")
            print(f"  [retry] {s_iso}~{e_iso} → split @{mid}")
            return fetch(s_iso, mid, mid_days) + fetch(mid, e_iso, days - mid_days)

    rows = []
    cur = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    while cur < end_dt:
        nxt_dt = min(cur + timedelta(days=init_days), end_dt + timedelta(days=1))
        s = cur.strftime("%Y-%m-%d")
        e = nxt_dt.strftime("%Y-%m-%d")
        chunk = fetch(s, e, init_days)
        print(f"    {s}~{e}: {len(chunk):,} rows")
        rows.extend(chunk)
        cur = nxt_dt
    return rows


def sb_insert(table, rows, batch=500):
    """on_conflict ignore 으로 배치 INSERT"""
    if not rows:
        return 0
    inserted = 0
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=date,stock_code,pattern_id"
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        try:
            r = requests.post(url, headers=WRITE_HEADERS, json=chunk, timeout=60)
            if r.status_code in (200, 201, 204):
                inserted += len(chunk)
            else:
                print(f"  [W] insert {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"  [X] {e}")
    return inserted


def sb_delete(table, params):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    r = requests.delete(url, headers=WRITE_HEADERS, timeout=60)
    return r.status_code in (200, 204)


# ---------- 데이터 로딩 ----------

def load_stock_data(start, end):
    """daily_supply_v2 (subject=개인) — split-retry로 안전하게 페치"""
    print(f"[1] 종목 OHLCV 로딩 ({start}~{end})...")
    base = "subject=eq.개인&select=date,stock_code,stock_name,market,open,high,low,close,volume,market_cap"
    rows = sb_get_split('daily_supply_v2', base, start, end, init_days=30, min_days=3)
    data = {}
    for r in rows:
        sc = r['stock_code']
        if sc not in data:
            data[sc] = []
        data[sc].append({
            'date': r['date'],
            'stock_name': r.get('stock_name'),
            'market': r.get('market'),
            'open': r.get('open') or 0,
            'high': r.get('high') or 0,
            'low': r.get('low') or 0,
            'close': r.get('close') or 0,
            'volume': r.get('volume') or 0,
            'market_cap': r.get('market_cap') or 0,
        })
    for sc in data:
        data[sc].sort(key=lambda x: x['date'])
    print(f"    [OK] {len(data):,} 종목 (총 {len(rows):,} rows)")
    return data


def load_smart_money(start, end):
    """5주체 수급 — split-retry"""
    print(f"[2] 5주체 수급 로딩 ({start}~{end})...")
    base = "subject=in.(사모펀드,연기금,외국인)&select=date,stock_code,subject,buy_amt,sell_amt"
    rows = sb_get_split('daily_supply_v2', base, start, end, init_days=30, min_days=3)
    bucket = defaultdict(lambda: {'사모펀드': 0, '연기금': 0, '외국인': 0})
    for r in rows:
        key = (r['stock_code'], r['date'])
        bucket[key][r['subject']] = (r.get('buy_amt') or 0) - (r.get('sell_amt') or 0)
    triple = defaultdict(set)
    for (sc, d), net in bucket.items():
        if net['사모펀드'] > 0 and net['연기금'] > 0 and net['외국인'] > 0:
            triple[sc].add(d)
    triple_count = sum(len(v) for v in triple.values())
    print(f"    [OK] 3주체 동시매수 {triple_count:,}건 ({len(triple):,} 종목)")
    return triple


def load_index_returns(start, end):
    print("[3] 시장 등락률...")
    rows = sb_get('daily_index', f"date=gte.{start}&date=lte.{end}&select=date,index_code,close")
    by_idx = defaultdict(list)
    for r in rows:
        by_idx[r['index_code']].append({'date': r['date'], 'close': r['close'] or 0})
    for k in by_idx:
        by_idx[k].sort(key=lambda x: x['date'])
    idx_ret = defaultdict(dict)
    for code, lst in by_idx.items():
        for i in range(1, len(lst)):
            prev = lst[i - 1]['close']; curr = lst[i]['close']
            if prev > 0:
                idx_ret[code][lst[i]['date']] = (curr / prev - 1) * 100
    return idx_ret


# ---------- 시그널 추출 ----------

def detect_signals(stock_data, smart_money, idx_ret, target_dates=None):
    """target_dates가 None이면 모든 날짜, 지정되면 해당 날짜만 처리.
    returns: list of dict (guru_signals INSERT용)"""
    out = []
    for sc, lst in stock_data.items():
        if len(lst) < 60:
            continue
        for i in range(60, len(lst)):
            today = lst[i]
            if target_dates is not None and today['date'] not in target_dates:
                continue
            prev = lst[i - 1]
            if today['close'] <= 0 or prev['close'] <= 0 or today['volume'] <= 0:
                continue

            ret_1d = (today['close'] / prev['close'] - 1) * 100

            # ── 이상치 가드 (분할/병합/액면변경/감자 등 가격 조정 사건 제외) ──
            # 한국 거래소 일일 변동 상한 ±30%, 초과 시 비정상 가격 점프
            if ret_1d > 30 or ret_1d < -30:
                continue
            # 최근 5거래일 변동폭이 종가의 0.1% 미만 = 거래정지/희석 직후
            recent_range = sum(x['high'] - x['low'] for x in lst[i - 5:i])
            if recent_range < today['close'] * 0.005:  # 5일 누적 변동 < 0.5%
                continue

            vol_20 = sum(x['volume'] for x in lst[i - 20:i]) / 20
            vol_50 = sum(x['volume'] for x in lst[i - 50:i]) / 50
            vr20 = today['volume'] / vol_20 if vol_20 > 0 else 0
            vr50 = today['volume'] / vol_50 if vol_50 > 0 else 0
            max_close_20 = max(x['close'] for x in lst[i - 20:i])
            max_close_60 = max(x['close'] for x in lst[i - 60:i])
            dd_60 = (today['close'] / max_close_60 - 1) * 100
            range_recent = sum(x['high'] - x['low'] for x in lst[i - 10:i]) / 10
            range_prior = sum(x['high'] - x['low'] for x in lst[i - 20:i - 10]) / 10
            range_ratio = range_recent / range_prior if range_prior > 0 else 99

            # 시장 대비 초과수익
            mkt = today.get('market') or 'KOSPI'
            mkt_ret = idx_ret.get(mkt, {}).get(today['date'], 0)
            excess_1d = ret_1d - mkt_ret

            trade_value = today['close'] * today['volume']
            common = {
                'date': today['date'],
                'stock_code': sc,
                'stock_name': today.get('stock_name'),
                'market': mkt,
                'ret_1d': round(ret_1d, 2),
                'vol_ratio_20': round(vr20, 2),
                'excess_1d': round(excess_1d, 2),
                'trade_value': trade_value,
                'market_cap': today.get('market_cap') or 0,
            }

            # ① O'Neil Pivot Buy
            if today['close'] >= max_close_20 and vr50 >= 1.4 and ret_1d >= 5:
                out.append({**common, 'pattern_id': 'P1', 'pattern_name': PATTERN_NAMES['P1'],
                            'meta': json.dumps({'max_close_20': max_close_20, 'vr50': round(vr50, 2)})})

            # ② Minervini VCP
            if range_ratio < 0.7 and vr20 >= 2.0 and ret_1d >= 5:
                out.append({**common, 'pattern_id': 'P2', 'pattern_name': PATTERN_NAMES['P2'],
                            'meta': json.dumps({'range_ratio': round(range_ratio, 2)})})

            # ③ Wyckoff Re-accumulation
            if dd_60 <= -15 and vr20 >= 2.0 and ret_1d >= 5:
                out.append({**common, 'pattern_id': 'P3', 'pattern_name': PATTERN_NAMES['P3'],
                            'meta': json.dumps({'dd_60': round(dd_60, 2)})})

            # ④ O'Neil Volume Surge
            if vr20 >= 2.5 and ret_1d >= 5:
                out.append({**common, 'pattern_id': 'P4', 'pattern_name': PATTERN_NAMES['P4'],
                            'meta': json.dumps({})})

            # ⑤ Smart Money Triple
            if today['date'] in smart_money.get(sc, set()) and vr20 >= 1.5:
                out.append({**common, 'pattern_id': 'P5', 'pattern_name': PATTERN_NAMES['P5'],
                            'meta': json.dumps({})})

            # ⑥ Follow-Through Day
            if 2 <= ret_1d and vr20 >= 1.5:
                for j in range(max(0, i - 10), i - 3):
                    trig = lst[j]; pj = lst[j - 1] if j > 0 else None
                    if not pj or pj['close'] <= 0:
                        continue
                    trig_ret = (trig['close'] / pj['close'] - 1) * 100
                    trig_vol = sum(x['volume'] for x in lst[j - 20:j]) / 20 if j >= 20 else 0
                    trig_vr = trig['volume'] / trig_vol if trig_vol > 0 else 0
                    if trig_ret >= 5 and trig_vr >= 2.5:
                        between_low = min(x['low'] for x in lst[j + 1:i])
                        if between_low < trig['close']:
                            out.append({**common, 'pattern_id': 'P6', 'pattern_name': PATTERN_NAMES['P6'],
                                        'meta': json.dumps({'trigger_date': trig['date'], 'trigger_ret': round(trig_ret, 2)})})
                            break

            # ⑦ Darvas Box (52주 신고가)
            if i >= 250:
                high_52w = max(x['close'] for x in lst[i - 250:i])
                if today['close'] >= high_52w and vr20 >= 1.5 and ret_1d >= 3:
                    out.append({**common, 'pattern_id': 'P7', 'pattern_name': PATTERN_NAMES['P7'],
                                'meta': json.dumps({'high_52w': high_52w})})

            # ⑧ Weinstein Stage 2
            if i >= 151:
                ma_150 = sum(x['close'] for x in lst[i - 150:i]) / 150
                ma_150_prev = sum(x['close'] for x in lst[i - 151:i - 1]) / 150
                if (today['close'] > ma_150 and prev['close'] <= ma_150
                        and ma_150 > ma_150_prev and vr20 >= 1.5 and ret_1d >= 2):
                    out.append({**common, 'pattern_id': 'P8', 'pattern_name': PATTERN_NAMES['P8'],
                                'meta': json.dumps({'ma_150': round(ma_150, 2)})})

            # ⑨ Livermore Pivotal Points
            if i >= 50:
                max_close_50 = max(x['close'] for x in lst[i - 50:i])
                if today['close'] > max_close_50 and vr20 >= 1.5 and ret_1d >= 3:
                    out.append({**common, 'pattern_id': 'P9', 'pattern_name': PATTERN_NAMES['P9'],
                                'meta': json.dumps({'max_close_50': max_close_50})})
    return out


# ---------- 메인 ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='특정일만 처리 (YYYYMMDD)')
    ap.add_argument('--dates', help='다중일 처리 (YYYYMMDD,YYYYMMDD,...)')
    ap.add_argument('--backfill-from', default='2025-05-08', help='백필 시작일')
    ap.add_argument('--backfill-to', default=None, help='백필 종료일 (default: 오늘)')
    args = ap.parse_args()

    # 모드 결정
    if args.dates:
        date_list = [datetime.strptime(d.strip(), "%Y%m%d").strftime("%Y-%m-%d")
                     for d in args.dates.split(',') if d.strip()]
        target_dates = set(date_list)
        load_start = (min(datetime.strptime(d, "%Y-%m-%d") for d in date_list)
                      - timedelta(days=400)).strftime("%Y-%m-%d")
        load_end = max(date_list)
        print(f"[*] 다중일 모드: {len(date_list)}일 {sorted(date_list)}")
    elif args.date:
        target = datetime.strptime(args.date, "%Y%m%d").strftime("%Y-%m-%d")
        target_dates = {target}
        # 시그널 계산용 lookback (250D 필요): 1년 + 여유
        load_start = (datetime.strptime(target, "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")
        load_end = target
        print(f"[*] 단일일 모드: {target}")
    else:
        load_end = args.backfill_to or datetime.now().strftime("%Y-%m-%d")
        # 백필 lookback (250D 필요): 시작일 - 1년
        load_start = (datetime.strptime(args.backfill_from, "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")
        target_dates = None  # 모든 날짜 처리하되 백필 시작일 이후만 저장
        backfill_from = args.backfill_from
        print(f"[*] 백필 모드: {backfill_from}~{load_end}")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[X] env 미설정"); return 1

    t0 = time.time()
    stock_data = load_stock_data(load_start, load_end)
    smart_money = load_smart_money(load_start, load_end)
    idx_ret = load_index_returns(load_start, load_end)

    print("[5] 시그널 추출...")
    signals = detect_signals(stock_data, smart_money, idx_ret, target_dates)

    # 백필 모드에서 시작일 이전 시그널 제거
    if not args.date and 'backfill_from' in dir():
        signals = [s for s in signals if s['date'] >= args.backfill_from]

    print(f"    [OK] 총 {len(signals):,} 시그널 추출")

    # 패턴별 통계
    by_pat = defaultdict(int)
    for s in signals:
        by_pat[s['pattern_id']] += 1
    for p in sorted(by_pat):
        print(f"      {p} ({PATTERN_NAMES[p]}): {by_pat[p]:,}")

    # 단일일/다중일 모드: 해당 날짜들 기존 데이터 삭제 후 재삽입 (idempotent)
    if args.date or args.dates:
        for d in sorted(target_dates):
            print(f"[6] 기존 {d} 데이터 삭제...")
            sb_delete('guru_signals', f"date=eq.{d}")

    # INSERT
    print(f"[7] guru_signals INSERT (배치 500)...")
    inserted = sb_insert('guru_signals', signals, batch=500)
    print(f"    [OK] {inserted:,} rows inserted")
    print(f"\n[OK] 총 {time.time() - t0:.0f}초")


if __name__ == '__main__':
    sys.exit(main() or 0)
