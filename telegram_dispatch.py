"""
[*] 텔레그램 사용자 알림 디스패처
=====================================
guru_signals / daily_supply_v2 / daily_reports를 기반으로
구독자 (alert_subscriptions)에게 텔레그램 알림을 전송.

사용법:
  # CLI 직접 실행 — 특정 알림 종류만
  python telegram_dispatch.py --type super_golden --date 20260511
  python telegram_dispatch.py --type watch_stocks --date 20260511
  python telegram_dispatch.py --type daily_summary --date 20260511
  python telegram_dispatch.py --type index_signal --date 20260511
  python telegram_dispatch.py --type all --date 20260511   # 모두

  # run_daily.py에서 import
  from telegram_dispatch import dispatch_all
  dispatch_all('2026-05-11')

알림 종류 토글 + paused + telegram_chat_id 체크 후 발송.
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
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_KEY", "")  # 호환성
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# service_role 우선, 없으면 anon (RLS 우회 못함)
ADMIN_KEY = SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY
ADMIN_HEADERS = {
    "apikey": ADMIN_KEY,
    "Authorization": f"Bearer {ADMIN_KEY}",
    "Content-Type": "application/json",
}


# ────────────────────────────────────────────────────────
# Supabase helpers
# ────────────────────────────────────────────────────────

def sb_get(table, params, page_size=1000):
    rows, offset = [], 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={page_size}&offset={offset}"
        try:
            r = requests.get(url, headers=ADMIN_HEADERS, timeout=60)
            if r.status_code != 200:
                print(f"  [E] sb_get {table}: {r.status_code} {r.text[:200]}")
                break
            chunk = r.json()
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        except Exception as e:
            print(f"  [X] sb_get {table}: {e}")
            break
    return rows


def sb_insert(table, rows):
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        r = requests.post(url, headers=ADMIN_HEADERS, json=rows, timeout=30)
        if r.status_code in (200, 201, 204):
            return len(rows)
        print(f"  [E] sb_insert {table}: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  [X] sb_insert {table}: {e}")
    return 0


# ────────────────────────────────────────────────────────
# Telegram helpers
# ────────────────────────────────────────────────────────

def send_telegram(chat_id, text, user_id=None, alert_type='generic', payload=None):
    """텔레그램 메시지 전송 + alert_history 기록.
    Returns: (ok: bool, error: str|None)
    """
    if not TELEGRAM_TOKEN:
        return False, 'TELEGRAM_BOT_TOKEN 누락'
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
        }, timeout=15)
        ok = r.status_code == 200 and r.json().get('ok')
        err = None if ok else f"{r.status_code} {r.text[:200]}"
    except Exception as e:
        ok, err = False, str(e)

    # alert_history 기록
    if user_id:
        history_row = {
            'user_id': user_id,
            'telegram_chat_id': chat_id,
            'alert_type': alert_type,
            'payload': payload or {'text_preview': text[:200]},
            'status': 'sent' if ok else 'failed',
        }
        if err:
            history_row['error_message'] = err[:500]
        sb_insert('alert_history', [history_row])

    return ok, err


# ────────────────────────────────────────────────────────
# 구독자 조회 (조건별 필터링)
# ────────────────────────────────────────────────────────

def get_subscribers(alert_field, watch_stock=None):
    """알림 토글이 ON + 연결됨 + 일시정지 아님인 구독자 조회.
    watch_stock 지정 시: watch_stocks 배열에 그 종목 코드 포함된 사용자만.
    """
    params = (
        f"select=user_id,telegram_chat_id,telegram_username,tier,watch_stocks"
        f"&{alert_field}=eq.true"
        f"&paused=eq.false"
        f"&telegram_chat_id=not.is.null"
    )
    if watch_stock:
        # text[] 컨테인먼트: watch_stocks @> '{005930}'
        params += f"&watch_stocks=cs.{{{watch_stock}}}"
    rows = sb_get('alert_subscriptions', params)
    return rows


# ────────────────────────────────────────────────────────
# 1) 슈퍼 황금 (VCP × CMF≥+0.25)
# ────────────────────────────────────────────────────────

def dispatch_super_golden(target_date):
    """target_date에 VCP(P2) + CMF≥+0.25 동시 매칭 종목을 모든 슈퍼황금 구독자에게 발송."""
    print(f"\n[1] 슈퍼 황금 디스패치 ({target_date})...")

    # P2 시그널 종목 조회
    sigs = sb_get('guru_signals',
        f"select=stock_code,stock_name,market,ret_1d,vol_ratio_20,excess_1d"
        f"&date=eq.{target_date}&pattern_id=eq.P2")
    if not sigs:
        print(f"    P2(VCP) 시그널 없음 — 발송 안 함")
        return 0

    # 각 종목의 cmf_20 조회 (subject=개인)
    codes = [s['stock_code'] for s in sigs]
    code_list = ','.join(f'"{c}"' for c in codes)
    ind_rows = sb_get('daily_supply_v2',
        f"select=stock_code,cmf_20&date=eq.{target_date}"
        f"&subject=eq.개인&stock_code=in.({code_list})")
    cmf_map = {r['stock_code']: float(r['cmf_20']) if r.get('cmf_20') is not None else None
               for r in ind_rows}

    # 슈퍼 황금 필터: CMF ≥ +0.25
    golden = []
    for s in sigs:
        cmf = cmf_map.get(s['stock_code'])
        if cmf is not None and cmf >= 0.25:
            s['cmf'] = cmf
            golden.append(s)

    print(f"    P2 시그널 {len(sigs)}건 중 슈퍼 황금 {len(golden)}건")
    if not golden:
        return 0

    # 구독자 조회
    subs = get_subscribers('alert_super_golden')
    print(f"    슈퍼 황금 구독자 {len(subs)}명")
    if not subs:
        return 0

    # 메시지 작성 (모든 슈퍼 황금 종목 1건으로)
    lines = [f"🏆 <b>슈퍼 황금 발생</b> ({target_date})", '',
             '<b>VCP × CMF≥+0.25</b> — 백테스트 +17.12%, 시장대비 +13.12%', '']
    for s in golden[:10]:
        mkt = s.get('market', '')
        emoji = '🔵' if mkt == 'KOSPI' else '🟣'
        lines.append(
            f"{emoji} <b>{s.get('stock_name','-')}</b> ({s['stock_code']})\n"
            f"   📈 {s.get('ret_1d',0):+.2f}% · "
            f"vol {s.get('vol_ratio_20',0):.1f}x · "
            f"CMF {s['cmf']:+.2f}"
        )
    if len(golden) > 10:
        lines.append(f"\n... 외 {len(golden) - 10}건")
    lines.append('')
    lines.append(f"🔗 https://ai-pathway-web.vercel.app/scanner")
    text = '\n'.join(lines)

    # 발송
    sent = 0
    payload = {
        'date': target_date, 'count': len(golden),
        'stocks': [{'code': g['stock_code'], 'name': g['stock_name'], 'cmf': g['cmf']}
                   for g in golden],
    }
    for sub in subs:
        ok, err = send_telegram(sub['telegram_chat_id'], text,
                                user_id=sub['user_id'],
                                alert_type='super_golden', payload=payload)
        if ok:
            sent += 1
        else:
            print(f"    [W] chat_id={sub['telegram_chat_id']} 발송 실패: {err}")
        time.sleep(0.05)  # 텔레그램 rate limit

    print(f"    [OK] {sent}/{len(subs)}명 발송")
    return sent


# ────────────────────────────────────────────────────────
# 2) 관심 종목 그루 시그널
# ────────────────────────────────────────────────────────

GURU_PATTERN_LABEL = {
    'P1': "O'Neil Pivot 20D",  'P2': 'Minervini VCP',
    'P3': 'Wyckoff Re-acc',    'P4': "O'Neil Volume Surge",
    'P5': 'Smart Money Triple','P6': "O'Neil Follow-Through",
    'P7': 'Darvas 52주',       'P8': 'Weinstein Stage 2',
    'P9': 'Livermore 50D',
}

def dispatch_watch_stocks(target_date):
    """target_date의 모든 그루 시그널 → 각 종목을 watch 등록한 구독자에게 발송."""
    print(f"\n[2] 관심 종목 디스패치 ({target_date})...")

    # 모든 시그널 조회 (종목별 그루핑)
    sigs = sb_get('guru_signals',
        f"select=stock_code,stock_name,market,pattern_id,ret_1d,vol_ratio_20"
        f"&date=eq.{target_date}")
    if not sigs:
        print(f"    시그널 없음")
        return 0

    by_stock = defaultdict(list)
    for s in sigs:
        by_stock[s['stock_code']].append(s)

    # 활성 watch_stocks 구독자 조회
    subs = sb_get('alert_subscriptions',
        f"select=user_id,telegram_chat_id,watch_stocks,tier"
        f"&alert_watch_stocks=eq.true&paused=eq.false"
        f"&telegram_chat_id=not.is.null")
    print(f"    활성 watch 구독자 {len(subs)}명")
    if not subs:
        return 0

    sent_count = 0
    for sub in subs:
        watch = sub.get('watch_stocks') or []
        if not watch:
            continue
        # 이 사용자의 관심 종목 중 오늘 시그널 발생한 것
        my_hits = []
        for code in watch:
            if code in by_stock:
                patterns = sorted(set(s['pattern_id'] for s in by_stock[code]))
                first_sig = by_stock[code][0]
                my_hits.append({
                    'code': code,
                    'name': first_sig.get('stock_name', '-'),
                    'market': first_sig.get('market', ''),
                    'patterns': patterns,
                    'ret_1d': first_sig.get('ret_1d', 0),
                    'vol': first_sig.get('vol_ratio_20', 0),
                })
        if not my_hits:
            continue

        # 메시지 작성
        lines = [f"📌 <b>관심 종목 그루 시그널</b> ({target_date})", '']
        for h in my_hits:
            emoji = '🔵' if h['market'] == 'KOSPI' else '🟣'
            pat_str = ' · '.join(h['patterns'])
            pat_n = len(h['patterns'])
            super_mark = ' ⭐' if pat_n >= 3 else ''
            lines.append(
                f"{emoji} <b>{h['name']}</b> ({h['code']}){super_mark}\n"
                f"   📈 {h['ret_1d']:+.2f}% · vol {h['vol']:.1f}x\n"
                f"   매칭 패턴: {pat_str}"
            )
        lines.append('')
        lines.append(f"🔗 https://ai-pathway-web.vercel.app/scanner")
        text = '\n'.join(lines)

        ok, err = send_telegram(sub['telegram_chat_id'], text,
                                user_id=sub['user_id'],
                                alert_type='watch_stocks',
                                payload={'date': target_date, 'hits': my_hits})
        if ok:
            sent_count += 1
        else:
            print(f"    [W] chat_id={sub['telegram_chat_id']} 발송 실패: {err}")
        time.sleep(0.05)

    print(f"    [OK] {sent_count}명 발송")
    return sent_count


# ────────────────────────────────────────────────────────
# 3) 일일 시장 요약
# ────────────────────────────────────────────────────────

def dispatch_daily_summary(target_date):
    """daily_reports의 최신 리포트 기반 일일 요약."""
    print(f"\n[3] 일일 시장 요약 디스패치 ({target_date})...")

    # daily_reports 조회 (최신)
    reports = sb_get('daily_reports',
        f"select=date,title,summary,top_stocks,leading_sectors,smart_money,signals"
        f"&date=eq.{target_date}&order=created_at.desc&limit=1")
    if not reports:
        print(f"    {target_date} 리포트 없음 — 발송 안 함")
        return 0

    rpt = reports[0]
    title = rpt.get('title', '오늘의 시장')
    summary = rpt.get('summary', '')
    top_stocks = rpt.get('top_stocks') or []
    leading_sectors = rpt.get('leading_sectors') or []
    smart = rpt.get('smart_money') or {}
    signals = rpt.get('signals') or {}

    # 오늘 그루 시그널 통계
    sigs = sb_get('guru_signals',
        f"select=pattern_id,stock_code&date=eq.{target_date}")
    pat_count = defaultdict(int)
    unique_stocks = set()
    for s in sigs:
        pat_count[s['pattern_id']] += 1
        unique_stocks.add(s['stock_code'])

    # 구독자 조회
    subs = get_subscribers('alert_daily_summary')
    print(f"    일일 요약 구독자 {len(subs)}명")
    if not subs:
        return 0

    # HTML 안전 처리 (jsonb summary는 텍스트라 < > 제거)
    def safe(s, n=300):
        if not s:
            return ''
        s = str(s).replace('<', '').replace('>', '').replace('&', '&amp;')
        return s[:n] + ('…' if len(s) > n else '')

    # 메시지 작성
    lines = [
        f"📊 <b>{target_date} 시장 요약</b>",
        f"<i>{safe(title, 80)}</i>",
        '',
        safe(summary, 400),
        '',
    ]

    # TOP 종목
    if top_stocks:
        lines.append('<b>🎯 TOP 종목</b>')
        for s in top_stocks[:5]:
            if not isinstance(s, dict):
                continue
            name = safe(s.get('stock_name', '-'), 20)
            code = s.get('stock_code', '')
            mkt = s.get('market', '')
            emoji = '🔵' if mkt == 'KOSPI' else '🟣'
            ret20 = s.get('return_20d', 0)
            super_mark = ' ⭐' if s.get('is_super_leader') else ''
            lines.append(f"{emoji} <b>{name}</b> ({code}){super_mark} +{ret20:.1f}%")
        lines.append('')

    # 주도 섹터
    if leading_sectors:
        lines.append('<b>🏷 주도 섹터</b>')
        for sec in leading_sectors[:5]:
            if not isinstance(sec, dict):
                continue
            name = safe(sec.get('sector', '-'), 20)
            count = sec.get('count', 0)
            avg_pctl = sec.get('avg_pctl', 0)
            lines.append(f"  • {name} ({count}종목, 평균 {avg_pctl:.0f}점)")
        lines.append('')

    # 스마트머니 (사모+연기금+외인 동시 매수)
    triple_buy_count = smart.get('triple_buy_count', 0)
    if triple_buy_count > 0:
        lines.append(f"<b>💰 스마트머니 3주체 동시 매수</b>: {triple_buy_count}종목")
        for s in (smart.get('top_triple_buy') or [])[:3]:
            if not isinstance(s, dict):
                continue
            name = safe(s.get('stock_name', '-'), 20)
            code = s.get('stock_code', '')
            amt = s.get('net_amount', 0)
            lines.append(f"  • {name} ({code}) — {amt:,}억")
        lines.append('')

    # 그루 시그널 요약
    if pat_count:
        lines.append(f"<b>🎯 그루 시그널</b>: {len(unique_stocks)}종목 / {len(sigs)}건")
        for pid in sorted(pat_count.keys()):
            label = GURU_PATTERN_LABEL.get(pid, pid)
            lines.append(f"  {pid} {label}: {pat_count[pid]}건")
        lines.append('')

    lines.append(f"🔗 https://ai-pathway-web.vercel.app/report")
    text = '\n'.join(lines)

    sent = 0
    payload = {'date': target_date, 'signal_count': len(sigs),
               'stock_count': len(unique_stocks)}
    for sub in subs:
        ok, err = send_telegram(sub['telegram_chat_id'], text,
                                user_id=sub['user_id'],
                                alert_type='daily_summary', payload=payload)
        if ok:
            sent += 1
        else:
            print(f"    [W] chat_id={sub['telegram_chat_id']} 발송 실패: {err}")
        time.sleep(0.05)

    print(f"    [OK] {sent}/{len(subs)}명 발송")
    return sent


# ────────────────────────────────────────────────────────
# 4) 지수 시그널 (외인 매도→매수 등) — Phase 2 stub
# ────────────────────────────────────────────────────────

def dispatch_index_signal(target_date):
    """지수 시그널 디스패치 — 후속 구현."""
    print(f"\n[4] 지수 시그널 디스패치 ({target_date})...")
    print(f"    Phase 2 후속 구현 예정 — skip")
    return 0


# ────────────────────────────────────────────────────────
# 통합 디스패치
# ────────────────────────────────────────────────────────

def dispatch_all(target_date):
    """모든 알림 종류 통합 디스패치."""
    print(f"\n{'='*60}")
    print(f"  텔레그램 사용자 알림 디스패치 — {target_date}")
    print(f"{'='*60}")

    total = 0
    total += dispatch_super_golden(target_date)
    total += dispatch_watch_stocks(target_date)
    total += dispatch_daily_summary(target_date)
    total += dispatch_index_signal(target_date)

    print(f"\n{'='*60}")
    print(f"  [완료] 총 {total}건 발송")
    print(f"{'='*60}\n")
    return total


# ────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='YYYYMMDD (기본: 오늘)')
    ap.add_argument('--type', default='all',
                    choices=['all', 'super_golden', 'watch_stocks',
                             'daily_summary', 'index_signal'])
    args = ap.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y%m%d").strftime("%Y-%m-%d")
    else:
        target_date = datetime.now().strftime("%Y-%m-%d")

    # 환경 점검
    if not SUPABASE_URL or not ADMIN_KEY or not TELEGRAM_TOKEN:
        print("[X] 환경변수 누락 (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / TELEGRAM_BOT_TOKEN)")
        return 1
    if not SUPABASE_SERVICE_ROLE_KEY:
        print("[!] WARNING: SUPABASE_SERVICE_ROLE_KEY 없음, anon으로 동작 (RLS 우회 불가)")

    if args.type == 'all':
        dispatch_all(target_date)
    elif args.type == 'super_golden':
        dispatch_super_golden(target_date)
    elif args.type == 'watch_stocks':
        dispatch_watch_stocks(target_date)
    elif args.type == 'daily_summary':
        dispatch_daily_summary(target_date)
    elif args.type == 'index_signal':
        dispatch_index_signal(target_date)

    return 0


if __name__ == '__main__':
    sys.exit(main())
