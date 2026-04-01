import requests, json, os, sys, time
from dotenv import load_dotenv
load_dotenv()
from market_indicators import get_market_indicators

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

sb_headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Content-Type': 'application/json'}

def send_telegram(text, parse_mode='HTML'):
    r = requests.post(
        f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',
        json={'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': parse_mode}
    )
    return r.json()

def get_updates(offset=None):
    params = {'timeout': 60}
    if offset:
        params['offset'] = offset
    r = requests.get(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates', params=params)
    return r.json().get('result', [])

def get_scores(date):
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/analysis_scores?date=eq.{date}&order=final_score.desc&limit=25',
        headers=sb_headers
    )
    return r.json()

def get_market(date):
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/daily_market?date=eq.{date}&limit=2000',
        headers=sb_headers
    )
    return {m['stock_code']: m for m in r.json()}

def get_supply(date):
    """주체별 매수/매도 데이터 가져오기"""
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/daily_supply?date=eq.{date}&limit=5000',
        headers=sb_headers
    )
    supply_by_stock = {}
    for row in r.json():
        code = row['stock_code']
        if code not in supply_by_stock:
            supply_by_stock[code] = {}
        subj = row['subject']
        amt = row.get('amount', 0)
        if row['direction'] == '매수':
            supply_by_stock[code][subj] = supply_by_stock[code].get(subj, 0) + amt
        else:
            supply_by_stock[code][subj] = supply_by_stock[code].get(subj, 0) + amt
    return supply_by_stock

def get_d_strategy(date):
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/daily_supply?date=eq.{date}&direction=eq.매수&limit=2000',
        headers=sb_headers
    )
    buy_data = {}
    for row in r.json():
        code = row['stock_code']
        if code not in buy_data:
            buy_data[code] = set()
        buy_data[code].add(row['subject'])
    d_stocks = [code for code, subjects in buy_data.items()
                if all(s in subjects for s in ['외국인', '연기금', '사모펀드'])]
    return d_stocks

def get_prev_top3_performance(date, market_dict):
    """전일 TOP3 성과 추적"""
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/top3_history?date=lt.{date}&order=date.desc&limit=3',
        headers=sb_headers
    )
    prev_top3 = r.json()
    if not prev_top3:
        return None, None

    prev_date = prev_top3[0]['date']
    prev_stocks = [t for t in prev_top3 if t['date'] == prev_date]
    prev_stocks.sort(key=lambda x: x['rank'])

    results = []
    for t in prev_stocks:
        m = market_dict.get(t['stock_code'], {})
        current_price = m.get('close_price', 0)
        base_price = t.get('base_price', 0)
        ret = ((current_price - base_price) / base_price * 100) if base_price > 0 else 0
        results.append({
            'rank': t['rank'],
            'name': t['stock_name'],
            'sector': t['sector'],
            'base_price': base_price,
            'current_price': current_price,
            'return_pct': round(ret, 2),
            'combo': t.get('combo', '')
        })

    avg_ret = sum(r['return_pct'] for r in results) / len(results) if results else 0
    return prev_date, {'stocks': results, 'avg_return': round(avg_ret, 2)}

def generate_ai_analysis(stock, supply_data, market_data):
    """Claude API로 종목별 상세 분석 생성 (20~30줄)"""
    if not ANTHROPIC_API_KEY:
        return ''

    amounts = supply_data.get(stock['stock_code'], {})
    m = market_data.get(stock['stock_code'], {})
    mcap = round(m.get('market_cap', 0) / 100000000) if m else 0

    amounts_str = ', '.join([f"{k}: {v:+,}백만원" for k, v in amounts.items() if v != 0])

    prompt = f"""당신은 한국 주식시장 전문 애널리스트입니다. 아래 종목의 수급 데이터를 바탕으로 상세한 투자 분석 리포트를 작성해주세요.

[종목 정보]
- 종목명: {stock['stock_name']}
- 섹터/주요제품: {stock['sector']}
- 매수주체 조합: {stock['combo']} ({stock['n_buyers']}주체 동시매수)
- 최종점수: {stock['final_score']:.1f}점
- 당일 등락률: {stock['change_pct']:+.2f}%
- 시가총액: {mcap:,}억원
- 주체별 순매수 금액: {amounts_str}
- 충돌패널티: {stock['conflict_penalty']}

[작성 요구사항]
존댓말(습니다체)로 작성하고, 아래 항목을 모두 포함하여 최소 15줄 이상 작성해주세요:

1. 수급 분석: 각 매수 주체(외국인/연기금/투신/사모펀드/기타법인)의 매수 금액과 그 의미. 특히 어떤 주체의 매수가 가장 주목할 만한지, 그리고 복수 주체 동시 매수의 시그널 의미를 설명.

2. 섹터 및 종목 특성: 이 종목이 속한 섹터의 현재 시장 트렌드, 종목의 핵심 사업 내용과 경쟁력. 해당 섹터가 주목받는 이유.

3. 기술적 관점: 당일 {stock['change_pct']:+.2f}% 등락의 의미, 시가총액 {mcap:,}억원 대비 수급 강도, 단기 모멘텀 전망.

4. 투자 포인트: 매수 관점에서의 긍정적 요인과 리스크 요인을 균형있게 제시.

5. 종합 의견: 해당 종목의 수급 패턴에 대한 종합적인 평가와 향후 관전 포인트.

마지막 문장까지 반드시 완결성 있게 마무리해주세요. 절대로 문장이 중간에 끊기면 안 됩니다. 마지막 문장은 반드시 마침표(.)로 끝나야 합니다.
마크다운 형식(## 소제목)을 사용하여 가독성 있게 작성해주세요."""

    try:
        r = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json'
            },
            json={
                'model': 'claude-sonnet-4-20250514',
                'max_tokens': 2000,
                'messages': [{'role': 'user', 'content': prompt}]
            },
            timeout=120
        )
        if r.status_code == 200:
            return r.json()['content'][0]['text']
    except Exception as e:
        print(f'  AI 분석 생성 실패 ({stock["stock_name"]}): {e}')
    return ''

def save_top3(date, picks, scores, market_dict):
    for rank, idx in enumerate(picks, 1):
        s = scores[idx]
        m = market_dict.get(s['stock_code'], {})
        base_price = m.get('close_price', 0)
        record = {
            'date': date,
            'rank': rank,
            'stock_code': s['stock_code'],
            'stock_name': s['stock_name'],
            'sector': s['sector'],
            'score': round(s['final_score'], 1),
            'base_price': base_price,
            'combo': s['combo'],
            'selected_by': 'shawn',
            'expires_date': None
        }
        requests.post(
            f'{SUPABASE_URL}/rest/v1/top3_history',
            headers={**sb_headers, 'Prefer': 'return=representation'},
            json=record
        )
    return True

def save_blog_post(date, scores, picks, market_dict, d_count, supply_data, prev_perf, indicators=None):
    top3_stocks = [scores[i] for i in picks]
    avg_chg = sum(s['change_pct'] for s in scores) / len(scores) if scores else 0
    n5 = sum(1 for s in scores if s['n_buyers'] == 5)

    title = f"[수급분석] {date} TOP3: {', '.join(s['stock_name'] for s in top3_stocks)}"
    summary = f"분석 {len(scores)}종목, 평균 {avg_chg:+.2f}%, 5주체전원매수 {n5}개, D전략 {d_count}개"

    sections = []

    # 섹션 1: 주요 시장 지표 (있는 경우)
    if indicators:
        sections.append({
            'title': '주요 시장 지표',
            'type': 'market_indicators',
            'body': '',
            'indicators': indicators
        })

    # 섹션 2: 최종선정 TOP3 (AI 분석 포함)
    print('  [>] TOP3 종목별 AI 분석 생성 중...')
    stocks_data = []
    for i, s in enumerate(top3_stocks):
        m = market_dict.get(s['stock_code'], {})
        amounts = supply_data.get(s['stock_code'], {})

        # AI 분석 생성
        analysis = generate_ai_analysis(s, supply_data, market_dict)
        if analysis:
            print(f'    {s["stock_name"]}: AI 분석 완료')
        else:
            print(f'    {s["stock_name"]}: AI 분석 건너뜀')
        time.sleep(1)  # API rate limit

        # 주체별 금액 (억원 단위)
        amounts_display = {}
        for subj in ['외국인', '연기금', '투신', '사모펀드', '기타법인']:
            amt = amounts.get(subj, 0)
            if amt != 0:
                amounts_display[subj] = round(amt / 100)  # 백만원 -> 억원

        stocks_data.append({
            'rank': i + 1,
            'name': s['stock_name'],
            'code': s['stock_code'],
            'sector': s['sector'],
            'combo': s['combo'],
            'n_buyers': s['n_buyers'],
            'score': round(s['final_score'], 1),
            'change_pct': s['change_pct'],
            'base_price': m.get('close_price', 0),
            'market_cap': round(m.get('market_cap', 0) / 100000000),
            'amounts': amounts_display,
            'analysis': analysis
        })

    sections.append({
        'title': '오늘 최종선정 TOP3',
        'type': 'top3',
        'body': 'AI 수급분석 점수와 기관 매수 패턴을 종합하여 최종 선정된 TOP3 종목입니다.',
        'stocks': stocks_data
    })

    report = {
        'date': date,
        'title': title,
        'category': '수급분석',
        'summary': summary,
        'content': json.dumps({'sections': sections}, ensure_ascii=False),
        'top3': json.dumps([{'rank': i+1, 'name': s['stock_name'], 'sector': s['sector'], 'score': round(s['final_score'], 1)} for i, s in enumerate(top3_stocks)], ensure_ascii=False),
        'market_summary': json.dumps({'avg_change': round(avg_chg, 2), 'total_stocks': len(scores), 'five_buyers': n5, 'd_strategy': d_count}, ensure_ascii=False)
    }

    # upsert (같은 날짜 중복 방지)
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/blog_posts?date=eq.{date}&limit=1',
        headers=sb_headers
    )
    existing = r.json()
    if existing:
        r = requests.patch(
            f'{SUPABASE_URL}/rest/v1/blog_posts?date=eq.{date}',
            headers={**sb_headers, 'Prefer': 'return=minimal'},
            json=report
        )
    else:
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/blog_posts',
            headers={**sb_headers, 'Prefer': 'return=representation'},
            json=report
        )
    return r.status_code in [200, 201, 204]

def main():
    date = sys.argv[1] if len(sys.argv) > 1 else None
    if not date:
        from datetime import datetime
        date = datetime.now().strftime('%Y-%m-%d')

    if len(date) == 8:
        date = f'{date[:4]}-{date[4:6]}-{date[6:8]}'

    print(f'[*] AI+패스웨이 텔레그램 봇 ({date})')

    scores = get_scores(date)
    if not scores:
        send_telegram(f'[!] {date} 분석 데이터가 없습니다.')
        print('분석 데이터 없음')
        return

    market_dict = get_market(date)
    supply_data = get_supply(date)
    d_stocks = get_d_strategy(date)
    d_count = len(d_stocks)

    avg_chg = sum(s['change_pct'] for s in scores) / len(scores)
    n5 = sum(1 for s in scores if s['n_buyers'] == 5)
    n4 = sum(1 for s in scores if s['n_buyers'] == 4)

    # 전일 TOP3 성과
    prev_perf = get_prev_top3_performance(date, market_dict)

    # D전략 종목 메시지
    d_msg = f'<b>[D전략] 외+연+사 동시매수: {d_count}개</b>\n'
    d_scored = [s for s in scores if s['stock_code'] in d_stocks]
    d_scored.sort(key=lambda x: x['final_score'], reverse=True)
    for i, s in enumerate(d_scored[:15], 1):
        chg = s['change_pct']
        chg_icon = '+' if chg >= 0 else ''
        d_msg += f'{i}. {s["stock_name"]} ({s["sector"]}) {s["final_score"]:.1f}점 {chg_icon}{chg:.2f}%\n'

    # TOP25 메시지
    msg = f'<b>[수급분석] {date}</b>\n'
    msg += f'분석 {len(scores)}종목 | 평균 {avg_chg:+.2f}%\n'
    msg += f'5주체전원 {n5}개 | 4주체 {n4}개 | D전략 {d_count}개\n'
    msg += f'------------------------------\n'
    msg += f'<b>[TOP25 랭킹]</b>\n'
    for i, s in enumerate(scores[:25], 1):
        chg = s['change_pct']
        chg_icon = '+' if chg >= 0 else ''
        star = '*' if s['n_buyers'] == 5 else ''
        conflict = '!' if s['conflict_penalty'] < 0 else ''
        msg += f'{i}. {star}{s["stock_name"]} [{s["combo"]}] {s["final_score"]:.1f} {chg_icon}{chg:.2f}% {conflict}\n'

    # 전일 TOP3 성과 메시지
    prev_date, prev_data = prev_perf
    if prev_data and prev_data['stocks']:
        msg += f'\n<b>[전일 TOP3 성과] {prev_date}</b>\n'
        for ps in prev_data['stocks']:
            ret_icon = '+' if ps['return_pct'] >= 0 else ''
            msg += f'{ps["rank"]}위 {ps["name"]}: {ret_icon}{ps["return_pct"]:.2f}%\n'
        msg += f'평균: {prev_data["avg_return"]:+.2f}%\n'

    msg += f'\n<b>TOP3 선정: 번호 입력 (예: 1,2,8)</b>'

    send_telegram(d_msg)
    time.sleep(1)
    send_telegram(msg)

    print('[>] 텔레그램 전송 완료. TOP3 입력 대기중...')

    # TOP3 입력 대기
    last_update_id = None
    updates = get_updates()
    if updates:
        last_update_id = updates[-1]['update_id'] + 1

    timeout = 3600  # 60분 대기 → 무제한 대기 (리마인더용 기준)
    start_time = time.time()
    last_reminder = start_time

    while True:
        # 30분마다 리마인더
        if time.time() - last_reminder >= 1800:
            elapsed = int((time.time() - start_time) / 60)
            send_telegram(f'[⏰] TOP3 선정 대기 중... ({elapsed}분 경과)\n번호 입력 또는 /skip 입력하세요')
            last_reminder = time.time()

        updates = get_updates(offset=last_update_id)
        for update in updates:
            last_update_id = update['update_id'] + 1
            if 'message' not in update:
                continue
            text = update['message'].get('text', '').strip()
            chat_id = update['message']['chat']['id']

            if str(chat_id) != str(TELEGRAM_CHAT_ID):
                continue

            if text.lower() == '/skip':
                send_telegram('[OK] TOP3 선정을 건너뛰었습니다.')
                print('[>] TOP3 선정 스킵')
                return

            try:
                picks = [int(x.strip()) - 1 for x in text.split(',')]
                if len(picks) != 3:
                    send_telegram('[!] 3개 번호를 입력하세요 (예: 1,2,8)')
                    continue
                if any(p < 0 or p >= len(scores) for p in picks):
                    send_telegram(f'[!] 1~{len(scores)} 범위의 번호를 입력하세요')
                    continue

                # TOP3 확인 메시지
                confirm_msg = f'<b>[TOP3 확인]</b>\n'
                for rank, idx in enumerate(picks, 1):
                    s = scores[idx]
                    confirm_msg += f'{rank}위: {s["stock_name"]} ({s["sector"]}) {s["final_score"]:.1f}점\n'
                confirm_msg += f'\n맞으면 "Y", 다시 입력하려면 번호를 다시 입력하세요'
                send_telegram(confirm_msg)

                # 확인 대기
                while True:
                    confirms = get_updates(offset=last_update_id)
                    for conf in confirms:
                        last_update_id = conf['update_id'] + 1
                        if 'message' not in conf:
                            continue
                        conf_text = conf['message'].get('text', '').strip().upper()

                        if conf_text == 'Y':
                            save_top3(date, picks, scores, market_dict)
                            print('[>] TOP3 저장 완료. 블로그 리포트 생성 중...')
                            print('[>] 주요 시장 지표 수집 중...')
                            indicators = get_market_indicators(date)
                            save_blog_post(date, scores, picks, market_dict, d_count, supply_data, prev_perf, indicators)

                            result_msg = f'<b>[OK] TOP3 저장 완료!</b>\n'
                            for rank, idx in enumerate(picks, 1):
                                s = scores[idx]
                                m = market_dict.get(s['stock_code'], {})
                                result_msg += f'{rank}위: {s["stock_name"]} | 기준가 {m.get("close_price", 0):,}원\n'
                            result_msg += f'\n웹사이트 자동 반영됩니다.'
                            result_msg += f'\nhttps://ai-pathway-web.vercel.app/dashboard'
                            send_telegram(result_msg)
                            print('[OK] TOP3 저장 + 블로그 리포트 생성 완료!')
                            return

                        elif ',' in conf_text:
                            try:
                                picks = [int(x.strip()) - 1 for x in conf_text.split(',')]
                                if len(picks) == 3 and all(0 <= p < len(scores) for p in picks):
                                    confirm_msg = f'<b>[TOP3 확인]</b>\n'
                                    for rank, idx in enumerate(picks, 1):
                                        s = scores[idx]
                                        confirm_msg += f'{rank}위: {s["stock_name"]} ({s["sector"]}) {s["final_score"]:.1f}점\n'
                                    confirm_msg += f'\n맞으면 "Y", 다시 입력하려면 번호를 다시 입력하세요'
                                    send_telegram(confirm_msg)
                            except ValueError:
                                send_telegram('[!] 숫자를 콤마로 구분하여 입력하세요 (예: 1,2,8)')

                    time.sleep(2)

            except ValueError:
                send_telegram('[!] 숫자를 콤마로 구분하여 입력하세요 (예: 1,2,8)')
                continue

        time.sleep(2)

    # 무제한 대기이므로 여기 도달하지 않음
    send_telegram('[!] 대기 종료. 수동으로 다시 실행하세요.')
    print('[!] 대기 종료')

if __name__ == '__main__':
    main()
