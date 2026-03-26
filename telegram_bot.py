import requests, json, os, sys, time
from dotenv import load_dotenv
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

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

def save_blog_post(date, scores, picks, market_dict, d_count):
    top3_stocks = [scores[i] for i in picks]
    avg_chg = sum(s['change_pct'] for s in scores) / len(scores) if scores else 0
    n5 = sum(1 for s in scores if s['n_buyers'] == 5)

    title = f"[수급분석] {date} TOP3: {', '.join(s['stock_name'] for s in top3_stocks)}"
    summary = f"분석 {len(scores)}종목, 평균 {avg_chg:+.2f}%, 5주체전원매수 {n5}개, D전략 {d_count}개"

    sections = []
    sections.append({
        'title': '1. 시장 요약',
        'type': 'market_summary',
        'body': f'{date} 분석 대상 {len(scores)}개 종목의 평균 등락률은 {avg_chg:+.2f}%를 기록했습니다. 5주체 전원매수 {n5}개, D전략(외+연+사) {d_count}개 종목이 포착되었습니다.',
        'data': {'total_stocks': len(scores), 'avg_change': round(avg_chg, 2), 'five_buyers': n5, 'd_strategy': d_count}
    })

    stocks_data = []
    for i, s in enumerate(top3_stocks):
        m = market_dict.get(s['stock_code'], {})
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
            'analysis': ''
        })

    sections.append({
        'title': '2. 최종선정 TOP3',
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

    r = requests.post(
        f'{SUPABASE_URL}/rest/v1/blog_posts',
        headers={**sb_headers, 'Prefer': 'return=representation'},
        json=report
    )
    return r.status_code in [200, 201]

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
    d_stocks = get_d_strategy(date)
    d_count = len(d_stocks)

    avg_chg = sum(s['change_pct'] for s in scores) / len(scores)
    n5 = sum(1 for s in scores if s['n_buyers'] == 5)
    n4 = sum(1 for s in scores if s['n_buyers'] == 4)

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
    msg += f'--------------------\n'
    msg += f'<b>[TOP25 랭킹]</b>\n'
    for i, s in enumerate(scores[:25], 1):
        chg = s['change_pct']
        chg_icon = '+' if chg >= 0 else ''
        star = '*' if s['n_buyers'] == 5 else ''
        conflict = '!' if s['conflict_penalty'] < 0 else ''
        msg += f'{i}. {star}{s["stock_name"]} [{s["combo"]}] {s["final_score"]:.1f} {chg_icon}{chg:.2f}% {conflict}\n'

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

    timeout = 3600  # 60분 대기
    start_time = time.time()

    while time.time() - start_time < timeout:
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
                confirm_msg += f'\n맞으면 "Y", 다시 입력하려면 번호를 다시 입력하세요.'
                send_telegram(confirm_msg)

                # 확인 대기
                while time.time() - start_time < timeout:
                    confirms = get_updates(offset=last_update_id)
                    for conf in confirms:
                        last_update_id = conf['update_id'] + 1
                        if 'message' not in conf:
                            continue
                        conf_text = conf['message'].get('text', '').strip().upper()

                        if conf_text == 'Y':
                            save_top3(date, picks, scores, market_dict)
                            save_blog_post(date, scores, picks, market_dict, d_count)

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
                                    confirm_msg += f'\n맞으면 "Y", 다시 입력하려면 번호를 다시 입력하세요.'
                                    send_telegram(confirm_msg)
                            except ValueError:
                                send_telegram('[!] 숫자를 콤마로 구분하여 입력하세요 (예: 1,2,8)')

                    time.sleep(2)

            except ValueError:
                send_telegram('[!] 숫자를 콤마로 구분하여 입력하세요 (예: 1,2,8)')
                continue

        time.sleep(2)

    send_telegram('[!] 10분 타임아웃. 나중에 다시 실행하세요.')
    print('[!] 타임아웃')

if __name__ == '__main__':
    main()
