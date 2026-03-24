import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')

# Use service_role key for DDL if available, otherwise use SQL via REST
# Create blog_posts table via Supabase SQL
headers = {
    'apikey': key,
    'Authorization': f'Bearer {key}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

# Check if blog_posts table exists
r = requests.get(f'{url}/rest/v1/blog_posts?limit=1', headers=headers)
if r.status_code == 200:
    print("blog_posts table already exists!")
else:
    print(f"blog_posts table status: {r.status_code}")
    print("Please create the table manually in Supabase SQL Editor.")
    print()
    print("Go to: https://supabase.com/dashboard/project/ofclchxfrjldmrzswgwi/sql/new")
    print("And run this SQL:")
    print()
    print("""
CREATE TABLE IF NOT EXISTS blog_posts (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    title TEXT NOT NULL,
    category TEXT DEFAULT '수급분석',
    summary TEXT,
    content JSONB NOT NULL,
    top3 JSONB,
    market_summary JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_blog_posts_date ON blog_posts(date DESC);
""")

# If table exists, insert 3/23 report
if r.status_code == 200:
    # Check if 3/23 already exists
    r2 = requests.get(f'{url}/rest/v1/blog_posts?date=eq.2026-03-23', headers=headers)
    if r2.json():
        print("3/23 report already exists!")
    else:
        report = {
            "date": "2026-03-23",
            "title": "[수급분석] 하락장 속 5주체 전원매수 3종목 포착 (feat. 화장품 섹터 강세)",
            "category": "수급분석",
            "summary": "코스피 하락 속에서도 기관 5주체가 동시에 매수한 종목 3개를 포착했습니다. 화장품 섹터에 기관 자금이 집중되고 있습니다.",
            "content": json.dumps({
                "sections": [
                    {
                        "title": "1. 시장 요약",
                        "type": "market_summary",
                        "body": "3월 23일(월) 시장은 전반적으로 하락세를 보였습니다. 분석 대상 212개 종목의 평균 등락률은 -3.73%를 기록했습니다. 그럼에도 5주체 전원이 동시에 매수한 종목이 3개 포착되었고, D전략(외국인+연기금+사모펀드 동시매수) 종목은 14개로 확인되었습니다.",
                        "data": {
                            "total_stocks": 212,
                            "avg_change": -3.73,
                            "five_buyers": 3,
                            "four_buyers": 12,
                            "d_strategy": 14
                        }
                    },
                    {
                        "title": "2. 최종선정 TOP3",
                        "type": "top3",
                        "body": "AI 수급분석 점수와 기관 매수 패턴을 종합하여 최종 선정된 TOP3 종목입니다.",
                        "stocks": [
                            {
                                "rank": 1,
                                "name": "덕산네오룩스",
                                "code": "213420",
                                "sector": "디스플레이",
                                "combo": "외+연+투+사+기",
                                "n_buyers": 5,
                                "score": 124.6,
                                "change_pct": 1.15,
                                "base_price": 48500,
                                "market_cap": 12043,
                                "amounts": {"외국인": 2224, "연기금": 1000, "투신": 1244, "사모펀드": 655, "기타법인": 137},
                                "analysis": "5주체 전원매수 중 가장 높은 점수를 기록했습니다. 디스플레이 소재 핵심 기업으로, OLED 시장 확대 수혜가 기대됩니다. 외국인(22억)과 투신(12억)이 주도적으로 매수했으며, 하락장에서도 +1.15% 상승하며 수급 강도를 입증했습니다."
                            },
                            {
                                "rank": 2,
                                "name": "LG이노텍",
                                "code": "011070",
                                "sector": "카메라모듈",
                                "combo": "외+연+투+사+기",
                                "n_buyers": 5,
                                "score": 122.6,
                                "change_pct": -2.03,
                                "base_price": 290000,
                                "market_cap": 68635,
                                "amounts": {"외국인": 7035, "연기금": 9073, "투신": 4879, "사모펀드": 1121, "기타법인": 920},
                                "analysis": "연기금이 90억원으로 가장 강력하게 매수했습니다. 외국인(70억), 투신(48억)도 대규모 순매수를 기록했습니다. 카메라모듈 글로벌 1위 기업으로, 5주체 합계 230억원의 압도적 매수세가 포착되었습니다."
                            },
                            {
                                "rank": 3,
                                "name": "씨엠티엑스",
                                "code": "388210",
                                "sector": "반도체식각",
                                "combo": "외+연+투+사",
                                "n_buyers": 4,
                                "score": 99.6,
                                "change_pct": -4.03,
                                "base_price": 145100,
                                "market_cap": 13806,
                                "amounts": {"외국인": 2645, "연기금": 1529, "투신": 3753, "사모펀드": 513, "기타법인": -550},
                                "analysis": "투신이 37억원으로 가장 적극적으로 매수했습니다. 반도체 식각 장비 전문기업으로, 기타법인이 매도(-5.5억)하여 충돌패널티(-15점)가 적용되었지만, 나머지 4주체의 매수 강도가 이를 상쇄합니다."
                            }
                        ]
                    },
                    {
                        "title": "3. 주목할 섹터: 화장품",
                        "type": "sector_focus",
                        "body": "오늘 TOP25 중 화장품 섹터가 4종목이나 포함되었습니다. 한국콜마(121.4점), 코스맥스(117.4점), 아모레퍼시픽(117.6점), 실리콘투(78.8점)가 모두 기관 다수 매수를 받았습니다. K-뷰티 수출 호조와 중국 소비 회복 기대감이 반영된 것으로 보입니다.",
                        "stocks": [
                            {"name": "한국콜마", "score": 121.4, "combo": "외+연+투+사", "change_pct": 0.00},
                            {"name": "코스맥스", "score": 117.4, "combo": "외+연+투+사", "change_pct": 0.97},
                            {"name": "아모레퍼시픽", "score": 117.6, "combo": "외+연+투+사+기", "change_pct": -3.41},
                            {"name": "실리콘투", "score": 78.8, "combo": "외+투+사", "change_pct": -1.30}
                        ]
                    },
                    {
                        "title": "4. D전략 스크리닝 (월요일 특별)",
                        "type": "d_strategy",
                        "body": "월요일 특별 분석인 D전략(외국인+연기금+사모펀드 동시매수)에서 14개 종목이 포착되었습니다. 이 조합은 역대 백테스트에서 양의 피크수익률 100%를 기록한 가장 강력한 시그널입니다.",
                        "count": 14
                    },
                    {
                        "title": "5. 투자 포인트 정리",
                        "type": "conclusion",
                        "body": "시장이 -3.73% 하락한 약세장에서도 기관 5주체가 동시에 매수한 3종목(덕산네오룩스, LG이노텍, 아모레퍼시픽)이 포착되었습니다. 특히 덕산네오룩스는 하락장에서 +1.15% 상승하며 수급 강도를 입증했습니다. 화장품 섹터에 기관 자금이 집중되는 흐름이 주목됩니다. D전략 14종목은 향후 D+1~D+5 추적을 통해 성과를 검증할 예정입니다."
                    }
                ]
            }, ensure_ascii=False),
            "top3": json.dumps([
                {"rank": 1, "name": "덕산네오룩스", "sector": "디스플레이", "score": 124.6},
                {"rank": 2, "name": "LG이노텍", "sector": "카메라모듈", "score": 122.6},
                {"rank": 3, "name": "씨엠티엑스", "sector": "반도체식각", "score": 99.6}
            ], ensure_ascii=False),
            "market_summary": json.dumps({
                "avg_change": -3.73,
                "total_stocks": 212,
                "five_buyers": 3,
                "d_strategy": 14
            }, ensure_ascii=False)
        }
        
        r3 = requests.post(
            f'{url}/rest/v1/blog_posts',
            headers={**headers, 'Prefer': 'return=representation'},
            json=report
        )
        if r3.status_code in [200, 201]:
            print("3/23 report inserted successfully!")
            print(json.dumps(r3.json(), ensure_ascii=False, indent=2)[:500])
        else:
            print(f"Insert failed: {r3.status_code}")
            print(r3.text)
