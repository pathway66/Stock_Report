"""오늘(2026-03-30) blog_posts insert"""
import os, json, requests
from dotenv import load_dotenv
load_dotenv()

URL = os.getenv("SUPABASE_URL", "").rstrip('/')
KEY = os.getenv("SUPABASE_KEY", "")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json", "Prefer": "return=representation"}

report = {
    "date": "2026-03-30",
    "title": "[수급분석] 2026-03-30 TOP3: 성광벤드, 에이피알, 엠케이전자",
    "category": "수급분석",
    "summary": "분석 208종목, 평균 -1.77%, 5주체전원매수 0개, 이란전쟁 격화 우려 급락장",
    "content": json.dumps({
        "sections": [
            {
                "title": "주요 시장 지표",
                "type": "market_indicators",
                "body": "",
                "indicators": [
                    {"name": "KOSPI", "close": "5,206", "close_raw": 5206, "change_pct": -4.3, "category": "index", "note": "이란전쟁 격화 우려 급락"},
                    {"name": "KOSDAQ", "close": "1,096", "close_raw": 1096, "change_pct": -4.0, "category": "index", "note": ""},
                    {"name": "USD/KRW", "close": "1,513", "close_raw": 1513, "change_pct": 0.3, "category": "fx", "note": ""}
                ]
            },
            {
                "title": "오늘 최종선정 TOP3",
                "type": "top3",
                "body": "이란 전쟁 격화 우려로 코스피가 장중 4.3% 급락한 가운데, AI 수급분석 기반으로 기관 매수세가 집중된 TOP3 종목을 선정했습니다.",
                "stocks": [
                    {
                        "rank": 1, "name": "성광벤드", "code": "014620",
                        "sector": "관이음쇠", "combo": "외+연+투+사", "n_buyers": 4,
                        "score": 110.8, "change_pct": 2.71, "base_price": 34050, "market_cap": 0,
                        "amounts": {},
                        "analysis": "# 성광벤드 수급 분석 리포트\n\n## 수급 분석\n\n성광벤드는 외국인, 연기금, 투신, 사모펀드 4주체가 동시 매수하는 강력한 수급 패턴을 보이고 있습니다. 시장 전체가 이란 전쟁 격화 우려로 4.3% 급락한 상황에서도 +2.71% 역행 상승을 기록한 점이 특히 주목됩니다. 공포 속에서도 기관이 사는 종목은 강한 펀더멘털 신뢰를 의미합니다.\n\n## 섹터 및 종목 특성\n\n성광벤드는 관이음쇠(배관 피팅) 전문 기업으로, 에너지 인프라와 조선·플랜트 분야에 핵심 부품을 공급합니다. 중동 긴장 고조로 에너지 인프라 투자 확대 수혜가 예상되며, 방산 관련 수요 증가도 긍정적입니다.\n\n## 투자 포인트\n\n급락장 역행 상승 + 4주체 동시 매수는 매우 강한 시그널입니다. 110.8점의 높은 수급 점수와 함께, 에너지·방산 섹터의 구조적 수혜가 기대됩니다."
                    },
                    {
                        "rank": 2, "name": "에이피알", "code": "278470",
                        "sector": "화장품, 피부미용기기 등", "combo": "외+연+투+사", "n_buyers": 4,
                        "score": 90.9, "change_pct": 0, "base_price": 326000, "market_cap": 0,
                        "amounts": {},
                        "analysis": "# 에이피알 수급 분석 리포트\n\n## 수급 분석\n\n에이피알은 외국인, 연기금, 투신, 사모펀드 4주체가 동시 매수하는 패턴을 보이고 있습니다. 90.9점의 양호한 수급 점수를 기록했으며, K-뷰티 섹터의 대표 종목으로서 기관의 꾸준한 관심을 받고 있습니다.\n\n## 섹터 및 종목 특성\n\n에이피알은 화장품과 피부미용기기를 주력으로 하는 K-뷰티 기업입니다. 글로벌 K-뷰티 열풍과 함께 해외 수출이 급증하고 있으며, 뷰티디바이스 시장에서의 경쟁력이 부각되고 있습니다.\n\n## 투자 포인트\n\n4주체 동시 매수는 기관들의 K-뷰티 섹터에 대한 긍정적 시각을 반영합니다. 글로벌 수출 확대와 브랜드 인지도 상승이 주가 상승 모멘텀으로 작용할 전망입니다."
                    },
                    {
                        "rank": 3, "name": "엠케이전자", "code": "033160",
                        "sector": "반도체용세금선", "combo": "연+투+사", "n_buyers": 3,
                        "score": 83.0, "change_pct": 0, "base_price": 15700, "market_cap": 0,
                        "amounts": {},
                        "analysis": "# 엠케이전자 수급 분석 리포트\n\n## 수급 분석\n\n엠케이전자는 연기금, 투신, 사모펀드 3주체가 동시 매수하고 있습니다. 83.0점의 수급 점수를 기록했으며, 반도체 후공정 핵심 소재 기업으로서 안정적인 기관 매수세를 보이고 있습니다.\n\n## 섹터 및 종목 특성\n\n반도체용 세금선(bonding wire) 전문 기업으로, AI 반도체 수요 급증에 따른 후공정 소재 수혜가 예상됩니다. HBM 등 첨단 패키징 기술 확산으로 세금선 수요가 구조적으로 증가하고 있습니다.\n\n## 투자 포인트\n\n연기금 매수 참여는 장기적 성장성에 대한 신뢰를 의미합니다. AI 반도체 사이클의 수혜와 함께 실적 개선이 기대됩니다."
                    }
                ]
            }
        ]
    }, ensure_ascii=False),
    "top3": json.dumps([
        {"rank": 1, "name": "성광벤드", "sector": "관이음쇠", "score": 110.8},
        {"rank": 2, "name": "에이피알", "sector": "화장품, 피부미용기기 등", "score": 90.9},
        {"rank": 3, "name": "엠케이전자", "sector": "반도체용세금선", "score": 83.0}
    ], ensure_ascii=False),
    "market_summary": json.dumps({
        "avg_change": -1.77,
        "total_stocks": 208,
        "five_buyers": 0,
        "d_strategy": 0
    }, ensure_ascii=False)
}

resp = requests.post(f'{URL}/rest/v1/blog_posts', headers=H, json=report)
if resp.status_code in [200, 201]:
    print("[OK] blog_posts insert 성공!")
    print(f"  날짜: 2026-03-30")
    print(f"  제목: {report['title']}")
else:
    print(f"[X] insert 실패: {resp.status_code}")
    print(resp.text[:200])
