"""
DART OpenAPI를 이용한 상장기업 주요제품 자동 수집
- sector_map의 2,717종목에 대해 DART에서 주요제품 정보를 가져옴
- Supabase sector_map 테이블 업데이트
"""
import requests, json, os, time, zipfile, xmltodict
from io import BytesIO
from dotenv import load_dotenv
load_dotenv()

DART_KEY = os.getenv('DART_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

sb_headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

# Step 1: DART 전체 기업코드 다운로드 (stock_code -> corp_code 매핑)
print("=" * 60)
print("[STEP 1] DART 전체 기업코드 다운로드...")
print("=" * 60)

corp_code_file = 'dart_corp_codes.json'

if os.path.exists(corp_code_file):
    print(f"  캐시 파일 사용: {corp_code_file}")
    with open(corp_code_file, 'r', encoding='utf-8') as f:
        corp_list = json.load(f)
else:
    r = requests.get('https://opendart.fss.or.kr/api/corpCode.xml',
                     params={'crtfc_key': DART_KEY})
    z = zipfile.ZipFile(BytesIO(r.content))
    xml_data = z.read('CORPCODE.xml').decode('utf-8')
    data_dict = json.loads(json.dumps(xmltodict.parse(xml_data)))
    corp_list = data_dict.get('result', {}).get('list', [])
    
    with open(corp_code_file, 'w', encoding='utf-8') as f:
        json.dump(corp_list, f, ensure_ascii=False, indent=2)
    print(f"  총 {len(corp_list)}개 기업코드 다운로드 완료")

# stock_code -> corp_code 매핑 (상장기업만)
stock_to_corp = {}
for corp in corp_list:
    sc = corp.get('stock_code', '')
    if sc and sc.strip():
        stock_to_corp[sc.strip()] = corp['corp_code']

print(f"  상장기업 매핑: {len(stock_to_corp)}개")

# Step 2: Supabase sector_map에서 종목 목록 가져오기
print("\n" + "=" * 60)
print("[STEP 2] Supabase sector_map 종목 목록 조회...")
print("=" * 60)

all_stocks = []
offset = 0
while True:
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/sector_map?select=stock_code,stock_name,sector&order=id&offset={offset}&limit=1000',
        headers=sb_headers
    )
    rows = r.json()
    if not rows:
        break
    all_stocks.extend(rows)
    offset += 1000
    if len(rows) < 1000:
        break

print(f"  sector_map 총 {len(all_stocks)}개 종목")

# Step 3: DART API로 주요제품 조회
print("\n" + "=" * 60)
print("[STEP 3] DART API로 주요제품 조회 시작...")
print("=" * 60)
print("  (DART API는 분당 약 100건 제한, 천천히 진행합니다)")

results = []
not_found = []
errors = []
skipped = []

for i, stock in enumerate(all_stocks):
    code = stock['stock_code']
    name = stock['stock_name']
    
    corp_code = stock_to_corp.get(code)
    if not corp_code:
        skipped.append(code)
        continue
    
    try:
        r = requests.get('https://opendart.fss.or.kr/api/company.json',
                        params={'crtfc_key': DART_KEY, 'corp_code': corp_code})
        data = r.json()
        
        if data.get('status') == '000':
            induty = data.get('induty_code', '')
            product = data.get('prdct', '')  # 주요제품
            est_dt = data.get('est_dt', '')
            
            if product and product.strip():
                results.append({
                    'stock_code': code,
                    'stock_name': name,
                    'product': product.strip(),
                    'old_sector': stock['sector']
                })
        else:
            not_found.append(f"{code} {name}: {data.get('message','')}")
    except Exception as e:
        errors.append(f"{code} {name}: {str(e)}")
    
    # Progress
    if (i + 1) % 50 == 0:
        print(f"  진행: {i+1}/{len(all_stocks)} ({len(results)}개 제품정보 수집)")
    
    # Rate limit: 약 0.5초 간격
    time.sleep(0.5)

print(f"\n  완료! 수집: {len(results)}개, 미발견: {len(not_found)}개, 에러: {len(errors)}개, 매핑없음: {len(skipped)}개")

# Step 4: 결과 저장 (CSV)
print("\n" + "=" * 60)
print("[STEP 4] 결과 저장...")
print("=" * 60)

with open('dart_products.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print(f"  dart_products.json 저장 ({len(results)}건)")

# 미리보기
print("\n  [미리보기 - 상위 20건]")
print(f"  {'종목코드':<8} {'종목명':<14} {'DART 주요제품':<30} {'기존 섹터'}")
print("  " + "-" * 80)
for r in results[:20]:
    product = r['product'][:28] if len(r['product']) > 28 else r['product']
    old = r['old_sector'][:20] if len(r['old_sector']) > 20 else r['old_sector']
    print(f"  {r['stock_code']:<8} {r['stock_name']:<14} {product:<30} {old}")

print(f"\n  총 {len(results)}건 수집 완료!")
print("  다음 단계: 이 데이터를 검토 후 Supabase sector_map에 반영할 수 있습니다.")
print("  dart_products.json 파일을 Claude에 업로드해주세요.")
