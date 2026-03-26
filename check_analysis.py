import requests, os, json
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
h = {'apikey': key, 'Authorization': f'Bearer {key}'}

r = requests.get(f'{url}/rest/v1/blog_posts?date=eq.2026-03-26&limit=1', headers=h)
d = r.json()[0]
content = json.loads(d['content'])

for sec in content['sections']:
    if sec['type'] == 'top3':
        for s in sec['stocks']:
            analysis = s.get('analysis', '')
            name = s.get('name', '')
            last50 = analysis[-80:] if len(analysis) > 80 else analysis
            print(f'{name}: {len(analysis)}chars')
            print(f'  끝: ...{last50}')
            print(f'  마침표 끝: {analysis.rstrip().endswith(".")}')
            print()
