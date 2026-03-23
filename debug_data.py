import requests, json, os
from dotenv import load_dotenv
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
headers = {'apikey': key, 'Authorization': f'Bearer {key}'}

# Raw response check
r1 = requests.get(f'{url}/rest/v1/daily_supply?limit=3', headers=headers)
print(f"Status: {r1.status_code}")
print(f"Type: {type(r1.json())}")
print(f"Raw response:\n{r1.text[:1000]}")

print("\n" + "="*50)

r2 = requests.get(f'{url}/rest/v1/daily_market?limit=3', headers=headers)
print(f"Status: {r2.status_code}")
print(f"Type: {type(r2.json())}")
print(f"Raw response:\n{r2.text[:1000]}")
