"""여러 종목 combo 테스트 실행기"""
import subprocess, sys

codes = [
    ('267260', 'HD현대일렉트릭'),
    ('240810', '원익IPS'),
    ('036930', '주성엔지니어링'),
]

# 원본 소스 읽기
with open('test_samsung_combos.py', 'r', encoding='utf-8') as f:
    src = f.read()

for code, name in codes:
    modified = src.replace("CODE = '005930'", f"CODE = '{code}'")
    with open('test_temp.py', 'w', encoding='utf-8') as f:
        f.write(modified)
    print(f"\n{'#'*70}")
    print(f"# {name} ({code})")
    print(f"{'#'*70}")
    subprocess.run([sys.executable, 'test_temp.py'])
