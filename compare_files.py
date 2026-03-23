"""
영웅문4 CSV vs API CSV 비교 스크립트
====================================
사용법:
  1) 영웅문4에서 다운받은 파일을 hero_data 폴더에 넣기
  2) API로 생성된 파일은 kiwoom_data 폴더에 있음
  3) python compare_files.py
"""

import csv
import os

# ============================================================
# ★ 폴더 경로 수정하세요 ★
# ============================================================
HERO_DIR = "./hero_data"      # 영웅문4 다운로드 파일 폴더
API_DIR = "./kiwoom_data"     # API 자동수집 파일 폴더
# ============================================================

def load_csv(filepath):
    """CSV 파일 로드 → {종목코드: 순매수금액} 딕셔너리"""
    data = {}
    try:
        with open(filepath, encoding='euc-kr') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                code = row[0].strip().replace("'", "").zfill(6)
                name = row[1].strip()
                try:
                    amt = int(row[3].strip().replace(',','').replace('+',''))
                except:
                    amt = 0
                data[code] = {'name': name, 'amt': amt}
    except Exception as e:
        print(f"  [W]️ 파일 읽기 실패: {filepath} ({e})")
    return data


def compare_one(hero_file, api_file, label):
    """파일 1쌍 비교"""
    print(f"\n{'='*55}")
    print(f"  [G] {label}")
    print(f"{'='*55}")

    hero_path = os.path.join(HERO_DIR, hero_file)
    api_path = os.path.join(API_DIR, api_file)

    if not os.path.exists(hero_path):
        print(f"  [W]️ 영웅문4 파일 없음: {hero_file}")
        return
    if not os.path.exists(api_path):
        print(f"  [W]️ API 파일 없음: {api_file}")
        return

    hero = load_csv(hero_path)
    api = load_csv(api_path)

    print(f"  영웅문4: {len(hero)}종목  |  API: {len(api)}종목")

    # 공통 종목 비교
    common = set(hero.keys()) & set(api.keys())
    hero_only = set(hero.keys()) - set(api.keys())
    api_only = set(api.keys()) - set(hero.keys())

    print(f"  공통: {len(common)}종목 | 영웅문4만: {len(hero_only)} | API만: {len(api_only)}")

    # 금액 일치 확인 (상위 10종목)
    match_count = 0
    mismatch = []
    for code in common:
        if hero[code]['amt'] == api[code]['amt']:
            match_count += 1
        else:
            diff = api[code]['amt'] - hero[code]['amt']
            mismatch.append((code, hero[code]['name'], hero[code]['amt'], api[code]['amt'], diff))

    print(f"  금액 일치: {match_count}/{len(common)}")

    if mismatch:
        print(f"\n  [W]️ 금액 불일치 상위 5개:")
        mismatch.sort(key=lambda x: abs(x[4]), reverse=True)
        for code, name, h_amt, a_amt, diff in mismatch[:5]:
            print(f"    {name}({code}): 영웅문4={h_amt:,} API={a_amt:,} 차이={diff:+,}")
    else:
        print(f"  [OK] 전 종목 금액 완벽 일치!")

    # 상위 5종목 비교
    hero_top5 = sorted(hero.items(), key=lambda x: x[1]['amt'], reverse=True)[:5]
    api_top5 = sorted(api.items(), key=lambda x: x[1]['amt'], reverse=True)[:5]

    print(f"\n  TOP5 비교:")
    print(f"  {'순위':>4}  {'영웅문4':<20}  {'API':<20}")
    print(f"  {'':>4}  {'────────────────────':<20}  {'────────────────────':<20}")
    for i in range(5):
        h = f"{hero_top5[i][1]['name']}({hero_top5[i][1]['amt']:,})" if i < len(hero_top5) else "-"
        a = f"{api_top5[i][1]['name']}({api_top5[i][1]['amt']:,})" if i < len(api_top5) else "-"
        print(f"  {i+1:>4}  {h:<20}  {a:<20}")


def main():
    print("[*] 영웅문4 vs API 데이터 비교")
    print(f"   영웅문4 폴더: {os.path.abspath(HERO_DIR)}")
    print(f"   API 폴더: {os.path.abspath(API_DIR)}")

    if not os.path.exists(HERO_DIR):
        print(f"\n[W]️ hero_data 폴더가 없습니다!")
        print(f"   1) {HERO_DIR} 폴더를 만들고")
        print(f"   2) 영웅문4에서 다운받은 CSV 파일을 넣으세요")
        print(f"   (파일명 예: 외국인_순매수_260319.csv)")
        os.makedirs(HERO_DIR, exist_ok=True)
        return

    subjects = ['외국인', '연기금', '투신', '사모펀드', '기타법인']
    for subj in subjects:
        for trade in ['매수', '매도']:
            fname = f"{subj}_순{trade}_260319.csv"
            compare_one(fname, fname, f"{subj} 순{trade}")

    print(f"\n{'='*55}")
    print("비교 완료!")
    print("='*55")


if __name__ == "__main__":
    main()
