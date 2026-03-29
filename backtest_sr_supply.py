"""
============================================================
S/R Supply Backtest Engine v1.1
============================================================
v1.0 → v1.1 변경사항:
  - KOSPI200 + KOSDAQ150 (350종목) 필터링 추가
  - 개별 종목별 수급 에너지 프로파일 분석 추가
  - index_stocks_350.json 파일 필요

사용법:
  python backtest_sr_supply.py

필요 환경:
  - .env 파일에 SUPABASE_URL, SUPABASE_KEY 설정
  - index_stocks_350.json (같은 폴더)
  - pip install requests python-dotenv pandas

출력:
  - 콘솔: 콤보 티어별 지지선 성공률 매트릭스
  - CSV: backtest_sr_supply_result.csv (상세 이벤트 로그)
  - CSV: backtest_sr_supply_summary.csv (요약 테이블)
  - CSV: backtest_per_stock_profile.csv (종목별 수급 에너지 프로파일)
============================================================
"""

import os
import sys
import json
import time
from datetime import datetime
from collections import defaultdict, Counter
from dotenv import load_dotenv

try:
    import pandas as pd
except ImportError:
    print("pandas 필요: pip install pandas")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("requests 필요: pip install requests")
    sys.exit(1)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: .env에 SUPABASE_URL, SUPABASE_KEY를 설정하세요.")
    sys.exit(1)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

INDEX_STOCKS_FILE = "index_stocks_350.json"

def load_index_stocks():
    if not os.path.exists(INDEX_STOCKS_FILE):
        print(f"ERROR: {INDEX_STOCKS_FILE} 파일이 없습니다.")
        sys.exit(1)
    with open(INDEX_STOCKS_FILE, "r", encoding="utf-8") as f:
        stocks = json.load(f)
    k200 = sum(1 for v in stocks.values() if v["index"] == "KOSPI200")
    k150 = sum(1 for v in stocks.values() if v["index"] == "KOSDAQ150")
    print(f"📋 대상: KOSPI200({k200}) + KOSDAQ150({k150}) = {len(stocks)}종목")
    return stocks

BUY_COMBOS = {
    "S":  {"name": "외+연+사", "required": {"외국인", "연기금", "사모펀드"}},
    "A1": {"name": "외+연",   "required": {"외국인", "연기금"}},
    "A2": {"name": "외+사",   "required": {"외국인", "사모펀드"}},
    "B1": {"name": "외+투신", "required": {"외국인", "투신"}},
    "B2": {"name": "연+사",   "required": {"연기금", "사모펀드"}},
}

SELL_COMBOS = {
    "S":  {"name": "외sell+연sell", "required": {"외국인", "연기금"}},
    "A1": {"name": "외sell(대량)",  "required": {"외국인"}},
    "A2": {"name": "연sell(희귀)",  "required": {"연기금"}},
    "B1": {"name": "사sell",        "required": {"사모펀드"}},
}

TEST_WINDOWS = [5, 10, 20]
SUPPORT_SUCCESS_THRESHOLD = -0.05
SUPPORT_FAILURE_THRESHOLD = -0.07
MIN_NET_BUY_AMOUNT = 500

def fetch_supabase(table, params="", limit=1000):
    all_data = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={limit}&offset={offset}"
        if "order=" not in params:
            url += "&order=date.asc"
        r = requests.get(url, headers={**HEADERS, "Prefer": "count=exact"})
        if r.status_code not in (200, 206):
            print(f"  ERROR {table}: {r.status_code} {r.text[:200]}")
            break
        data = r.json()
        if not data:
            break
        all_data.extend(data)
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.1)
    return all_data

def load_supply_data(index_codes):
    print("\n📥 daily_supply 로딩 중...")
    code_list = list(index_codes)
    all_data = []
    for i in range(0, len(code_list), 50):
        batch = code_list[i:i+50]
        params = f"select=date,stock_code,stock_name,subject,direction,amount&stock_code=in.({','.join(batch)})"
        all_data.extend(fetch_supabase("daily_supply", params))
        if (i // 50 + 1) % 3 == 0:
            print(f"  진행: {min(i+50, len(code_list))}/{len(code_list)} 종목...")
    if not all_data:
        print("  ERROR: 데이터 없음"); return None
    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    print(f"  → {len(df):,}건 ({df['date'].min()} ~ {df['date'].max()}) / {df['stock_code'].nunique()}종목 / {df['date'].nunique()}일")
    return df

def load_ohlcv_data(index_codes):
    print("\n📥 daily_ohlcv 로딩 중...")
    code_list = list(index_codes)
    all_data = []
    for i in range(0, len(code_list), 50):
        batch = code_list[i:i+50]
        params = f"select=date,stock_code,open,high,low,close,volume&stock_code=in.({','.join(batch)})"
        all_data.extend(fetch_supabase("daily_ohlcv", params))
        if (i // 50 + 1) % 3 == 0:
            print(f"  진행: {min(i+50, len(code_list))}/{len(code_list)} 종목...")
    if not all_data:
        print("  ERROR: 데이터 없음"); return None
    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["typical_price"] = ((df["high"]+df["low"]+df["close"])/3).round(0)
    print(f"  → {len(df):,}건 ({df['date'].min()} ~ {df['date'].max()}) / {df['stock_code'].nunique()}종목 / {df['date'].nunique()}일")
    return df

def calculate_net_supply(supply_df):
    print("\n🔧 순매수 계산 중...")
    buy = supply_df[supply_df["direction"]=="매수"].groupby(["date","stock_code","subject"])["amount"].sum().reset_index().rename(columns={"amount":"buy_amount"})
    sell = supply_df[supply_df["direction"]=="매도"].groupby(["date","stock_code","subject"])["amount"].sum().reset_index().rename(columns={"amount":"sell_amount"})
    net = pd.merge(buy, sell, on=["date","stock_code","subject"], how="outer")
    net["buy_amount"] = net["buy_amount"].fillna(0)
    net["sell_amount"] = net["sell_amount"].fillna(0)
    net["net_amount"] = net["buy_amount"] - net["sell_amount"]
    print(f"  → {len(net):,}건")
    return net

def detect_combo_events(net_df, ohlcv_df, index_stocks):
    print("\n🔍 콤보 이벤트 감지 중...")
    events = []
    grouped = net_df.groupby(["date","stock_code"])
    total = len(grouped)
    for idx, ((date, code), group) in enumerate(grouped):
        if (idx+1) % 5000 == 0:
            print(f"  진행: {idx+1}/{total} ({(idx+1)/total*100:.1f}%)")
        buyers = set(group[group["net_amount"]>MIN_NET_BUY_AMOUNT]["subject"])
        sellers = set(group[group["net_amount"]<-MIN_NET_BUY_AMOUNT]["subject"])
        info = index_stocks.get(code, {})
        for tier, c in BUY_COMBOS.items():
            if c["required"].issubset(buyers):
                row = ohlcv_df[(ohlcv_df["date"]==date)&(ohlcv_df["stock_code"]==code)]
                if len(row)==0: continue
                bp = row.iloc[0]["typical_price"]
                if bp <= 0: continue
                net_amt = group[group["subject"].isin(c["required"])]["net_amount"].sum()
                events.append({"date":date,"stock_code":code,"stock_name":info.get("name",""),"stock_index":info.get("index",""),"event_type":"buy","combo_tier":tier,"combo_name":c["name"],"buy_price":bp,"close_price":row.iloc[0]["close"],"volume":row.iloc[0]["volume"],"total_net_amount":net_amt,"net_buyers":",".join(sorted(buyers))})
                break
        for tier, c in SELL_COMBOS.items():
            if c["required"].issubset(sellers):
                row = ohlcv_df[(ohlcv_df["date"]==date)&(ohlcv_df["stock_code"]==code)]
                if len(row)==0: continue
                sp = row.iloc[0]["typical_price"]
                net_amt = abs(group[group["subject"].isin(c["required"])]["net_amount"].sum())
                events.append({"date":date,"stock_code":code,"stock_name":info.get("name",""),"stock_index":info.get("index",""),"event_type":"sell","combo_tier":tier,"combo_name":c["name"],"buy_price":sp,"close_price":row.iloc[0]["close"],"volume":0,"total_net_amount":net_amt,"net_buyers":""})
                break
    edf = pd.DataFrame(events)
    if len(edf)>0:
        be = edf[edf["event_type"]=="buy"]; se = edf[edf["event_type"]=="sell"]
        print(f"  → 매수: {len(be):,}건 / 매도: {len(se):,}건")
        for idx_name in ["KOSPI200","KOSDAQ150"]:
            print(f"  → {idx_name} 매수: {len(be[be['stock_index']==idx_name]):,}건")
        print("\n  📊 매수 콤보 분포:")
        for t in ["S","A1","A2","B1","B2"]:
            cnt = len(be[be["combo_tier"]==t])
            if cnt>0: print(f"     {t} ({BUY_COMBOS[t]['name']}): {cnt:,}건")
    return edf

def validate_support(events_df, ohlcv_df):
    print("\n📈 지지선 검증 중...")
    be = events_df[events_df["event_type"]=="buy"].copy()
    if len(be)==0: print("  없음"); return pd.DataFrame()
    ohlcv_by = {code: g.sort_values("date").reset_index(drop=True) for code, g in ohlcv_df.groupby("stock_code")}
    results = []
    total = len(be)
    for idx, (_, ev) in enumerate(be.iterrows()):
        if (idx+1)%500==0: print(f"  진행: {idx+1}/{total} ({(idx+1)/total*100:.1f}%)")
        code, edate, bp = ev["stock_code"], ev["date"], ev["buy_price"]
        if code not in ohlcv_by: continue
        fut = ohlcv_by[code][ohlcv_by[code]["date"]>edate]
        if len(fut)==0: continue
        for w in TEST_WINDOWS:
            wd = fut.head(w)
            if len(wd)<max(3,w//2): continue
            ml, mh, fc = wd["low"].min(), wd["high"].max(), wd.iloc[-1]["close"]
            mdd = (ml-bp)/bp; mup = (mh-bp)/bp; fr = (fc-bp)/bp
            res = "SUCCESS" if mdd>=SUPPORT_SUCCESS_THRESHOLD else ("PARTIAL" if mdd>=SUPPORT_FAILURE_THRESHOLD else "FAILURE")
            rd = None
            mi = wd["low"].idxmin(); mp = wd.index.get_loc(mi)
            rec = wd.iloc[mp:][wd.iloc[mp:]["close"]>=bp]
            if len(rec)>0: rd = wd.index.get_loc(rec.index[0])-mp
            results.append({"date":edate,"stock_code":code,"stock_name":ev["stock_name"],"stock_index":ev["stock_index"],"combo_tier":ev["combo_tier"],"combo_name":ev["combo_name"],"buy_price":bp,"total_net_amount":ev["total_net_amount"],"window":w,"max_drawdown":round(mdd*100,2),"max_upside":round(mup*100,2),"final_return":round(fr*100,2),"result":res,"recovery_days":rd,"data_points":len(wd)})
    rdf = pd.DataFrame(results)
    print(f"  → {len(rdf):,}건 완료")
    return rdf

def classify_market_regime(ohlcv_df):
    print("\n🌐 시장 국면 분류 중...")
    ss = ohlcv_df[ohlcv_df["stock_code"]=="005930"].sort_values("date").copy()
    if len(ss)<25: print("  ⚠️ 데이터 부족"); return {}
    ss["ma20"]=ss["close"].rolling(20).mean(); ss["slope"]=ss["ma20"].pct_change(5)
    rm = {}
    for _, r in ss.iterrows():
        rm[r["date"]] = "uptrend" if r["slope"]>0.01 else ("downtrend" if r["slope"]<-0.01 else "sideways") if not pd.isna(r["slope"]) else "unknown"
    c = Counter(rm.values())
    print(f"  → 상승:{c.get('uptrend',0)}일 하락:{c.get('downtrend',0)}일 횡보:{c.get('sideways',0)}일")
    return rm

def generate_summary(result_df, regime_map):
    if len(result_df)==0: print("\n⚠️ 결과 없음"); return pd.DataFrame()
    result_df["market_regime"] = result_df["date"].map(regime_map).fillna("unknown")
    summaries = []
    print("\n"+"="*75); print("📊 전체 콤보 티어별 결과"); print("="*75)
    for w in TEST_WINDOWS:
        print(f"\n--- {w}일 윈도우 ---")
        print(f"{'티어':<6} {'콤보':<12} {'건수':>6} {'성공':>6} {'부분':>6} {'실패':>6} {'성공률':>7} {'MDD':>8} {'수익':>8}")
        print("-"*75)
        wd = result_df[result_df["window"]==w]
        for t in ["S","A1","A2","B1","B2"]:
            td = wd[wd["combo_tier"]==t]
            if len(td)==0: continue
            n=len(td); s=len(td[td["result"]=="SUCCESS"]); p=len(td[td["result"]=="PARTIAL"]); f=len(td[td["result"]=="FAILURE"])
            r=s/n*100; m=td["max_drawdown"].mean(); ret=td["final_return"].mean()
            nm=BUY_COMBOS.get(t,{}).get("name",t)
            print(f"{t:<6} {nm:<12} {n:>6} {s:>6} {p:>6} {f:>6} {r:>6.1f}% {m:>7.2f}% {ret:>7.2f}%")
            summaries.append({"window":w,"combo_tier":t,"combo_name":nm,"total_events":n,"success":s,"partial":p,"failure":f,"success_rate":round(r,1),"avg_max_drawdown":round(m,2),"avg_final_return":round(ret,2),"segment":"all","market_regime":"all"})
    # KOSPI200 vs KOSDAQ150
    print("\n\n--- KOSPI200 vs KOSDAQ150 (20일) ---")
    w20 = result_df[result_df["window"]==20]
    for ix in ["KOSPI200","KOSDAQ150"]:
        id_ = w20[w20["stock_index"]==ix]
        if len(id_)==0: continue
        print(f"\n  📌 {ix}")
        print(f"  {'티어':<6} {'콤보':<12} {'건수':>6} {'성공률':>7} {'MDD':>8} {'수익':>8}")
        print(f"  {'-'*55}")
        for t in ["S","A1","A2","B1","B2"]:
            td=id_[id_["combo_tier"]==t]
            if len(td)==0: continue
            n=len(td); s=len(td[td["result"]=="SUCCESS"]); r=s/n*100; m=td["max_drawdown"].mean(); ret=td["final_return"].mean()
            nm=BUY_COMBOS.get(t,{}).get("name",t)
            print(f"  {t:<6} {nm:<12} {n:>6} {r:>6.1f}% {m:>7.2f}% {ret:>7.2f}%")
            summaries.append({"window":20,"combo_tier":t,"combo_name":nm,"total_events":n,"success":s,"partial":len(td[td["result"]=="PARTIAL"]),"failure":len(td[td["result"]=="FAILURE"]),"success_rate":round(r,1),"avg_max_drawdown":round(m,2),"avg_final_return":round(ret,2),"segment":ix,"market_regime":"all"})
    # 시장 국면별
    print("\n\n--- 시장 국면별 (20일) ---")
    for rg in ["uptrend","downtrend","sideways"]:
        rd=w20[w20["market_regime"]==rg]
        if len(rd)==0: continue
        rk={"uptrend":"상승장","downtrend":"하락장","sideways":"횡보장"}[rg]
        print(f"\n  📌 {rk}")
        print(f"  {'티어':<6} {'콤보':<12} {'건수':>6} {'성공률':>7} {'MDD':>8}")
        print(f"  {'-'*45}")
        for t in ["S","A1","A2","B1","B2"]:
            td=rd[rd["combo_tier"]==t]
            if len(td)==0: continue
            n=len(td); s=len(td[td["result"]=="SUCCESS"]); r=s/n*100; m=td["max_drawdown"].mean()
            nm=BUY_COMBOS.get(t,{}).get("name",t)
            print(f"  {t:<6} {nm:<12} {n:>6} {r:>6.1f}% {m:>7.2f}%")
            summaries.append({"window":20,"combo_tier":t,"combo_name":nm,"total_events":n,"success":s,"partial":len(td[td["result"]=="PARTIAL"]),"failure":len(td[td["result"]=="FAILURE"]),"success_rate":round(r,1),"avg_max_drawdown":round(m,2),"avg_final_return":round(td["final_return"].mean(),2),"segment":"all","market_regime":rg})
    return pd.DataFrame(summaries)

def generate_per_stock_profile(result_df, index_stocks):
    print("\n\n"+"="*75); print("🧬 개별 종목 수급 에너지 프로파일 (20일)"); print("="*75)
    w20 = result_df[result_df["window"]==20]
    if len(w20)==0: print("  데이터 없음"); return pd.DataFrame()
    profiles = []
    for code, sdf in w20.groupby("stock_code"):
        sn=sdf.iloc[0]["stock_name"]; si=sdf.iloc[0]["stock_index"]; n=len(sdf)
        if n<3: continue
        sc=len(sdf[sdf["result"]=="SUCCESS"]); sr=sc/n*100; mdd=sdf["max_drawdown"].mean(); ret=sdf["final_return"].mean()
        best_c=None; best_r=-1; best_n=0; worst_c=None; worst_r=101; dom_c=None; dom_n=0; details=[]
        for t in ["S","A1","A2","B1","B2"]:
            td=sdf[sdf["combo_tier"]==t]
            if len(td)==0: continue
            cn=len(td); cs=len(td[td["result"]=="SUCCESS"]); cr=cs/cn*100
            nm=BUY_COMBOS.get(t,{}).get("name",t)
            details.append(f"{t}({nm}):{cr:.0f}%/{cn}건")
            if cn>dom_n: dom_n=cn; dom_c=f"{t}({nm})"
            if cn>=2 and cr>best_r: best_r=cr; best_c=f"{t}({nm})"; best_n=cn
            if cn>=2 and cr<worst_r: worst_r=cr; worst_c=f"{t}({nm})"
        profiles.append({"stock_code":code,"stock_name":sn,"stock_index":si,"total_events":n,"overall_success_rate":round(sr,1),"overall_avg_mdd":round(mdd,2),"overall_avg_return":round(ret,2),"dominant_combo":dom_c or "N/A","dominant_count":dom_n,"best_combo":best_c or "N/A","best_combo_rate":round(best_r,1) if best_r>=0 else 0,"best_combo_count":best_n,"worst_combo":worst_c or "N/A","worst_combo_rate":round(worst_r,1) if worst_r<=100 else 0,"combo_details":" | ".join(details)})
    pdf = pd.DataFrame(profiles).sort_values("overall_success_rate",ascending=False)
    if len(pdf)==0: print("  분석 종목 없음"); return pd.DataFrame()
    print(f"\n  분석 종목: {len(pdf)}개")
    print(f"\n  🏆 지지 성공률 TOP 10:")
    print(f"  {'종목명':<14} {'인덱스':<10} {'이벤트':>6} {'성공률':>7} {'MDD':>8} {'베스트콤보':<16}")
    print(f"  {'-'*65}")
    for _,r in pdf.head(10).iterrows():
        print(f"  {r['stock_name']:<14} {r['stock_index']:<10} {r['total_events']:>6} {r['overall_success_rate']:>6.1f}% {r['overall_avg_mdd']:>7.2f}% {r['best_combo']:<16}")
    print(f"\n  ⚠️ 지지 성공률 하위 10:")
    print(f"  {'종목명':<14} {'인덱스':<10} {'이벤트':>6} {'성공률':>7} {'MDD':>8} {'워스트콤보':<16}")
    print(f"  {'-'*65}")
    for _,r in pdf.tail(10).iterrows():
        print(f"  {r['stock_name']:<14} {r['stock_index']:<10} {r['total_events']:>6} {r['overall_success_rate']:>6.1f}% {r['overall_avg_mdd']:>7.2f}% {r['worst_combo']:<16}")
    print(f"\n  📊 인덱스별 평균:")
    for ix in ["KOSPI200","KOSDAQ150"]:
        id_=pdf[pdf["stock_index"]==ix]
        if len(id_)>0: print(f"     {ix}: 성공률 {id_['overall_success_rate'].mean():.1f}%, MDD {id_['overall_avg_mdd'].mean():.2f}% ({len(id_)}종목)")
    return pdf

def main():
    print("="*75); print("🔬 S/R Supply 백테스트 엔진 v1.1"); print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"); print(f"   대상: KOSPI200 + KOSDAQ150 (350종목)"); print("="*75)
    t0 = time.time()
    ix = load_index_stocks(); codes = set(ix.keys())
    sdf = load_supply_data(codes)
    if sdf is None: return
    odf = load_ohlcv_data(codes)
    if odf is None: return
    sd = set(sdf["date"].unique()); od = set(odf["date"].unique()); cd = sorted(sd & od)
    if not cd: print(f"\n❌ 공통 날짜 없음! supply:{min(sd)}~{max(sd)} ohlcv:{min(od)}~{max(od)}"); return
    print(f"\n📅 공통: {cd[0]} ~ {cd[-1]} ({len(cd)}일)")
    sdf=sdf[sdf["date"].isin(cd)]; odf=odf[odf["date"].isin(cd)]
    net = calculate_net_supply(sdf)
    evts = detect_combo_events(net, odf, ix)
    if len(evts)==0: print(f"\n❌ 이벤트 없음. MIN_NET_BUY_AMOUNT({MIN_NET_BUY_AMOUNT}) 조정 필요"); return
    res = validate_support(evts, odf)
    rm = classify_market_regime(odf)
    summ = generate_summary(res, rm)
    prof = generate_per_stock_profile(res, ix)
    if len(res)>0: res.to_csv("backtest_sr_supply_result.csv",index=False,encoding="utf-8-sig"); print(f"\n💾 상세: backtest_sr_supply_result.csv ({len(res):,}건)")
    if len(summ)>0: summ.to_csv("backtest_sr_supply_summary.csv",index=False,encoding="utf-8-sig"); print(f"💾 요약: backtest_sr_supply_summary.csv")
    if len(prof)>0: save_grades_to_supabase(prof)
    if len(prof)>0: prof.to_csv("backtest_per_stock_profile.csv",index=False,encoding="utf-8-sig"); print(f"💾 종목별: backtest_per_stock_profile.csv ({len(prof)}종목)")
    el = time.time()-t0; print(f"\n⏱ 소요: {el:.1f}초 ({el/60:.1f}분)")
    if len(summ)>0:
        a20 = summ[(summ["window"]==20)&(summ["segment"]=="all")&(summ["market_regime"]=="all")]
        if len(a20)>0:
            print("\n"+"="*75); print("🎯 핵심 결론 (20일)"); print("="*75)
            for _,r in a20.iterrows():
                g = "✅" if r["success_rate"]>=70 else ("⚠️" if r["success_rate"]>=50 else "❌")
                print(f"  {g} {r['combo_tier']} ({r['combo_name']}): 성공률 {r['success_rate']}% | MDD {r['avg_max_drawdown']}% | 수익 {r['avg_final_return']}% | {r['total_events']}건")
            print(); print("  ✅ S > A > B 순서 유지 → 콤보 검증 성공"); print("  ❌ 순서 뒤집힘 → 콤보 재검토 필요")

def save_grades_to_supabase(profile_df):
    if len(profile_df) == 0: return
    print("\n📤 Supabase sr_supply_grades 저장 중...")
    today = datetime.now().strftime("%Y-%m-%d")
    records = []
    for _, r in profile_df.iterrows():
        rate = float(r["overall_success_rate"])
        grade = "strong" if rate >= 60 else ("normal" if rate >= 40 else "weak")
        records.append({"stock_code":r["stock_code"],"stock_name":r["stock_name"],"stock_index":r["stock_index"],"grade":grade,"success_rate":rate,"avg_mdd":float(r["overall_avg_mdd"]),"avg_return":float(r["overall_avg_return"]),"best_combo":r["best_combo"],"dominant_combo":r["dominant_combo"],"total_events":int(r["total_events"]),"backtest_date":today})
    ok=0; ng=0
    for i in range(0, len(records), 50):
        batch = records[i:i+50]
        resp = requests.post(f"{SUPABASE_URL}/rest/v1/sr_supply_grades", headers={**HEADERS, "Prefer":"resolution=merge-duplicates"}, json=batch)
        if resp.status_code in (200, 201): ok += len(batch)
        else: ng += len(batch); print(f"  ERROR: {resp.status_code} {resp.text[:100]}")
    print(f"  -> 저장: {ok}건 성공, {ng}건 실패")
    s = sum(1 for x in records if x["grade"]=="strong")
    n = sum(1 for x in records if x["grade"]=="normal")
    w = sum(1 for x in records if x["grade"]=="weak")
    print(f"  -> 등급: 강({s}) / 보통({n}) / 약({w})")

if __name__ == "__main__":
    main()
