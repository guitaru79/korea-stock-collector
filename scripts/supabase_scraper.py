import sys
import os
import json
import time
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import math
from supabase import create_client, Client

# 프로젝트 내장 pykrx 라이브러리를 우선 참조
_lib_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib")
sys.path.insert(0, _lib_dir)

import pandas as pd
from pykrx import stock

# --- 전역 유틸리티 함수 ---

def get_market_date():
    """KST 기준 오전 8시 전까지는 전일 데이터를 유지하도록 시장 날짜를 반환"""
    now = datetime.now(timezone(timedelta(hours=9)))
    target = now - timedelta(hours=8)
    weekday = target.weekday()
    if weekday == 5: target -= timedelta(days=1)
    elif weekday == 6: target -= timedelta(days=2)
    return target

def to_json_float(val):
    """NaN 또는 Infinity 값을 0.0으로 안전하게 변환"""
    try:
        f_val = float(val)
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except:
        return 0.0

def safe_int(v):
    if not v: return 0
    try:
        clean_v = ''.join(c for c in v if c.isdigit() or c == '-')
        return int(clean_v) if clean_v else 0
    except: return 0

# --- 수급 데이터(Main) 수집 로직 ---

def fetch_naver_net_buyers(market_code, investor_code, trade_type='buy', sort_by_volume=False):
    url = f"https://finance.naver.com/sise/sise_deal_rank_iframe.naver?sosok={market_code}&investor_gubun={investor_code}&type={trade_type}"
    results = []
    for page in range(1, 4):
        p_url = f"{url}&page={page}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            res = requests.get(p_url, headers=headers, timeout=10)
            res.encoding = 'euc-kr'
            soup = BeautifulSoup(res.text, 'html.parser')
            tables = soup.select("table.type_1")
            if not tables: break
            target_table = tables[-1]
            rows = target_table.find_all("tr")
            page_results = 0
            for r in rows:
                cols = r.find_all("td")
                if len(cols) >= 4:
                    name_tag = cols[0].find("a")
                    if name_tag:
                        name = name_tag.text.strip()
                        ticker = name_tag.get("href", "").split("code=")[1]
                        vol_str = cols[1].text.strip().replace(",", "")
                        amt_str = cols[2].text.strip().replace(",", "")
                        try:
                            volume_shares = int(vol_str) * 1000
                            amount_won = int(amt_str) * 1000000
                            results.append({"ticker": ticker, "name": name, "netBuyVolume": volume_shares, "netBuyAmount": amount_won})
                            page_results += 1
                        except: continue
            if page_results == 0 or len(results) >= 30: break
            time.sleep(0.5)
        except: break
    if sort_by_volume:
        results = sorted(results, key=lambda x: x['netBuyVolume'], reverse=True)
    return results[:30]

def fetch_foreign_hold(market_code):
    url = f"https://finance.naver.com/sise/sise_foreign_hold.naver?sosok={market_code}"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'html.parser')
        rows = soup.select("table.type_2 tr")
        results = []
        for r in rows:
            cols = r.find_all("td")
            if len(cols) >= 9:
                name_tag = cols[1].find("a")
                if name_tag:
                    name = name_tag.text.strip()
                    ticker = name_tag.get("href", "").split("code=")[1]
                    rate = to_json_float(cols[8].text.strip().replace("%", ""))
                    results.append({"ticker": ticker, "name": name, "foreignRatio": rate})
        return results[:30]
    except: return []

def fetch_pension_from_judal(trade_type):
    url = f"https://www.judal.co.kr/?view=stockList&type=fund{'Buy' if trade_type=='buy' else 'Sell'}"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        soup = BeautifulSoup(res.content, 'html.parser')
        rows = soup.find('table').find_all('tr')[1:]
        kospi, kosdaq = [], []
        for row in rows:
            th = row.find('th')
            if not th: continue
            match = re.search(r'(.+?)\s+(KOSPI|KOSDAQ)\s+(\d{6})', th.text.strip().replace('\n', ' '))
            if not match: continue
            cols = row.find_all('td')
            amt_match = re.search(r'(\d+[\d\.]*)억', cols[0].text.strip().replace(',', ''))
            item = {
                "ticker": match.group(3), "name": match.group(1).strip(), "market": match.group(2),
                "price": safe_int(cols[1].text.strip().split()[0].replace(',', '')),
                "changeRate": to_json_float(cols[2].text.strip().replace('%', '')),
                "netBuyVolume": 0, "netBuyAmount": int(float(amt_match.group(1)) * 100000000) if amt_match else 0
            }
            if item["market"] == "KOSPI" and len(kospi) < 30: kospi.append(item)
            elif item["market"] == "KOSDAQ" and len(kosdaq) < 30: kosdaq.append(item)
        return {"KOSPI": kospi, "KOSDAQ": kosdaq}
    except: return {"KOSPI": [], "KOSDAQ": []}

def get_current_price_change(ticker):
    try:
        ohlcv = stock.get_market_ohlcv((datetime.today() - timedelta(days=7)).strftime("%Y%m%d"), datetime.today().strftime("%Y%m%d"), ticker)
        if not ohlcv.empty:
            last = ohlcv.iloc[-1]
            return int(last['종가']), to_json_float(last['등락률'])
    except: pass
    return 0, 0.0

# --- 거래량 데이터(Volume) 수집 로직 ---

def fetch_naver_sise_list(url):
    """
    네이버 금융 시세 리스트(거래량 상위, 급증, 급락 등)를 수집합니다.
    헤더 텍스트를 기반으로 컬럼을 동적으로 매핑하여 견고한 파싱을 보장합니다.
    """
    if 'quant_high' in url:
        menu = 'quant_high'
    elif 'quant_low' in url:
        menu = 'quant_low'
    else:
        menu = 'quant'
        
    submit_url = "https://finance.naver.com/sise/field_submit.naver"
    payload = {
        'menu': menu,
        'returnUrl': url,
        'fieldIds': ['quant', 'amount', 'prev_quant', 'ask_buy', 'ask_sell', 'frgn_rate']
    }

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    
    try:
        session = requests.Session()
        res = session.post(submit_url, data=payload, headers=headers, timeout=10)
        res.raise_for_status()
        
        # Naver Finance는 EUC-KR/CP949 인코딩을 사용합니다.
        res.encoding = 'cp949'
        
        soup = BeautifulSoup(res.text, 'html.parser')
        table = soup.select_one("table.type_2")
        if not table: return []
            
        headers_tags = table.find_all("th")
        if not headers_tags: return []
        
        headers_text = [h.text.strip() for h in headers_tags]
        
        def find_idx(name_list):
            # 1단계: 정확히 일치하는 헤더 검색
            for name in name_list:
                for i, h in enumerate(headers_text):
                    if name == h: return i
            # 2단계: 부분 일치 헤더 검색
            for name in name_list:
                for i, h in enumerate(headers_text):
                    # '거래량' 검색 시 '전일거래량'이 먼저 잡히지 않도록 예외 처리
                    if name == '거래량' and '전일' in h: continue
                    if name in h: return i
            return -1

        idx_map = {
            "name": find_idx(["종목명"]),
            "price": find_idx(["현재가"]),
            "change": find_idx(["전일비"]),
            "rate": find_idx(["등락률"]),
            "volume": find_idx(["거래량"]),
            "amount": find_idx(["거래대금"]),
            "prev_vol": find_idx(["전일거래량", "이전거래량"]),
            "buy": find_idx(["매수호가"]),
            "sell": find_idx(["매도호가"])
        }
        
        is_surge_plunge = "high" in url or "low" in url
        
        # Fallback indices if header parsing fails (updated for 2026-04-21 with fieldIds)
        if is_surge_plunge:
            # ['N', '증가율', '종목명', '현재가', '전일비', '등락률', '거래량', '거래대금', '전일거래량', '매수호가', '매도호가']
            default_map = {"name": 2, "price": 3, "change": 4, "rate": 5, "volume": 6, "amount": 7, "prev_vol": 8, "buy": 9, "sell": 10}
        else:
            # ['N', '종목명', '현재가', '전일비', '등락률', '거래량', '거래대금', '전일거래량', '매수호가', '매도호가']
            default_map = {"name": 1, "price": 2, "change": 3, "rate": 4, "volume": 5, "amount": 6, "prev_vol": 7, "buy": 8, "sell": 9}
            
        for key, def_val in default_map.items():
            if idx_map[key] == -1: idx_map[key] = def_val
        
        rows = table.find_all("tr")
        results = []
        for r in rows:
            if 'class' in r.attrs and 'line' in r.attrs['class']: continue
            cols = r.find_all("td")
            if len(cols) < 5 or not cols[0].text.strip().isdigit(): continue
            
            try:
                name_td = cols[idx_map["name"]]
                name_tag = name_td.find("a")
                if not name_tag: continue
                
                name = name_tag.text.strip()
                ticker = name_tag.get("href", "").split("code=")[-1]
                
                def get_val(key):
                    i = idx_map.get(key, -1)
                    if i == -1 or i >= len(cols): return "0"
                    return cols[i].text.strip().replace(",", "").replace("%", "")

                price = safe_int(get_val("price"))
                change_val = safe_int(get_val("change"))
                change_td = cols[idx_map["change"]]
                
                # 가려진 텍스트(blind)나 em 클래스에서 부호 찾기
                blind_span = change_td.select_one("span.blind")
                blind_text = blind_span.text.strip() if blind_span else ""
                
                # 클래스 기반 부호 판별 (nv01: 하락, red02: 상승)
                num_span = change_td.select_one("span.tah")
                span_class = "".join(num_span.get("class", [])) if num_span else ""
                
                # em 태그 클래스 확인 (bu_pdn: 하락, bu_pup: 상승)
                em_tag = change_td.find("em")
                em_class = "".join(em_tag.get("class", [])) if em_tag else ""
                
                # 부호 결정 우선순위: blind 텍스트 > 클래스
                if "하락" in blind_text or "하한" in blind_text or "nv01" in span_class or "bu_pdn" in em_class:
                    change_val = -abs(change_val)
                elif "상승" in blind_text or "상한" in blind_text or "red02" in span_class or "bu_pup" in em_class:
                    change_val = abs(change_val)
                # 아이콘(img) 태그도 여전히 체크 (예외 대비)
                else:
                    ico = change_td.find("img")
                    if ico:
                        alt = ico.get("alt", "")
                        if "하락" in alt or "하한" in alt: 
                            change_val = -abs(change_val)
                        elif "상승" in alt or "상한" in alt: 
                            change_val = abs(change_val)
                    
                rate = to_json_float(get_val("rate"))
                # change_val의 부호에 따라 rate의 부호도 맞춤
                if change_val < 0:
                    rate = -abs(rate)
                elif change_val > 0:
                    rate = abs(rate)
                # 아이콘/클래스를 못찾은 경우를 대비해 rate 부호로 역추적
                elif rate < 0:
                    change_val = -abs(change_val)
                elif rate > 0:
                    change_val = abs(change_val)
                
                volume = safe_int(get_val("volume"))
                raw_amount = safe_int(get_val("amount"))
                amount = raw_amount * 1000000 
                
                # Fallback for amount if column missing but volume exists
                if amount == 0 and volume > 0 and price > 0:
                    amount = price * volume
                
                prev_volume = safe_int(get_val("prev_vol"))
                buy_quote = safe_int(get_val("buy"))
                sell_quote = safe_int(get_val("sell"))
                
                results.append({
                    "no": cols[0].text.strip(),
                    "ticker": ticker,
                    "name": name,
                    "price": price,
                    "change": change_val,
                    "changeRate": rate,
                    "volume": volume,
                    "prevVolume": prev_volume,
                    "amount": amount,
                    "buyQuote": buy_quote,
                    "sellQuote": sell_quote,
                    "market": "KOSPI" if "sosok=0" in url else "KOSDAQ"
                })
            except: continue
        print(f"  Successfully fetched {len(results)} items from {url}")
        return results
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return []

# --- 실행 로직 ---

def collect_main_data():
    today = get_market_date()
    # Scrape each group
    inst_kospi = {"buy": fetch_naver_net_buyers('01', '1000', 'buy'), "sell": fetch_naver_net_buyers('01', '1000', 'sell')}
    inst_kosdaq = {"buy": fetch_naver_net_buyers('02', '1000', 'buy'), "sell": fetch_naver_net_buyers('02', '1000', 'sell')}
    for_kospi = {"buy": fetch_naver_net_buyers('01', '9000', 'buy'), "sell": fetch_naver_net_buyers('01', '9000', 'sell')}
    for_kosdaq = {"buy": fetch_naver_net_buyers('02', '9000', 'buy'), "sell": fetch_naver_net_buyers('02', '9000', 'sell')}
    ind_kospi = {"buy": fetch_naver_net_buyers('01', '8000', 'buy', True), "sell": fetch_naver_net_buyers('01', '8000', 'sell', True)}
    ind_kosdaq = {"buy": fetch_naver_net_buyers('02', '8000', 'buy', True), "sell": fetch_naver_net_buyers('02', '8000', 'sell', True)}
    pen_buy = fetch_pension_from_judal('buy')
    pen_sell = fetch_pension_from_judal('sell')
    pen_kospi = {"buy": pen_buy["KOSPI"], "sell": pen_sell["KOSPI"]}
    pen_kosdaq = {"buy": pen_buy["KOSDAQ"], "sell": pen_sell["KOSDAQ"]}
    hold_kospi = fetch_foreign_hold('0')
    hold_kosdaq = fetch_foreign_hold('1')

    # Update prices and market tags for all lists
    updates = [
        (inst_kospi["buy"], 'KOSPI'), (inst_kospi["sell"], 'KOSPI'),
        (inst_kosdaq["buy"], 'KOSDAQ'), (inst_kosdaq["sell"], 'KOSDAQ'),
        (for_kospi["buy"], 'KOSPI'), (for_kospi["sell"], 'KOSPI'),
        (for_kosdaq["buy"], 'KOSDAQ'), (for_kosdaq["sell"], 'KOSDAQ'),
        (ind_kospi["buy"], 'KOSPI'), (ind_kospi["sell"], 'KOSPI'),
        (ind_kosdaq["buy"], 'KOSDAQ'), (ind_kosdaq["sell"], 'KOSDAQ'),
        (hold_kospi, 'KOSPI'), (hold_kosdaq, 'KOSDAQ')
    ]
    
    for data_list, market_name in updates:
        for item in data_list:
            item['price'], item['changeRate'] = get_current_price_change(item['ticker'])
            item['market'] = market_name
            time.sleep(0.05)

    return {
        "baseDate": today.strftime("%Y-%m-%d"),
        "institution": {"KOSPI": inst_kospi, "KOSDAQ": inst_kosdaq},
        "foreigner": {"KOSPI": for_kospi, "KOSDAQ": for_kosdaq},
        "individual": {"KOSPI": ind_kospi, "KOSDAQ": ind_kosdaq},
        "pension": {"KOSPI": pen_kospi, "KOSDAQ": pen_kosdaq},
        "foreigner_hold": {"KOSPI": hold_kospi, "KOSDAQ": hold_kosdaq},
        "updatedAt": datetime.now(timezone(timedelta(hours=9))).isoformat()
    }

def collect_volume_data():
    today = get_market_date()
    
    top_vol_kospi = fetch_naver_sise_list("https://finance.naver.com/sise/sise_quant.naver?sosok=0")
    top_vol_kosdaq = fetch_naver_sise_list("https://finance.naver.com/sise/sise_quant.naver?sosok=1")
    
    # Validation: If both markets' Top Volume is empty, something is wrong.
    if not top_vol_kospi and not top_vol_kosdaq:
        print("CRITICAL: Scraped empty data for both KOSPI and KOSDAQ. Skipping this update.")
        return None

    return {
        "baseDate": today.strftime("%Y-%m-%d"),
        "topVolume": {"KOSPI": top_vol_kospi, "KOSDAQ": top_vol_kosdaq},
        "volumeSurge": {"KOSPI": fetch_naver_sise_list("https://finance.naver.com/sise/sise_quant_high.naver?sosok=0"), "KOSDAQ": fetch_naver_sise_list("https://finance.naver.com/sise/sise_quant_high.naver?sosok=1")},
        "volumePlunge": {"KOSPI": fetch_naver_sise_list("https://finance.naver.com/sise/sise_quant_low.naver?sosok=0"), "KOSDAQ": fetch_naver_sise_list("https://finance.naver.com/sise/sise_quant_low.naver?sosok=1")},
        "volumeUpdatedAt": datetime.now(timezone(timedelta(hours=9))).isoformat()
    }

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['main', 'volume'], required=True)
    args = parser.parse_args()

    # Load from .env.local if exists (for local testing)
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env.local")
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                if '=' in line:
                    k, v = line.strip().split('=', 1)
                    if k.startswith("NEXT_PUBLIC_"):
                        # Map NEXT_PUBLIC_SUPABASE_URL to SUPABASE_URL if needed
                        new_k = k.replace("NEXT_PUBLIC_", "")
                        if new_k not in os.environ: os.environ[new_k] = v
                    if k not in os.environ: os.environ[k] = v

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_KEY are required env vars.")
        sys.exit(1)
        
    supabase: Client = create_client(url, key)
    today = get_market_date()
    base_date = today.strftime("%Y-%m-%d")
    
    if args.type == 'main':
        print("Collecting Main Investor Trends...")
        data = collect_main_data()
    else:
        print("Collecting Volume Data...")
        data = collect_volume_data()

    if data is None:
        print(f"Skipping Supabase update for {args.type} due to missing data.")
        sys.exit(0)

    # Supabase Upsert
    try:
        response = supabase.table("daily_stock_data").upsert({
            "base_date": base_date,
            "data_type": args.type,
            "data": data
        }, on_conflict="base_date, data_type").execute()
        print(f"Successfully upserted {args.type} data for {base_date}")
    except Exception as e:
        print(f"Supabase upsert failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
