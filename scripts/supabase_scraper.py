from concurrent.futures import ThreadPoolExecutor
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

# 실시간 조회를 위해 네이버 폴링 API 사용

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

def fetch_foreign_hold(market_code):
    """
    네이버 신규 주식 API를 통해 외국인 보유율 상위 종목을 수집합니다.
    market_code: '0' 또는 'KOSPI' -> KOSPI, '1' 또는 'KOSDAQ' -> KOSDAQ
    """
    market = "KOSPI" if market_code in ['0', 'KOSPI'] else "KOSDAQ"
    url = f"https://stock.naver.com/api/stockSecurity/individual-stocks/v3/domestic?listingType=foreignHoldRatioDesc&exchangeType=krx&marketType={market}&size=30"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://stock.naver.com/"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        items = res.json().get("items", [])
        results = []
        for it in items:
            ticker = it.get("itemCode")
            name = it.get("itemName")
            ratio = to_json_float(it.get("foreignHoldRatio", 0.0))
            if ticker and name:
                results.append({"ticker": ticker, "name": name, "foreignRatio": ratio})
        print(f"  Successfully fetched {len(results)} foreign hold items for {market}")
        return results[:30]
    except Exception as e:
        print(f"Error fetching foreign hold for {market}: {e}")
        return []

def fetch_investor_net_trends(market_name):
    """
    네이버 신규 종목 리스트와 모바일 통합 API를 조합하여
    기관, 외국인, 개인의 당일 순매수/순매도 상위 종목을 병렬로 수집합니다.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://stock.naver.com/"
    }
    candidate_codes = {}
    for ltype in ["tradingValueDesc", "tradingVolumeDesc"]:
        url = f"https://stock.naver.com/api/stockSecurity/individual-stocks/v3/domestic?listingType={ltype}&exchangeType=krx&marketType={market_name}&size=60"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                for it in r.json().get("items", []):
                    code = it.get("itemCode")
                    name = it.get("itemName")
                    if code and name:
                        candidate_codes[code] = name
        except Exception as e:
            print(f"Candidate fetch error ({ltype}): {e}")

    candidates = list(candidate_codes.items())
    print(f"  Fetching investor trends for {len(candidates)} candidates in {market_name}...")

    def fetch_single(item):
        code, name = item
        try:
            int_url = f"https://m.stock.naver.com/api/stock/{code}/integration"
            r = requests.get(int_url, headers=headers, timeout=5)
            if r.status_code == 200:
                trends = r.json().get("dealTrendInfos", [])
                if trends:
                    t = trends[0]
                    def parse_q(val):
                        if not val: return 0
                        clean = str(val).replace(",", "").replace("+", "").strip()
                        try: return int(clean)
                        except: return 0
                    
                    price = safe_int(str(t.get("closePrice", "0")).replace(",", ""))
                    organ_vol = parse_q(t.get("organPureBuyQuant"))
                    frgn_vol = parse_q(t.get("foreignerPureBuyQuant"))
                    ind_vol = parse_q(t.get("individualPureBuyQuant"))
                    
                    return {
                        "ticker": code,
                        "name": name,
                        "price": price,
                        "organVol": organ_vol,
                        "organAmt": organ_vol * price,
                        "frgnVol": frgn_vol,
                        "frgnAmt": frgn_vol * price,
                        "indVol": ind_vol,
                        "indAmt": ind_vol * price,
                    }
        except:
            pass
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        items = [res for res in executor.map(fetch_single, candidates) if res]

    # Helper function to format result
    def to_item(d, vol_key, amt_key):
        return {
            "ticker": d["ticker"],
            "name": d["name"],
            "netBuyVolume": abs(d[vol_key]),
            "netBuyAmount": abs(d[amt_key])
        }

    # 기관 (순매수 / 순매도)
    inst_buy = [to_item(x, "organVol", "organAmt") for x in sorted([x for x in items if x["organVol"] > 0], key=lambda x: x["organAmt"], reverse=True)[:30]]
    inst_sell = [to_item(x, "organVol", "organAmt") for x in sorted([x for x in items if x["organVol"] < 0], key=lambda x: abs(x["organAmt"]), reverse=True)[:30]]

    # 외국인 (순매수 / 순매도)
    frgn_buy = [to_item(x, "frgnVol", "frgnAmt") for x in sorted([x for x in items if x["frgnVol"] > 0], key=lambda x: x["frgnAmt"], reverse=True)[:30]]
    frgn_sell = [to_item(x, "frgnVol", "frgnAmt") for x in sorted([x for x in items if x["frgnVol"] < 0], key=lambda x: abs(x["frgnAmt"]), reverse=True)[:30]]

    # 개인 (순매수 / 순매도 - 금액순 정렬)
    ind_buy = [to_item(x, "indVol", "indAmt") for x in sorted([x for x in items if x["indVol"] > 0], key=lambda x: x["indAmt"], reverse=True)[:30]]
    ind_sell = [to_item(x, "indVol", "indAmt") for x in sorted([x for x in items if x["indVol"] < 0], key=lambda x: abs(x["indAmt"]), reverse=True)[:30]]

    return {
        "institution": {"buy": inst_buy, "sell": inst_sell},
        "foreigner": {"buy": frgn_buy, "sell": frgn_sell},
        "individual": {"buy": ind_buy, "sell": ind_sell}
    }

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

def fetch_realtime_prices(tickers):
    """
    네이버 증권의 실시간 폴링 API를 사용하여 주가와 등락률을 벌크로 가져옵니다.
    네이버 증권 메인 페이지와 100% 일치하도록 정규장 현재가/종가(nv)를 사용합니다.
    """
    if not tickers:
        return {}
        
    results = {}
    batch_size = 10
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{','.join(batch)}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://finance.naver.com/",
            "Accept": "*/*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            data = resp.json()
            
            items = data.get('result', {}).get('areas', [{}])[0].get('datas', [])
            for item in items:
                ticker = item.get('cd')
                if ticker:
                    # 기본 시세 (정규장 현재가/종가 기준)
                    price = item.get('nv', 0)
                    rate = float(item.get('cr', 0.0))
                    sv = item.get('sv', 0) # 전일 종가
                    
                    # 부호 보정 (polling API의 cr은 절대값인 경우가 있음)
                    if price < sv:
                        rate = -abs(rate)
                    elif price > sv:
                        rate = abs(rate)
                    
                    results[ticker] = (price, rate, sv)
        except Exception as e:
            print(f"Error fetching realtime prices for batch {i}: {e}")
            
    return results

# --- 거래량 데이터(Volume) 수집 로직 ---

def fetch_naver_volume_ranking(market_name='KOSPI', listing_type='tradingVolumeDesc', size=30):
    """
    네이버 신규 주식 API를 활용하여 거래량 상위/급증/급락 순위 데이터를 수집합니다.
    listing_type:
      - tradingVolumeDesc: 거래량 상위
      - tradingVolumeIncreaseRateDesc: 거래량 급증
      - tradingVolumeIncreaseRateAsc: 거래량 급락
    """
    url = f"https://stock.naver.com/api/stockSecurity/individual-stocks/v3/domestic?listingType={listing_type}&exchangeType=krx&marketType={market_name}&size={size}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://stock.naver.com/"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        items = res.json().get("items", [])
        results = []
        for idx, it in enumerate(items):
            ticker = it.get("itemCode", "")
            name = it.get("itemName", "")
            if not ticker or not name:
                continue

            krx = it.get("krx", {})
            price = safe_int(str(krx.get("currentPrice", 0)).replace(",", ""))
            change_val = safe_int(str(krx.get("changePrice", 0)).replace(",", ""))
            rate = to_json_float(krx.get("changeRate", 0.0))
            volume = safe_int(str(krx.get("tradingVolume", 0)).replace(",", ""))
            prev_volume = safe_int(str(it.get("prevTradingVolume", 0)).replace(",", ""))
            amount = safe_int(str(krx.get("tradingValue", 0)).replace(",", ""))
            buy_quote = safe_int(str(krx.get("bidPrice", 0)).replace(",", ""))
            sell_quote = safe_int(str(krx.get("askPrice", 0)).replace(",", ""))

            results.append({
                "no": str(idx + 1),
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
                "market": market_name
            })
        print(f"  Successfully fetched {len(results)} items from Naver Volume API ({market_name}, {listing_type})")
        return results
    except Exception as e:
        print(f"Error fetching Naver volume ranking for {market_name} ({listing_type}): {e}")
        return []

# --- 실행 로직 ---

def update_list_with_realtime_prices(data_lists, market_name=None):
    """모든 데이터 리스트의 티커를 수집하여 네이버 폴링 API로 실시간 주가를 반영합니다."""
    all_tickers = set()
    for dl in data_lists:
        for item in dl:
            if 'ticker' in item:
                all_tickers.add(item['ticker'])
    
    if not all_tickers:
        return

    print(f"Updating realtime prices for {len(all_tickers)} tickers...")
    realtime_data = fetch_realtime_prices(list(all_tickers))
    
    for dl in data_lists:
        for item in dl:
            ticker = item.get('ticker')
            if ticker in realtime_data:
                price, rate, sv = realtime_data[ticker]
                item['price'] = price
                item['changeRate'] = rate
                item['prevPrice'] = sv
            if market_name:
                item['market'] = market_name

def collect_main_data():
    today = get_market_date()
    
    kospi_trends = fetch_investor_net_trends('KOSPI')
    kosdaq_trends = fetch_investor_net_trends('KOSDAQ')
    
    inst_kospi = kospi_trends["institution"]
    inst_kosdaq = kosdaq_trends["institution"]
    for_kospi = kospi_trends["foreigner"]
    for_kosdaq = kosdaq_trends["foreigner"]
    ind_kospi = kospi_trends["individual"]
    ind_kosdaq = kosdaq_trends["individual"]
    
    pen_buy = fetch_pension_from_judal('buy')
    pen_sell = fetch_pension_from_judal('sell')
    pen_kospi = {"buy": pen_buy["KOSPI"], "sell": pen_sell["KOSPI"]}
    pen_kosdaq = {"buy": pen_buy["KOSDAQ"], "sell": pen_sell["KOSDAQ"]}
    
    hold_kospi = fetch_foreign_hold('KOSPI')
    hold_kosdaq = fetch_foreign_hold('KOSDAQ')

    # Realtime price updates
    update_list_with_realtime_prices([inst_kospi["buy"], inst_kospi["sell"], ind_kospi["buy"], ind_kospi["sell"], for_kospi["buy"], for_kospi["sell"], pen_kospi["buy"], pen_kospi["sell"], hold_kospi], 'KOSPI')
    update_list_with_realtime_prices([inst_kosdaq["buy"], inst_kosdaq["sell"], ind_kosdaq["buy"], ind_kosdaq["sell"], for_kosdaq["buy"], for_kosdaq["sell"], pen_kosdaq["buy"], pen_kosdaq["sell"], hold_kosdaq], 'KOSDAQ')

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
    
    top_vol_kospi = fetch_naver_volume_ranking('KOSPI', 'tradingVolumeDesc', size=30)
    top_vol_kosdaq = fetch_naver_volume_ranking('KOSDAQ', 'tradingVolumeDesc', size=30)
    
    surge_kospi = fetch_naver_volume_ranking('KOSPI', 'tradingVolumeIncreaseRateDesc', size=30)
    surge_kosdaq = fetch_naver_volume_ranking('KOSDAQ', 'tradingVolumeIncreaseRateDesc', size=30)
    
    plunge_kospi = fetch_naver_volume_ranking('KOSPI', 'tradingVolumeIncreaseRateAsc', size=30)
    plunge_kosdaq = fetch_naver_volume_ranking('KOSDAQ', 'tradingVolumeIncreaseRateAsc', size=30)
    
    if not top_vol_kospi and not top_vol_kosdaq:
        print("CRITICAL: Scraped empty data for both KOSPI and KOSDAQ. Skipping this update.")
        return None

    # Realtime price updates for volume sections
    all_vol_lists = [top_vol_kospi, top_vol_kosdaq, surge_kospi, surge_kosdaq, plunge_kospi, plunge_kosdaq]
    update_list_with_realtime_prices(all_vol_lists)

    return {
        "baseDate": today.strftime("%Y-%m-%d"),
        "topVolume": {"KOSPI": top_vol_kospi, "KOSDAQ": top_vol_kosdaq},
        "volumeSurge": {"KOSPI": surge_kospi, "KOSDAQ": surge_kosdaq},
        "volumePlunge": {"KOSPI": plunge_kospi, "KOSDAQ": plunge_kosdaq},
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
