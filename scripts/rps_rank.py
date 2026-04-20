#!/usr/bin/env python3
"""
RPS市场排名计算
计算股票在市场中的相对排名百分位
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser('~/stock_analysis/data/market_rps.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS stock_returns (
            date TEXT,
            code TEXT,
            name TEXT,
            price REAL,
            ret_5 REAL, ret_10 REAL, ret_20 REAL, ret_50 REAL, ret_120 REAL,
            PRIMARY KEY (date, code)
        )
    ''')
    conn.commit()
    conn.close()

def get_stock_hist_sina(symbol, days=150):
    try:
        url = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
        params = {'symbol': symbol, 'scale': '240', 'ma': '5', 'datalen': days}
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        df['day'] = pd.to_datetime(df['day'])
        df = df.sort_values('day')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        return df
    except: return pd.DataFrame()

def get_stock_name_tx(symbol):
    try:
        url = f'https://qt.gtimg.cn/q={symbol}'
        r = requests.get(url, timeout=5)
        parts = r.text.split('~')
        return parts[1] if len(parts) > 1 else ''
    except: return ''

def get_hs300_codes():
    """获取沪深300成分股代码（简化版：用常见大市值股票）"""
    # 这里用一个大市值股票池，实际应该从沪深300指数获取
    codes = []
    # 沪市主板
    for i in range(600000, 600100):
        codes.append(f'6{i:06d}')
    # 深市主板
    for i in range(1, 100):
        codes.append(f'000{i:03d}')
    # 创业板
    for i in range(1, 100):
        codes.append(f'300{i:03d}')
    return codes[:200]  # 限制数量避免太慢

def calculate_returns(code):
    """计算单只股票的各周期收益率"""
    if code.startswith('6') or code.startswith('5'):
        symbol = f'sh{code}'
    else:
        symbol = f'sz{code}'
    
    df = get_stock_hist_sina(symbol, days=180)
    if df.empty or len(df) < 130:
        return None
    
    close = df['close'].values
    name = get_stock_name_tx(symbol)
    price = close[-1]
    today = datetime.now().strftime('%Y-%m-%d')
    
    result = {
        'date': today,
        'code': code,
        'name': name,
        'price': price
    }
    
    for period in [5, 10, 20, 50, 120]:
        if len(close) >= period + 5:
            ret = (close[-1] - close[-period-1]) / close[-period-1] * 100
            result[f'ret_{period}'] = ret
        else:
            result[f'ret_{period}'] = np.nan
    
    return result

def calculate_rank(stock_codes, period=20):
    """
    计算市场排名百分位
    RPS = 打败了市场上X%的股票
    """
    ret_col = f'ret_{period}'
    
    # 过滤掉无效数据
    valid_data = [r for r in stock_codes if r and not np.isnan(r.get(ret_col, np.nan))]
    
    if not valid_data:
        return None
    
    # 获取目标股票的收益率
    target_returns = {r['code']: r[ret_col] for r in valid_data}
    
    # 计算排名
    all_returns = sorted(target_returns.values())
    total = len(all_returns)
    
    ranks = {}
    for code, ret in target_returns.items():
        # 计算有多少股票收益率低于这只
        below = sum(1 for r in all_returns if r < ret)
        rps = (below / total) * 100
        ranks[code] = round(rps, 1)
    
    return ranks

def main():
    init_db()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 获取市场股票池
    print("📥 获取市场股票池...")
    market_codes = get_hs300_codes()
    print(f"   市场股票池: {len(market_codes)} 只")
    
    # 计算市场收益率
    print("🔢 计算市场收益率...")
    market_data = []
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(calculate_returns, code): code for code in market_codes}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"   已完成 {done}/{len(market_codes)}")
            try:
                result = future.result()
                if result and not np.isnan(result.get('price', np.nan)):
                    market_data.append(result)
            except:
                pass
    
    print(f"   有效数据: {len(market_data)} 只")
    
    # 保存到数据库
    if market_data:
        conn = sqlite3.connect(DB_PATH)
        df_market = pd.DataFrame(market_data)
        conn.execute(f'DELETE FROM stock_returns WHERE date = "{today}"')
        df_market.to_sql('stock_returns', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        print(f"   ✅ 数据已保存")
    
    # 计算目标股票的排名
    print("\n📊 计算目标股票排名...")
    
    results = []
    for code, name in STOCKS.items():
        stock_result = calculate_returns(code)
        if stock_result:
            results.append(stock_result)
    
    # 计算排名
    ranks_20 = calculate_rank(market_data + results, period=20)
    ranks_50 = calculate_rank(market_data + results, period=50)
    ranks_120 = calculate_rank(market_data + results, period=120)
    
    # 打印结果
    print("\n" + "="*70)
    print("📊 RPS 市场排名报告")
    print("="*70)
    print(f"📅 {today} | 市场股票数: {len(market_data)} 只")
    print("="*70)
    
    for r in results:
        code = r['code']
        name = r['name']
        price = r['price']
        
        rps20 = ranks_20.get(code, 'N/A') if ranks_20 else 'N/A'
        rps50 = ranks_50.get(code, 'N/A') if ranks_50 else 'N/A'
        rps120 = ranks_120.get(code, 'N/A') if ranks_120 else 'N/A'
        
        # 趋势判断
        try:
            r20 = float(rps20)
            r50 = float(rps50)
            r120 = float(rps120)
            
            if r20 > r50 > r120 and r20 > 50:
                trend = "📈 加速上涨"
            elif r20 > 50 and r50 < 50:
                trend = "📊 反弹回暖"
            elif r20 < r50 < r120 and r20 < 50:
                trend = "📉 加速下跌"
            elif r20 < 50 and r50 > 50:
                trend = "➡️ 涨多回调"
            elif r20 < r50 < 50:
                trend = "🔻 持续调整"
            else:
                trend = "➡️ 震荡整理"
        except:
            trend = "计算中..."
        
        ret20 = r.get('ret_20', 0) or 0
        ret50 = r.get('ret_50', 0) or 0
        ret120 = r.get('ret_120', 0) or 0
        
        print(f"\n🔹 {name} ({code})")
        print(f"   💰 价格: {price:.2f}")
        print(f"   📊 市场排名 (RPS):")
        print(f"      20日: {rps20}%" if isinstance(rps20, float) else f"      20日: {rps20}")
        print(f"      50日: {rps50}%" if isinstance(rps50, float) else f"      50日: {rps50}")
        print(f"     120日: {rps120}%" if isinstance(rps120, float) else f"     120日: {rps120}")
        print(f"   📈 绝对涨幅:")
        print(f"      20日: {ret20:+.1f}%")
        print(f"      50日: {ret50:+.1f}%")
        print(f"     120日: {ret120:+.1f}%")
        print(f"   趋势: {trend}")
    
    print("\n" + "="*70)
    print("💡 RPS排名含义: 打败了市场上X%的股票")
    print("   RPS > 80: 强势股 (打败80%的股票)")
    print("   RPS > 50: 超过市场平均")
    print("   RPS < 50: 弱于市场平均")
    print("="*70)

if __name__ == "__main__":
    main()
