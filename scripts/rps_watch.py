#!/usr/bin/env python3
"""
RPS 自选股追踪系统
分析你的自选股排名变化，发现强势股轮动
"""

import requests
import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

DB_PATH = os.path.expanduser('~/stock_analysis/data/rps_tracker.db')
WATCHLIST_PATH = os.path.expanduser('~/stock_analysis/my_stocks.txt')

# ============== 初始化 ==============

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_rps (
            date TEXT, code TEXT, name TEXT, price REAL,
            rps_20 REAL, rps_50 REAL, rps_120 REAL, rps_250 REAL,
            rank_20 INTEGER, rank_50 INTEGER, rank_120 INTEGER, rank_250 INTEGER,
            PRIMARY KEY (date, code)
        )
    ''')
    conn.commit()
    conn.close()

# ============== API ==============

def get_stock_hist_sina(symbol, days=300):
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

# ============== 分析 ==============

def analyze_stock(code):
    if code.startswith('6') or code.startswith('5'): symbol = f'sh{code}'
    elif code.startswith('0') or code.startswith('1') or code.startswith('2') or code.startswith('3'): symbol = f'sz{code}'
    elif code.startswith('4') or code.startswith('8') or code.startswith('9'): symbol = f'bj{code}'
    else: symbol = code
    
    df = get_stock_hist_sina(symbol, 400)
    if df.empty or len(df) < 260: return None
    
    close = df['close'].values
    name = get_stock_name_tx(symbol)
    price = close[-1]
    
    result = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'code': code, 'name': name, 'price': price
    }
    for period in [20, 50, 120, 250]:
        if len(close) >= period + 5:
            result[f'rps_{period}'] = (close[-1] - close[-period-1]) / close[-period-1] * 100
        else:
            result[f'rps_{period}'] = np.nan
    return result

# ============== 主程序 ==============

def main():
    init_db()
    
    # 读取自选股
    if not os.path.exists(WATCHLIST_PATH):
        print(f"⚠️ 自选股列表不存在: {WATCHLIST_PATH}")
        print("请创建 my_stocks.txt，每行一个股票代码")
        return
    
    with open(WATCHLIST_PATH) as f:
        stocks = [line.strip() for line in f if line.strip()]
    
    print(f"\n📥 分析 {len(stocks)} 只自选股...")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
    
    # 获取数据
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(analyze_stock, code): code for code in stocks}
        for future in as_completed(futures):
            try:
                r = future.result()
                if r: results.append(r)
            except: pass
    
    if not results:
        print("⚠️ 未能获取任何数据")
        return
    
    # 保存
    conn = sqlite3.connect(DB_PATH)
    df = pd.DataFrame(results)
    today = datetime.now().strftime('%Y-%m-%d')
    conn.execute(f'DELETE FROM daily_rps WHERE date = "{today}" AND code IN ({",".join([f"'{c}'" for c in stocks])})')
    df.to_sql('daily_rps', conn, if_exists='append', index=False)
    conn.commit()
    print(f"✅ 已更新 {len(results)} 只股票\n")
    
    # 分析排名
    for period in [20, 50, 120]:
        print("=" * 80)
        print(f"📊 RPS{period} 自选股排名")
        print("=" * 80)
        
        c = conn.cursor()
        c.execute(f'''
            SELECT code, name, rps_{period}, rps_20, rps_50, rps_120, rps_250, price
            FROM daily_rps 
            WHERE date = ? AND rps_{period} IS NOT NULL
            ORDER BY rps_{period} DESC
        ''', (today,))
        
        rows = c.fetchall()
        if not rows:
            print("  暂无数据")
            continue
        
        print(f"{'排名':>4} {'代码':<8} {'名称':<12} {'RPS':>8} {'20日':>8} {'50日':>8} {'120日':>8} {'价格':>10}")
        print("-" * 80)
        
        for i, row in enumerate(rows):
            rps_val = row[2] if not np.isnan(row[2]) else 0
            rps_20 = row[3] if row[3] and not np.isnan(row[3]) else 0
            rps_50 = row[4] if row[4] and not np.isnan(row[4]) else 0
            rps_120 = row[5] if row[5] and not np.isnan(row[5]) else 0
            
            # 趋势判断
            if rps_20 > rps_50 > rps_120: trend = "📈"
            elif rps_20 > 0 and rps_50 < 0: trend = "📊"
            elif rps_20 < rps_50 < 0: trend = "📉"
            else: trend = "➡️"
            
            print(f"{i+1:>4} {row[0]:<8} {row[1][:10]:<12} {rps_val:>+7.2f}% {trend} {rps_20:>+7.2f}% {rps_50:>+7.2f}% {rps_120:>+7.2f}% ¥{row[7]:>8.2f}")
        
        print()
    
    # 排名变化分析
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    c.execute(f'SELECT COUNT(*) FROM daily_rps WHERE date = ?', (yesterday,))
    has_yesterday = c.fetchone()[0] > 0
    
    if has_yesterday:
        print("=" * 80)
        print("🔄 排名变化追踪")
        print("=" * 80)
        
        for period in [20, 50]:
            print(f"\n【RPS{period} 进出统计】")
            
            # 今日排名
            c.execute(f'''
                SELECT code, name, rps_{period} FROM daily_rps 
                WHERE date = ? AND rps_{period} IS NOT NULL
                ORDER BY rps_{period} DESC
            ''', (today,))
            today_data = {r[0]: (r[1], r[2]) for r in c.fetchall()}
            
            # 昨日排名
            c.execute(f'''
                SELECT code, name, rps_{period} FROM daily_rps 
                WHERE date = ? AND rps_{period} IS NOT NULL
                ORDER BY rps_{period} DESC
            ''', (yesterday,))
            yesterday_data = {r[0]: (r[1], r[2]) for r in c.fetchall()}
            
            # 找变化
            today_codes = set(today_data.keys())
            yesterday_codes = set(yesterday_data.keys())
            
            new_codes = today_codes - yesterday_codes
            exit_codes = yesterday_codes - today_codes
            
            if new_codes:
                print(f"  🟢 新进: {', '.join([f'{c}({today_data[c][0][:6]})' for c in new_codes])}")
            if exit_codes:
                print(f"  🔴 退出: {', '.join([f'{c}({yesterday_data[c][0][:6]})' for c in exit_codes])}")
            
            # 排名上升/下降
            for code in today_codes & yesterday_codes:
                t_rank = list(today_data.keys()).index(code) + 1
                y_rank = list(yesterday_data.keys()).index(code) + 1
                if abs(t_rank - y_rank) >= 3:
                    direction = "⬆️" if t_rank < y_rank else "⬇️"
                    print(f"  {direction} {code}({today_data[code][0][:6]}): #{y_rank} → #{t_rank}")
        
        print()
    else:
        print("=" * 80)
        print("📝 提示: 明天再来查看，可获得『排名进出变化』统计")
        print("=" * 80)
    
    conn.close()

if __name__ == "__main__":
    main()
