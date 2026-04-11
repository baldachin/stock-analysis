#!/usr/bin/env python3
"""
RPS 排名追踪系统
追踪排名进出变化，捕捉强势股轮动
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

# ============== 数据库 ==============

def init_db():
    """初始化数据库"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 每日RPS排名表
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_rps (
            date TEXT,
            code TEXT,
            name TEXT,
            price REAL,
            rps_20 REAL,
            rps_50 REAL,
            rps_120 REAL,
            rps_250 REAL,
            rank_20 INTEGER,
            rank_50 INTEGER,
            rank_120 INTEGER,
            rank_250 INTEGER,
            PRIMARY KEY (date, code)
        )
    ''')
    
    # 排名进出记录表
    c.execute('''
        CREATE TABLE IF NOT EXISTS rank_changes (
            date TEXT,
            code TEXT,
            name TEXT,
            period TEXT,
            action TEXT,  -- 'ENTER' or 'EXIT'
            prev_rank INTEGER,
            curr_rank INTEGER,
            rps_value REAL,
            price REAL
        )
    ''')
    
    conn.commit()
    conn.close()

# ============== API ==============

def get_stock_hist_sina(symbol: str, days: int = 300) -> pd.DataFrame:
    """获取历史K线"""
    try:
        url = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
        params = {'symbol': symbol, 'scale': '240', 'ma': '5', 'datalen': days}
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['day'] = pd.to_datetime(df['day'])
        df = df.sort_values('day')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        return df
    except:
        return pd.DataFrame()

def get_stock_realtime_tx(symbol: str) -> dict:
    """获取实时行情"""
    try:
        url = f'https://qt.gtimg.cn/q={symbol}'
        r = requests.get(url, timeout=5)
        parts = r.text.split('~')
        if len(parts) < 10:
            return {}
        return {
            'name': parts[1] if len(parts) > 1 else '',
            'price': float(parts[3]) if parts[3] else 0,
        }
    except:
        return {}

def get_stock_name(symbol: str) -> str:
    """获取股票名称"""
    info = get_stock_realtime_tx(symbol)
    return info.get('name', symbol)

# ============== RPS 计算 ==============

def calculate_rps_for_stock(code: str) -> dict:
    """计算单只股票RPS"""
    if code.startswith('6'):
        symbol = f'sh{code}'
    else:
        symbol = f'sz{code}'
    
    df = get_stock_hist_sina(symbol, days=400)
    if df.empty or len(df) < 260:
        return {}
    
    close = df['close'].values
    name = get_stock_name(symbol)
    price = close[-1]
    
    result = {
        'code': code,
        'name': name,
        'price': price,
        'date': df['day'].iloc[-1].strftime('%Y-%m-%d')
    }
    
    for period in [20, 50, 120, 250]:
        if len(close) >= period + 5:
            ret = (close[-1] - close[-period-1]) / close[-period-1] * 100
            result[f'rps_{period}'] = ret
        else:
            result[f'rps_{period}'] = np.nan
    
    return result

def get_top_stocks_by_rps(date: str, period: int = 20, top_n: int = 100) -> list:
    """获取某日某周期RPS排名前N"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f'''
        SELECT * FROM daily_rps 
        WHERE date = "{date}" AND rps_{period} IS NOT NULL
        ORDER BY rps_{period} DESC
        LIMIT {top_n}
    ''', conn)
    conn.close()
    return df

# ============== 排名变化分析 ==============

def analyze_rank_changes(prev_date: str, curr_date: str, period: int = 20, top_n: int = 50) -> dict:
    """分析排名进出变化"""
    prev_data = get_top_stocks_by_rps(prev_date, period, top_n * 2)  # 取更多便于比较
    curr_data = get_top_stocks_by_rps(curr_date, period, top_n * 2)
    
    if prev_data.empty or curr_data.empty:
        return {'error': '数据不足，请先运行数据采集'}
    
    prev_codes = set(prev_data['code'].tolist())
    curr_codes = set(curr_data['code'].tolist())
    
    # 新进入者
    new_entries = curr_codes - prev_codes
    # 挤出者
    exited = prev_codes - curr_codes
    
    # 计算变化
    entry_details = []
    for code in new_entries:
        row = curr_data[curr_data['code'] == code].iloc[0]
        # 查找之前排名
        prev_rank = None
        if not prev_data.empty and code in prev_codes:
            prev_rank = prev_data[prev_data['code'] == code].index[0] + 1 if code in prev_data['code'].values else None
        entry_details.append({
            'code': code,
            'name': row['name'],
            'curr_rank': list(curr_data['code']).index(code) + 1,
            'prev_rank': prev_rank,
            'rps': row[f'rps_{period}'],
            'price': row['price']
        })
    
    exit_details = []
    for code in exited:
        row = prev_data[prev_data['code'] == code].iloc[0]
        curr_rank = None
        if code in curr_codes:
            curr_rank = list(curr_data['code']).index(code) + 1
        exit_details.append({
            'code': code,
            'name': row['name'],
            'prev_rank': list(prev_data['code']).index(code) + 1,
            'curr_rank': curr_rank,
            'rps': row[f'rps_{period}'],
            'price': row['price']
        })
    
    # 按当前排名排序
    entry_details.sort(key=lambda x: x['curr_rank'])
    exit_details.sort(key=lambda x: x['prev_rank'])
    
    return {
        'date': curr_date,
        'period': period,
        'top_n': top_n,
        'new_entries': entry_details[:top_n],
        'exited': exit_details[:top_n]
    }

def print_rank_report(changes: dict):
    """打印排名变化报告"""
    if 'error' in changes:
        print(f"⚠️ {changes['error']}")
        return
    
    print(f"\n{'='*80}")
    print(f"📊 RPS{changes['period']} 排名变化报告")
    print(f"📅 日期: {changes['date']} | 追踪范围: 前{changes['top_n']}名")
    print(f"{'='*80}")
    
    print(f"\n🟢 【新进入前{changes['top_n']}】({len(changes['new_entries'])}只)")
    print(f"{'-'*80}")
    if changes['new_entries']:
        print(f"{'代码':<8} {'名称':<10} {'当前排名':>8} {'RPS':>10} {'价格':>10}")
        print(f"{'-'*80}")
        for e in changes['new_entries']:
            rank_str = f"# {e['curr_rank']}" if e['curr_rank'] else 'NEW'
            prev_str = f"(前#{e['prev_rank']})" if e['prev_rank'] else "(新进)"
            print(f"{e['code']:<8} {e['name']:<10} {rank_str:>8} {e['rps']:>+10.2f}% {prev_str:<12} ¥{e['price']:>8.2f}")
    else:
        print("  无新进入者")
    
    print(f"\n🔴 【挤出前{changes['top_n']}】({len(changes['exited'])}只)")
    print(f"{'-'*80}")
    if changes['exited']:
        print(f"{'代码':<8} {'名称':<10} {'原排名':>8} {'RPS':>10} {'价格':>10}")
        print(f"{'-'*80}")
        for e in changes['exited']:
            prev_str = f"# {e['prev_rank']}" if e['prev_rank'] else '?'
            curr_str = f"(现#{e['curr_rank']})" if e['curr_rank'] else "(跌出)"
            print(f"{e['code']:<8} {e['name']:<10} {prev_str:>8} {e['rps']:>+10.2f}% {curr_str:<12} ¥{e['price']:>8.2f}")
    else:
        print("  无挤出者")
    
    print(f"\n{'='*80}\n")

# ============== 主程序 ==============

def update_daily_data(stocks: list):
    """更新每日RPS数据"""
    print(f"📥 正在更新 {len(stocks)} 只股票数据...")
    today = datetime.now().strftime('%Y-%m-%d')
    
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(calculate_rps_for_stock, code): code for code in stocks}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"  已完成 {done}/{len(stocks)}")
            try:
                result = future.result()
                if result:
                    results.append(result)
            except:
                pass
    
    # 存储到数据库
    if results:
        conn = sqlite3.connect(DB_PATH)
        df = pd.DataFrame(results)
        
        # 删除旧数据
        conn.execute(f'DELETE FROM daily_rps WHERE date = "{today}"')
        
        # 插入新数据
        df.to_sql('daily_rps', conn, if_exists='append', index=False)
        
        # 更新排名
        for period in [20, 50, 120, 250]:
            rank_col = f'rank_{period}'
            conn.execute(f'''
                UPDATE daily_rps 
                SET {rank_col} = (
                    SELECT COUNT(*) + 1 FROM daily_rps d2 
                    WHERE d2.date = daily_rps.date 
                    AND d2.rps_{period} > daily_rps.rps_{period}
                )
                WHERE date = "{today}"
            ''')
        
        conn.commit()
        conn.close()
        print(f"✅ 已保存 {len(results)} 只股票数据")
    
    return results

def main():
    init_db()
    
    if len(sys.argv) < 2:
        print("""
📊 RPS排名追踪系统

用法:
  python3 rps_tracker.py update [股票代码文件]   # 更新数据
  python3 rps_tracker.py report [周期] [N]      # 查看排名变化报告
  python3 rps_tracker.py top [周期] [N]          # 查看当前Top N

例:
  python3 rps_tracker.py update my_stocks.txt    # 从文件读取股票列表
  python3 rps_tracker.py report 20 50             # 20日RPS，前50名进出
  python3 rps_tracker.py top 50 30                # 查看50日RPS前30名
        """)
        return
    
    cmd = sys.argv[1]
    
    if cmd == 'update':
        # 默认用沪深300成分股
        stocks = [f'{i:06d}' for i in range(1, 301)]  # 简化版，实际应用中指数接口
        update_daily_data(stocks)
    
    elif cmd == 'report':
        period = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 50
        
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        changes = analyze_rank_changes(yesterday, today, period, top_n)
        print_rank_report(changes)
    
    elif cmd == 'top':
        period = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 30
        
        today = datetime.now().strftime('%Y-%m-%d')
        data = get_top_stocks_by_rps(today, period, top_n)
        
        if not data.empty:
            print(f"\n{'='*80}")
            print(f"📈 RPS{period} Top {top_n} (截至 {today})")
            print(f"{'='*80}")
            print(f"{'排名':>4} {'代码':<8} {'名称':<12} {'RPS':>10} {'价格':>10}")
            print(f"{'-'*80}")
            for i, row in data.iterrows():
                print(f"{i+1:>4} {row['code']:<8} {row['name']:<12} {row[f'rps_{period}']:>+10.2f}% ¥{row['price']:>8.2f}")
            print(f"{'='*80}\n")
        else:
            print("⚠️ 暂无数据，请先运行 update")

if __name__ == "__main__":
    main()
