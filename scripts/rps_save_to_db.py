#!/usr/bin/env python3
"""
全市场RPS计算并保存到数据库
计算所有A股的RPS排名并存储，方便随时查询
"""

import sqlite3
import os
from datetime import datetime
from daily_data import get_stock_list, batch_get_returns, calculate_rps, get_db_connection

DB_PATH = os.path.expanduser('~/stock_analysis/data/our_data.db')

def create_rps_table():
    """创建RPS表（如果不存在）"""
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stock_rps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            period INTEGER NOT NULL,
            price REAL,
            ret REAL,
            rps REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date, period)
        )
    ''')
    # 创建索引加速查询
    conn.execute('CREATE INDEX IF NOT EXISTS idx_rps_code ON stock_rps(code)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_rps_date ON stock_rps(trade_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_rps_period ON stock_rps(period)')
    conn.commit()
    conn.close()

def save_rps_to_db(period: int, returns_list: list, trade_date: str):
    """保存某周期RPS数据到数据库"""
    conn = get_db_connection()
    
    for item in returns_list:
        code, name, price, ret = item['code'], item['name'], item['price'], item['ret']
        rps = item['rps']
        
        conn.execute('''
            INSERT OR REPLACE INTO stock_rps (code, trade_date, period, price, ret, rps, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (code, trade_date, period, price, ret, rps, datetime.now().isoformat()))
    
    conn.commit()
    conn.close()

def calculate_and_save_all_rps():
    """计算所有周期的RPS并保存到数据库"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    print(f"\n{'='*60}")
    print(f"📊 全市场RPS计算并保存到数据库")
    print(f"{'='*60}")
    print(f"📅 {today}")
    print(f"💾 数据库: {DB_PATH}")
    
    # 确保表存在
    create_rps_table()
    
    # 获取股票列表
    stocks = get_stock_list()
    print(f"📂 A股总数: {len(stocks)} 只")
    
    # 计算各周期RPS
    periods = [5, 10, 20, 50, 120, 250]
    
    for period in periods:
        print(f"\n📐 计算 RPS{period}...")
        returns_list = batch_get_returns(period)
        if not returns_list:
            print(f"   ⚠️ 无数据，跳过")
            continue
        
        rps_results = calculate_rps(returns_list)
        save_rps_to_db(period, rps_results, today)
        print(f"   ✅ 已保存 {len(rps_results)} 只股票的RPS{period}")
    
    print(f"\n{'='*60}")
    print(f"💡 所有周期RPS计算完成")

def get_rps_from_db(code: str, periods: list = None) -> dict:
    """
    从数据库获取某股票的RPS数据
    返回格式: {period: {'price': x, 'ret': x, 'rps': x}, ...}
    """
    conn = get_db_connection()
    
    if periods:
        placeholders = ','.join(['?'] * len(periods))
        query = f'''
            SELECT period, price, ret, rps, trade_date 
            FROM stock_rps 
            WHERE code = ? AND period IN ({placeholders})
            ORDER BY period
        '''
        rows = conn.execute(query, [code] + periods).fetchall()
    else:
        rows = conn.execute('''
            SELECT period, price, ret, rps, trade_date 
            FROM stock_rps 
            WHERE code = ?
            ORDER BY period
        ''', (code,)).fetchall()
    
    conn.close()
    
    result = {}
    for period, price, ret, rps, trade_date in rows:
        result[period] = {'price': price, 'ret': ret, 'rps': rps, 'date': trade_date}
    return result

def get_top_stocks(period: int, limit: int = 20, min_rps: float = 80) -> list:
    """获取某周期RPS排名靠前的股票"""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT code, price, ret, rps 
        FROM stock_rps 
        WHERE period = ? AND rps >= ?
        ORDER BY rps DESC
        LIMIT ?
    ''', (period, min_rps, limit)).fetchall()
    conn.close()
    return [{'code': r[0], 'price': r[1], 'ret': r[2], 'rps': r[3]} for r in rows]

if __name__ == "__main__":
    calculate_and_save_all_rps()