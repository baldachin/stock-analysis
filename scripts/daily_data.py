#!/usr/bin/env python3
"""
我们的数据层 - 从 SQLite 数据库读取
"""

import sqlite3
import os
from datetime import date, timedelta
from typing import Optional, List, Dict
import numpy as np

DB_PATH = os.path.expanduser('~/stock_analysis/data/our_data.db')

def get_db_connection():
    """获取数据库连接"""
    return sqlite3.connect(DB_PATH)

def read_daily_bars(code: str, days: int = 300) -> List[Dict]:
    """
    读取股票的日线数据
    """
    conn = get_db_connection()
    
    # 获取最近days个交易日的日期
    result = conn.execute('''
        SELECT trade_date, open, high, low, close, volume, amount
        FROM daily_bars
        WHERE code = ?
        ORDER BY trade_date DESC
        LIMIT ?
    ''', (code, days)).fetchall()
    
    conn.close()
    
    if not result:
        return []
    
    # 返回从旧到新的顺序
    bars = []
    for row in reversed(result):
        bars.append({
            'date': row[0],
            'open': row[1],
            'high': row[2],
            'low': row[3],
            'close': row[4],
            'volume': row[5],
            'amount': row[6]
        })
    
    return bars

def get_stock_list() -> List[tuple]:
    """获取所有股票列表"""
    conn = get_db_connection()
    stocks = conn.execute('''
        SELECT code, name, market 
        FROM stocks 
        WHERE is_ashare = 1
    ''').fetchall()
    conn.close()
    return stocks

def get_latest_date(code: str) -> Optional[str]:
    """获取某股票最新数据的日期"""
    conn = get_db_connection()
    result = conn.execute('''
        SELECT MAX(trade_date) FROM daily_bars WHERE code = ?
    ''', (code,)).fetchone()
    conn.close()
    return result[0] if result else None

def get_trading_dates(days: int = 300) -> List[str]:
    """获取最近days个交易日日期"""
    conn = get_db_connection()
    dates = conn.execute('''
        SELECT DISTINCT trade_date 
        FROM daily_bars 
        ORDER BY trade_date DESC 
        LIMIT ?
    ''', (days,)).fetchall()
    conn.close()
    return [d[0] for d in dates]

def get_close_prices(date_str: str) -> Dict[str, float]:
    """获取某日期所有股票的收盘价"""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT code, close 
        FROM daily_bars 
        WHERE trade_date = ?
    ''', (date_str,)).fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}

def calculate_returns_for_stocks(codes: List[str], period: int) -> Dict[str, float]:
    """
    计算多只股票的某周期收益率
    返回 {code: return_pct}
    """
    conn = get_db_connection()
    
    results = {}
    for code in codes:
        # 获取最新价格和period天前的价格
        data = conn.execute('''
            SELECT trade_date, close FROM daily_bars 
            WHERE code = ? 
            ORDER BY trade_date DESC 
            LIMIT ?
        ''', (code, period + 1)).fetchall()
        
        if len(data) >= period + 1:
            current_price = data[0][1]
            old_price = data[period][1]
            if old_price > 0:
                ret = (current_price - old_price) / old_price * 100
                results[code] = ret
    
    conn.close()
    return results

def batch_get_returns(period: int) -> List[tuple]:
    """
    批量获取所有股票某周期的收益率
    返回 [(code, name, close, return_pct), ...]
    按收益率降序
    """
    conn = get_db_connection()
    
    # 获取所有股票的最新和历史价格
    stocks = conn.execute('''
        SELECT code FROM stocks WHERE is_ashare = 1
    ''').fetchall()
    
    results = []
    for (code,) in stocks:
        data = conn.execute('''
            SELECT trade_date, close FROM daily_bars 
            WHERE code = ? 
            ORDER BY trade_date DESC 
            LIMIT ?
        ''', (code, period + 1)).fetchall()
        
        if len(data) >= period + 1:
            current_price = data[0][1]
            old_price = data[period][1]
            if old_price > 0:
                ret = (current_price - old_price) / old_price * 100
                # 获取名称
                name_row = conn.execute(
                    'SELECT name FROM stocks WHERE code = ?', (code,)
                ).fetchone()
                name = name_row[0] if name_row else code
                results.append((code, name, current_price, ret))
    
    conn.close()
    
    # 按收益率降序排列
    results.sort(key=lambda x: x[3], reverse=True)
    return results

def calculate_rps(returns_list: List[tuple]) -> List[dict]:
    """
    根据收益率列表计算RPS
    returns_list: [(code, name, close, return_pct), ...]
    返回带RPS排名的列表
    """
    if not returns_list:
        return []
    
    total = len(returns_list)
    rps_results = []
    
    for i, (code, name, price, ret) in enumerate(returns_list):
        rps = ((total - i) / total) * 100
        rps_results.append({
            'code': code,
            'name': name,
            'price': price,
            'ret': ret,
            'rps': round(rps, 2)
        })
    
    return rps_results

def show_stats():
    """显示数据库统计"""
    if not os.path.exists(DB_PATH):
        print("❌ 数据库不存在")
        return
    
    conn = get_db_connection()
    stock_count = conn.execute('SELECT COUNT(*) FROM stocks').fetchone()[0]
    bar_count = conn.execute('SELECT COUNT(*) FROM daily_bars').fetchone()[0]
    newest = conn.execute('SELECT MAX(trade_date) FROM daily_bars').fetchone()[0]
    oldest = conn.execute('SELECT MIN(trade_date) FROM daily_bars').fetchone()[0]
    size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
    conn.close()
    
    print(f"\n📊 数据统计:")
    print(f"   数据库: {DB_PATH}")
    print(f"   大小: {size_mb:.1f} MB")
    print(f"   股票: {stock_count} 只")
    print(f"   K线: {bar_count:,} 条")
    print(f"   范围: {oldest} ~ {newest}")

if __name__ == '__main__':
    show_stats()
    
    # 测试读取
    print("\n测试读取几只股票:")
    test_codes = ['000001', '603986', '300496', '002049']
    for code in test_codes:
        bars = read_daily_bars(code, days=5)
        if bars:
            latest = bars[-1]
            print(f"  {code}: {latest['date']} 收盘 {latest['close']:.2f}")
        else:
            print(f"  {code}: 无数据")
