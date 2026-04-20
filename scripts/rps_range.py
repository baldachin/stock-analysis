#!/usr/bin/env python3
"""
区间RPS - 基于价格区间的动量指标
解决传统RPS在趋势反转时失真的问题

核心思想：
- 涨跌由区间最高/最低点的先后顺序判断
- 最高在最低之前 = 跌势（从高点回落）
- 最低在最高之前 = 涨势（从低点反弹）
- 用区间涨幅和位置强度综合评分
"""

import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from daily_data import get_db_connection, read_daily_bars

DB_PATH = os.path.expanduser('~/stock_analysis/data/our_data.db')

def calculate_range_metrics(bars: List[Dict], period: int) -> Optional[Dict]:
    """
    计算区间度量
    返回: {
        'code': str,
        'period': int,
        'high': float,           # 区间最高价
        'low': float,            # 区间最低价
        'high_idx': int,         # 最高价在区间的位置(0=最早)
        'low_idx': int,          # 最低价在区间的位置
        'trend': str,            # 'UP' or 'DOWN'
        'range_pct': float,      # (high-low)/low * 100
        'current': float,        # 当前价格
        'pos_strength': float,   # (current-low)/(high-low) * 100
        'ret_from_low': float,    # (current-low)/low * 100
        'ret_from_high': float,  # (current-high)/high * 100
    }
    """
    if len(bars) < period:
        return None
    
    # 取最近period个交易日（从旧到新）
    window = bars[-period:] if len(bars) >= period else bars
    
    prices = [b['close'] for b in window]
    
    high = max(prices)
    low = min(prices)
    high_idx = prices.index(high)  # 最高价的位置
    low_idx = prices.index(low)    # 最低价的位置
    
    current = prices[-1]  # 最新价格
    
    # 判断趋势：最低在最高之前 = 涨势（从低点涨到高点再到现在）
    # 最高在最低之前 = 跌势（从高点跌到低点再到现在）
    trend = 'UP' if low_idx < high_idx else 'DOWN'
    
    # 区间幅度
    range_pct = (high - low) / low * 100 if low > 0 else 0
    
    # 位置强度：(当前价-最低)/(最高-最低)，100% = 接近最高
    pos_strength = (current - low) / (high - low) * 100 if high > low else 50
    
    # 从最低的涨幅
    ret_from_low = (current - low) / low * 100 if low > 0 else 0
    
    # 从最高的跌幅
    ret_from_high = (current - high) / high * 100 if high > 0 else 0
    
    return {
        'high': high,
        'low': low,
        'high_idx': high_idx,
        'low_idx': low_idx,
        'trend': trend,
        'range_pct': range_pct,
        'current': current,
        'pos_strength': pos_strength,
        'ret_from_low': ret_from_low,
        'ret_from_high': ret_from_high
    }

def batch_calculate_range(stocks: List[tuple], period: int) -> List[Dict]:
    """
    批量计算所有股票的区间度量
    stocks: [(code, name, market), ...]
    """
    results = []
    
    for code, name, market in stocks:
        bars = read_daily_bars(code, days=period + 10)  # 多取一些避免数据不足
        if not bars or len(bars) < period:
            continue
        
        metrics = calculate_range_metrics(bars, period)
        if metrics:
            metrics['code'] = code
            metrics['name'] = name
            metrics['period'] = period
            results.append(metrics)
    
    return results

def calculate_range_rps(results: List[Dict], sort_key: str = 'range_pct') -> List[Dict]:
    """
    根据区间度量计算RPS
    sort_key: 'range_pct'(区间幅度) | 'pos_strength'(位置强度)
    """
    if not results:
        return []
    
    total = len(results)
    sorted_results = sorted(results, key=lambda x: x[sort_key], reverse=True)
    
    for i, item in enumerate(sorted_results):
        item[f'{sort_key}_rps'] = ((total - i) / total) * 100
    
    return sorted_results

def create_range_rps_table():
    """创建区间RPS表"""
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS range_rps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            period INTEGER NOT NULL,
            high REAL,
            low REAL,
            trend TEXT,
            range_pct REAL,
            pos_strength REAL,
            ret_from_low REAL,
            ret_from_high REAL,
            range_pct_rps REAL,
            pos_strength_rps REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date, period)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_range_code ON range_rps(code)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_range_period ON range_rps(period)')
    conn.commit()
    conn.close()

def save_range_rps_to_db(period: int, results: List[Dict], trade_date: str):
    """保存区间RPS数据到数据库"""
    conn = get_db_connection()
    
    for item in results:
        conn.execute('''
            INSERT OR REPLACE INTO range_rps 
            (code, trade_date, period, high, low, trend, range_pct, pos_strength,
             ret_from_low, ret_from_high, range_pct_rps, pos_strength_rps, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item['code'], trade_date, period,
            item['high'], item['low'], item['trend'],
            item['range_pct'], item['pos_strength'],
            item['ret_from_low'], item['ret_from_high'],
            item.get('range_pct_rps', 0), item.get('pos_strength_rps', 0),
            datetime.now().isoformat()
        ))
    
    conn.commit()
    conn.close()

def calculate_all_range_rps():
    """计算所有周期、所有股票的区间RPS"""
    from daily_data import get_stock_list
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    print(f"\n{'='*70}")
    print(f"📊 区间RPS计算")
    print(f"{'='*70}")
    print(f"📅 {today}")
    
    create_range_rps_table()
    
    stocks = get_stock_list()
    print(f"📂 A股总数: {len(stocks)} 只")
    
    periods = [20, 50, 120, 250]
    
    for period in periods:
        print(f"\n📐 计算 {period}日区间...")
        
        # 批量计算
        results = batch_calculate_range(stocks, period)
        print(f"   有效数据: {len(results)} 只")
        
        if not results:
            continue
        
        # 计算两种RPS
        results = calculate_range_rps(results, 'range_pct')
        results = calculate_range_rps(results, 'pos_strength')
        
        # 保存
        save_range_rps_to_db(period, results, today)
        print(f"   ✅ 已保存")
        
        # 统计趋势
        up_count = sum(1 for r in results if r['trend'] == 'UP')
        down_count = len(results) - up_count
        print(f"   涨势: {up_count} ({up_count*100/len(results):.1f}%), 跌势: {down_count} ({down_count*100/len(results):.1f}%)")
    
    print(f"\n{'='*70}")

def get_stock_range_rps(code: str, periods: List[int] = None) -> Dict:
    """获取某股票的区间RPS"""
    conn = get_db_connection()
    
    if periods:
        placeholders = ','.join(['?'] * len(periods))
        query = f'''
            SELECT period, high, low, trend, range_pct, pos_strength,
                   ret_from_low, ret_from_high, range_pct_rps, pos_strength_rps
            FROM range_rps 
            WHERE code = ? AND period IN ({placeholders})
            ORDER BY period
        '''
        rows = conn.execute(query, [code] + periods).fetchall()
    else:
        rows = conn.execute('''
            SELECT period, high, low, trend, range_pct, pos_strength,
                   ret_from_low, ret_from_high, range_pct_rps, pos_strength_rps
            FROM range_rps 
            WHERE code = ?
            ORDER BY period
        ''', (code,)).fetchall()
    
    conn.close()
    
    result = {}
    for row in rows:
        result[row[0]] = {
            'high': row[1], 'low': row[2], 'trend': row[3],
            'range_pct': row[4], 'pos_strength': row[5],
            'ret_from_low': row[6], 'ret_from_high': row[7],
            'range_pct_rps': row[8], 'pos_strength_rps': row[9]
        }
    return result

def get_top_range_stocks(period: int, sort_by: str = 'range_pct', 
                         trend: str = None, limit: int = 20) -> List[Dict]:
    """
    获取区间RPS排名靠前的股票
    sort_by: 'range_pct_rps' | 'pos_strength_rps'
    trend: 'UP' | 'DOWN' | None(不筛选)
    """
    conn = get_db_connection()
    
    if trend:
        rows = conn.execute(f'''
            SELECT code, high, low, trend, range_pct, pos_strength,
                   ret_from_low, ret_from_high, range_pct_rps, pos_strength_rps
            FROM range_rps 
            WHERE period = ? AND trend = ?
            ORDER BY {sort_by} DESC
            LIMIT ?
        ''', (period, trend, limit)).fetchall()
    else:
        rows = conn.execute(f'''
            SELECT code, high, low, trend, range_pct, pos_strength,
                   ret_from_low, ret_from_high, range_pct_rps, pos_strength_rps
            FROM range_rps 
            WHERE period = ?
            ORDER BY {sort_by} DESC
            LIMIT ?
        ''', (period, limit)).fetchall()
    
    conn.close()
    
    return [
        {
            'code': r[0], 'high': r[1], 'low': r[2], 'trend': r[3],
            'range_pct': r[4], 'pos_strength': r[5],
            'ret_from_low': r[6], 'ret_from_high': r[7],
            'range_pct_rps': r[8], 'pos_strength_rps': r[9]
        }
        for r in rows
    ]

def analyze_stock(code: str) -> str:
    """分析单只股票，返回文字描述"""
    data = get_stock_range_rps(code, [20, 50, 120])
    
    if not data:
        return f"{code}: 无数据"
    
    lines = [f"\n{'='*60}",
             f"📊 {code} 区间RPS分析",
             f"{'='*60}"]
    
    for period in [20, 50, 120]:
        if period not in data:
            continue
        d = data[period]
        trend_icon = "📈" if d['trend'] == 'UP' else "📉"
        
        lines.append(f"\n  📐 {period}日:")
        lines.append(f"     区间: {d['low']:.2f} ~ {d['high']:.2f} ({d['range_pct']:.1f}%)")
        lines.append(f"     趋势: {trend_icon} {'涨势(低点先于高点)' if d['trend']=='UP' else '跌势(高点先于低点)'}")
        lines.append(f"     位置: {d['pos_strength']:.1f}% (距低{d['ret_from_low']:+.1f}%, 距高{d['ret_from_high']:+.1f}%)")
        lines.append(f"     RPS:  区间幅度 {d['range_pct_rps']:.1f}% | 位置强度 {d['pos_strength_rps']:.1f}%")
    
    lines.append(f"\n{'='*60}")
    return "\n".join(lines)

if __name__ == "__main__":
    calculate_all_range_rps()