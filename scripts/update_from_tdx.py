#!/usr/bin/env python3
"""
通达信数据更新脚本 - 直接更新到SQLite
"""
import sqlite3
import os
import struct
from datetime import datetime
from pytdx.hq import TdxHq_API

DB_PATH = '/home/stock_analysis/data/our_data.db'
TDX_SERVERS = [
    ('110.41.147.114', 7709),
    ('110.41.2.72', 7709),
    ('110.41.4.4', 7709),
    ('124.70.176.52', 7709),
    ('122.51.120.217', 7709),
]

def get_market_id(code):
    return 1 if code.startswith(('6', '5', '4', '8', '9')) else 0

def is_ashare(code):
    return (code.startswith(('600', '601', '603', '605', '688')) or
            code.startswith(('000', '001', '002', '300', '301')))

def get_all_codes():
    """从数据库获取所有股票代码"""
    conn = sqlite3.connect(DB_PATH)
    codes = [r[0] for r in conn.execute('SELECT code FROM stocks WHERE is_ashare=1')]
    conn.close()
    return codes

def get_latest_date(code):
    """获取数据库中某股票最新日期"""
    conn = sqlite3.connect(DB_PATH)
    r = conn.execute('SELECT MAX(trade_date) FROM daily_bars WHERE code=?', (code,)).fetchone()
    conn.close()
    return r[0] if r and r[0] else '1990-01-01'

def update_stock(api, code, days=10):
    """更新单只股票"""
    market_id = get_market_id(code)
    try:
        data = api.get_security_bars(category=5, market=market_id, code=code, start=0, count=days)
        if not data:
            return 0
        
        records = []
        for bar in data:
            trade_date = f"{bar['year']}-{bar['month']:02d}-{bar['day']:02d}"
            records.append((code, trade_date, bar['open'], bar['high'], bar['low'],
                          bar['close'], int(bar['vol']), bar.get('amount', 0)))
        return records
    except Exception as e:
        return []

def main():
    print(f"📡 通达信 → SQLite 数据更新")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 连接
    api = TdxHq_API()
    connected = False
    for host, port in TDX_SERVERS:
        try:
            api.connect(host, port)
            print(f"  ✅ 已连接 {host}:{port}")
            connected = True
            break
        except:
            continue
    
    if not connected:
        print("  ❌ 无法连接服务器")
        return
    
    # 获取所有代码
    codes = get_all_codes()
    print(f"  📊 数据库有 {len(codes)} 只股票")
    print()
    
    # 批量更新
    updated = 0
    inserted = 0
    failed = 0
    
    conn = sqlite3.connect(DB_PATH)
    
    for i, code in enumerate(codes):
        if (i + 1) % 200 == 0:
            print(f"  进度: {i+1}/{len(codes)}")
        
        records = update_stock(api, code, days=20)
        if not records:
            failed += 1
            continue
        
        for r in records:
            try:
                conn.execute('''
                    INSERT OR REPLACE INTO daily_bars 
                    (code, trade_date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', r)
                inserted += 1
            except:
                pass
        
        updated += 1
    
    conn.commit()
    conn.close()
    api.disconnect()
    
    print()
    print(f"✅ 更新完成!")
    print(f"   更新股票: {updated}")
    print(f"   新增记录: {inserted}")
    print(f"   失败: {failed}")

if __name__ == "__main__":
    main()