#!/usr/bin/env python3
"""
通达信数据转存工具
将 .day 文件数据转存到统一的 SQLite 数据库

数据流程:
  .day 文件 → our_data.db (我们自己的格式)
  pytdx 更新 → our_data.db

用法:
    python3 convert_to_sqlite.py              # 全量转换
    python3 convert_to_sqlite.py --incremental  # 增量更新
"""

import struct
import os
import sys
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path

# 路径配置
TDX_PATH = os.path.expanduser('~/.local/share/tdxcfv/drive_c/tc/vipdoc')
DB_PATH = os.path.expanduser('~/stock_analysis/data/our_data.db')

# A股代码前缀判断
def is_ashare(code, market):
    if market == 'sh':
        return code.startswith(('600', '601', '603', '605', '688'))
    elif market == 'sz':
        return code.startswith(('000', '001', '002', '300', '301'))
    return False

def get_market_id(code):
    return 1 if code.startswith(('6', '5', '4', '8', '9')) else 0

def init_db():
    """初始化数据库"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 股票列表
    c.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            code TEXT PRIMARY KEY,
            name TEXT DEFAULT '',
            market TEXT DEFAULT '',  -- 'sh' or 'sz'
            list_date TEXT,         -- 上市日期
            is_ashare INTEGER DEFAULT 1
        )
    ''')
    
    # 日线数据 (我们自己的格式)
    c.execute('''
        CREATE TABLE IF NOT EXISTS daily_bars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,  -- YYYY-MM-DD
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            amount REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')
    
    # 创建索引
    c.execute('CREATE INDEX IF NOT EXISTS idx_bars_code_date ON daily_bars(code, trade_date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_bars_date ON daily_bars(trade_date)')
    
    # 元数据表
    c.execute('''
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"✅ 数据库初始化: {DB_PATH}")

def read_day_file(market, code):
    """读取 .day 文件"""
    if market == 'sh':
        path = os.path.join(TDX_PATH, 'sh', 'lday', f'sh{code}.day')
    else:
        path = os.path.join(TDX_PATH, 'sz', 'lday', f'sz{code}.day')
    
    if not os.path.exists(path):
        return []
    
    records = []
    try:
        with open(path, 'rb') as f:
            while True:
                data = f.read(32)
                if len(data) < 32:
                    break
                
                date_val = struct.unpack('<I', data[0:4])[0]
                year = date_val // 10000
                month = (date_val % 10000) // 100
                day = date_val % 100
                
                if year < 1990 or year > 2100:
                    continue
                
                trade_date = f"{year}-{month:02d}-{day:02d}"
                
                record = {
                    'code': code,
                    'trade_date': trade_date,
                    'open': struct.unpack('<I', data[4:8])[0] / 100.0,
                    'high': struct.unpack('<I', data[8:12])[0] / 100.0,
                    'low': struct.unpack('<I', data[12:16])[0] / 100.0,
                    'close': struct.unpack('<I', data[16:20])[0] / 100.0,
                    'volume': struct.unpack('<I', data[20:24])[0],
                    'amount': struct.unpack('<I', data[24:28])[0],
                }
                records.append(record)
    except Exception as e:
        print(f"  ⚠️ 读取 {market}/{code} 失败: {e}")
    
    return records

def get_all_local_stocks():
    """获取本地所有股票"""
    stocks = []
    
    sh_path = os.path.join(TDX_PATH, 'sh', 'lday')
    sz_path = os.path.join(TDX_PATH, 'sz', 'lday')
    
    if os.path.exists(sh_path):
        for f in os.listdir(sh_path):
            if f.endswith('.day') and f.startswith('sh'):
                code = f[2:-4]
                if is_ashare(code, 'sh'):
                    stocks.append(('sh', code))
    
    if os.path.exists(sz_path):
        for f in os.listdir(sz_path):
            if f.endswith('.day') and f.startswith('sz'):
                code = f[2:-4]
                if is_ashare(code, 'sz'):
                    stocks.append(('sz', code))
    
    return stocks

def convert_full():
    """全量转换 .day 文件到 SQLite"""
    print("=" * 60)
    print("📦 全量数据转换")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    init_db()
    
    stocks = get_all_local_stocks()
    print(f"\n📂 找到 {len(stocks)} 只A股")
    
    conn = sqlite3.connect(DB_PATH)
    total_records = 0
    total_stocks = 0
    
    for i, (market, code) in enumerate(stocks):
        if (i + 1) % 500 == 0:
            print(f"  进度: {i+1}/{len(stocks)}")
        
        records = read_day_file(market, code)
        if not records:
            continue
        
        # 插入股票记录
        conn.execute('''
            INSERT OR REPLACE INTO stocks (code, market, is_ashare)
            VALUES (?, ?, 1)
        ''', (code, market))
        
        # 批量插入日线数据
        for r in records:
            conn.execute('''
                INSERT OR REPLACE INTO daily_bars 
                (code, trade_date, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (r['code'], r['trade_date'], r['open'], r['high'], r['low'], 
                  r['close'], r['volume'], r['amount']))
        
        total_records += len(records)
        total_stocks += 1
    
    # 更新元数据
    conn.execute('''
        INSERT OR REPLACE INTO meta (key, value)
        VALUES ('last_full_convert', ?)
    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ 转换完成!")
    print(f"   股票数: {total_stocks}")
    print(f"   记录数: {total_records}")

def convert_incremental():
    """增量更新 - 从 .day 文件读取最新数据并更新到数据库"""
    print("=" * 60)
    print("📥 增量更新")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    init_db()
    
    # 获取数据库中已有的股票
    conn = sqlite3.connect(DB_PATH)
    existing = set(r[0] for r in conn.execute('SELECT code FROM stocks').fetchall())
    conn.close()
    
    print(f"📂 数据库已有 {len(existing)} 只股票")
    
    # 扫描本地文件
    all_stocks = get_all_local_stocks()
    new_stocks = [(m, c) for m, c in all_stocks if c not in existing]
    
    print(f"📂 本地共有 {len(all_stocks)} 只股票")
    print(f"🆕 新增 {len(new_stocks)} 只")
    
    if new_stocks:
        print("\n📥 导入新增股票...")
        conn = sqlite3.connect(DB_PATH)
        
        for i, (market, code) in enumerate(new_stocks):
            if (i + 1) % 100 == 0:
                print(f"  进度: {i+1}/{len(new_stocks)}")
            
            records = read_day_file(market, code)
            if not records:
                continue
            
            conn.execute('''
                INSERT OR REPLACE INTO stocks (code, market, is_ashare)
                VALUES (?, ?, 1)
            ''', (code, market))
            
            for r in records:
                conn.execute('''
                    INSERT OR REPLACE INTO daily_bars 
                    (code, trade_date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (r['code'], r['trade_date'], r['open'], r['high'], r['low'],
                      r['close'], r['volume'], r['amount']))
        
        conn.commit()
        conn.close()
    
    print("\n✅ 增量更新完成!")

def show_stats():
    """显示数据库统计"""
    if not os.path.exists(DB_PATH):
        print("❌ 数据库不存在，请先运行 convert_to_sqlite.py")
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    stock_count = conn.execute('SELECT COUNT(*) FROM stocks').fetchone()[0]
    bar_count = conn.execute('SELECT COUNT(*) FROM daily_bars').fetchone()[0]
    
    # 最新和最老的日期
    newest = conn.execute('SELECT MAX(trade_date) FROM daily_bars').fetchone()[0]
    oldest = conn.execute('SELECT MIN(trade_date) FROM daily_bars').fetchone()[0]
    
    last_convert = conn.execute(
        "SELECT value FROM meta WHERE key='last_full_convert'"
    ).fetchone()
    
    conn.close()
    
    size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
    
    print("=" * 50)
    print("📊 数据统计")
    print("=" * 50)
    print(f"  数据库路径: {DB_PATH}")
    print(f"  数据库大小: {size_mb:.2f} MB")
    print(f"  股票数量: {stock_count}")
    print(f"  K线记录: {bar_count:,}")
    print(f"  数据范围: {oldest} ~ {newest}")
    if last_convert:
        print(f"  最后转换: {last_convert[0]}")
    print("=" * 50)

def main():
    parser = argparse.ArgumentParser(description='通达信数据转存工具')
    parser.add_argument('--incremental', '-i', action='store_true', help='增量更新')
    parser.add_argument('--stats', '-s', action='store_true', help='显示统计')
    args = parser.parse_args()
    
    if args.stats:
        show_stats()
        return
    
    if args.incremental:
        convert_incremental()
    else:
        convert_full()
        show_stats()

if __name__ == "__main__":
    main()
