#!/usr/bin/env python3
"""
高效同步数据到群晖NAS
同步内容: stocks, daily_bars, stock_rps, range_rps, range_rps_history
"""

import psycopg2
import sqlite3
import time
import gc
from datetime import datetime

NAS_CONFIG = {
    'host': '10.0.0.250',
    'port': 5433,
    'user': 'admin',
    'password': 'hs781014',
    'database': 'stockdata'
}
LOCAL_DB = '/home/braveyun/stock_analysis/data/our_data.db'

BATCH_SIZE = 1000

def create_rps_tables(nas_cur):
    """创建RPS相关表"""
    nas_cur.execute('''
        CREATE TABLE IF NOT EXISTS stock_rps (
            id SERIAL PRIMARY KEY,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            period INTEGER NOT NULL,
            price REAL,
            ret REAL,
            rps REAL,
            updated_at TEXT,
            UNIQUE(code, trade_date, period)
        )
    ''')
    
    nas_cur.execute('''
        CREATE TABLE IF NOT EXISTS range_rps (
            id SERIAL PRIMARY KEY,
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
            updated_at TEXT,
            UNIQUE(code, trade_date, period)
        )
    ''')
    
    nas_cur.execute('''
        CREATE TABLE IF NOT EXISTS range_rps_history (
            id SERIAL PRIMARY KEY,
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
            updated_at TEXT,
            UNIQUE(code, trade_date, period)
        )
    ''')
    
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_sr_code ON stock_rps(code)')
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_sr_date ON stock_rps(trade_date)')
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_rr_code ON range_rps(code)')
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_rr_date ON range_rps(trade_date)')
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_rh_code ON range_rps_history(code)')
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_rh_date ON range_rps_history(trade_date)')
    nas_cur.execute('CREATE INDEX IF NOT EXISTS idx_rh_period ON range_rps_history(period)')

def sync_table(nas_conn, nas_cur, local_conn, local_cur, table_name, columns, batch_size=1000):
    """同步单个表"""
    print(f"\n📊 同步 {table_name}...")
    
    local_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    total = local_cur.fetchone()[0]
    if total == 0:
        print(f"  为空，跳过")
        return 0
    
    print(f"  待同步: {total:,} 条")
    
    # 获取字段
    local_cur.execute(f"PRAGMA table_info({table_name})")
    cols_info = local_cur.fetchall()
    col_names = [c[1] for c in cols_info]
    col_str = ', '.join(col_str for col_str in col_names)
    placeholders = ', '.join(['%s'] * len(col_names))
    
    # 清空表
    nas_cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    nas_conn.commit()
    
    offset = 0
    t0 = time.time()
    inserted = 0
    
    while offset < total:
        local_cur.execute(f'''
            SELECT {col_str}
            FROM {table_name}
            ORDER BY {col_names[0]}, {col_names[1]}
            LIMIT {batch_size} OFFSET {offset}
        ''')
        rows = local_cur.fetchall()
        
        if not rows:
            break
        
        for r in rows:
            try:
                nas_cur.execute(f'''
                    INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})
                ''', r)
                inserted += 1
            except Exception as e:
                pass
        
        nas_conn.commit()
        offset += len(rows)
        
        elapsed = time.time() - t0
        speed = offset / elapsed if elapsed > 0 else 1
        eta = (total - offset) / speed / 60 if speed > 0 else 0
        pct = offset / total * 100
        
        print(f"\r  进度: {offset:,}/{total:,} ({pct:.1f}%) | {speed:.0f}条/秒 | ETA: {eta:.0f}分钟", end='', flush=True)
        
        del rows
        gc.collect()
    
    print()
    return inserted

def full_sync():
    """全量同步"""
    print(f"📡 全量同步到群晖NAS")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    nas_conn = psycopg2.connect(**NAS_CONFIG)
    nas_cur = nas_conn.cursor()
    
    local_conn = sqlite3.connect(LOCAL_DB)
    local_cur = local_conn.cursor()
    
    # 创建RPS表
    print("🔧 创建RPS相关表...")
    create_rps_tables(nas_cur)
    nas_conn.commit()
    
    # 同步 stocks
    print("\n📊 同步 stocks...")
    local_cur.execute('SELECT code, name, market FROM stocks WHERE is_ashare = 1')
    stocks = local_cur.fetchall()
    for code, name, market in stocks:
        nas_cur.execute('''
            INSERT INTO stocks (code, name, market, is_ashare)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
        ''', (code, name, market))
    nas_conn.commit()
    print(f"✅ stocks ({len(stocks)} 只)")
    
    # 同步各表
    sync_table(nas_conn, nas_cur, local_conn, local_cur, 'daily_bars', None)
    sync_table(nas_conn, nas_cur, local_conn, local_cur, 'stock_rps', None)
    sync_table(nas_conn, nas_cur, local_conn, local_cur, 'range_rps', None)
    sync_table(nas_conn, nas_cur, local_conn, local_cur, 'range_rps_history', None)
    
    # 更新同步时间
    nas_cur.execute('''
        INSERT INTO meta (key, value) VALUES ('last_sync', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
    nas_conn.commit()
    
    nas_conn.close()
    local_conn.close()
    print(f"\n✅ 全量同步完成!")

def incremental_sync():
    """增量同步"""
    print(f"📡 增量同步到群晖NAS")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    nas_conn = psycopg2.connect(**NAS_CONFIG)
    nas_cur = nas_conn.cursor()
    
    local_conn = sqlite3.connect(LOCAL_DB)
    local_cur = local_conn.cursor()
    
    t0 = time.time()
    total_insert = 0
    
    # 增量同步 stock_rps
    print("\n📊 增量同步 stock_rps...")
    nas_cur.execute("SELECT MAX(trade_date) FROM stock_rps")
    last_date = nas_cur.fetchone()[0]
    print(f"  NAS最新: {last_date}")
    
    local_cur.execute('''
        SELECT code, trade_date, period, price, ret, rps, updated_at
        FROM stock_rps WHERE trade_date > ?
    ''', (last_date,))
    rows = local_cur.fetchall()
    for r in rows:
        try:
            nas_cur.execute('''
                INSERT INTO stock_rps (code, trade_date, period, price, ret, rps, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code, trade_date, period) DO UPDATE SET
                    price = EXCLUDED.price, ret = EXCLUDED.ret, rps = EXCLUDED.rps
            ''', r)
            total_insert += 1
        except:
            pass
    nas_conn.commit()
    print(f"  ✅ {len(rows)} 条")
    
    # 增量同步 range_rps
    print("\n📊 增量同步 range_rps...")
    nas_cur.execute("SELECT MAX(trade_date) FROM range_rps")
    last_date = nas_cur.fetchone()[0]
    print(f"  NAS最新: {last_date}")
    
    local_cur.execute('''
        SELECT code, trade_date, period, high, low, trend, range_pct,
               pos_strength, ret_from_low, ret_from_high, 
               range_pct_rps, pos_strength_rps, updated_at
        FROM range_rps WHERE trade_date > ?
    ''', (last_date,))
    rows = local_cur.fetchall()
    for r in rows:
        try:
            nas_cur.execute('''
                INSERT INTO range_rps (code, trade_date, period, high, low, trend, range_pct,
                     pos_strength, ret_from_low, ret_from_high, 
                     range_pct_rps, pos_strength_rps, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code, trade_date, period) DO UPDATE SET
                    high = EXCLUDED.high, low = EXCLUDED.low, trend = EXCLUDED.trend,
                    range_pct = EXCLUDED.range_pct, pos_strength = EXCLUDED.pos_strength
            ''', r)
            total_insert += 1
        except:
            pass
    nas_conn.commit()
    print(f"  ✅ {len(rows)} 条")
    
    # 更新同步时间
    nas_cur.execute('''
        INSERT INTO meta (key, value) VALUES ('last_sync', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
    nas_conn.commit()
    
    nas_conn.close()
    local_conn.close()
    
    t1 = time.time()
    print(f"\n✅ 增量同步完成! 新增: {total_insert} 条 | {t1-t0:.1f}秒")

if __name__ == '__main__':
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else 'full'
    
    nas_conn = psycopg2.connect(**NAS_CONFIG)
    nas_cur = nas_conn.cursor()
    nas_cur.execute("SELECT COUNT(*) FROM daily_bars")
    existing = nas_cur.fetchone()[0]
    nas_conn.close()
    
    if existing == 0:
        print("📊 NAS为空，执行全量同步")
        full_sync()
    elif mode == 'full':
        print(f"📊 NAS已有 {existing:,} 条，强制全量同步")
        full_sync()
    else:
        print(f"📊 NAS已有 {existing:,} 条，执行增量同步")
        incremental_sync()