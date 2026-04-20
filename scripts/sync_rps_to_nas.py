#!/usr/bin/env python3
"""
快速同步RPS数据到群晖NAS（增量模式）
"""

import psycopg2
import sqlite3
import time
from datetime import datetime

NAS_CONFIG = {
    'host': '10.0.0.250',
    'port': 5433,
    'user': 'admin',
    'password': 'hs781014',
    'database': 'stockdata'
}
LOCAL_DB = '/home/braveyun/stock_analysis/data/our_data.db'

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

def sync_table_incremental(nas_conn, nas_cur, local_conn, local_cur, table_name, local_table):
    """增量同步单个表"""
    print(f"\n📊 同步 {table_name}...")
    
    # 获取NAS最新日期
    nas_cur.execute(f"SELECT MAX(trade_date) FROM {table_name}")
    result = nas_cur.fetchone()[0]
    last_date = result if result else '1900-01-01'
    print(f"  NAS最新: {last_date}")
    
    # 获取本地数据
    local_cur.execute(f'SELECT * FROM {local_table} WHERE trade_date > ?', (last_date,))
    rows = local_cur.fetchall()
    
    if not rows:
        print(f"  无新数据")
        return 0
    
    print(f"  待同步: {len(rows)} 条")
    
    # 获取字段
    local_cur.execute(f"PRAGMA table_info({local_table})")
    cols_info = local_cur.fetchall()
    col_names = [c[1] for c in cols_info]
    placeholders = ', '.join(['%s'] * len(col_names))
    col_str = ', '.join(col_names)
    
    inserted = 0
    for r in rows:
        try:
            nas_cur.execute(f'''
                INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})
                ON CONFLICT (code, trade_date, period) DO UPDATE SET
                    {', '.join(f"{col} = EXCLUDED.{col}" for col in col_names if col not in ['code', 'trade_date', 'period'])}
            ''', r)
            inserted += 1
        except Exception as e:
            pass
    
    nas_conn.commit()
    print(f"  ✅ {inserted} 条")
    return inserted

def full_sync_rps():
    """全量同步RPS表"""
    print(f"📡 全量同步RPS到NAS")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    nas_conn = psycopg2.connect(**NAS_CONFIG)
    nas_cur = nas_conn.cursor()
    
    local_conn = sqlite3.connect(LOCAL_DB)
    local_cur = local_conn.cursor()
    
    # 创建表
    print("🔧 创建RPS表...")
    create_rps_tables(nas_cur)
    nas_conn.commit()
    
    t0 = time.time()
    
    # 全量同步各表
    for local_table, nas_table in [
        ('stock_rps', 'stock_rps'),
        ('range_rps', 'range_rps'),
        ('range_rps_history', 'range_rps_history')
    ]:
        # 清空并重新插入
        nas_cur.execute(f"TRUNCATE TABLE {nas_table} RESTART IDENTITY CASCADE")
        nas_conn.commit()
        
        local_cur.execute(f'SELECT * FROM {local_table}')
        rows = local_cur.fetchall()
        
        local_cur.execute(f"PRAGMA table_info({local_table})")
        cols_info = local_cur.fetchall()
        col_names = [c[1] for c in cols_info]
        placeholders = ', '.join(['%s'] * len(col_names))
        col_str = ', '.join(col_names)
        
        print(f"\n📊 同步 {nas_table}... ({len(rows)} 条)")
        
        for r in rows:
            try:
                nas_cur.execute(f'INSERT INTO {nas_table} ({col_str}) VALUES ({placeholders})', r)
            except:
                pass
        
        nas_conn.commit()
        print(f"  ✅ 完成")
    
    nas_conn.close()
    local_conn.close()
    
    t1 = time.time()
    print(f"\n✅ 完成! 耗时: {t1-t0:.1f}秒")

def incremental_sync_rps():
    """增量同步RPS表"""
    print(f"📡 增量同步RPS到NAS")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    nas_conn = psycopg2.connect(**NAS_CONFIG)
    nas_cur = nas_conn.cursor()
    
    local_conn = sqlite3.connect(LOCAL_DB)
    local_cur = local_conn.cursor()
    
    t0 = time.time()
    total = 0
    
    total += sync_table_incremental(nas_conn, nas_cur, local_conn, local_cur, 'stock_rps', 'stock_rps')
    total += sync_table_incremental(nas_conn, nas_cur, local_conn, local_cur, 'range_rps', 'range_rps')
    total += sync_table_incremental(nas_conn, nas_cur, local_conn, local_cur, 'range_rps_history', 'range_rps_history')
    
    # 更新同步时间
    nas_cur.execute('''
        INSERT INTO meta (key, value) VALUES ('last_rps_sync', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
    nas_conn.commit()
    
    nas_conn.close()
    local_conn.close()
    
    t1 = time.time()
    print(f"\n✅ 增量同步完成! 新增: {total} 条 | {t1-t0:.1f}秒")

if __name__ == '__main__':
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else 'incremental'
    
    nas_conn = psycopg2.connect(**NAS_CONFIG)
    nas_cur = nas_conn.cursor()
    nas_cur.execute("SELECT COUNT(*) FROM range_rps_history")
    existing = nas_cur.fetchone()[0]
    nas_conn.close()
    
    if existing == 0:
        print("📊 NAS RPS历史为空，执行全量同步")
        full_sync_rps()
    elif mode == 'full':
        print("📊 强制全量同步")
        full_sync_rps()
    else:
        print(f"📊 NAS已有RPS历史，执行增量同步")
        incremental_sync_rps()