#!/usr/bin/env python3
"""
从本地SQLite增量同步到PostgreSQL
使用COPY命令高速写入
"""
import sqlite3
import psycopg2
from io import StringIO
from datetime import datetime

SQLITE_DB = '/home/stock_analysis/data/our_data.db'
PG_CONFIG = {
    'host': '10.0.0.250',
    'port': 5433,
    'user': 'admin',
    'password': 'hs781014',
    'dbname': 'stockdata'
}

def get_pg_latest_date(pg_conn, code):
    """获取PG中某股票最新日期"""
    cur = pg_conn.cursor()
    cur.execute('SELECT MAX(trade_date) FROM daily_bars WHERE code=%s', (code,))
    r = cur.fetchone()[0]
    return r if r else '1990-01-01'

def sync_incremental():
    """增量同步"""
    print(f"📥 SQLite → PostgreSQL 增量同步")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 连接SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    
    # 连接PG
    pg_conn = psycopg2.connect(**PG_CONFIG)
    pg_conn.autocommit = False
    
    # 获取PG中所有股票的最新日期
    print("  📊 获取PG最新日期...")
    pg_latest = {}
    cur = pg_conn.cursor()
    cur.execute('SELECT code, MAX(trade_date) FROM daily_bars GROUP BY code')
    for code, max_date in cur.fetchall():
        pg_latest[code] = max_date
    print(f"  PG有 {len(pg_latest)} 只股票的数据")
    
    # 获取所有股票
    stocks = [r[0] for r in sqlite_conn.execute('SELECT code FROM stocks WHERE is_ashare=1').fetchall()]
    print(f"  本地有 {len(stocks)} 只股票")
    print()
    
    total_inserted = 0
    updated_stocks = 0
    
    for i, code in enumerate(stocks):
        if (i + 1) % 500 == 0:
            print(f"  进度: {i+1}/{len(stocks)}")
        
        pg_date = pg_latest.get(code, '1990-01-01')
        
        # 读取SQLite中比PG更新的数据
        rows = sqlite_conn.execute('''
            SELECT code, trade_date, open, high, low, close, volume, amount
            FROM daily_bars
            WHERE code=? AND trade_date>?
            ORDER BY trade_date
        ''', (code, pg_date)).fetchall()
        
        if not rows:
            continue
        
        # 用COPY写入
        buf = StringIO()
        for r in rows:
            buf.write('\t'.join([str(x) for x in r]) + '\n')
        buf.seek(0)
        
        cur.copy_from(buf, 'daily_bars', columns=('code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'))
        total_inserted += len(rows)
        updated_stocks += 1
    
    pg_conn.commit()
    
    sqlite_conn.close()
    pg_conn.close()
    
    print()
    print(f"✅ 同步完成!")
    print(f"   更新股票: {updated_stocks}")
    print(f"   新增记录: {total_inserted}")

if __name__ == "__main__":
    sync_incremental()