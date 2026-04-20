#!/usr/bin/env python3
"""
全市场 RPS 计算（过滤版）
- 排除 ST 股票
- 排除上市一年以内新股
- 基于过滤后的股票池计算传统 RPS
- 新增：成交额排名、市场成交占比排名
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

DB_PATH = '/home/stock_analysis/data/our_data.db'

# 输出目录
OUTPUT_DIR = '/home/stock_analysis/data'

# 全部A股数据文件（含行业、地区信息）
ASHARE_FILE = '/home/stock_analysis/data/全部A股.txt'

def load_industry_map() -> Dict[str, Dict[str, str]]:
    """
    从全部A股.txt加载行业和地区信息
    返回: {code: {'name': xxx, 'industry': xxx, 'region': xxx}, ...}
    """
    industry_map = {}
    try:
        import codecs
        with codecs.open(ASHARE_FILE, 'r', 'gbk') as f:
            lines = f.readlines()
        
        if len(lines) < 2:
            return industry_map
        
        header = lines[0].strip().split('\t')
        code_idx = header.index('代码')
        name_idx = header.index('名称')
        industry_idx = header.index('细分行业')
        region_idx = header.index('地区')
        
        for line in lines[1:]:
            cols = line.strip().split('\t')
            if len(cols) > max(code_idx, name_idx, industry_idx, region_idx):
                code = cols[code_idx].strip()
                if code:
                    industry_map[code] = {
                        'name': cols[name_idx].strip() if name_idx < len(cols) else '',
                        'industry': cols[industry_idx].strip() if industry_idx < len(cols) else '',
                        'region': cols[region_idx].strip() if region_idx < len(cols) else ''
                    }
        print(f"📂 已加载 {len(industry_map)} 只股票的行业/地区信息")
    except Exception as e:
        print(f"⚠️ 加载行业信息失败: {e}")
    
    return industry_map

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def get_filtered_stock_pool() -> List[Tuple[str, str]]:
    """
    获取过滤后的股票池：上市一年以上且非ST
    返回: [(code, name), ...]
    """
    conn = get_db_connection()
    
    # 上市一年以上的判断：list_date 距离今天超过365天
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    rows = conn.execute('''
        SELECT code, name 
        FROM stock_pool 
        WHERE is_st = 0 
          AND (list_date IS NOT NULL AND list_date != '' AND list_date <= ?)
    ''', (one_year_ago,)).fetchall()
    
    conn.close()
    
    print(f"📂 过滤后股票池: {len(rows)} 只 (排除ST & 上市一年以内)")
    return rows

def get_stock_amount_rank(conn, trade_date: str = None) -> List[Dict]:
    """
    获取某日成交额排名
    返回: [{code, name, amount, amount_rps, market_share}, ...]
    """
    if trade_date is None:
        # 获取最新有数据的日期
        trade_date = conn.execute(
            'SELECT MAX(trade_date) FROM daily_bars'
        ).fetchone()[0]
    
    # 获取当日所有成交额
    rows = conn.execute('''
        SELECT db.code, sp.name, db.amount
        FROM daily_bars db
        LEFT JOIN stock_pool sp ON db.code = sp.code
        WHERE db.trade_date = ?
        ORDER BY db.amount DESC
        LIMIT 5000
    ''', (trade_date,)).fetchall()
    
    if not rows:
        # 如果没有当日数据，尝试获取最近可用日期
        rows = conn.execute('''
            SELECT db.code, sp.name, db.amount
            FROM daily_bars db
            LEFT JOIN stock_pool sp ON db.code = sp.code
            WHERE db.trade_date = (
                SELECT MAX(trade_date) FROM daily_bars WHERE amount > 0
            )
            ORDER BY db.amount DESC
            LIMIT 5000
        ''').fetchall()
        if rows:
            trade_date = conn.execute('SELECT MAX(trade_date) FROM daily_bars WHERE amount > 0').fetchone()[0]
    
    if not rows:
        return []
    
    total_amount = sum(r[2] or 0 for r in rows if r[2])
    
    results = []
    for code, name, amount in rows:
        if not amount or amount <= 0:
            continue
        market_share = (amount / total_amount * 100) if total_amount > 0 else 0
        results.append({
            'code': code,
            'name': name or code,
            'amount': amount,
            'market_share': market_share
        })
    
    # 计算排名
    total = len(results)
    for i, r in enumerate(results):
        r['amount_rank'] = i + 1
        r['amount_rps'] = round(((total - i) / total) * 100, 2)
        r['share_rps'] = round((r['market_share'] / 100) * 100, 2)  # 成交占比本身的RPS
    
    conn.close()
    return results

def get_amount_rank_by_period(conn, periods: List[int] = None) -> Dict[int, List[Dict]]:
    """
    获取不同周期成交额排名
    返回: {period: [{code, name, total_amount, amount_rps, market_share}, ...], ...}
    """
    if periods is None:
        periods = [5, 20, 120]
    
    # 获取最新日期
    latest_date = conn.execute('SELECT MAX(trade_date) FROM daily_bars').fetchone()[0]
    
    # 获取过滤后的股票池
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    pool_codes = [r[0] for r in conn.execute('''
        SELECT code FROM stock_pool 
        WHERE is_st = 0 
          AND (list_date IS NOT NULL AND list_date != '' AND list_date <= ?)
    ''', (one_year_ago,)).fetchall()]
    pool_set = set(pool_codes)
    
    results = {}
    for period in periods:
        # 计算周期内成交额总和
        rows = conn.execute(f'''
            SELECT db.code, SUM(db.amount) as total_amount
            FROM daily_bars db
            WHERE db.code IN ({','.join(['?' for _ in pool_codes])})
              AND db.trade_date >= date('{latest_date}', '-{period} days')
            GROUP BY db.code
            ORDER BY total_amount DESC
            LIMIT 200
        ''', pool_codes).fetchall()
        
        if not rows:
            results[period] = []
            continue
        
        # 计算总成交额（用于市场占比）
        total_market_amount = sum(r[1] or 0 for r in rows if r[1])
        
        items = []
        for code, total_amount in rows:
            if not total_amount or total_amount <= 0:
                continue
            # 获取名称
            name_row = conn.execute('SELECT name FROM stock_pool WHERE code = ?', (code,)).fetchone()
            name = name_row[0] if name_row else code
            
            market_share = (total_amount / total_market_amount * 100) if total_market_amount > 0 else 0
            items.append({
                'code': code,
                'name': name,
                'amount': total_amount,
                'market_share': market_share
            })
        
        # 计算排名RPS
        total = len(items)
        for i, item in enumerate(items):
            item['amount_rank'] = i + 1
            item['amount_rps'] = round(((total - i) / total) * 100, 2)
        
        results[period] = items
        print(f"   成交额RPS{period}: {len(items)} 只有效")
    
    return results

def calculate_traditional_rps(pool: List[Tuple[str, str]], periods: List[int] = None) -> Dict[int, List[Dict]]:
    """
    计算传统RPS
    pool: [(code, name), ...]
    返回: {period: [{code, name, price, ret, rps}, ...], ...}
    """
    if periods is None:
        periods = [5, 10, 20, 50, 120, 250]
    
    conn = get_db_connection()
    
    # 预加载所有股票的数据
    print("📥 加载股票数据...")
    stock_data = {}
    for code, name in pool:
        data = conn.execute('''
            SELECT trade_date, close FROM daily_bars 
            WHERE code = ? 
            ORDER BY trade_date DESC 
            LIMIT 300
        ''', (code,)).fetchall()
        
        if len(data) >= 130:  # 至少250天数据
            stock_data[code] = {
                'name': name,
                'closes': [row[1] for row in reversed(data)],
                'dates': [row[0] for row in reversed(data)]
            }
    
    conn.close()
    print(f"   有效数据: {len(stock_data)} 只")
    
    results = {}
    for period in periods:
        print(f"📐 计算 RPS{period}...")
        
        returns = []
        for code, data in stock_data.items():
            closes = data['closes']
            if len(closes) >= period + 5:
                ret = (closes[-1] - closes[-period-1]) / closes[-period-1] * 100
                returns.append({
                    'code': code,
                    'name': data['name'],
                    'price': closes[-1],
                    'ret': ret
                })
        
        if not returns:
            results[period] = []
            continue
        
        # 排序并计算RPS
        returns.sort(key=lambda x: x['ret'], reverse=True)
        total = len(returns)
        
        for i, r in enumerate(returns):
            r['rps'] = round(((total - i) / total) * 100, 2)
        
        results[period] = returns
        print(f"   RPS{period}: {len(returns)} 只有效")
    
    return results

def save_rps_results(results: Dict[int, List[Dict]], trade_date: str):
    """保存RPS结果到数据库"""
    conn = get_db_connection()
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS filtered_rps (
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
    
    conn.execute('CREATE INDEX IF NOT EXISTS idx_frps_code ON filtered_rps(code)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_frps_period ON filtered_rps(period)')
    
    for period, data in results.items():
        for item in data:
            conn.execute('''
                INSERT OR REPLACE INTO filtered_rps 
                (code, trade_date, period, price, ret, rps, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                item['code'], trade_date, period,
                item['price'], item['ret'], item['rps'],
                datetime.now().isoformat()
            ))
    
    conn.commit()
    conn.close()
    print("✅ RPS结果已保存到数据库")

def save_amount_results(results: List[Dict], trade_date: str):
    """保存成交额排名到数据库"""
    conn = get_db_connection()
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS filtered_amount_rank (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            amount REAL,
            amount_rank INTEGER,
            amount_rps REAL,
            market_share REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, trade_date)
        )
    ''')
    
    conn.execute('CREATE INDEX IF NOT EXISTS idx_far_code ON filtered_amount_rank(code)')
    
    for item in results:
        conn.execute('''
            INSERT OR REPLACE INTO filtered_amount_rank 
            (code, trade_date, amount, amount_rank, amount_rps, market_share, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            item['code'], trade_date,
            item['amount'], item['amount_rank'], item['amount_rps'],
            item['market_share'], datetime.now().isoformat()
        ))
    
    conn.commit()
    conn.close()
    print("✅ 成交额排名已保存到数据库")

def get_stock_info(code: str) -> Optional[Dict]:
    """获取股票详细信息"""
    conn = get_db_connection()
    row = conn.execute(
        'SELECT code, name, list_date, is_st, is_new FROM stock_pool WHERE code = ?',
        (code,)
    ).fetchone()
    conn.close()
    if row:
        return {
            'code': row[0], 'name': row[1], 
            'list_date': row[2], 'is_st': row[3], 'is_new': row[4]
        }
    return None

def get_stock_industry_em(code: str) -> str:
    """从东方财富API获取股票行业板块"""
    try:
        import urllib.request
        import json
        if code.startswith(('6', '5', '4', '8', '9')):
            mkt = '1'
        else:
            mkt = '0'
        # 东方财富板块API
        url = f'http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fid=f3&fs=m:{mkt}+t:6,m:{mkt}+t:13,m:{mkt}+t:2,m:{mkt}+t:23&fields=f14,f100&secid={mkt}.{code}'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        r = urllib.request.urlopen(req, timeout=5)
        data = json.loads(r.read())
        if data.get('data'):
            # f100 is the industry/sector
            return data['data'].get('f100', '') or ''
    except:
        pass
    return ''

def get_batch_industry(codes: List[str]) -> Dict[str, str]:
    """批量获取股票行业（带缓存）"""
    industry_map = {}
    conn = get_db_connection()
    
    # Check if industry_cache table exists
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='industry_cache'").fetchone()
    if not tables:
        conn.execute('''
            CREATE TABLE industry_cache (
                code TEXT PRIMARY KEY,
                industry TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    
    # Get cached industries
    cached = {}
    rows = conn.execute('SELECT code, industry FROM industry_cache').fetchall()
    for code, industry in rows:
        cached[code] = industry
    conn.close()
    
    # Fetch missing ones
    import time
    for code in codes:
        if code not in cached:
            industry = get_stock_industry_em(code)
            if industry:
                industry_map[code] = industry
                # Save to cache
                conn2 = get_db_connection()
                conn2.execute('INSERT OR REPLACE INTO industry_cache (code, industry, updated_at) VALUES (?, ?, ?)',
                            (code, industry, datetime.now().isoformat()))
                conn2.commit()
                conn2.close()
                time.sleep(0.1)  # Rate limit
            else:
                industry_map[code] = ''
        else:
            industry_map[code] = cached[code]
    
    return industry_map

def get_filtered_range_rps(conn, periods: List[int] = None) -> Dict[int, List[Dict]]:
    """
    获取过滤后股票池的区间RPS
    返回: {period: [{code, name, range_pct, pos_strength, trend, range_pct_rps, pos_strength_rps}, ...], ...}
    """
    if periods is None:
        periods = [5, 20, 50]
    
    # Get latest date
    latest = conn.execute('SELECT MAX(trade_date) FROM range_rps').fetchone()[0]
    
    # Get one year ago date for filtering
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    results = {}
    for period in periods:
        # Get range_rps data for filtered pool
        rows = conn.execute(f'''
            SELECT rr.code, sp.name, rr.high, rr.low, rr.trend, 
                   rr.range_pct, rr.pos_strength, rr.range_pct_rps, rr.pos_strength_rps
            FROM range_rps rr
            INNER JOIN stock_pool sp ON rr.code = sp.code
            WHERE rr.trade_date = ?
              AND rr.period = ?
              AND sp.is_st = 0
              AND (sp.list_date IS NOT NULL AND sp.list_date != '' AND sp.list_date <= ?)
            ORDER BY rr.range_pct_rps DESC
            LIMIT 100
        ''', (latest, period, one_year_ago)).fetchall()
        
        items = []
        for row in rows:
            items.append({
                'code': row[0],
                'name': row[1] or row[0],
                'high': row[2],
                'low': row[3],
                'trend': row[4],
                'range_pct': row[5],
                'pos_strength': row[6],
                'range_pct_rps': row[7],
                'pos_strength_rps': row[8]
            })
        results[period] = items
        print(f"   区间RPS{period}: {len(items)} 只有效")
    
    return results

def format_top_report(results: Dict[int, List[Dict]], 
                      range_rps_results: Dict[int, List[Dict]],
                      amount_results_by_period: Dict[int, List[Dict]],
                      industry_map: Dict[str, str],
                      pool_size: int, trade_date: str) -> str:
    """格式化报告"""
    lines = []
    lines.append(f"{'='*80}")
    lines.append(f"📊 全市场RPS报告（过滤版）")
    lines.append(f"{'='*80}")
    lines.append(f"📅 {trade_date}")
    lines.append(f"📂 股票池: {pool_size} 只（已排除ST & 上市一年以内新股）")
    lines.append(f"{'='*80}")
    
    # 传统RPS - 只保留5, 20, 120
    for period in [5, 20, 120]:
        if period not in results or not results[period]:
            continue
        
        data = results[period][:10]
        lines.append(f"\n📈 RPS{period} 前10名:")
        lines.append(f"{'排名':>4} {'代码':<8} {'名称':<10} {'细分行业':<10} {'地区':<6} {'价格':>8} {'收益率':>8} {'RPS':>6}")
        lines.append("-" * 75)
        
        for i, item in enumerate(data):
            info = industry_map.get(item['code'], {})
            industry = info.get('industry', '')[:10]
            region = info.get('region', '')[:6]
            lines.append(
                f"{i+1:>4} {item['code']:<8} {item['name']:<10} {industry:<10} {region:<6} "
                f"{item['price']:>8.2f} {item['ret']:>+7.2f}% {item['rps']:>6.1f}%"
            )
    
    # 区间RPS - 前10 (only for periods 20, 50, 120 since 5 doesn't exist in DB)
    for period in [20, 50, 120]:
        if period not in range_rps_results or not range_rps_results[period]:
            continue
        
        data = range_rps_results[period][:10]
        lines.append(f"\n📐 区间RPS{period} 前10名 (区间幅度):")
        lines.append(f"{'排名':>4} {'代码':<8} {'名称':<10} {'细分行业':<10} {'地区':<6} {'区间幅度':>8} {'位置强度':>8} {'趋势':>4} {'RPS':>6}")
        lines.append("-" * 85)
        
        for i, item in enumerate(data):
            trend_icon = "📈" if item['trend'] == 'UP' else "📉"
            info = industry_map.get(item['code'], {})
            industry = info.get('industry', '')[:10]
            region = info.get('region', '')[:6]
            lines.append(
                f"{i+1:>4} {item['code']:<8} {item['name']:<10} {industry:<10} {region:<6} "
                f"{item['range_pct']:>+7.1f}% {item['pos_strength']:>7.1f}% {trend_icon} {item['range_pct_rps']:>6.1f}%"
            )
    
    # 成交额排名 - 多周期
    for period in [5, 20, 120]:
        if period not in amount_results_by_period or not amount_results_by_period[period]:
            continue
        
        data = amount_results_by_period[period][:10]
        lines.append(f"\n💰 {period}日成交额排名（前10）:")
        lines.append(f"{'排名':>4} {'代码':<8} {'名称':<10} {'细分行业':<10} {'地区':<6} {'成交额(万)':>12} {'占比':>8} {'RPS':>6}")
        lines.append("-" * 85)
        
        for i, item in enumerate(data):
            info = industry_map.get(item['code'], {})
            industry = info.get('industry', '')[:10]
            region = info.get('region', '')[:6]
            amount_w = item['amount'] / 10000 if item['amount'] else 0
            lines.append(
                f"{i+1:>4} {item['code']:<8} {item['name']:<10} {industry:<10} {region:<6} "
                f"{amount_w:>12.2f} {item['market_share']:>7.3f}% {item['amount_rps']:>6.1f}%"
            )
    
    lines.append(f"\n{'='*80}")
    lines.append(f"_💾 our_data.db | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_")
    
    return "\n".join(lines)

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    
    print(f"\n{'='*70}")
    print(f"📊 全市场RPS计算（过滤版）")
    print(f"{'='*70}")
    print(f"📅 {today}")
    
    # 1. 获取过滤后的股票池
    pool = get_filtered_stock_pool()
    if not pool:
        print("❌ 股票池为空")
        return
    
    # 2. 计算传统RPS (只计算5, 20, 120)
    periods = [5, 20, 120]
    rps_results = calculate_traditional_rps(pool, periods)
    
    # 保存到数据库
    save_rps_results(rps_results, today)
    
    # 3. 计算成交额排名（多周期）
    print(f"\n💰 计算成交额排名（多周期）...")
    conn2 = get_db_connection()
    amount_results_by_period = get_amount_rank_by_period(conn2, periods=[5, 20, 120])
    conn2.close()
    
    # 4. 获取区间RPS
    print(f"\n📐 获取区间RPS...")
    conn3 = get_db_connection()
    range_rps_results = get_filtered_range_rps(conn3, periods=[20, 50, 120])
    conn3.close()
    
    # 5. 加载行业信息
    print(f"\n🏭 加载行业/地区信息...")
    industry_map = load_industry_map()
    
    # 6. 输出报告
    report = format_top_report(rps_results, range_rps_results, amount_results_by_period, industry_map, len(pool), today)
    print("\n" + report)
    
    # 保存到文件
    output_path = os.path.join(OUTPUT_DIR, 'filtered_rps_report.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"\n✅ 报告已保存到: {output_path}")

if __name__ == "__main__":
    main()
