#!/usr/bin/env python3
"""
生成HTML报告（可打印为PDF）
"""
import sqlite3
import codecs
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

DB_PATH = '/home/stock_analysis/data/our_data.db'
ASHARE_FILE = '/home/stock_analysis/data/全部A股.txt'
OUTPUT_HTML = '/home/stock_analysis/data/rps_report.html'

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def load_industry_map() -> Dict[str, Dict[str, str]]:
    industry_map = {}
    try:
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
        print(f"已加载 {len(industry_map)} 只股票的行业/地区信息")
    except Exception as e:
        print(f"加载行业信息失败: {e}")
    
    return industry_map

def get_filtered_stock_pool() -> List[Tuple[str, str]]:
    conn = get_db_connection()
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    rows = conn.execute('''
        SELECT code, name 
        FROM stock_pool 
        WHERE is_st = 0 
          AND (list_date IS NOT NULL AND list_date != '' AND list_date <= ?)
    ''', (one_year_ago,)).fetchall()
    conn.close()
    return rows

def calculate_rps(pool, periods):
    conn = get_db_connection()
    stock_data = {}
    for code, name in pool:
        data = conn.execute('''
            SELECT trade_date, close FROM daily_bars 
            WHERE code = ? 
            ORDER BY trade_date DESC 
            LIMIT 300
        ''', (code,)).fetchall()
        
        if len(data) >= 130:
            stock_data[code] = {
                'name': name,
                'closes': [row[1] for row in reversed(data)],
            }
    conn.close()
    
    results = {}
    for period in periods:
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
        
        returns.sort(key=lambda x: x['ret'], reverse=True)
        total = len(returns)
        for i, r in enumerate(returns):
            r['rps'] = round(((total - i) / total) * 100, 2)
        results[period] = returns[:10]
    
    return results

def get_range_rps(periods):
    conn = get_db_connection()
    latest = conn.execute('SELECT MAX(trade_date) FROM range_rps').fetchone()[0]
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    results = {}
    for period in periods:
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
            LIMIT 10
        ''', (latest, period, one_year_ago)).fetchall()
        
        items = []
        for row in rows:
            items.append({
                'code': row[0],
                'name': row[1] or row[0],
                'range_pct': row[5],
                'pos_strength': row[6],
                'trend': row[4],
                'rps': row[7]
            })
        results[period] = items
    conn.close()
    return results

def get_amount_rank(periods):
    conn = get_db_connection()
    latest_date = conn.execute('SELECT MAX(trade_date) FROM daily_bars').fetchone()[0]
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    pool_codes = [r[0] for r in conn.execute('''
        SELECT code FROM stock_pool 
        WHERE is_st = 0 
          AND (list_date IS NOT NULL AND list_date != '' AND list_date <= ?)
    ''', (one_year_ago,)).fetchall()]
    
    results = {}
    for period in periods:
        placeholders = ','.join(['?' for _ in pool_codes])
        rows = conn.execute(f'''
            SELECT db.code, SUM(db.amount) as total_amount
            FROM daily_bars db
            WHERE db.code IN ({placeholders})
              AND db.trade_date >= date('{latest_date}', '-{period} days')
            GROUP BY db.code
            ORDER BY total_amount DESC
            LIMIT 10
        ''', pool_codes).fetchall()
        
        total_market = sum(r[1] or 0 for r in rows if r[1])
        items = []
        for code, total_amount in rows:
            if not total_amount or total_amount <= 0:
                continue
            name_row = conn.execute('SELECT name FROM stock_pool WHERE code = ?', (code,)).fetchone()
            name = name_row[0] if name_row else code
            market_share = (total_amount / total_market * 100) if total_market > 0 else 0
            items.append({
                'code': code,
                'name': name,
                'amount': total_amount,
                'market_share': market_share
            })
        
        total = len(items)
        for i, item in enumerate(items):
            item['rank'] = i + 1
            item['rps'] = round(((total - i) / total) * 100, 2)
        results[period] = items
    conn.close()
    return results

def format_num(val):
    if val >= 10000:
        return f"{val/10000:.2f}万"
    return f"{val:.2f}"

def generate_html(rps_results, range_results, amount_results, industry_map, pool_size, trade_date):
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>RPS报告 - {trade_date}</title>
<style>
    body {{ font-family: Arial, sans-serif; margin: 20px; font-size: 12px; }}
    h1 {{ text-align: center; color: #333; }}
    h2 {{ color: #555; border-bottom: 2px solid #ddd; padding-bottom: 5px; }}
    h3 {{ color: #666; margin-left: 10px; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
    th {{ background-color: #f5f5f5; }}
    tr:nth-child(even) {{ background-color: #fafafa; }}
    .section {{ margin-bottom: 30px; }}
    .subtitle {{ color: #888; font-size: 11px; }}
    @media print {{
        body {{ margin: 0; font-size: 10px; }}
        table {{ page-break-inside: avoid; }}
        .section {{ page-break-inside: avoid; }}
    }}
</style>
</head>
<body>
<h1>全市场RPS报告（过滤版）</h1>
<p style="text-align:center" class="subtitle">
    📅 {trade_date} | 📂 股票池: {pool_size} 只（已排除ST & 上市一年以内新股）
</p>
"""

    # RPS tables
    html += '<div class="section">'
    for period in [5, 20, 120]:
        if period not in rps_results:
            continue
        html += f"<h2>📈 RPS{period} 前10名</h2>"
        html += f"""<table>
<tr><th>#</th><th>代码</th><th>名称</th><th>细分行业</th><th>地区</th><th>价格</th><th>收益率</th><th>RPS</th></tr>
"""
        for i, item in enumerate(rps_results[period]):
            info = industry_map.get(item['code'], {})
            industry = info.get('industry', '')[:12]
            region = info.get('region', '')[:6]
            html += f"""<tr>
<td>{i+1}</td><td>{item['code']}</td><td>{item['name']}</td>
<td>{industry}</td><td>{region}</td>
<td>{item['price']:.2f}</td><td>{item['ret']:+.2f}%</td><td>{item['rps']:.1f}%</td>
</tr>
"""
        html += '</table>'
    html += '</div>'

    # 区间RPS tables
    html += '<div class="section">'
    for period in [20, 50, 120]:
        if period not in range_results:
            continue
        html += f"<h2>📐 区间RPS{period} 前10名</h2>"
        html += f"""<table>
<tr><th>#</th><th>代码</th><th>名称</th><th>细分行业</th><th>地区</th><th>区间幅度</th><th>位置强度</th><th>趋势</th><th>RPS</th></tr>
"""
        for i, item in enumerate(range_results[period]):
            info = industry_map.get(item['code'], {})
            industry = info.get('industry', '')[:12]
            region = info.get('region', '')[:6]
            trend_icon = "📈" if item['trend'] == 'UP' else "📉"
            html += f"""<tr>
<td>{i+1}</td><td>{item['code']}</td><td>{item['name']}</td>
<td>{industry}</td><td>{region}</td>
<td>{item['range_pct']:+.1f}%</td><td>{item['pos_strength']:.1f}%</td>
<td>{trend_icon}</td><td>{item['rps']:.1f}%</td>
</tr>
"""
        html += '</table>'
    html += '</div>'

    # 成交额 tables
    html += '<div class="section">'
    for period in [5, 20, 120]:
        if period not in amount_results:
            continue
        html += f"<h2>💰 {period}日成交额排名 前10名</h2>"
        html += f"""<table>
<tr><th>#</th><th>代码</th><th>名称</th><th>细分行业</th><th>地区</th><th>成交额</th><th>占比</th><th>RPS</th></tr>
"""
        for i, item in enumerate(amount_results[period]):
            info = industry_map.get(item['code'], {})
            industry = info.get('industry', '')[:12]
            region = info.get('region', '')[:6]
            amount_str = format_num(item['amount'])
            html += f"""<tr>
<td>{i+1}</td><td>{item['code']}</td><td>{item['name']}</td>
<td>{industry}</td><td>{region}</td>
<td>{amount_str}</td><td>{item['market_share']:.3f}%</td><td>{item['rps']:.1f}%</td>
</tr>
"""
        html += '</table>'
    html += '</div>'

    html += f"""
<p class="subtitle" style="text-align:center">
    💾 our_data.db | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</p>
</body>
</html>"""

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ HTML报告已保存到: {OUTPUT_HTML}")
    print(f"   请在浏览器中打开并打印为PDF（Ctrl+P → 保存为PDF）")

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\n{'='*60}")
    print(f"📊 生成RPS报告HTML")
    print(f"{'='*60}")
    
    pool = get_filtered_stock_pool()
    print(f"📂 股票池: {len(pool)} 只")
    
    industry_map = load_industry_map()
    
    print("📈 计算RPS...")
    rps_results = calculate_rps(pool, [5, 20, 120])
    
    print("📐 获取区间RPS...")
    range_results = get_range_rps([20, 50, 120])
    
    print("💰 计算成交额排名...")
    amount_results = get_amount_rank([5, 20, 120])
    
    generate_html(rps_results, range_results, amount_results, industry_map, len(pool), today)

if __name__ == "__main__":
    main()
