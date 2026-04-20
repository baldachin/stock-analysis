#!/usr/bin/env python3
"""
每日RPS追踪 - 专注三只股票
中科创达(300496)、兆易创新(603986)、紫光国微(002049)
使用我们的 SQLite 数据层
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime

# 使用我们的数据层
from daily_data import read_daily_bars, get_close_prices, get_trading_dates

DB_PATH = os.path.expanduser('~/stock_analysis/data/our_data.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

def analyze_stock(code):
    """使用我们的数据库分析股票"""
    bars = read_daily_bars(code, days=400)
    if not bars or len(bars) < 260:
        return None
    
    closes = [b['close'] for b in bars]
    prices = np.array(closes)
    
    name_row = None
    conn = sqlite3.connect(DB_PATH)
    try:
        name_row = conn.execute(
            'SELECT name FROM stocks WHERE code = ?', (code,)
        ).fetchone()
    except:
        pass
    conn.close()
    
    name = (name_row[0] if name_row and name_row[0] else STOCKS.get(code, code))
    price = closes[-1]
    prev_price = closes[-2] if len(closes) > 1 else price
    
    result = {
        'code': code,
        'name': name,
        'price': price,
        'change': (price - prev_price) / prev_price * 100,
        'date': bars[-1]['date'],
        'source': 'our_db'
    }
    
    for period in [5, 10, 20, 50, 120, 250]:
        if len(closes) >= period + 2:
            ret = (closes[-1] - closes[-period-1]) / closes[-period-1] * 100
            result[f'rps_{period}'] = ret
        else:
            result[f'rps_{period}'] = np.nan
    
    # 计算MACD (12,26,9)
    if len(closes) >= 34:
        ema12 = pd.Series(closes).ewm(span=12).mean().iloc[-1]
        ema26 = pd.Series(closes).ewm(span=26).mean().iloc[-1]
        macd_dif = ema12 - ema26
        macd_dea = pd.Series([macd_dif]).ewm(span=9).mean().iloc[-1]
        macd_bar = 2 * (macd_dif - macd_dea)
        result['macd'] = {'dif': macd_dif, 'dea': macd_dea, 'bar': macd_bar}
    
    return result

def format_report(results, today):
    """格式化报告"""
    msg = []
    msg.append(f"📊 **三只股票每日RPS追踪**")
    msg.append(f"📅 {today} | 🕐 {datetime.now().strftime('%H:%M')}")
    msg.append("")
    
    # 数据来源
    db_count = sum(1 for r in results if r and r.get('source') == 'our_db')
    if db_count > 0:
        msg.append(f"💾 数据: our_data.db")
        msg.append("")
    
    for code, name in STOCKS.items():
        data = next((r for r in results if r and r['code'] == code), None)
        
        if not data:
            msg.append(f"⚠️ **{name}({code})** - 数据获取失败")
            continue
        
        source_emoji = "💾"
        change_emoji = "🟢" if data['change'] > 0 else "🔴"
        change_sign = "+" if data['change'] > 0 else ""
        msg.append(f"{change_emoji} **{name}({code})** {source_emoji}")
        msg.append(f"   💰 {data['price']:.2f} ({change_sign}{data['change']:.2f}%)")
        
        # 各周期RPS
        periods = [(5,'短'), (10,'次短'), (20,'中短'), (50,'中介'), (120,'中长'), (250,'长')]
        rps_strs = []
        for period, label in periods:
            key = f'rps_{period}'
            if key in data and not np.isnan(data.get(key, np.nan)):
                val = data[key]
                if val > 0:
                    rps_strs.append(f"{label}:🟢{val:+.1f}%")
                else:
                    rps_strs.append(f"{label}:🔴{val:+.1f}%")
        msg.append(f"   📊 RPS: {', '.join(rps_strs)}")
        
        # 趋势判断
        rps_5 = data.get('rps_5', 0) or 0
        rps_20 = data.get('rps_20', 0) or 0
        rps_50 = data.get('rps_50', 0) or 0
        rps_120 = data.get('rps_120', 0) or 0
        
        if rps_5 > rps_20 > rps_50 > rps_120 and rps_20 > 0:
            trend = "📈 加速上涨"
        elif rps_20 > 0 and rps_50 < 0:
            trend = "📊 反弹回暖"
        elif rps_5 < rps_20 < rps_50 < 0:
            trend = "📉 加速下跌"
        elif rps_20 < 0 and rps_50 > 0:
            trend = "➡️ 涨多回调"
        elif rps_20 < rps_50 < 0:
            trend = "🔻 持续调整"
        else:
            trend = "➡️ 震荡整理"
        
        msg.append(f"   趋势: {trend}")
        
        if rps_20 > 15:
            msg.append(f"   ⚠️ 短期涨幅过大，注意回调风险")
        elif rps_20 < -15:
            msg.append(f"   💡 短期超跌，可能有反弹机会")
        
        msg.append("")
    
    # 对比分析
    msg.append("─" * 40)
    msg.append("📈 **横向对比**")
    
    sorted_by_20 = sorted([r for r in results if r], key=lambda x: x.get('rps_20', -999), reverse=True)
    for i, r in enumerate(sorted_by_20):
        medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "  "
        rps_val = r.get('rps_20', 0) or 0
        source = "💾"
        msg.append(f"{medal} {r['name']}: RPS20={rps_val:+.1f}% {source}")
    
    msg.append("")
    msg.append("─" * 40)
    msg.append("📝 **操作参考**")
    
    for r in sorted_by_20:
        if r['code'] in STOCKS:
            rps_20 = r.get('rps_20', 0) or 0
            rps_50 = r.get('rps_50', 0) or 0
            
            if rps_20 > 10 and rps_50 > 5:
                suggestion = "✅ 强势，持有/关注"
            elif rps_20 > 0 and rps_50 < -10:
                suggestion = "⚠️ 短期反弹，中期仍弱"
            elif rps_20 < -10:
                suggestion = "💡 超跌，观望"
            elif rps_50 > 15:
                suggestion = "✅ 中期强势"
            else:
                suggestion = "➡️ 等待信号"
            
            msg.append(f"• {r['name']}: {suggestion}")
    
    msg.append("")
    msg.append(f"_💾 our_data.db | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_")
    
    return "\n".join(msg)

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    
    print(f"📥 分析三只股票: {', '.join(STOCKS.values())}")
    print(f"💾 使用 our_data.db 数据层")
    
    results = []
    for code in STOCKS.keys():
        r = analyze_stock(code)
        if r:
            results.append(r)
            print(f"  ✅ {r['name']}: RPS20={r.get('rps_20', 0):+.1f}%")
        else:
            print(f"  ⚠️ {STOCKS[code]}: 数据获取失败")
    
    if not results:
        print("❌ 未能获取任何数据")
        return
    
    report = format_report(results, today)
    print("\n" + "=" * 50)
    print(report)
    print("=" * 50)
    
    # 保存
    output_path = os.path.expanduser('~/stock_analysis/data/daily_rps_3stocks.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✅ 报告已保存到: {output_path}")

if __name__ == "__main__":
    main()
