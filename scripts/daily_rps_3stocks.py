#!/usr/bin/env python3
"""
每日RPS追踪 - 专注三只股票
中科创达(300496)、兆易创新(603986)、紫光国微(002049)
"""

import requests
import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime, timedelta
import json

DB_PATH = os.path.expanduser('~/stock_analysis/data/rps_tracker.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

# ============== API ==============

def get_stock_hist_sina(symbol, days=300):
    try:
        url = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
        params = {'symbol': symbol, 'scale': '240', 'ma': '5', 'datalen': days}
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        df['day'] = pd.to_datetime(df['day'])
        df = df.sort_values('day')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        return df
    except: return pd.DataFrame()

def get_stock_name_tx(symbol):
    try:
        url = f'https://qt.gtimg.cn/q={symbol}'
        r = requests.get(url, timeout=5)
        parts = r.text.split('~')
        return parts[1] if len(parts) > 1 else ''
    except: return ''

def get_realtime(symbol):
    try:
        if symbol.startswith('6'): s = f'sh{symbol}'
        else: s = f'sz{symbol}'
        url = f'https://qt.gtimg.cn/q={s}'
        r = requests.get(url, timeout=5)
        parts = r.text.split('~')
        if len(parts) < 10: return {}
        return {
            'name': parts[1],
            'price': float(parts[3]) if parts[3] else 0,
            'yesterday_close': float(parts[4]) if parts[4] else 0,
            'open': float(parts[5]) if parts[5] else 0,
            'high': float[33] if len(parts) > 33 and parts[33] else 0,
            'low': float(parts[34]) if len(parts) > 34 and parts[34] else 0,
            'volume': int(parts[6]) if parts[6] else 0,
            'date': parts[30] if len(parts) > 30 else '',
            'time': parts[31] if len(parts) > 31 else '',
        }
    except: return {}

# ============== 分析 ==============

def analyze_stock(code):
    if code.startswith('6'): symbol = f'sh{code}'
    else: symbol = f'sz{code}'
    
    df = get_stock_hist_sina(symbol, 400)
    if df.empty or len(df) < 260: return None
    
    close = df['close'].values
    name = get_stock_name_tx(symbol)
    price = close[-1]
    prev_price = close[-2] if len(close) > 1 else price
    
    result = {
        'code': code,
        'name': name,
        'price': price,
        'change': (price - prev_price) / prev_price * 100,
        'date': df['day'].iloc[-1].strftime('%Y-%m-%d')
    }
    
    for period in [5, 10, 20, 50, 120, 250]:
        if len(close) >= period + 2:
            ret = (close[-1] - close[-period-1]) / close[-period-1] * 100
            result[f'rps_{period}'] = ret
        else:
            result[f'rps_{period}'] = np.nan
    
    # 计算MACD (12,26,9)
    if len(close) >= 34:
        ema12 = pd.Series(close).ewm(span=12).mean().iloc[-1]
        ema26 = pd.Series(close).ewm(span=26).mean().iloc[-1]
        macd_dif = ema12 - ema26
        macd_dea = pd.Series([macd_dif]).ewm(span=9).mean().iloc[-1]
        macd_bar = 2 * (macd_dif - macd_dea)
        result['macd'] = {'dif': macd_dif, 'dea': macd_dea, 'bar': macd_bar}
    
    # KDJ (9,3,3)
    if len(close) >= 9:
        high9 = pd.Series(close[-9:]).rolling(9).max().iloc[-1]
        low9 = pd.Series(close[-9:]).rolling(9).min().iloc[-1]
        rsv = (close[-1] - low9) / (high9 - low9) * 100 if high9 != low9 else 50
        result['kdj'] = {'k': 50, 'd': 50, 'j': rsv}  # 简化版
    
    return result

def format_report(results, today):
    """格式化报告"""
    msg = []
    msg.append(f"📊 **三只股票每日RPS追踪**")
    msg.append(f"📅 {today} | 🕐 {datetime.now().strftime('%H:%M')}")
    msg.append("")
    
    for code, name in STOCKS.items():
        data = next((r for r in results if r and r['code'] == code), None)
        
        if not data:
            msg.append(f"⚠️ **{name}({code})** - 数据获取失败")
            continue
        
        # 股票头
        change_emoji = "🟢" if data['change'] > 0 else "🔴"
        change_sign = "+" if data['change'] > 0 else ""
        msg.append(f"{change_emoji} **{name}({code})**")
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
        
        # 关键价位提醒
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
        msg.append(f"{medal} {r['name']}: RPS20={rps_val:+.1f}%")
    
    msg.append("")
    msg.append("─" * 40)
    msg.append("📝 **操作参考**")
    
    # 简单建议
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
    msg.append(f"_数据来源: 新浪财经 | 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_")
    
    return "\n".join(msg)

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    
    print(f"📥 分析三只股票: {', '.join(STOCKS.values())}")
    
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
    
    # 生成报告
    report = format_report(results, today)
    print("\n" + "=" * 50)
    print(report)
    print("=" * 50)
    
    # 保存到文件供调用
    output_path = os.path.expanduser('~/stock_analysis/data/daily_rps_3stocks.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✅ 报告已保存到: {output_path}")

if __name__ == "__main__":
    main()
