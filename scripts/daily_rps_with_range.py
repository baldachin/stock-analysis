#!/usr/bin/env python3
"""
每日RPS追踪 - 包含传统RPS和区间RPS
"""
import sys
sys.path.insert(0, '/home/braveyun/stock_analysis/scripts')

import os
from datetime import datetime
from daily_rps_3stocks import analyze_stock as analyze_traditional_rps
from rps_range import get_stock_range_rps

OUTPUT_PATH = os.path.expanduser('~/stock_analysis/data/daily_rps_with_range.txt')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新',
    '002049': '紫光国微'
}

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    
    lines = []
    lines.append(f"📊 **三只股票每日RPS追踪** (含区间RPS)")
    lines.append(f"📅 {today} | 🕐 {datetime.now().strftime('%H:%M')}")
    lines.append("")
    lines.append("💾 数据: our_data.db")
    lines.append("")
    
    for code, name in STOCKS.items():
        # 传统RPS
        trad_rps = analyze_traditional_rps(code)
        
        # 区间RPS
        range_data = get_stock_range_rps(code, [20, 50, 120])
        
        lines.append(f"🟢 **{name}({code})**")
        if trad_rps:
            lines.append(f"   💰 {trad_rps['price']:.2f} ({trad_rps['change']:+.2f}%)")
            lines.append(f"   📊 传统RPS: RPS20={trad_rps['rps_20']:+.1f}%, RPS50={trad_rps['rps_50']:+.1f}%, RPS120={trad_rps['rps_120']:+.1f}%")
            lines.append(f"   趋势: {trad_rps.get('trend', '➡️')}")
        
        if range_data:
            lines.append(f"   📐 区间RPS:")
            for period in [20, 50, 120]:
                if period in range_data:
                    d = range_data[period]
                    trend_icon = '📈' if d['trend'] == 'UP' else '📉'
                    lines.append(f"      RPS{period}: {d['pos_strength_rps']:.1f}% | 位置{d['pos_strength']:.1f}% | {trend_icon}{'涨' if d['trend']=='UP' else '跌'}")
        
        lines.append("")
    
    # 横向对比
    lines.append("-" * 40)
    lines.append("📈 **横向对比 (传统RPS20)**")
    
    rps20_list = []
    for code, name in STOCKS.items():
        trad_rps = analyze_traditional_rps(code)
        if trad_rps:
            rps20_list.append((name, trad_rps['rps_20']))
    
    rps20_list.sort(key=lambda x: x[1], reverse=True)
    for i, (name, rps) in enumerate(rps20_list):
        medal = ["🥇", "🥈", "🥉"][i] if i < 3 else ""
        lines.append(f"{medal} {name}: RPS20={rps:+.1f}%")
    
    lines.append("")
    lines.append("-" * 40)
    lines.append(f"_💾 our_data.db | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_")
    
    output = "\n".join(lines)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print(output)
    print(f"\n✅ 报告已保存到: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()