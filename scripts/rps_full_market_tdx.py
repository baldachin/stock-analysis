#!/usr/bin/env python3
"""
沪深A股 RPS 排名计算
使用我们的 SQLite 数据层
基于全市场 5100+ 只A股计算RPS排名
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import os

# 使用我们的数据层
from daily_data import get_stock_list, batch_get_returns, calculate_rps, show_stats

DB_PATH = os.path.expanduser('~/stock_analysis/data/our_data.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

def calculate_all_ranks_from_db(period):
    """从数据库批量获取收益率并计算RPS排名"""
    returns_list = batch_get_returns(period)
    return calculate_rps(returns_list)

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    
    print(f"\n{'='*80}")
    print(f"📊 A股全市场RPS排名报告")
    print(f"{'='*80}")
    print(f"📅 {today}")
    print(f"💾 数据源: our_data.db")
    print(f"{'='*80}\n")
    
    # 获取股票列表
    stocks = get_stock_list()
    print(f"📂 A股总数: {len(stocks)} 只\n")
    
    # 计算各周期RPS
    periods = [5, 10, 20, 50, 120, 250]
    all_ranks = {}
    
    for period in periods:
        print(f"📐 计算 RPS{period}...")
        ranks = calculate_all_ranks_from_db(period)
        all_ranks[period] = {r['code']: r for r in ranks}
        print(f"   完成 ({len(ranks)} 只有效数据)")
    
    # 输出结果
    print(f"\n{'='*80}")
    print(f"📊 三只重点股票RPS排名")
    print(f"{'='*80}")
    
    for code, name in STOCKS.items():
        price_row = None
        try:
            conn = sqlite3.connect(DB_PATH)
            price_row = conn.execute('''
                SELECT close FROM daily_bars 
                WHERE code = ? 
                ORDER BY trade_date DESC LIMIT 1
            ''', (code,)).fetchone()
            conn.close()
        except:
            pass
        
        price = price_row[0] if price_row else 0
        
        print(f"\n🔹 {name} ({code})")
        print(f"   💰 价格: {price:.2f}")
        print(f"   📊 全市场RPS排名 (基于{len(stocks)}只):")
        
        for period in [20, 50, 120, 250]:
            r = all_ranks.get(period, {}).get(code)
            if r:
                rps = r['rps']
                ret = r['ret']
                emoji = "🟢" if rps > 50 else "🔴"
                bar = "█" * int(rps/10) + "░" * (10 - int(rps/10))
                print(f"      {emoji} RPS{period:3d}: {rps:5.1f}% | {bar} | {ret:+7.2f}%")
        
        # 趋势判断
        r20 = all_ranks.get(20, {}).get(code)
        r50 = all_ranks.get(50, {}).get(code)
        r120 = all_ranks.get(120, {}).get(code)
        
        if r20 and r50 and r120:
            r20_v, r50_v, r120_v = r20['rps'], r50['rps'], r120['rps']
            if r20_v > r50_v > r120_v and r20_v > 60:
                trend = "📈 加速上涨"
            elif r20_v > 60 and r50_v < 50:
                trend = "📊 反弹回暖"
            elif r20_v < r50_v < r120_v and r20_v < 40:
                trend = "📉 加速下跌"
            elif r20_v < 50 and r50_v > 50:
                trend = "➡️ 涨多回调"
            elif r20_v < r50_v < 50:
                trend = "🔻 持续调整"
            else:
                trend = "➡️ 震荡整理"
            print(f"   趋势: {trend}")
    
    print(f"\n{'='*80}")
    print(f"💡 基于 {len(stocks)} 只股票统计 | RPS = 打败市场上X%的股票")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    show_stats()
    main()
