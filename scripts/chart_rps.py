#!/usr/bin/env python3
"""
股票RPS和成交量图表生成
"""

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import os

# 设置中文字体
import matplotlib.font_manager as fm
# 注册Noto Sans CJK字体
font_path = '/usr/share/fonts/opentype/noto/NotoSansCJK-Medium.ttc'
fm.fontManager.addfont(font_path)
font_prop = fm.FontProperties(fname=font_path)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.unicode_minus'] = False

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

def get_stock_hist_sina(symbol, days=120):
    """获取历史K线"""
    try:
        url = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
        params = {'symbol': symbol, 'scale': '240', 'ma': '5', 'datalen': days}
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['day'] = pd.to_datetime(df['day'])
        df = df.sort_values('day')
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def calculate_rps_series(df, periods=[20, 50]):
    """计算RPS序列"""
    close = df['close'].values
    result = {}
    
    for period in periods:
        rps_list = []
        for i in range(period, len(close)):
            ret = (close[i] - close[i-period]) / close[i-period] * 100
            rps_list.append(ret)
        result[period] = rps_list
    
    return result

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    output_dir = os.path.expanduser('~/stock_analysis/data')
    os.makedirs(output_dir, exist_ok=True)
    
    all_data = {}
    
    print("📥 获取数据中...")
    for code, name in STOCKS.items():
        if code.startswith('6'):
            symbol = f'sh{code}'
        else:
            symbol = f'sz{code}'
        
        df = get_stock_hist_sina(symbol, days=120)
        if not df.empty:
            all_data[code] = {
                'name': name,
                'df': df
            }
            print(f"  ✅ {name}: {len(df)} 条数据")
        else:
            print(f"  ⚠️ {name}: 获取失败")
    
    if not all_data:
        print("❌ 没有任何数据")
        return
    
    # 创建图表
    fig, axes = plt.subplots(len(all_data), 2, figsize=(16, 5*len(all_data)))
    if len(all_data) == 1:
        axes = axes.reshape(1, -1)
    
    for idx, (code, info) in enumerate(all_data.items()):
        df = info['df']
        name = info['name']
        
        # 准备数据
        close = df['close'].values
        volume = df['volume'].values
        dates = df['day'].values
        
        # 计算RPS
        rps_20 = []
        rps_50 = []
        for i in range(50, len(close)):
            if i >= 20:
                r20 = (close[i] - close[i-20]) / close[i-20] * 100
            else:
                r20 = np.nan
            if i >= 50:
                r50 = (close[i] - close[i-50]) / close[i-50] * 100
            else:
                r50 = np.nan
            rps_20.append(r20)
            rps_50.append(r50)
        
        # 日期对齐
        dates_for_plot = dates[50:]
        
        # 左图：价格 + RPS
        ax1 = axes[idx, 0]
        ax1_twin = ax1.twinx()
        
        # 价格线
        ax1.plot(dates, close, 'b-', linewidth=1.5, label='Price')
        ax1.fill_between(dates, close*0.95, close, alpha=0.1)
        ax1.set_ylabel('Price (CNY)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        
        # RPS20
        rps_20_arr = np.array(rps_20)
        colors_20 = ['red' if x > 0 else 'green' for x in rps_20_arr]
        ax1_twin.bar(dates_for_plot, rps_20_arr, alpha=0.3, color='orange', label='RPS20')
        ax1_twin.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)
        ax1_twin.set_ylabel('RPS 20', color='orange')
        ax1_twin.tick_params(axis='y', labelcolor='orange')
        
        ax1.set_title(f'{name} ({code}) - Price & RPS20', fontsize=12, fontweight='bold')
        ax1.set_xlabel('Date')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 右图：成交量 + RPS50
        ax2 = axes[idx, 1]
        ax2_twin = ax2.twinx()
        
        # 成交量
        colors_vol = ['red' if close[i] >= close[i-1] else 'green' for i in range(1, len(close))]
        colors_vol.insert(0, 'gray')
        ax2.bar(dates, volume, color=colors_vol, alpha=0.6, label='Volume')
        ax2.set_ylabel('Volume', color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
        
        # RPS50
        rps_50_arr = np.array(rps_50)
        ax2_twin.plot(dates_for_plot, rps_50_arr, 'purple', linewidth=1.5, label='RPS50')
        ax2_twin.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)
        ax2_twin.set_ylabel('RPS 50', color='purple')
        ax2_twin.tick_params(axis='y', labelcolor='purple')
        
        ax2.set_title(f'{name} ({code}) - Volume & RPS50', fontsize=12, fontweight='bold')
        ax2.set_xlabel('Date')
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    # 保存
    output_path = os.path.join(output_dir, 'rps_charts.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\n✅ 图表已保存: {output_path}")
    
    # 同时生成一个汇总表格
    summary = []
    summary.append(f"# {today} 股票RPS汇总\n")
    summary.append("| 股票 | 代码 | 最新价 | RPS5 | RPS10 | RPS20 | RPS50 | RPS120 | 趋势 |")
    summary.append("|------|------|--------|------|-------|-------|-------|--------|------|")
    
    for code, info in all_data.items():
        df = info['df']
        close = df['close'].values
        name = info['name']
        price = close[-1]
        
        rps_vals = {}
        for period in [5, 10, 20, 50, 120]:
            if len(close) >= period + 1:
                rps_vals[period] = (close[-1] - close[-period-1]) / close[-period-1] * 100
            else:
                rps_vals[period] = None
        
        # 趋势判断
        rps_5 = rps_vals.get(5, 0) or 0
        rps_20 = rps_vals.get(20, 0) or 0
        rps_50 = rps_vals.get(50, 0) or 0
        rps_120 = rps_vals.get(120, 0) or 0
        
        if rps_5 > rps_20 > rps_50 > 0:
            trend = "📈 加速"
        elif rps_20 > 0 and rps_50 < 0:
            trend = "📊 反弹"
        elif rps_20 < rps_50 < 0:
            trend = "📉 下跌"
        else:
            trend = "➡️ 整理"
        
        def fmt(v):
            if v is None: return "N/A"
            return f"{v:+.1f}%"
        
        summary.append(f"| {name} | {code} | {price:.2f} | {fmt(rps_vals.get(5))} | {fmt(rps_vals.get(10))} | {fmt(rps_vals.get(20))} | {fmt(rps_vals.get(50))} | {fmt(rps_vals.get(120))} | {trend} |")
    
    summary_path = os.path.join(output_dir, 'rps_summary.md')
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(summary))
    print(f"✅ 汇总已保存: {summary_path}")
    
    return output_path

if __name__ == "__main__":
    main()
