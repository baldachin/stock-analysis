#!/usr/bin/env python3
"""
股票RPS交互式HTML图表生成
使用matplotlib生成可交互的HTML5图表
"""

import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import os
import json

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

def get_stock_hist_sina(symbol, days=120):
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
    except: return pd.DataFrame()

def generate_html(all_data, output_path):
    """生成交互式HTML"""
    
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>股票RPS追踪</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }
        h1 { text-align: center; color: #00d4ff; }
        .stock-card { background: #16213e; border-radius: 10px; padding: 20px; margin: 20px 0; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        .stock-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }
        .stock-name { font-size: 24px; font-weight: bold; color: #00d4ff; }
        .stock-code { color: #888; font-size: 14px; }
        .price { font-size: 32px; font-weight: bold; color: #fff; }
        .change { font-size: 18px; margin-left: 10px; }
        .up { color: #ff4757; }
        .down { color: #2ed573; }
        .rps-summary { display: flex; gap: 15px; margin: 15px 0; flex-wrap: wrap; }
        .rps-item { background: #0f3460; padding: 10px 15px; border-radius: 8px; text-align: center; min-width: 80px; }
        .rps-label { font-size: 12px; color: #888; }
        .rps-value { font-size: 18px; font-weight: bold; }
        .chart-container { margin-top: 15px; }
        .trend { display: inline-block; padding: 5px 10px; border-radius: 5px; font-size: 14px; }
        .trend-up { background: #2ed57333; color: #2ed573; }
        .trend-down { background: #ff475733; color: #ff4757; }
        .trend-side { background: #ffa50233; color: #ffa502; }
        .updated { text-align: right; color: #666; font-size: 12px; margin-top: 20px; }
    </style>
</head>
<body>
    <h1>📊 股票RPS追踪</h1>
'''
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    for code, info in all_data.items():
        df = info['df']
        name = info['name']
        close = df['close'].values
        volume = df['volume'].values
        dates = df['day'].dt.strftime('%Y-%m-%d').values
        
        price = close[-1]
        prev_price = close[-2] if len(close) > 1 else price
        change_pct = (price - prev_price) / prev_price * 100
        change_class = 'up' if change_pct > 0 else 'down'
        change_sign = '+' if change_pct > 0 else ''
        
        # 计算各周期RPS
        rps_data = {}
        for period in [5, 10, 20, 50, 120]:
            if len(close) >= period + 1:
                rps_data[period] = (close[-1] - close[-period-1]) / close[-period-1] * 100
            else:
                rps_data[period] = 0
        
        # 趋势判断
        r5, r10, r20, r50, r120 = rps_data.get(5,0), rps_data.get(10,0), rps_data.get(20,0), rps_data.get(50,0), rps_data.get(120,0)
        if r5 > r20 > r50 > 0:
            trend, trend_class = '📈 加速上涨', 'trend-up'
        elif r20 > 0 and r50 < 0:
            trend, trend_class = '📊 反弹回暖', 'trend-side'
        elif r5 < r20 < r50 < 0:
            trend, trend_class = '📉 加速下跌', 'trend-down'
        elif r20 < 0 and r50 > 0:
            trend, trend_class = '➡️ 涨多回调', 'trend-side'
        elif r20 < r50 < 0:
            trend, trend_class = '🔻 持续调整', 'trend-down'
        else:
            trend, trend_class = '➡️ 震荡整理', 'trend-side'
        
        # 计算RPS序列用于图表
        rps20_seq = []
        rps50_seq = []
        dates_for_rps = []
        for i in range(50, len(close)):
            rps20_seq.append((close[i] - close[i-20]) / close[i-20] * 100 if i >= 20 else 0)
            rps50_seq.append((close[i] - close[i-50]) / close[i-50] * 100 if i >= 50 else 0)
            dates_for_rps.append(dates[i])
        
        html += f'''
    <div class="stock-card">
        <div class="stock-header">
            <div>
                <span class="stock-name">{name}</span>
                <span class="stock-code">({code})</span>
                <span class="trend {trend_class}">{trend}</span>
            </div>
            <div>
                <span class="price">¥{price:.2f}</span>
                <span class="change {change_class}">{change_sign}{change_pct:.2f}%</span>
            </div>
        </div>
        <div class="rps-summary">
            <div class="rps-item">
                <div class="rps-label">RPS5</div>
                <div class="rps-value" style="color: {'#2ed573' if r5 > 0 else '#ff4757'}">{r5:+.1f}%</div>
            </div>
            <div class="rps-item">
                <div class="rps-label">RPS10</div>
                <div class="rps-value" style="color: {'#2ed573' if r10 > 0 else '#ff4757'}">{r10:+.1f}%</div>
            </div>
            <div class="rps-item">
                <div class="rps-label">RPS20</div>
                <div class="rps-value" style="color: {'#2ed573' if r20 > 0 else '#ff4757'}">{r20:+.1f}%</div>
            </div>
            <div class="rps-item">
                <div class="rps-label">RPS50</div>
                <div class="rps-value" style="color: {'#2ed573' if r50 > 0 else '#ff4757'}">{r50:+.1f}%</div>
            </div>
            <div class="rps-item">
                <div class="rps-label">RPS120</div>
                <div class="rps-value" style="color: {'#2ed573' if r120 > 0 else '#ff4757'}">{r120:+.1f}%</div>
            </div>
        </div>
        <div class="chart-container">
            <div id="chart_{code}" style="height: 350px;"></div>
        </div>
    </div>
    
    <script>
        var trace1_{code} = {{
            x: {json.dumps(list(dates_for_rps))},
            y: {json.dumps(rps20_seq)},
            type: 'scatter',
            mode: 'lines',
            name: 'RPS20',
            line: {{color: '#ffa502', width: 2}},
            yaxis: 'y2'
        }};
        var trace2_{code} = {{
            x: {json.dumps(list(dates))},
            y: {json.dumps(list(close))},
            type: 'scatter',
            mode: 'lines+markers',
            name: '价格',
            line: {{color: '#00d4ff', width: 2}},
            marker: {{size: 4}},
            yaxis: 'y'
        }};
        var trace3_{code} = {{
            x: {json.dumps(list(dates))},
            y: {json.dumps(list(volume))},
            type: 'bar',
            name: '成交量',
            marker: {{color: 'rgba(0,212,255,0.3)', width: 1}},
            yaxis: 'y3'
        }};
        var data_{code} = [trace1_{code}, trace2_{code}, trace3_{code}];
        var layout_{code} = {{
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: {{color: '#eee', family: 'sans-serif'}},
            showlegend: true,
            legend: {{orientation: 'h', x: 0.5, xanchor: 'center', y: 1.12}},
            xaxis: {{
                showgrid: true,
                gridcolor: 'rgba(255,255,255,0.1)',
                type: 'date'
            }},
            yaxis: {{
                title: '价格',
                showgrid: true,
                gridcolor: 'rgba(255,255,255,0.1)',
                side: 'left',
                tickprefix: '¥'
            }},
            yaxis2: {{
                title: 'RPS20%',
                showgrid: false,
                side: 'right',
                overlaying: 'y',
                tickformat: '+.0f',
                ticksuffix: '%'
            }},
            yaxis3: {{
                title: '成交量',
                showgrid: false,
                side: 'right',
                overlaying: 'y',
                position: 0.85,
                showticklabels: false
            }},
            margin: {{t: 10, r: 120, b: 40, l: 60}},
            hovermode: 'x unified'
        }};
        Plotly.newPlot('chart_{code}', data_{code}, layout_{code}, {{responsive: true}});
    </script>
'''
    
    html += f'''
    <div class="updated">更新时间: {today} {datetime.now().strftime('%H:%M:%S')}</div>
</body>
</html>'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

def main():
    output_dir = os.path.expanduser('~/stock_analysis/data')
    os.makedirs(output_dir, exist_ok=True)
    
    all_data = {}
    print("📥 获取数据中...")
    
    for code, name in STOCKS.items():
        if code.startswith('6'):
            symbol = f'sh{code}'
        else:
            symbol = f'sz{code}'
        
        df = get_stock_hist_sina(symbol, days=150)
        if not df.empty:
            all_data[code] = {'name': name, 'df': df}
            print(f"  ✅ {name}: {len(df)} 条数据")
        else:
            print(f"  ⚠️ {name}: 获取失败")
    
    if not all_data:
        print("❌ 没有任何数据")
        return
    
    output_path = os.path.join(output_dir, 'rps_interactive.html')
    generate_html(all_data, output_path)
    print(f"\n✅ 交互式图表已保存: {output_path}")
    return output_path

if __name__ == "__main__":
    main()
