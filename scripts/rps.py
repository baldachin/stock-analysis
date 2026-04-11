#!/usr/bin/env python3
"""
RPS (Relative Performance Strength) 计算脚本
使用腾讯/新浪财经API获取数据
用法: python3 rps.py [股票代码列表] [N日]
例: python3 rps.py 603986,300496 20
"""

import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============== API 函数 ==============

def get_stock_hist_sina(symbol: str, days: int = 250) -> pd.DataFrame:
    """通过新浪财经获取历史K线"""
    try:
        # symbol: sh603986 或 sz000001
        url = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
        params = {
            'symbol': symbol,
            'scale': '240',  # 日K
            'ma': '5',
            'datalen': days
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        
        if not data or len(data) == 0:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['day'] = pd.to_datetime(df['day'])
        df = df.sort_values('day')
        
        # 转换数值列
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        
        return df
    except Exception as e:
        print(f"  ⚠ Sina API error for {symbol}: {e}")
        return pd.DataFrame()

def get_stock_realtime_tx(symbol: str) -> dict:
    """通过腾讯API获取实时行情"""
    try:
        # symbol: sh603986
        url = f'https://qt.gtimg.cn/q={symbol}'
        r = requests.get(url, timeout=5)
        text = r.text
        
        # 解析 v_sh603986="1~..."
        if '~' not in text:
            return {}
        
        parts = text.split('~')
        return {
            'name': parts[1] if len(parts) > 1 else '',
            'code': parts[2] if len(parts) > 2 else '',
            'price': float(parts[3]) if len(parts) > 3 and parts[3] else 0,
            'yesterday_close': float(parts[4]) if len(parts) > 4 and parts[4] else 0,
            'open': float(parts[5]) if len(parts) > 5 and parts[5] else 0,
            'volume': int(parts[6]) if len(parts) > 6 and parts[6] else 0,
            'b1_price': float(parts[9]) if len(parts) > 9 and parts[9] else 0,
            'date': parts[30] if len(parts) > 30 else '',
            'time': parts[31] if len(parts) > 31 else '',
        }
    except:
        return {}

def get_stock_name_sina(symbol: str) -> str:
    """获取股票名称"""
    info = get_stock_realtime_tx(symbol)
    return info.get('name', symbol)

# ============== RPS 计算 ==============

def calculate_returns(df: pd.DataFrame, periods: list = [20, 50, 120, 250]) -> dict:
    """计算各周期收益率"""
    if df.empty or len(df) < 250:
        return {}
    
    close = df['close'].values
    results = {}
    
    for period in periods:
        if len(close) >= period + 1:
            ret = (close[-1] - close[-period-1]) / close[-period-1] * 100
            results[f'rps_{period}'] = ret
    
    return results

def get_market_benchmark(days: int = 250) -> list:
    """获取市场基准收益率（日经/沪深300指数）"""
    # 使用沪深300指数作为基准
    index_data = get_stock_hist_sina('sh000300', days=days)
    if index_data.empty:
        return []
    close = index_data['close'].values
    returns = []
    for i in range(20, len(close)):
        ret = (close[i] - close[i-20]) / close[i-20] * 100
        returns.append(ret)
    return returns

# ============== 主程序 ==============

def analyze_stock(stock_code: str, periods: list = [20, 50, 120, 250]) -> dict:
    """分析单只股票"""
    # 自动添加交易所前缀
    if stock_code.startswith('6'):
        symbol = f'sh{stock_code}'
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        symbol = f'sz{stock_code}'
    else:
        symbol = stock_code
    
    # 获取实时数据
    realtime = get_stock_realtime_tx(symbol)
    name = realtime.get('name', stock_code)
    current_price = realtime.get('price', 0)
    
    # 获取历史数据
    df = get_stock_hist_sina(symbol, days=400)
    
    result = {
        'code': stock_code,
        'name': name,
        'price': current_price,
        'date': df['day'].iloc[-1].strftime('%Y-%m-%d') if not df.empty else 'N/A',
        'data_ok': not df.empty
    }
    
    if df.empty:
        return result
    
    # 计算各周期RPS
    returns = calculate_returns(df, periods)
    result.update(returns)
    
    return result

def print_rps_result(stocks_data: list):
    """打印RPS结果"""
    print(f"\n{'='*70}")
    print(f"📊 A股 RPS 分析报告")
    print(f"{'='*70}")
    print(f"📅 分析日期: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")
    
    for data in stocks_data:
        if not data.get('data_ok', False):
            print(f"\n⚠️  {data['code']} - 数据获取失败")
            continue
        
        print(f"\n📈 {data['name']} ({data['code']})")
        print(f"   💰 最新价: {data['price']:.2f}")
        print(f"   📅 数据日期: {data['date']}")
        print(f"   📊 各周期涨幅:")
        
        for period in [20, 50, 120, 250]:
            key = f'rps_{period}'
            if key in data:
                pct = data[key]
                emoji = "🟢" if pct > 0 else "🔴"
                sign = "+" if pct > 0 else ""
                # 进度条
                bar_len = min(int(abs(pct) / 3), 25)
                if pct >= 0:
                    bar = "█" * bar_len + "░" * (25 - bar_len)
                else:
                    bar = "▓" * bar_len + "░" * (25 - bar_len)
                print(f"      {emoji} {period:3d}日: {sign}{pct:7.2f}% |{bar}|")
        
        # 综合评分
        rps_20 = data.get('rps_20', 0)
        rps_50 = data.get('rps_50', 0)
        rps_120 = data.get('rps_120', 0)
        
        # 简单评分：近期强于中期强于长期
        if rps_20 > 0 and rps_50 > 0:
            trend = "📈 强势上涨"
        elif rps_20 > 0 and rps_50 < 0:
            trend = "📊 反弹回暖"
        elif rps_20 < 0 and rps_20 > rps_50:
            trend = "📉 趋势转弱"
        else:
            trend = "🔻 持续调整"
        
        print(f"   {trend}")
    
    print(f"\n{'='*70}")
    print(f"✅ 分析完成")
    print(f"{'='*70}\n")

def main():
    # 默认分析
    stocks = ['603986', '300496']
    periods = [20, 50, 120, 250]
    
    if len(sys.argv) > 1:
        stocks = sys.argv[1].split(',')
    if len(sys.argv) > 2:
        periods = [int(sys.argv[2])]
    
    print(f"\n🔍 正在获取 {len(stocks)} 只股票数据...")
    
    # 并行获取
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(analyze_stock, code, periods): code for code in stocks}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                code = futures[future]
                results.append({'code': code, 'data_ok': False})
    
    # 打印结果
    print_rps_result(results)

if __name__ == "__main__":
    main()
