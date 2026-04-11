#!/usr/bin/env python3
"""
每日股票新闻简报
抓取三只重点股票的最新新闻
"""

import requests
import pandas as pd
from datetime import datetime
import sqlite3
import os
import time
from concurrent.futures import ThreadPoolExecutor
import json

DB_PATH = os.path.expanduser('~/stock_analysis/data/news.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新',
    '002049': '紫光国微'
}

def search_stock_news(code, name, max_results=5):
    """搜索单只股票新闻"""
    news_list = []
    
    try:
        # 东方财富个股新闻接口
        url = f'https://np-anotice-stock.eastmoney.com/api/security/ann'
        params = {
            'sr': '-1',
            'page_size': max_results,
            'page_index': 1,
            'ann_type': 'A,SH,SZ,BK',
            'client_source': 'web',
            'stock_list': code
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        
        if data.get('data'):
            for item in data['data'].get('list', [])[:max_results]:
                news_list.append({
                    'code': code,
                    'name': name,
                    'title': item.get('title', ''),
                    'publish_time': item.get('publish_time', ''),
                    'notice_type': item.get('notice_type', ''),
                    'source': '东方财富'
                })
    except Exception as e:
        print(f"  {name} 搜索失败: {e}")
    
    # 腾讯新闻搜索
    try:
        search_url = 'https://api.inews.qq.com/search/v2/news/search'
        search_params = {
            'keyword': name,
            'page': 1,
            'pagesize': max_results,
            'sort': 'time'
        }
        r = requests.get(search_url, params=search_params, timeout=10)
        data = r.json()
        
        if data.get('data'):
            for item in data['data'].get('list', [])[:max_results]:
                # 去重
                title = item.get('title', '')
                if title and not any(n['title'] == title for n in news_list):
                    news_list.append({
                        'code': code,
                        'name': name,
                        'title': title,
                        'publish_time': item.get('time', ''),
                        'notice_type': item.get('category', ''),
                        'source': item.get('source', '腾讯新闻')
                    })
    except Exception as e:
        print(f"  {name} 腾讯新闻失败: {e}")
    
    return news_list[:max_results]

def get_all_news():
    """获取所有股票新闻"""
    all_news = []
    
    print("📥 搜索股票新闻...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(search_stock_news, code, name): (code, name) 
                  for code, name in STOCKS.items()}
        
        for i, future in enumerate(futures):
            code, name = futures[future]
            try:
                news = future.result()
                all_news.extend(news)
                print(f"  {name}: {len(news)} 条新闻")
            except Exception as e:
                print(f"  {name}: 失败 {e}")
    
    return all_news

def format_time(timestamp):
    """格式化时间戳"""
    if not timestamp:
        return ''
    try:
        if isinstance(timestamp, int):
            dt = datetime.fromtimestamp(timestamp / 1000)
        else:
            dt = pd.to_datetime(timestamp)
        return dt.strftime('%m-%d %H:%M')
    except:
        return str(timestamp)[:16]

def save_news(news_list):
    """保存新闻到数据库"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS news (
            date TEXT,
            code TEXT,
            name TEXT,
            title TEXT,
            publish_time TEXT,
            notice_type TEXT,
            source TEXT,
            PRIMARY KEY (date, code, title)
        )
    ''')
    
    for news in news_list:
        try:
            c.execute('''
                INSERT OR REPLACE INTO news 
                (date, code, name, title, publish_time, notice_type, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (today, news['code'], news['name'], news['title'], 
                  news['publish_time'], news['notice_type'], news['source']))
        except:
            pass
    
    conn.commit()
    conn.close()

def generate_briefing(news_list):
    """生成新闻简报"""
    today = datetime.now().strftime('%Y年%m月%d日')
    weekday = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][datetime.now().weekday()]
    
    briefing = []
    briefing.append("=" * 60)
    briefing.append(f"📰 股票每日新闻简报")
    briefing.append(f"📅 {today} {weekday}")
    briefing.append("=" * 60)
    
    # 按股票分组
    for code, name in STOCKS.items():
        stock_news = [n for n in news_list if n['code'] == code]
        
        briefing.append(f"\n🔹 {name} ({code})")
        
        if not stock_news:
            briefing.append("   暂无最新新闻")
            continue
        
        for i, news in enumerate(stock_news[:5], 1):
            title = news['title'][:50] + '...' if len(news['title']) > 50 else news['title']
            source = news.get('source', '')
            notice_type = news.get('notice_type', '')
            
            briefing.append(f"   {i}. {title}")
            briefing.append(f"      ⏰ {format_time(news['publish_time'])} | 📌 {source} {notice_type}")
    
    briefing.append("\n" + "=" * 60)
    briefing.append("💡 数据来源：东方财富、腾讯新闻")
    briefing.append("=" * 60)
    
    return '\n'.join(briefing)

def main():
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} 开始获取新闻...")
    
    news_list = get_all_news()
    
    if not news_list:
        print("⚠️ 未获取到新闻，尝试备用方案...")
        # 备用：生成一条提示
        news_list = []
        for code, name in STOCKS.items():
            news_list.append({
                'code': code,
                'name': name,
                'title': f'今日暂无最新新闻公告',
                'publish_time': '',
                'notice_type': '',
                'source': '系统'
            })
    
    # 保存
    save_news(news_list)
    
    # 生成简报
    briefing = generate_briefing(news_list)
    print(briefing)
    
    # 保存简报
    report_path = os.path.expanduser('~/stock_analysis/data/daily_news.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(briefing)
    
    print(f"\n✅ 简报已保存: {report_path}")
    
    return briefing

if __name__ == "__main__":
    main()
