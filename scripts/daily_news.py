#!/usr/bin/env python3
"""
每日股票新闻简报
抓取三只重点股票 + 相关行业市场新闻
"""

import requests
import pandas as pd
from datetime import datetime
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor

DB_PATH = os.path.expanduser('~/stock_analysis/data/news.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新',
    '002049': '紫光国微'
}

# 相关行业关键词（优先匹配）
INDUSTRY_KEYWORDS_PRIMARY = [
    '半导体', '芯片', '集成电路', '晶圆', '封测', 'MCU', 'Flash',
    'AI芯片', 'GPU', 'HBM', 'RISC-V', '光刻机', '国产替代'
]

INDUSTRY_KEYWORDS_SECONDARY = [
    '人工智能', '大模型', '智能驾驶', '自动驾驶', '车规级',
    '物联网', 'IoT', '智能终端', '边缘计算', '机器人'
]

def search_stock_news(code, name, max_results=5):
    """搜索单只股票公告"""
    news_list = []
    
    try:
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
        print(f"  {name} 公告失败: {e}")
    
    return news_list[:max_results]

def get_industry_news(max_results=8):
    """获取相关行业新闻（同花顺）"""
    news_list = []
    
    try:
        url = 'https://news.10jqka.com.cn/tapp/news/push/stock/'
        params = {
            'page': 1,
            'tag': '',
            'track': 'website',
            'pagesize': 50
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://news.10jqka.com.cn/'
        }
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()
        
        if data.get('data', {}).get('list'):
            for item in data['data']['list']:
                title = item.get('title', '') or item.get('digest', '')
                if not title:
                    continue
                
                news_list.append({
                    'title': title,
                    'time': item.get('ctime', ''),
                    'source': item.get('source', '同花顺'),
                    'url': item.get('url', '')
                })
    except Exception as e:
        print(f"  行业新闻获取失败: {e}")
    
    # 分类：优先一级关键词
    primary = [n for n in news_list if any(kw in n['title'] for kw in INDUSTRY_KEYWORDS_PRIMARY)]
    secondary = [n for n in news_list if n not in primary and any(kw in n['title'] for kw in INDUSTRY_KEYWORDS_SECONDARY)]
    other = [n for n in news_list if n not in primary and n not in secondary]
    
    # 优先返回行业相关新闻
    return (primary + secondary + other)[:max_results]

def get_market_news():
    """获取市场要闻"""
    news_list = []
    
    try:
        # 东方财富 市场快讯
        url = 'https://push2.eastmoney.com/api/qt/clist/get'
        params = {
            'pn': 1,
            'pz': 20,
            'po': 1,
            'np': 1,
            'fltt': 2,
            'invt': 2,
            'fid': 'f3',
            'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',
            'fields': 'f2,f3,f12,f14,f15,f16',
        }
        # 这个接口返回行情数据，不是新闻，换一个
        
    except:
        pass
    
    return news_list

def get_all_news():
    """获取所有股票新闻"""
    all_news = []
    
    print("📥 搜索个股公告...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(search_stock_news, code, name): (code, name) 
                  for code, name in STOCKS.items()}
        
        for future in futures:
            code, name = futures[future]
            try:
                news = future.result()
                all_news.extend(news)
                print(f"  {name}: {len(news)} 条")
            except Exception as e:
                print(f"  {name}: 失败")
    
    return all_news

def format_time(timestamp):
    """格式化时间"""
    if not timestamp:
        return ''
    try:
        if isinstance(timestamp, int):
            dt = datetime.fromtimestamp(timestamp / 1000)
        elif len(str(timestamp)) == 10:
            dt = datetime.fromtimestamp(int(timestamp))
        else:
            dt = pd.to_datetime(timestamp)
        return dt.strftime('%m-%d %H:%M')
    except:
        try:
            return str(timestamp)[:16]
        except:
            return ''

def save_news(news_list, industry_news):
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
            category TEXT,
            PRIMARY KEY (date, code, title)
        )
    ''')
    
    for news in news_list:
        try:
            c.execute('''
                INSERT OR REPLACE INTO news 
                (date, code, name, title, publish_time, notice_type, source, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, news['code'], news['name'], news['title'], 
                  news['publish_time'], news['notice_type'], news['source'], '个股公告'))
        except:
            pass
    
    for news in industry_news:
        try:
            c.execute('''
                INSERT OR REPLACE INTO news 
                (date, code, name, title, publish_time, notice_type, source, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, '', '', news['title'], news.get('time', ''), '', news['source'], '行业要闻'))
        except:
            pass
    
    conn.commit()
    conn.close()

def generate_briefing(news_list, industry_news):
    """生成新闻简报"""
    today = datetime.now().strftime('%Y年%m月%d日')
    weekday = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][datetime.now().weekday()]
    
    briefing = []
    briefing.append("=" * 60)
    briefing.append("📰 每日股票新闻简报")
    briefing.append(f"📅 {today} {weekday} | 🕘 09:30")
    briefing.append("=" * 60)
    
    # ===== 行业市场要闻 =====
    if industry_news:
        briefing.append("\n🌐 【行业市场要闻】")
        briefing.append("-" * 40)
        
        for i, news in enumerate(industry_news[:6], 1):
            title = news['title'][:48] + '...' if len(news['title']) > 48 else news['title']
            source = news.get('source', '同花顺')
            briefing.append(f"  {i}. {title}")
            briefing.append(f"     📌 {source}")
    
    # ===== 个股公告 =====
    briefing.append("\n" + "=" * 60)
    briefing.append("📋 【个股公告】")
    briefing.append("-" * 40)
    
    for code, name in STOCKS.items():
        stock_news = [n for n in news_list if n['code'] == code]
        
        briefing.append(f"\n  🔹 {name} ({code})")
        
        if not stock_news:
            briefing.append("     暂无最新公告")
            continue
        
        for i, news in enumerate(stock_news[:3], 1):
            title = news['title'][:42] + '...' if len(news['title']) > 42 else news['title']
            briefing.append(f"     {i}. {title}")
    
    briefing.append("\n" + "=" * 60)
    briefing.append("💡 数据来源：东方财富、同花顺 | ⏰ " + datetime.now().strftime('%H:%M'))
    briefing.append("=" * 60)
    
    return '\n'.join(briefing)

def main():
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')} 开始获取新闻...")
    
    # 获取个股公告
    news_list = get_all_news()
    
    # 获取行业新闻
    print("📥 搜索行业新闻...")
    industry_news = get_industry_news()
    print(f"  行业新闻: {len(industry_news)} 条")
    
    if not news_list and not industry_news:
        print("⚠️ 未获取到任何新闻")
        return
    
    # 保存
    save_news(news_list, industry_news)
    
    # 生成简报
    briefing = generate_briefing(news_list, industry_news)
    print(briefing)
    
    # 保存简报
    report_path = os.path.expanduser('~/stock_analysis/data/daily_news.txt')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(briefing)
    
    print(f"\n✅ 简报已保存: {report_path}")
    
    return briefing

if __name__ == "__main__":
    main()
