#!/usr/bin/env python3
"""
沪深300 RPS 排名计算
覆盖沪深300成分股 + 目标股票
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = os.path.expanduser('~/stock_analysis/data/full_market_rps.db')

STOCKS = {
    '300496': '中科创达',
    '603986': '兆易创新', 
    '002049': '紫光国微'
}

# 沪深300成分股（扩充版，约300只）
HS300_CODES = [
    # 沪市主板
    '600000','600016','600019','600028','600030','600031','600036','600048','600050','600104',
    '600109','600111','600115','600118','600150','600160','600170','600176','600183','600188','600196',
    '600197','600199','600209','600210','600219','600221','600233','600271','600276','600297',
    '600309','600315','600346','600352','600362','600383','600406','600426','600436','600438',
    '600456','600487','600489','600495','600519','600547','600570','600585','600588','600606',
    '600637','600655','600660','600690','600703','600745','600760','600795','600809','600837',
    '600850','600867','600887','600893','600900','600905','600918','600926','600941','600989',
    '600999','601006','601012','601066','601088','601118','601138','601166','601169','601186',
    '601211','601225','601236','601288','601318','601328','601336','601390','601398','601601',
    '601628','601658','601688','601698','601728','601766','601800','601816','601818','601857',
    '601888','601899','601919','601939','601985','601988','601989','601995','601998','603259',
    '603288','603501','603799','603806','603833','603858','603899',
    # 深市主板
    '000001','000002','000063','000066','000100','000153','000166','000301','000333','000338',
    '000400','000401','000402','000425','000488','000501','000538','000568','000596','000651',
    '000661','000708','000709','000725','000768','000778','000786','000858','000876','000895',
    '000938','000963','000977','000983','001965','001979','002001','002007','002008','002027',
    '002032','002042','002049','002056','002061','002092','002120','002127','002128','002138',
    '002142','002152','002153','002157','002176','002179','002180','002186','002191','002194',
    '002236','002244','002252','002270','002271','002301','002304','002311','002352','002371',
    '002385','002410','002415','002422','002428','002430','002460','002475','002493','002501',
    '002510','002530','002563','002594','002601','002602','002607','002624','002673','002714',
    '002736','002800','002812','002831','002841','002878','002920','002925','002926','002936',
    '002939','002945','002955','002965',
    # 创业板
    '300001','300002','300003','300010','300014','300015','300033','300059','300068','300070',
    '300073','300088','300101','300122','300124','300142','300144','300207','300223','300274',
    '300316','300363','300395','300408','300450','300496','300498','300529','300601','300661',
    '300676','300750','300760','300759','300896','300901','300998','300999','301069',
    # 科创板
    '688001','688008','688012','688036','688041','688050','688111','688122','688126','688139',
    '688180','688185','688190','688200','688223','688234','688256','688301','688363','688366',
    '688395','688408','688499','688521','688561','688599','688981'
]

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 删除旧表重新创建
    c.execute('DROP TABLE IF EXISTS market_returns')
    c.execute('''
        CREATE TABLE market_returns (
            date TEXT,
            code TEXT,
            name TEXT,
            price REAL,
            ret_5 REAL, ret_10 REAL, ret_20 REAL, ret_50 REAL, ret_120 REAL, ret_250 REAL,
            PRIMARY KEY (date, code)
        )
    ''')
    
    for period in [5, 10, 20, 50, 120, 250]:
        c.execute(f'DROP TABLE IF EXISTS rank_{period}')
        c.execute(f'''
            CREATE TABLE rank_{period} (
                date TEXT,
                code TEXT,
                name TEXT,
                price REAL,
                ret REAL,
                rps REAL,
                PRIMARY KEY (date, code)
            )
        ''')
    
    conn.commit()
    conn.close()

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

def calculate_returns(code):
    """计算单只股票的各周期收益率"""
    if code.startswith(('6','5','4','8','9')):
        symbol = f'sh{code}'
    else:
        symbol = f'sz{code}'
    
    df = get_stock_hist_sina(symbol, days=300)
    if df.empty or len(df) < 130:
        return None
    
    close = df['close'].values
    name = get_stock_name_tx(symbol)
    price = close[-1]
    today = df['day'].iloc[-1].strftime('%Y-%m-%d')
    
    result = {
        'date': today,
        'code': code,
        'name': name,
        'price': price
    }
    
    for period in [5, 10, 20, 50, 120, 250]:
        if len(close) >= period + 5:
            ret = (close[-1] - close[-period-1]) / close[-period-1] * 100
            result[f'ret_{period}'] = ret
        else:
            result[f'ret_{period}'] = np.nan
    
    return result

def calculate_all_ranks(data, period):
    ret_col = f'ret_{period}'
    valid = [r for r in data if r and not np.isnan(r.get(ret_col, np.nan))]
    if not valid: return {}
    
    returns = [(r['code'], r['name'], r['price'], r[ret_col]) for r in valid]
    returns.sort(key=lambda x: x[3], reverse=True)
    total = len(returns)
    
    ranks = {}
    for i, (code, name, price, ret) in enumerate(returns):
        rps = ((total - i) / total) * 100
        ranks[code] = {'code': code, 'name': name, 'price': price, 'ret': ret, 'rps': round(rps, 2)}
    
    return ranks

def main():
    init_db()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 去重
    hs300_set = set(HS300_CODES)
    for code in STOCKS:
        hs300_set.add(code)
    
    all_codes = list(hs300_set)
    print(f"📥 开始获取 {len(all_codes)} 只股票数据...")
    
    all_data = []
    done = 0
    errors = 0
    
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(calculate_returns, code): code for code in all_codes}
        
        for future in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"   进度: {done}/{len(all_codes)}")
            
            try:
                result = future.result()
                if result:
                    all_data.append(result)
                else:
                    errors += 1
            except Exception as e:
                errors += 1
    
    print(f"\n✅ 有效数据: {len(all_data)} 只 | 失败: {errors} 只")
    
    if not all_data:
        print("❌ 无有效数据")
        return
    
    # 保存
    conn = sqlite3.connect(DB_PATH)
    df_all = pd.DataFrame(all_data)
    # 先清空今日数据
    conn.execute(f'DELETE FROM market_returns')
    df_all.to_sql('market_returns', conn, if_exists='replace', index=False)
    
    # 计算排名
    print("\n📐 计算RPS排名...")
    for period in [5, 10, 20, 50, 120, 250]:
        ranks = calculate_all_ranks(all_data, period)
        if ranks:
            df_rank = pd.DataFrame(list(ranks.values()))
            conn.execute(f'DELETE FROM rank_{period}')
            df_rank.to_sql(f'rank_{period}', conn, if_exists='replace', index=False)
        print(f"   RPS{period}: 完成 ({len(ranks)} 只)")
    
    conn.close()
    
    # 输出
    total = len(all_data)
    print("\n" + "="*80)
    print("📊 沪深300 RPS排名报告")
    print("="*80)
    print(f"📅 {today} | 统计股票: {total} 只")
    print("="*80)
    
    for code, name in STOCKS.items():
        conn = sqlite3.connect(DB_PATH)
        result = conn.execute(f'SELECT price FROM market_returns WHERE code = "{code}"').fetchone()
        price = result[0] if result else 0
        
        r20 = conn.execute(f'SELECT rps, ret FROM rank_20 WHERE code = "{code}"').fetchone()
        r50 = conn.execute(f'SELECT rps, ret FROM rank_50 WHERE code = "{code}"').fetchone()
        r120 = conn.execute(f'SELECT rps, ret FROM rank_120 WHERE code = "{code}"').fetchone()
        r250 = conn.execute(f'SELECT rps, ret FROM rank_250 WHERE code = "{code}"').fetchone()
        conn.close()
        
        print(f"\n🔹 {name} ({code})")
        print(f"   💰 价格: {price:.2f}")
        print(f"   📊 全市场RPS排名 (基于{total}只):")
        
        for pname, data in [('RPS5', None), ('RPS10', None), ('RPS20', r20), ('RPS50', r50), ('RPS120', r120), ('RPS250', r250)]:
            if data is None:
                # 计算
                conn = sqlite3.connect(DB_PATH)
                period = int(pname[3:])
                res = conn.execute(f'SELECT rps, ret FROM rank_{period} WHERE code = "{code}"').fetchone()
                conn.close()
                data = res
            if data:
                rps, ret = data
                emoji = "🟢" if rps > 50 else "🔴"
                bar = "█" * int(rps/10) + "░" * (10 - int(rps/10))
                print(f"      {emoji} {pname:5s}: {rps:5.1f}% | {bar} | {ret:+7.2f}%")
        
        # 趋势
        if r20 and r50 and r120:
            r20_v, r50_v, r120_v = r20[0], r50[0], r120[0]
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
    
    print("\n" + "="*80)
    print(f"💡 基于 {total} 只股票统计 | RPS = 打败市场上X%的股票")
    print("="*80)

if __name__ == "__main__":
    main()
