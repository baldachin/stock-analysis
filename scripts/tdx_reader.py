#!/usr/bin/env python3
"""
通达信日线数据读取模块
数据路径: ~/.local/share/tdxcfv/drive_c/tc/vipdoc/
"""

import struct
import os
import pandas as pd
from datetime import date
from typing import Optional, List, Tuple

# 通达信数据目录
TDX_PATH = os.path.expanduser('~/.local/share/tdxcfv/drive_c/tc/vipdoc')

def get_tdx_day_file(code: str) -> Optional[str]:
    """
    根据股票代码获取通达信日线文件路径
    code: 6位股票代码，如 '000001', '603986'
    """
    if code.startswith(('6', '5', '4', '8', '9')):
        # 沪市
        path = os.path.join(TDX_PATH, 'sh', 'lday', f'sh{code}.day')
    else:
        # 深市
        path = os.path.join(TDX_PATH, 'sz', 'lday', f'sz{code}.day')
    
    if os.path.exists(path):
        return path
    return None

def read_tdx_day(code: str, days: int = 300) -> pd.DataFrame:
    """
    读取通达信日线数据
    
    Args:
        code: 6位股票代码
        days: 最多返回的天数
    
    Returns:
        DataFrame with columns: day, open, high, low, close, volume, amount
    """
    filepath = get_tdx_day_file(code)
    if not filepath:
        return pd.DataFrame()
    
    try:
        filesize = os.path.getsize(filepath)
        record_count = filesize // 32
        
        records = []
        with open(filepath, 'rb') as f:
            for i in range(record_count):
                data = f.read(32)
                if len(data) < 32:
                    break
                
                # 解析32字节记录
                date_val = struct.unpack('<I', data[0:4])[0]
                year = date_val // 10000
                month = (date_val % 10000) // 100
                day = date_val % 100
                
                # 跳过无效日期
                if year < 1990 or year > 2100:
                    continue
                
                open_p = struct.unpack('<I', data[4:8])[0] / 100.0
                high_p = struct.unpack('<I', data[8:12])[0] / 100.0
                low_p = struct.unpack('<I', data[12:16])[0] / 100.0
                close_p = struct.unpack('<I', data[16:20])[0] / 100.0
                vol = struct.unpack('<I', data[20:24])[0]
                amount = struct.unpack('<I', data[24:28])[0]
                
                try:
                    trade_date = date(year, month, day)
                    records.append({
                        'day': pd.Timestamp(trade_date),
                        'open': open_p,
                        'high': high_p,
                        'low': low_p,
                        'close': close_p,
                        'volume': vol,
                        'amount': amount
                    })
                except:
                    continue
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df = df.sort_values('day').reset_index(drop=True)
        
        if days and len(df) > days:
            df = df.tail(days)
        
        return df
        
    except Exception as e:
        print(f"  读取 {code} 通达信数据失败: {e}")
        return pd.DataFrame()

def is_ashare_a(code: str, market: str) -> bool:
    """判断是否为A股"""
    if market == 'sh':
        # 沪市A股: 600xxx, 601xxx, 603xxx, 605xxx(主板) + 688xxx(科创板)
        return code.startswith(('600', '601', '603', '605', '688'))
    elif market == 'sz':
        # 深市A股: 000xxx, 001xxx(主板) + 002xxx(中小板) + 300xxx(创业板) + 301xxx(北交所)
        return code.startswith(('000', '001', '002', '300', '301'))
    return False

def list_available_stocks(filter_ashare: bool = True) -> dict:
    """
    列出所有可用的通达信日线数据
    filter_ashare=True 时只返回A股
    """
    available = {'sh': [], 'sz': []}
    
    sh_path = os.path.join(TDX_PATH, 'sh', 'lday')
    if os.path.exists(sh_path):
        for f in os.listdir(sh_path):
            if f.endswith('.day') and f.startswith('sh'):
                code = f[2:-4]
                if not filter_ashare or is_ashare_a(code, 'sh'):
                    available['sh'].append(code)
    
    sz_path = os.path.join(TDX_PATH, 'sz', 'lday')
    if os.path.exists(sz_path):
        for f in os.listdir(sz_path):
            if f.endswith('.day') and f.startswith('sz'):
                code = f[2:-4]
                if not filter_ashare or is_ashare_a(code, 'sz'):
                    available['sz'].append(code)
    
    return available

def list_all_ashare() -> List[Tuple[str, str]]:
    """返回所有A股代码列表 [(market, code), ...]"""
    ashare = []
    available = list_available_stocks(filter_ashare=True)
    for code in available['sh']:
        ashare.append(('sh', code))
    for code in available['sz']:
        ashare.append(('sz', code))
    return ashare

def get_stock_name_from_tdx(code: str) -> str:
    """从通达信股票列表获取名称"""
    NAME_MAP = {
        '000001': '平安银行', '000002': '万科A', '000063': '中兴通讯', '000066': '中国长城',
        '000100': 'TCL科技', '000333': '美的集团', '000338': '潍柴动力', '000651': '格力电器',
        '000661': '长春高新', '000858': '五粮液', '000876': '新希望', '000895': '双汇发展',
        '000938': '紫光股份', '002049': '紫光国微', '002230': '科大讯飞', '002236': '大华股份',
        '002252': '上海莱士', '002294': '信立泰', '002371': '北方华创', '002415': '海康威视',
        '002460': '赣锋锂业', '002475': '立讯精密', '002594': '比亚迪', '002602': '世纪华通',
        '002714': '牧原股份', '002736': '国信证券', '300001': '特锐德', '300003': '乐普医疗',
        '300014': '亿纬锂能', '300033': '同花顺', '300059': '东方财富', '300124': '汇川技术',
        '300142': '沃森生物', '300496': '中科创达', '300498': '温氏股份', '300529': '健帆生物',
        '300750': '宁德时代', '300760': '迈瑞医疗', '600000': '浦发银行', '600009': '上海机场',
        '600016': '民生银行', '600019': '宝钢股份', '600028': '中国石化', '600030': '中信证券',
        '600031': '三一重工', '600036': '招商银行', '600048': '保利发展', '600050': '中国联通',
        '600104': '上汽集团', '600111': '北方稀土', '600150': '中国船舶', '600176': '中国巨石',
        '600183': '生益科技', '600276': '恒瑞医药', '600309': '万华化学', '600519': '贵州茅台',
        '600585': '海螺水泥', '600588': '用友网络', '600690': '海尔智家', '600745': '闻泰科技',
        '600760': '中航沈飞', '600837': '海通证券', '600887': '伊利股份', '600893': '航发动力',
        '600900': '长江电力', '600905': '三峡能源', '600918': '中泰证券', '600926': '杭州银行',
        '601006': '大秦铁路', '601012': '隆基绿能', '601066': '中信建投', '601088': '中国神华',
        '601118': '海南橡胶', '601138': '工业富联', '601166': '兴业银行', '601211': '国泰君安',
        '601225': '陕西煤业', '601236': '红塔证券', '601288': '农业银行', '601318': '中国平安',
        '601328': '交通银行', '601336': '新华保险', '601390': '中国中铁', '601398': '工商银行',
        '601601': '中国太保', '601628': '中国人寿', '601658': '邮储银行', '601688': '华泰证券',
        '601728': '中国电信', '601766': '中国中车', '601800': '中国交建', '601816': '京沪高铁',
        '601818': '光大银行', '601857': '中国石油', '601888': '中国中免', '601899': '紫金矿业',
        '601939': '建设银行', '601985': '中国核电', '601988': '中国银行', '601989': '中国重工',
        '601995': '中金公司', '603259': '药明康德', '603501': '韦尔股份', '603986': '兆易创新',
        '688008': '澜起科技', '688012': '中微公司', '688036': '传音控股', '688111': '金山办公',
        '688126': '沪硅产业', '688180': '君实生物', '688234': '天岳先进', '688363': '华熙生物',
        '688599': '正泰电器',
    }
    return NAME_MAP.get(code, code)

def calculate_returns_from_df(df: pd.DataFrame, periods: list = [5, 10, 20, 50, 120, 250]) -> dict:
    """从DataFrame计算收益率"""
    if df.empty or len(df) < max(periods) + 1:
        return {}
    
    close = df['close'].values
    result = {'date': df['day'].iloc[-1].strftime('%Y-%m-%d')}
    
    for period in periods:
        if len(close) >= period + 1:
            ret = (close[-1] - close[-period-1]) / close[-period-1] * 100
            result[f'ret_{period}'] = ret
        else:
            result[f'ret_{period}'] = None
    
    return result

def get_tdx_data_for_rps(code: str, days: int = 300) -> Optional[dict]:
    """获取股票数据用于RPS计算"""
    df = read_tdx_day(code, days)
    if df.empty:
        return None
    
    result = calculate_returns_from_df(df)
    result['code'] = code
    result['name'] = get_stock_name_from_tdx(code)
    result['price'] = df['close'].iloc[-1]
    
    return result

# 测试
if __name__ == '__main__':
    print("📂 通达信数据读取测试\n")
    
    # 列出可用股票
    available = list_available_stocks(filter_ashare=True)
    ashare_count = len(available['sh']) + len(available['sz'])
    print(f"📊 A股数量: 沪市 {len(available['sh'])} 只, 深市 {len(available['sz'])} 只, 共 {ashare_count} 只")
    
    # 测试读取
    test_codes = ['000001', '603986', '300496', '002049']
    
    for code in test_codes:
        df = read_tdx_day(code, days=5)
        if not df.empty:
            print(f"\n✅ {code} {get_stock_name_from_tdx(code)}")
            print(f"   最新: {df['day'].iloc[-1].strftime('%Y-%m-%d')} 收盘 {df['close'].iloc[-1]:.2f}")
            print(f"   数据条数: {len(df)}")
        else:
            print(f"\n❌ {code} - 无数据")