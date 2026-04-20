#!/usr/bin/env python3
"""
通达信在线数据更新脚本
使用 pytdx 库直连接通达信服务器，获取日线数据并写入本地 .day 文件

用法:
    python3 tdx_online_update.py              # 更新所有股票
    python3 tdx_online_update.py --days 30   # 只获取最近30天
    python3 tdx_online_update.py 603986 300496  # 更新指定股票
"""

import struct
import os
import sys
import argparse
from datetime import datetime
from pytdx.hq import TdxHq_API

# 通达信数据目录
TDX_PATH = os.path.expanduser('~/.local/share/tdxcfv/drive_c/tc/vipdoc')

# 服务器列表
TDX_SERVERS = [
    ('110.41.147.114', 7709),
    ('110.41.2.72', 7709),
    ('110.41.4.4', 7709),
    ('124.70.176.52', 7709),
    ('122.51.120.217', 7709),
]

def get_market_id(code):
    """根据股票代码判断市场ID"""
    if code.startswith(('6', '5', '4', '8', '9')):
        return 1  # 沪市
    else:
        return 0  # 深市

def is_ashare(code):
    """判断是否为A股"""
    if code.startswith(('600', '601', '603', '605', '688')):
        return True
    elif code.startswith(('000', '001', '002', '300', '301')):
        return True
    return False

def get_all_local_codes():
    """获取本地所有股票代码"""
    codes = {'sh': set(), 'sz': set()}
    
    sh_path = os.path.join(TDX_PATH, 'sh', 'lday')
    sz_path = os.path.join(TDX_PATH, 'sz', 'lday')
    
    if os.path.exists(sh_path):
        for f in os.listdir(sh_path):
            if f.endswith('.day') and f.startswith('sh'):
                codes['sh'].add(f[2:-4])
    
    if os.path.exists(sz_path):
        for f in os.listdir(sz_path):
            if f.endswith('.day') and f.startswith('sz'):
                codes['sz'].add(f[2:-4])
    
    return codes

def write_day_file(market, code, data_list):
    """将数据写入本地 .day 文件"""
    if market == 'sh':
        path = os.path.join(TDX_PATH, 'sh', 'lday', f'sh{code}.day')
    else:
        path = os.path.join(TDX_PATH, 'sz', 'lday', f'sz{code}.day')
    
    # 读取已有数据
    existing = {}
    if os.path.exists(path):
        try:
            with open(path, 'rb') as f:
                while True:
                    d = f.read(32)
                    if len(d) < 32:
                        break
                    date_val = struct.unpack('<I', d[0:4])[0]
                    existing[date_val] = d
        except:
            pass
    
    # 添加新数据
    for bar in data_list:
        date_int = bar['year'] * 10000 + bar['month'] * 100 + bar['day']
        
        record = struct.pack('<I', date_int)
        record += struct.pack('<I', int(bar['open'] * 100))
        record += struct.pack('<I', int(bar['high'] * 100))
        record += struct.pack('<I', int(bar['low'] * 100))
        record += struct.pack('<I', int(bar['close'] * 100))
        record += struct.pack('<I', int(bar['vol']))
        record += struct.pack('<I', min(int(bar.get('amount', 0)), 0xFFFFFFFF))
        record += struct.pack('<I', 0)
        
        existing[date_int] = record
    
    # 按日期排序并写入
    sorted_dates = sorted(existing.keys())
    with open(path, 'wb') as f:
        for date_int in sorted_dates:
            f.write(existing[date_int])
    
    return len(sorted_dates)

def update_stock(api, market, code, days):
    """更新单只股票"""
    market_id = get_market_id(code)
    try:
        data = api.get_security_bars(category=9, market=market_id, code=code, start=0, count=days)
        if data:
            count = write_day_file(market, code, data)
            return count, None
    except Exception as e:
        return 0, str(e)
    return 0, None

def main():
    parser = argparse.ArgumentParser(description='通达信在线数据更新')
    parser.add_argument('--days', type=int, default=300, help='获取历史天数 (默认300)')
    parser.add_argument('codes', nargs='*', help='指定股票代码 (不指定则更新全部)')
    args = parser.parse_args()
    
    print(f"📡 通达信在线数据更新")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 获取历史: {args.days} 天")
    print()
    
    # 连接
    api = TdxHq_API()
    connected = False
    for host, port in TDX_SERVERS:
        try:
            api.connect(host, port)
            print(f"  ✅ 已连接 {host}:{port}")
            connected = True
            break
        except:
            continue
    
    if not connected:
        print("  ❌ 无法连接到通达信服务器")
        return
    
    # 确定要更新的股票
    if args.codes:
        targets = []
        for code in args.codes:
            market = 'sh' if code.startswith(('6', '5', '4', '8', '9')) else 'sz'
            targets.append((market, code))
        print(f"  📝 更新指定股票: {[c for _, c in targets]}")
    else:
        all_codes = get_all_local_codes()
        targets = [(m, c) for m, cs in all_codes.items() for c in cs if is_ashare(c)]
        print(f"  📝 更新全部A股: {len(targets)} 只")
    
    print()
    
    updated = 0
    failed = 0
    errors = []
    
    for i, (market, code) in enumerate(targets):
        if (i + 1) % 100 == 0:
            print(f"  进度: {i+1}/{len(targets)}")
        
        count, err = update_stock(api, market, code, args.days)
        if count > 0:
            updated += 1
        else:
            failed += 1
            if err and len(errors) < 5:
                errors.append(f"    {code}: {err}")
    
    api.disconnect()
    
    print()
    print(f"✅ 更新完成!")
    print(f"   成功: {updated} 只")
    print(f"   失败: {failed} 只")
    
    if errors:
        print("\n部分错误:")
        for e in errors:
            print(e)

if __name__ == "__main__":
    main()
