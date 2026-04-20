#!/usr/bin/env python3
"""
run_backtests.py - 策略回测示例
在真实数据上运行多个策略并对比绩效
"""
import sys
sys.path.insert(0, "/home/stock_analysis/quant")

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from data_loader import DataLoader
from backtest import BacktestEngine
from analysis import analyze, print_analysis
from strategies.rps_strategy import rps_strategy
from strategies.ma_crossover import ma_cross_strategy
from strategies.mean_reversion import bollinger_strategy
from strategies.momentum_breakout import momentum_breakout_strategy


def load_sample_data(days: int = 500, n_stocks: int = 50) -> pd.DataFrame:
    """
    加载样本数据：随机选取 n_stocks 只股票最近 days 个交易日的数据
    实盘/研究时替换为全市场数据
    """
    loader = DataLoader()

    # 读取所有A股列表
    names = loader.get_stock_names()
    all_codes = names["code"].tolist()

    # 随机选 n_stocks 只（固定seed保证可复现）
    np.random.seed(42)
    sample_codes = np.random.choice(all_codes, size=min(n_stocks, len(all_codes)), replace=False).tolist()

    # 读取近 days 个交易日
    end_date = "2026-04-17"
    start_date = (pd.Timestamp(end_date) - pd.Timedelta(days=days * 2)).strftime("%Y-%m-%d")

    df = loader.get_bars(codes=sample_codes, start_date=start_date, end_date=end_date)

    # 计算所有技术指标（策略需要什么就加什么）
    df = loader.add_returns(df, windows=[1, 5, 20])
    df = loader.add_ma(df, windows=[5, 20, 60])
    df = loader.add_bollinger(df, window=20)
    df = loader.add_breakout(df, lookback=20)
    df = loader.add_volume_ma(df, window=20)
    df = loader.add_rsi(df, window=14)

    # 按日期过滤（确保有足够均线数据）
    df = df[df.index >= (pd.Timestamp(end_date) - pd.Timedelta(days=days)).strftime("%Y-%m-%d")]

    print(f"✅ 数据加载完成：{len(df)} 条记录，{df['code'].nunique()} 只股票，"
          f"日期范围 {df.index.min().date()} ~ {df.index.max().date()}")
    return df


def run_all_strategies(data: pd.DataFrame, start_cash: float = 500_000):
    """运行所有策略并输出对比报告"""

    results = {}

    # ── 策略1：RPS 强度策略 ────────────────────────────────────
    print("\n" + "▶" * 20)
    print("策略1：RPS 强度选股策略")
    print("▶" * 20)

    engine1 = BacktestEngine(initial_cash=start_cash, commission_rate=0.0003)
    engine1.load_data(data)
    engine1.add_strategy(rps_strategy)
    result1 = engine1.run()
    results["RPS强度策略"] = analyze(result1.equity_curve, result1.trades)

    # ── 策略2：均线金叉死叉 ───────────────────────────────────
    print("\n" + "▶" * 20)
    print("策略2：均线金叉死叉 (MA5/MA20)")
    print("▶" * 20)

    engine2 = BacktestEngine(initial_cash=start_cash, commission_rate=0.0003)
    engine2.load_data(data)
    engine2.add_strategy(ma_cross_strategy)
    result2 = engine2.run()
    results["均线金叉死叉"] = analyze(result2.equity_curve, result2.trades)

    # ── 策略3：布林带均值回归 ─────────────────────────────────
    print("\n" + "▶" * 20)
    print("策略3：布林带均值回归")
    print("▶" * 20)

    engine3 = BacktestEngine(initial_cash=start_cash, commission_rate=0.0003)
    engine3.load_data(data)
    engine3.add_strategy(bollinger_strategy)
    result3 = engine3.run()
    results["布林带均值回归"] = analyze(result3.equity_curve, result3.trades)

    # ── 策略4：动量突破 ───────────────────────────────────────
    print("\n" + "▶" * 20)
    print("策略4：动量突破策略")
    print("▶" * 20)

    engine4 = BacktestEngine(initial_cash=start_cash, commission_rate=0.0003)
    engine4.load_data(data)
    engine4.add_strategy(momentum_breakout_strategy)
    result4 = engine4.run()
    results["动量突破"] = analyze(result4.equity_curve, result4.trades)

    # ── 对比汇总 ──────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("📊 策略绩效对比总表")
    print("=" * 70)
    header = f"{'策略名称':<16} {'总收益':>8} {'年化':>8} {'夏普':>6} {'最大回撤':>9} {'交易次数':>8} {'胜率':>6}"
    print(header)
    print("-" * 70)
    for name, r in results.items():
        print(f"{name:<16} {r['total_return']:>7.1f}% {r['annual_return']:>7.1f}% "
              f"{r['sharpe_ratio']:>5.1f} {r['max_drawdown']:>8.1f}% {r['n_trades']:>8d} "
              f"{r['win_rate']:>5.1f}%")
    print("=" * 70)

    # 保存权益曲线到 CSV
    for name, result in [
        ("RPS强度策略", result1),
        ("均线金叉死叉", result2),
        ("布林带均值回归", result3),
        ("动量突破", result4),
    ]:
        csv_path = f"/home/stock_analysis/quant/examples/equity_{name}.csv"
        result.equity_curve.to_csv(csv_path)
        print(f"💾 权益曲线已保存: {csv_path}")

    return results


if __name__ == "__main__":
    print("=" * 60)
    print("🤖 量化策略回测框架 - 初学入门示例")
    print("=" * 60)

    # 加载数据（近500个交易日，随机50只股票）
    data = load_sample_data(days=500, n_stocks=50)

    # 运行所有策略
    results = run_all_strategies(data)

    # 打印各策略详细分析
    for name, r in results.items():
        print_analysis(r)
