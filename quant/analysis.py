"""
analysis.py - 绩效分析工具
计算收益率、夏普比率、最大回撤、胜率等核心指标
"""
import pandas as pd
import numpy as np


def analyze(equity_curve: pd.DataFrame, trades: list) -> dict:
    """
    综合绩效分析

    Parameters
    ----------
    equity_curve : pd.DataFrame  - 必须有 equity 列
    trades       : list           - Trade 对象列表

    Returns
    -------
    dict 各项指标
    """
    equity = equity_curve["equity"]

    # ── 收益率 ──────────────────────────────────────────────
    total_return = equity.iloc[-1] / equity.iloc[0] - 1

    # 年化收益率
    days = (equity.index[-1] - equity.index[0]).days
    annual_return = (1 + total_return) ** (365 / max(days, 1)) - 1

    # ── 波动率 & 夏普 ────────────────────────────────────────
    daily_returns = equity.pct_change().dropna()
    volatility = daily_returns.std() * np.sqrt(252)
    sharpe = (daily_returns.mean() / daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0.0

    # ── 最大回撤 ─────────────────────────────────────────────
    cummax = equity.cummax()
    drawdown = (equity - cummax) / cummax
    max_dd = drawdown.min()
    max_dd_date = drawdown.idxmin()

    # ── 交易统计 ─────────────────────────────────────────────
    n_trades = len(trades)
    buy_trades = [t for t in trades if t.quantity > 0]
    sell_trades = [t for t in trades if t.quantity < 0]

    if buy_trades and sell_trades:
        # 配对买卖算一笔完整交易
        buy_value = sum(t.price * t.quantity for t in buy_trades)
        sell_value = sum(abs(t.price * t.quantity) for t in sell_trades)
        avg_hold_days = 0  # 简化
        wins = sum(1 for b, s in zip(buy_trades, sell_trades) if s and s.price > b.price)
        win_rate = wins / max(len(buy_trades), 1)
    else:
        buy_value = sell_value = avg_hold_days = 0
        win_rate = 0.0

    # ── 盈亏比 ──────────────────────────────────────────────
    profits = []
    losses = []
    for t in trades:
        if t.quantity < 0:
            # 简化估算
            pass

    return {
        "total_return":    round(total_return * 100, 2),
        "annual_return":   round(annual_return * 100, 2),
        "sharpe_ratio":    round(float(sharpe), 2),
        "max_drawdown":    round(max_dd * 100, 2),
        "max_dd_date":     str(max_dd_date),
        "volatility":      round(volatility * 100, 2),
        "n_trades":        n_trades,
        "win_rate":        round(win_rate * 100, 1),
        "final_equity":    round(equity.iloc[-1], 2),
    }


def print_analysis(result: dict):
    """格式化打印分析结果"""
    print("\n📈 绩效分析报告")
    print("─" * 35)
    for key, val in result.items():
        if key == "max_dd_date":
            continue
        if isinstance(val, float):
            if key in ("total_return", "annual_return", "max_drawdown", "volatility"):
                print(f"  {key:<16}: {val:>8.2f}%")
            elif key == "win_rate":
                print(f"  {key:<16}: {val:>8.1f}%")
            else:
                print(f"  {key:<16}: {val:>8.2f}")
        else:
            print(f"  {key:<16}: {val}")
    print("─" * 35)
