"""
ma_crossover.py - 均线金叉死叉策略
经典的双均线/三均线趋势跟踪策略
"""
import pandas as pd


def ma_cross_strategy(
    day_data: pd.DataFrame,
    date: pd.Timestamp,
    positions: dict,
    fast: int = 5,
    slow: int = 20,
    mid: int = 60,
    max_stocks: int = 5,
) -> list[dict]:
    """
    均线金叉死叉策略（使用预计算均线）

    买入信号：fast MA 上穿 slow MA，且价格在60日均线上方（多头趋势）
    卖出信号：fast MA 下穿 slow MA 或价格跌破慢线

    预计算均线：ma5, ma20, ma60（由 data_loader.add_ma 提供）
    """
    df = day_data.copy()
    if "date" not in df.columns:
        df["date"] = df.index

    ma_fast_col = f"ma{fast}"
    ma_slow_col = f"ma{slow}"
    ma_mid_col = f"ma{mid}"

    # 使用预计算的均线列
    if ma_fast_col not in df.columns or ma_slow_col not in df.columns:
        return []

    # 计算交叉：今日 fast>slow 且 前一日 fast<=slow
    df["_ma_fast_prev"] = df.groupby("code")[ma_fast_col].shift(1)
    df["_ma_slow_prev"] = df.groupby("code")[ma_slow_col].shift(1)
    df["golden_cross"] = (df[ma_fast_col] > df[ma_slow_col]) & (
        df["_ma_fast_prev"] <= df["_ma_slow_prev"]
    )
    df["death_cross"] = (df[ma_fast_col] < df[ma_slow_col]) & (
        df["_ma_fast_prev"] >= df["_ma_slow_prev"]
    )

    # 多头趋势：价格 > 60日均线
    if ma_mid_col in df.columns:
        df["uptrend"] = df["close"] > df[ma_mid_col]
    else:
        df["uptrend"] = True

    signals = []

    # 平仓
    for code in list(positions.keys()):
        row = df[df["code"] == code]
        if row.empty:
            continue
        r = row.iloc[-1]
        if r["death_cross"] or r["close"] < r.get(ma_slow_col, r["close"]):
            signals.append({"code": code, "direction": "long", "size": 0.0})

    # 开仓
    buy_df = df[df["golden_cross"] & df["uptrend"]].head(max_stocks)
    for _, r in buy_df.iterrows():
        if r["code"] not in positions:
            signals.append({"code": r["code"], "direction": "long", "size": 1.0 / max_stocks})

    return signals
