"""
mean_reversion.py - 布林带均值回归策略
当价格触及布林带下轨时买入，触及上轨时卖出
适用于震荡市场
"""
import pandas as pd


def bollinger_strategy(
    day_data: pd.DataFrame,
    date: pd.Timestamp,
    positions: dict,
    bb_window: int = 20,
    bb_std_mult: float = 2.0,
    rsi_window: int = 14,
    rsi_oversold: float = 40.0,
    rsi_overbought: float = 70.0,
    max_stocks: int = 5,
) -> list[dict]:
    """
    布林带均值回归策略（使用预计算指标）

    买入信号：价格 < 布林下轨 且 RSI < rsi_oversold（超卖）
    卖出信号：价格 > 布林上轨 或 RSI > rsi_overbought（超买）

    指标已在 data_loader.add_bollinger / add_rsi 预计算好：
    bb_upper, bb_mid, bb_lower, rsi
    """
    df = day_data.copy()
    if "date" not in df.columns:
        df["date"] = df.index

    signals = []

    # 持仓处理
    for code in list(positions.keys()):
        row = df[df["code"] == code]
        if row.empty:
            continue
        r = row.iloc[-1]

        # 平仓条件：超买或触及上轨
        if (r.get("rsi", 50) > rsi_overbought) or (r["close"] >= r.get("bb_upper", float("inf"))):
            signals.append({"code": code, "direction": "long", "size": 0.0})

    # 买入信号
    buy_candidates = df[
        (df["close"] < df["bb_lower"]) &
        (df["rsi"] < rsi_oversold)
    ].head(max_stocks)

    for _, r in buy_candidates.iterrows():
        if r["code"] not in positions:
            signals.append({"code": r["code"], "direction": "long", "size": 1.0 / max_stocks})

    return signals
