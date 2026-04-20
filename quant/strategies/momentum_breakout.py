"""
momentum_breakout.py - 动量突破策略
当价格突破 N 日高点时买入，跌破 N 日低点时卖出
趋势行情中表现较好
"""
import pandas as pd


def momentum_breakout_strategy(
    day_data: pd.DataFrame,
    date: pd.Timestamp,
    positions: dict,
    lookback: int = 20,
    volume_mult: float = 1.5,
    stop_loss: float = 0.05,
    max_stocks: int = 5,
) -> list[dict]:
    """
    动量突破策略（使用预计算指标）

    买入信号：
      - 收盘价 > hh（N日高点，由 data_loader.add_breakout 预计算）
      - 成交量 > vol_ma * volume_mult

    卖出/止损：
      - 收盘价 < ll（N日低点）→ 平仓
      - 亏损超 stop_loss（默认5%）→ 止损

    预计算指标：hh, ll, vol_ma
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

        # 止损
        pos = positions[code]
        ret = (r["close"] / pos.entry_price) - 1 if pos.direction == "long" else 1 - (r["close"] / pos.entry_price)
        if ret < -stop_loss:
            signals.append({"code": code, "direction": "long", "size": 0.0})
            continue

        # 跌破低点
        if r["close"] < r["ll"]:
            signals.append({"code": code, "direction": "long", "size": 0.0})

    # 买入信号
    buy_candidates = df[
        (df["close"] > df["hh"]) &
        (df["volume"] > df["vol_ma"] * volume_mult)
    ].head(max_stocks)

    for _, r in buy_candidates.iterrows():
        if r["code"] not in positions:
            signals.append({"code": r["code"], "direction": "long", "size": 1.0 / max_stocks})

    return signals
