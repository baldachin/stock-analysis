"""
rps_strategy.py - RPS 强度策略
选取近期相对价格强度最强的股票
"""
import pandas as pd


def rps_strategy(
    day_data: pd.DataFrame,
    date: pd.Timestamp,
    positions: dict,
    rps_period: int = 20,
    top_n: int = 10,
    entry_threshold: float = 85.0,
    exit_threshold: float = 50.0,
    max_stocks: int = 8,
) -> list[dict]:
    """
    RPS 强度选股策略

    买入规则：RPS(20日) > entry_threshold，按 RPS 从高到低排序选取 top_n
    卖出规则：RPS(20日) < exit_threshold，或持仓股不在新的 top_n 里

    Parameters
    ----------
    day_data       : 当日行情 DataFrame（已按 date 索引筛选）
    date           : 当前交易日
    positions      : 当前持仓字典
    rps_period     : RPS 计算周期
    top_n          : 选取前 N 只
    entry_threshold: 入场 RPS 阈值
    exit_threshold : 出场 RPS 阈值
    max_stocks     : 最大持仓数
    """
    df = day_data.copy()

    # 计算简化 RPS：5日收益率作为相对强度代理
    if "ret5" not in df.columns:
        df["ret5"] = df.groupby("code")["close"].transform(
            lambda x: x.pct_change(5)
        )
    # 计算 RPS 排名（0~100），在"date"列上排名
    if "date" not in df.columns:
        df["date"] = df.index
    df["rps_score"] = df.groupby("date")["ret5"].rank(pct=True) * 100

    # 排序选股
    df = df.dropna(subset=["rps_score"])
    top_stocks = df.sort_values("rps_score", ascending=False).head(top_n)
    top_codes = set(top_stocks["code"].tolist())

    signals = []

    # 持仓中但已跌破阈值 → 平仓
    for code in list(positions.keys()):
        row = df[df["code"] == code]
        if row.empty:
            continue
        r = row.iloc[-1]
        if r["rps_score"] < exit_threshold:
            signals.append({"code": code, "direction": "long", "size": 0.0})

    # 新入场
    for _, r in top_stocks.iterrows():
        code = r["code"]
        if code in positions:
            continue
        if r["rps_score"] >= entry_threshold:
            signals.append({"code": code, "direction": "long", "size": 1.0 / max_stocks})

    # 平仓不在 top_n 的持仓
    for code in positions:
        if code not in top_codes:
            row = df[df["code"] == code]
            if not row.empty:
                signals.append({"code": code, "direction": "long", "size": 0.0})

    return signals
