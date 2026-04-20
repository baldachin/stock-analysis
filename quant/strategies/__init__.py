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

    买入规则：
    - RPS(20日) > entry_threshold
    - 按 RPS 从高到低排序，选取 top_n 只

    卖出规则：
    - RPS(20日) < exit_threshold
    - 或持仓股不在新的 top_n 里

    Parameters
    ----------
    day_data       : 当日行情（已按 date 索引筛选）
    date           : 当前交易日
    positions      : 当前持仓字典
    rps_period     : RPS 计算周期
    top_n          : 选取前 N 只
    entry_threshold: 入场 RPS 阈值
    exit_threshold : 出场 RPS 阈值

    Returns
    -------
    list[dict] 交易信号
    """
    # 读取 RPS 数据（实盘应预加载，此处演示从数据库读当天快照）
    # 本框架已在 data_loader 中加载完整数据，这里用 ret5 作为简化 RPS 替代
    if "rps" not in day_data.columns and "ret5" not in day_data.columns:
        # 计算简化 RPS：取今日 ret5（5日收益率）作为强度指标
        day_data = day_data.copy()
        if "ret5" not in day_data.columns:
            # 用 groupby 计算 5 日收益率
            day_data = day_data.sort_values("code")
            day_data["ret5"] = day_data.groupby("code")["close"].transform(
                lambda x: x.pct_change(5)
            )
        day_data["rps_score"] = day_data.groupby("date")["ret5"].rank(pct=True) * 100
    else:
        day_data = day_data.copy()
        if "rps" in day_data.columns:
            day_data["rps_score"] = day_data["rps"]
        else:
            day_data["rps_score"] = day_data["ret5"]

    # 过滤ST/退市（可选）
    # day_data = day_data[~day_data["code"].str.startswith("ST")]

    # 排序选股
    candidates = day_data.dropna(subset=["rps_score"])
    candidates = candidates.sort_values("rps_score", ascending=False)

    top_stocks = candidates.head(top_n)
    top_codes = set(top_stocks["code"].tolist())

    signals = []

    for _, row in top_stocks.iterrows():
        code = row["code"]
        rps = row["rps_score"]

        # 持仓检查
        if code in positions:
            # 持仓中但已跌破阈值 → 平仓
            if rps < exit_threshold:
                signals.append({"code": code, "direction": "long", "size": 0.0})
            continue

        # 新入场
        if rps >= entry_threshold:
            signals.append({"code": code, "direction": "long", "size": 1.0 / max_stocks})

    # 平仓不在 top_n 的持仓
    for code in positions:
        if code not in top_codes:
            # 找到对应收盘价
            row = day_data[day_data["code"] == code]
            if not row.empty:
                signals.append({"code": code, "direction": "long", "size": 0.0})

    return signals
