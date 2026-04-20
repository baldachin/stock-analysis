"""
backtest.py - 事件驱动回测引擎
适用于初学者的简洁回测框架，基于 OHLCV 数据模拟交易
"""
import pandas as pd
import numpy as np
from typing import Callable, Literal
from dataclasses import dataclass, field


@dataclass
class Trade:
    """单笔交易记录"""
    date: pd.Timestamp
    code: str
    direction: Literal["long", "short"]   # 做多 / 做空
    price: float
    quantity: int
    commission: float = 0.0


@dataclass
class Position:
    """持仓"""
    code: str
    direction: Literal["long", "short"]
    quantity: int
    entry_price: float
    entry_date: pd.Timestamp


@dataclass
class BacktestResult:
    """回测结果汇总"""
    trades: list[Trade] = field(default_factory=list)
    equity_curve: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def total_return(self) -> float:
        if self.equity_curve.empty:
            return 0.0
        return float(self.equity_curve["equity"].iloc[-1] / self.equity_curve["equity"].iloc[0] - 1)

    @property
    def sharpe_ratio(self) -> float:
        if len(self.equity_curve) < 2:
            return 0.0
        rets = self.equity_curve["equity"].pct_change().dropna()
        if rets.std() == 0:
            return 0.0
        return float(rets.mean() / rets.std() * np.sqrt(252))

    @property
    def max_drawdown(self) -> float:
        if self.equity_curve.empty:
            return 0.0
        cummax = self.equity_curve["equity"].cummax()
        drawdown = (self.equity_curve["equity"] - cummax) / cummax
        return float(drawdown.min())

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        wins = sum(1 for t in self.trades if t.quantity > 0)
        return wins / len(self.trades)


class BacktestEngine:
    """
    简洁的事件驱动回测引擎

    使用方法
    --------
    engine = BacktestEngine(initial_cash=100_000, commission_rate=0.0003)
    engine.load_data(my_dataframe)
    engine.add_strategy(my_strategy_function)
    result = engine.run()
    """

    def __init__(
        self,
        initial_cash: float = 100_000,
        commission_rate: float = 0.0003,   # 默认万三
        slippage: float = 0.0,              # 滑点（按价格比例）
    ):
        self.initial_cash = initial_cash
        self.commission_rate = commission_rate
        self.slippage = slippage
        self._data: pd.DataFrame | None = None
        self._strategy: Callable | None = None

    # ── 数据 & 策略注册 ─────────────────────────────────────────

    def load_data(self, df: pd.DataFrame) -> "BacktestEngine":
        """注入行情数据（必须有 code, close, open/high/low 列）"""
        self._data = df.copy()
        self._data = self._data.sort_values(["code", "trade_date"])
        return self

    def add_strategy(self, fn: Callable) -> "BacktestEngine":
        """
        注册交易策略函数

        fn(df: pd.DataFrame, date: pd.Timestamp, positions: dict) -> list[dict]
            返回信号列表，每条信号格式：
            {
                "code": str,          # 股票代码
                "direction": "long",   # long / short
                "size": float,         # 仓位比例 0~1（按当前资金）
            }
        """
        self._strategy = fn
        return self

    # ── 运行回测 ───────────────────────────────────────────────

    def run(self, max_stocks: int = 20) -> BacktestResult:
        if self._data is None:
            raise ValueError("请先调用 load_data() 加载数据")
        if self._strategy is None:
            raise ValueError("请先调用 add_strategy() 注册策略")

        # 取所有唯一交易日（MultiIndex: trade_date, code）
        # 取所有唯一交易日
        dates = sorted(self._data["trade_date"].unique())
        cash = self.initial_cash
        positions: dict[str, Position] = {}
        trades: list[Trade] = []
        equity_records: list[dict] = []

        for date in dates:
            # 按日期筛选（trade_date 已转为 Timestamp 列）
            day_data = self._data[self._data["trade_date"] == date].copy()

            # 推送信号
            try:
                signals = self._strategy(day_data, pd.Timestamp(date), positions)
            except Exception:
                signals = []

            # 更新持仓的当前盈亏（按收盘价）
            total_position_value = 0.0
            for pos in list(positions.values()):
                cur_price = float(
                    day_data.loc[day_data["code"] == pos.code, "close"].values[0]
                    if pos.code in day_data["code"].values
                    else pos.entry_price
                )
                if pos.direction == "long":
                    total_position_value += pos.quantity * cur_price
                else:
                    total_position_value += pos.quantity * (2 * pos.entry_price - cur_price)

            # 平仓不在当天信号里的持仓（止损/止盈逻辑可扩展）
            for pos in list(positions.values()):
                if pos.code not in [s["code"] for s in signals]:
                    # 简单平仓
                    cur_price = float(
                        day_data.loc[day_data["code"] == pos.code, "close"].values[0]
                        if pos.code in day_data["code"].values
                        else pos.entry_price
                    )
                    price_with_slip = cur_price * (1 - self.slippage) if pos.direction == "long" else cur_price * (1 + self.slippage)
                    proceeds = pos.quantity * price_with_slip
                    commission = proceeds * self.commission_rate
                    cash += proceeds - commission
                    trades.append(Trade(date, pos.code, pos.direction, price_with_slip, -pos.quantity, commission))
                    del positions[pos.code]

            # 开仓
            for sig in signals[:max_stocks]:
                if sig["code"] in positions:
                    continue
                code = sig["code"]
                size = min(sig.get("size", 0.1), 0.3)   # 单只仓位上限30%
                direction = sig.get("direction", "long")

                if code not in day_data["code"].values:
                    continue

                price_row = day_data.loc[day_data["code"] == code].iloc[0]
                price = float(price_row["close"])
                price_with_slip = price * (1 + self.slippage) if direction == "long" else price * (1 - self.slippage)
                invest = cash * size
                quantity = int(invest / price_with_slip / 100) * 100   # 按手买卖
                if quantity == 0:
                    continue
                commission = quantity * price_with_slip * self.commission_rate
                cost = quantity * price_with_slip + commission
                if cost > cash:
                    continue
                cash -= cost
                positions[code] = Position(code, direction, quantity, price_with_slip, pd.Timestamp(date))
                trades.append(Trade(date, code, direction, price_with_slip, quantity, commission))

            # 记录当日权益
            position_value = 0.0
            for pos in positions.values():
                cur_price = float(
                    day_data.loc[day_data["code"] == pos.code, "close"].values[0]
                    if pos.code in day_data["code"].values
                    else pos.entry_price
                )
                if pos.direction == "long":
                    position_value += pos.quantity * cur_price
                else:
                    position_value += pos.quantity * (2 * pos.entry_price - cur_price)

            total_equity = cash + position_value
            equity_records.append({
                "date": date,
                "cash": cash,
                "position_value": position_value,
                "equity": total_equity,
            })

        # 最终平仓
        last_date = dates[-1]
        last_data = self._data[self._data["trade_date"] == last_date]
        if isinstance(last_data, pd.Series):
            last_data = last_data.to_frame().T

        for pos in list(positions.values()):
            cur_price = float(
                last_data.loc[last_data["code"] == pos.code, "close"].values[0]
                if pos.code in last_data["code"].values
                else pos.entry_price
            )
            price_with_slip = cur_price * (1 - self.slippage) if pos.direction == "long" else cur_price * (1 + self.slippage)
            proceeds = pos.quantity * price_with_slip
            commission = proceeds * self.commission_rate
            cash += proceeds - commission
            trades.append(Trade(last_date, pos.code, pos.direction, price_with_slip, -pos.quantity, commission))
            del positions[pos.code]

        equity_df = pd.DataFrame(equity_records).set_index("date")

        result = BacktestResult(trades=trades, equity_curve=equity_df)

        # 打印摘要
        self._print_summary(result)

        return result

    # ── 结果输出 ───────────────────────────────────────────────

    def _print_summary(self, r: BacktestResult):
        print("\n" + "=" * 50)
        print("📊 回测结果摘要")
        print("=" * 50)
        print(f"  总收益率     : {r.total_return*100:.2f}%")
        print(f"  夏普比率     : {r.sharpe_ratio:.2f}")
        print(f"  最大回撤     : {r.max_drawdown*100:.2f}%")
        print(f"  交易次数     : {len(r.trades)}")
        print(f"  胜率         : {r.win_rate*100:.1f}%")
        print("=" * 50)
