"""
data_loader.py - 数据加载器
从 SQLite 数据库加载 OHLCV 行情数据和 RPS 数据
"""
import sqlite3
import pandas as pd
from typing import Optional


class DataLoader:
    """加载股票行情数据的工具类"""

    DB_PATH = "/home/stock_analysis/data/our_data.db"

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or self.DB_PATH

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ── K线数据 ────────────────────────────────────────────────

    def get_bars(
        self,
        codes: list[str] | str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        读取日线数据，返回带 trade_date 和 code 列的 DataFrame

        Parameters
        ----------
        codes : list/str, optional  - 股票代码列表，或单个代码
        start_date : str, optional  - 开始日期 'YYYY-MM-DD'
        end_date   : str, optional  - 结束日期 'YYYY-MM-DD'
        columns    : list, optional - 指定列
        """
        sql = "SELECT * FROM daily_bars WHERE 1=1"
        params: list = []

        if codes:
            if isinstance(codes, str):
                codes = [codes]
            placeholders = ",".join(["?"] * len(codes))
            sql += f" AND code IN ({placeholders})"
            params.extend(codes)

        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)

        sql += " ORDER BY trade_date, code"

        df = pd.read_sql(sql, self.connect(), params=params)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        if columns:
            df = df[[c for c in columns if c in df.columns]]

        return df

    def get_rps(
        self,
        codes: list[str] | str | None = None,
        period: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """读取 RPS 强度数据"""
        sql = "SELECT * FROM stock_rps WHERE 1=1"
        params: list = []

        if codes:
            if isinstance(codes, str):
                codes = [codes]
            placeholders = ",".join(["?"] * len(codes))
            sql += f" AND code IN ({placeholders})"
            params.extend(codes)

        if period:
            sql += " AND period = ?"
            params.append(period)

        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)

        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)

        sql += " ORDER BY trade_date, rps DESC"

        df = pd.read_sql(sql, self.connect(), params=params)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df

    def get_stock_names(self, codes: list[str] | str | None = None) -> pd.DataFrame:
        """读取股票代码与名称映射"""
        if codes:
            if isinstance(codes, str):
                codes = [codes]
            placeholders = ",".join(["?"] * len(codes))
            sql = f"SELECT * FROM stock_names WHERE code IN ({placeholders})"
            return pd.read_sql(sql, self.connect(), params=codes)
        return pd.read_sql("SELECT * FROM stock_names", self.connect())

    # ── 指标计算 ────────────────────────────────────────────────

    def add_ma(self, df: pd.DataFrame, windows: list[int] = [5, 10, 20, 60]) -> pd.DataFrame:
        """计算移动平均线"""
        for w in windows:
            df[f"ma{w}"] = df.groupby("code")["close"].transform(lambda x: x.rolling(w).mean())
        return df

    def add_volatility(self, df: pd.DataFrame, windows: list[int] = [20, 60]) -> pd.DataFrame:
        """计算历史波动率"""
        for w in windows:
            df[f"vol{w}"] = df.groupby("code")["close"].transform(lambda x: x.rolling(w).std())
        return df

    def add_returns(self, df: pd.DataFrame, windows: list[int] = [1, 5, 20]) -> pd.DataFrame:
        """计算收益率"""
        for w in windows:
            df[f"ret{w}"] = df.groupby("code")["close"].transform(lambda x: x.pct_change(w))
        return df

    def add_bollinger(self, df: pd.DataFrame, window: int = 20, std_mult: float = 2.0) -> pd.DataFrame:
        """计算布林带"""
        df = df.copy()
        df["bb_mid"] = df.groupby("code")["close"].transform(lambda x: x.rolling(window).mean())
        df["bb_std"] = df.groupby("code")["close"].transform(lambda x: x.rolling(window).std())
        df["bb_upper"] = df["bb_mid"] + std_mult * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - std_mult * df["bb_std"]
        return df

    def add_breakout(self, df: pd.DataFrame, lookback: int = 20) -> pd.DataFrame:
        """计算 N 日高低点（shift 1 = 前一日收盘后的最高/低价）"""
        df = df.copy()
        df["hh"] = df.groupby("code")["high"].transform(lambda x: x.rolling(lookback).max().shift(1))
        df["ll"] = df.groupby("code")["low"].transform(lambda x: x.rolling(lookback).min().shift(1))
        return df

    def add_volume_ma(self, df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
        """计算成交量均线"""
        df = df.copy()
        df["vol_ma"] = df.groupby("code")["volume"].transform(lambda x: x.rolling(window).mean())
        return df

    def add_rsi(self, df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
        """计算 RSI"""
        df = df.copy()
        delta = df.groupby("code")["close"].diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        # 按股票分别计算
        rsi_list = []
        for code, grp in df.groupby("code"):
            g = gain.loc[grp.index]
            l = loss.loc[grp.index]
            avgg = g.rolling(window, min_periods=1).mean()
            avgl = l.rolling(window, min_periods=1).mean()
            rs = avgg / avgl.replace(0, 1e-10)
            rsi = 100 - 100 / (1 + rs)
            rsi.name = "rsi"
            rsi_list.append(rsi)
        df["rsi"] = pd.concat(rsi_list).sort_index()
        return df
