# 🤖 量化框架 - 初学入门版

> 基于真实 A 股 OHLCV 数据的事件驱动回测框架

## 📁 目录结构

```
quant/
├── __init__.py              # 框架入口
├── data_loader.py            # 数据加载器（SQLite → DataFrame）
├── backtest.py               # 事件驱动回测引擎
├── analysis.py               # 绩效分析（夏普/回撤/胜率/盈亏比）
├── strategies/
│   ├── rps_strategy.py       # RPS 强度选股策略
│   ├── ma_crossover.py       # 均线金叉/死叉策略
│   ├── mean_reversion.py     # 布林带均值回归策略
│   └── momentum_breakout.py  # 动量突破策略
└── examples/
    └── run_backtests.py     # 策略回测入口
```

## 🔧 快速开始

```python
import sys; sys.path.insert(0, "/home/stock_analysis")
from quant.data_loader import DataLoader
from quant.backtest import BacktestEngine
from quant.analysis import analyze
from quant.strategies.mean_reversion import bollinger_strategy

# 1. 加载数据
loader = DataLoader()
df = loader.get_bars(
    codes=["600318", "000001", "300136"],  # 或 None = 全市场
    start_date="2025-01-01",
    end_date="2026-04-17"
)
df = loader.add_returns(df, windows=[1, 5, 20])
df = loader.add_ma(df, windows=[5, 20, 60])
df = loader.add_bollinger(df)     # 预计算布林带
df = loader.add_rsi(df)            # 预计算 RSI

# 2. 创建回测引擎
engine = BacktestEngine(initial_cash=200_000, commission_rate=0.0003)
engine.load_data(df)

# 3. 注册策略
engine.add_strategy(bollinger_strategy)

# 4. 运行
result = engine.run()

# 5. 分析
metrics = analyze(result.equity_curve, result.trades)
print(f"总收益: {metrics['total_return']}%")
print(f"夏普比率: {metrics['sharpe_ratio']}")
print(f"最大回撤: {metrics['max_drawdown']}%")
```

## 📊 数据指标说明

`data_loader.add_*` 系列方法预计算技术指标：

| 方法 | 输出列 | 用途 |
|------|--------|------|
| `add_returns(windows=[1,5,20])` | `ret1`, `ret5`, `ret20` | 收益率 |
| `add_ma(windows=[5,20,60])` | `ma5`, `ma20`, `ma60` | 移动平均线 |
| `add_bollinger(window=20, std_mult=2.0)` | `bb_upper`, `bb_mid`, `bb_lower` | 布林带 |
| `add_breakout(lookback=20)` | `hh`(N日高点), `ll`(N日低点) | 突破系统 |
| `add_volume_ma(window=20)` | `vol_ma` | 成交量均线 |
| `add_rsi(window=14)` | `rsi` | 相对强弱指标 |

## 📈 内置策略

### 1. RPS 强度选股
- **逻辑**：选取近期相对价格强度最强的股票
- **买入**：`ret5` 排名前 10% 且 RPS > 85
- **卖出**：RPS < 50 或不在新的 top 10 里
- **适合**：强势股、趋势跟随

### 2. 布林带均值回归
- **逻辑**：价格触及布林下轨超卖时买入，触及上轨超买时卖出
- **买入**：`close < bb_lower` 且 `RSI < 40`
- **卖出**：`close > bb_upper` 或 `RSI > 70`
- **适合**：震荡市场、高波动股

### 3. 均线金叉死叉
- **逻辑**：快线 MA5 上穿慢线 MA20 买入，下穿卖出
- **买入**：MA5 > MA20 且价格在 60 日均线上方
- **卖出**：MA5 < MA20
- **适合**：趋势市场

### 4. 动量突破
- **逻辑**：价格突破 N 日高点且放量时买入
- **买入**：`close > hh(N日高点)` 且 `volume > vol_ma × 1.5`
- **卖出**：收盘价跌破 `ll`（N日低点）或亏损超 5%
- **适合**：趋势启动阶段

## 🔬 策略绩效指标

| 指标 | 说明 | 优秀标准 |
|------|------|---------|
| `total_return` | 总收益率 | > 20% |
| `annual_return` | 年化收益率 | > 15% |
| `sharpe_ratio` | 夏普比率（风险调整收益） | > 1.5 |
| `max_drawdown` | 最大回撤 | < -15% |
| `win_rate` | 胜率 | > 50% |

## ⚠️ 注意事项

1. **回测≠实盘**：回测结果仅供参考，实际交易有滑点、流动性冲击等摩擦成本
2. **前视偏差**：策略不能使用当日数据计算信号（本框架已避免）
3. **过拟合**：参数过度优化会导致曲线漂亮但实盘失效
4. **样本偏差**：随机选取的 50 只股票不代表全市场

## 📂 数据来源

- 数据路径：`/home/stock_analysis/data/our_data.db`
- 主要表：`daily_bars` (~700万条, 2020至今)
- 辅助表：`stock_rps`, `stock_names`, `filtered_rps`
