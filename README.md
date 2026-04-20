# 📈 A股 RPS 分析工具集

A stock analysis toolkit for tracking Relative Performance Strength (RPS) of Chinese A-shares.

## 功能特点

- **RPS计算**：多周期（5/10/20/50/120/250日）收益率计算
- **市场排名**：基于沪深300成分股（289只）的真实市场排名百分位
- **交互式图表**：Plotly交互图表，悬停查看详细数据
- **自动更新**：支持定时任务，每日自动分析

## 目录结构

```
stock-analysis/
├── scripts/
│   ├── daily_rps_3stocks.py    # 三只重点股票每日追踪
│   ├── rps_full_market.py      # 沪深300全量RPS排名计算
│   ├── rps_watch.py           # 自选股RPS排名监控
│   ├── rps_tracker.py          # 数据库版排名追踪系统
│   ├── rps.py                 # 通用RPS计算
│   ├── chart_rps.py           # Matplotlib静态图表
│   └── chart_interactive.py   # Plotly交互式HTML图表
├── data/                      # 数据存储（需创建）
├── my_stocks.txt              # 自选股列表
├── run_daily_rps.sh          # 定时任务脚本
└── README.md
```

## 快速开始

### 1. 安装依赖

```bash
pip install requests pandas numpy matplotlib plotly
```

### 2. 三只重点股票每日追踪

```bash
python3 scripts/daily_rps_3stocks.py
```

输出示例：
```
📊 三只股票每日RPS追踪
📅 2026-04-11

🔹 兆易创新 (603986)
   💰 价格: 265.09
   📊 各周期涨幅:
      20日: -3.53%  50日: -13.83%  120日: +27.54%
```

### 3. 沪深300全量RPS排名

```bash
python3 scripts/rps_full_market.py
```

输出示例：
```
📊 沪深300 RPS排名报告
📅 2026-04-11 | 统计股票: 289 只

🔹 兆易创新 (603986)
   📊 全市场RPS排名:
      🟢 RPS20:  64.0% | ██████░░░░ | 涨幅: -3.53%
      🟢 RPS120: 90.7% | █████████░ | 涨幅: +27.54%
```

### 4. 生成交互式图表

```bash
python3 scripts/chart_interactive.py
# 输出: data/rps_interactive.html
# 浏览器打开即可交互式查看
```

## 核心概念

### RPS（相对强弱）

```
N日RPS = (当前价格 - N日前价格) / N日前价格 × 100%
```

### RPS排名（市场百分位）

```
RPS排名 = 打败了市场上X%的股票
```

| RPS值 | 含义 |
|--------|------|
| > 80% | 🥇 强势股（市场前20%） |
| > 50% | ✅ 超过市场平均 |
| < 50% | ⚠️ 弱于市场平均 |

### 趋势判断

| 形态 | 说明 |
|------|------|
| 📈 加速上涨 | 短期>中期>长期，均为正且递增 |
| 📊 反弹回暖 | 短期正，中期负 |
| 🔻 持续调整 | 各周期负，且中期最弱 |
| ➡️ 震荡整理 | 多空平衡 |

## 数据来源

- 实时行情：腾讯财经 API
- 历史K线：新浪财经 API
- 市场股票池：沪深300成分股（约300只）

## 定时任务

设置每日早上9点自动运行：

```bash
# 添加到crontab
0 9 * * 1-5 /path/to/run_daily_rps.sh
```

## 依赖

```
requests>=2.28.0
pandas>=1.5.0
numpy>=1.21.0
matplotlib>=3.5.0
plotly>=5.0.0
```

## License

MIT
