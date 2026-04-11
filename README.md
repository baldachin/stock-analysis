# Stock Analysis Tools

A股技术分析工具集，用于RPS排名追踪和技术指标分析。

## 功能

- **RPS追踪**：追踪股票相对强度排名变化
- **多周期分析**：支持5/10/20/50/120/250日RPS
- **趋势判断**：自动识别加速上涨、反弹、回撤等形态

## 依赖

```
pip install akshare pandas numpy requests
```

## 使用方法

### 每日RPS追踪（三只重点股）

```bash
python3 scripts/daily_rps_3stocks.py
```

### 自选股RPS排名

```bash
python3 scripts/rps_watch.py
```

### 通用RPS计算

```bash
python3 scripts/rps.py 603986,300496 20
```

## 股票列表

- 300496 中科创达
- 603986 兆易创新
- 002049 紫光国微

## 数据来源

新浪财经、腾讯财经 API
