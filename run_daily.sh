#!/bin/bash
# 每日9:30推送股票RPS分析+新闻简报
# 添加到crontab: 30 9 * * 1-5 /home/braveyun/stock_analysis/run_daily.sh

LOG_FILE="/home/braveyun/stock_analysis/data/cron.log"
DATE=$(date '+%Y-%m-%d %H:%M')

echo "[$DATE] ====== 开始执行每日分析 ======" >> $LOG_FILE

# 检查是否工作日
DAY=$(date +%w)
if [ "$DAY" = "0" ] || [ "$DAY" = "6" ]; then
    echo "[$DATE] 周末休息，跳过执行" >> $LOG_FILE
    exit 0
fi

cd /home/braveyun/stock_analysis

# 1. 执行RPS分析 (沪深300全量排名)
echo "[$DATE] 📊 执行RPS分析..." >> $LOG_FILE
python3 scripts/rps_full_market.py >> $LOG_FILE 2>&1
if [ $? -eq 0 ]; then
    echo "[$DATE] ✅ RPS分析完成" >> $LOG_FILE
else
    echo "[$DATE] ❌ RPS分析失败" >> $LOG_FILE
fi

# 2. 执行新闻简报
echo "[$DATE] 📰 执行新闻简报..." >> $LOG_FILE
python3 scripts/daily_news.py >> $LOG_FILE 2>&1
if [ $? -eq 0 ]; then
    echo "[$DATE] ✅ 新闻简报完成" >> $LOG_FILE
else
    echo "[$DATE] ❌ 新闻简报失败" >> $LOG_FILE
fi

echo "[$DATE] ====== 执行完成 ======" >> $LOG_FILE
