#!/bin/bash
# 下午盘后数据更新和邮件发送
# 15:30 更新数据
# 16:00 发送邮件

LOG_FILE="/home/stock_analysis/data/pm_cron.log"
DATE=$(date '+%Y-%m-%d %H:%M')

echo "[$DATE] ====== 开始下午盘后任务 ======" >> $LOG_FILE

# 检查是否工作日
DAY=$(date +%w)
if [ "$DAY" = "0" ] || [ "$DAY" = "6" ]; then
    echo "[$DATE] 周末休息，跳过执行" >> $LOG_FILE
    exit 0
fi

cd /home/stock_analysis

# 1. 更新数据 (15:30)
echo "[$DATE] 📥 更新数据..." >> $LOG_FILE
python3 scripts/rps_filtered.py >> $LOG_FILE 2>&1
if [ $? -eq 0 ]; then
    echo "[$DATE] ✅ 数据更新完成" >> $LOG_FILE
else
    echo "[$DATE] ❌ 数据更新失败" >> $LOG_FILE
fi

echo "[$DATE] ====== 下午任务完成 ======" >> $LOG_FILE
