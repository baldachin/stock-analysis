#!/bin/bash
# 发送RPS报告邮件
# 16:00 执行

LOG_FILE="/home/stock_analysis/data/email_cron.log"
DATE=$(date '+%Y-%m-%d %H:%M')

echo "[$DATE] ====== 开始发送邮件 ======" >> $LOG_FILE

# 检查是否工作日
DAY=$(date +%w)
if [ "$DAY" = "0" ] || [ "$DAY" = "6" ]; then
    echo "[$DATE] 周末休息，跳过执行" >> $LOG_FILE
    exit 0
fi

cd /home/stock_analysis

# 发送邮件
echo "[$DATE] 📧 发送报告邮件..." >> $LOG_FILE
python3 scripts/send_report_email.py >> $LOG_FILE 2>&1
if [ $? -eq 0 ]; then
    echo "[$DATE] ✅ 邮件发送完成" >> $LOG_FILE
else
    echo "[$DATE] ❌ 邮件发送失败" >> $LOG_FILE
fi

echo "[$DATE] ====== 邮件任务完成 ======" >> $LOG_FILE
