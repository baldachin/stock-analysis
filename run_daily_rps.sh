#!/bin/bash
# 每日RPS报告生成脚本
# 由系统cron调用: 0 9 * * 1-5 /home/braveyun/stock_analysis/run_daily_rps.sh

DATE_FILE="/home/braveyun/stock_analysis/data/last_run.txt"
OUTPUT_FILE="/home/braveyun/stock_analysis/data/daily_rps_3stocks.txt"
LOG_FILE="/home/braveyun/stock_analysis/data/cron_log.txt"

TODAY=$(date +%Y-%m-%d)

# 检查是否已运行
if [ -f "$DATE_FILE" ]; then
    LAST_RUN=$(cat "$DATE_FILE")
    if [ "$LAST_RUN" = "$TODAY" ]; then
        echo "$(date): Already ran today, skipping" >> "$LOG_FILE"
        exit 0
    fi
fi

# 执行分析
cd /home/braveyun
python3 stock_analysis/scripts/daily_rps_3stocks.py >> "$LOG_FILE" 2>&1

# 更新运行日期
echo "$TODAY" > "$DATE_FILE"

# 追加到历史
echo "===== $(date) =====" >> /home/braveyun/stock_analysis/data/rps_history.txt
cat "$OUTPUT_FILE" >> /home/braveyun/stock_analysis/data/rps_history.txt

echo "$(date): Report generated" >> "$LOG_FILE"
