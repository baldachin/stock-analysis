#!/usr/bin/env python3
"""
发送RPS报告到邮箱
"""
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# ========== 配置 ==========
SMTP_SERVER = 'smtp.qq.com'
SMTP_PORT = 465
SENDER_EMAIL = 'baldachin@qq.com'
SENDER_PASSWORD = 'oeemsppubnxvbifd'
RECIPIENT_EMAIL = 'baldachin@outlook.com'

# 报告文件路径
HTML_REPORT = '/home/stock_analysis/data/rps_report.html'

def send_email(subject, html_content, to_email):
    """发送HTML邮件"""
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("❌ 邮箱配置不完整，请设置 SENDER_EMAIL 和 SENDER_PASSWORD")
        return False
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = to_email
    
    # 添加HTML内容
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)
    
    try:
        print(f"📧 连接到 {SMTP_SERVER}:{SMTP_PORT}...")
        if SMTP_PORT == 465:
            server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
        else:
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            server.ehlo()
            server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, to_email, msg.as_string())
        server.quit()
        print(f"✅ 邮件已发送至 {to_email}")
        return True
    except Exception as e:
        print(f"❌ 发送失败: {e}")
        return False

def main():
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 读取HTML报告
    if not os.path.exists(HTML_REPORT):
        print(f"❌ 报告文件不存在: {HTML_REPORT}")
        return
    
    with open(HTML_REPORT, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    subject = f"📊 全市场RPS报告 {today}"
    
    success = send_email(subject, html_content, RECIPIENT_EMAIL)
    
    if success:
        print(f"✅ 报告已发送至 {RECIPIENT_EMAIL}")
    else:
        print(f"❌ 发送失败，请检查邮箱配置")

if __name__ == "__main__":
    main()
