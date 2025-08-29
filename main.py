#!/usr/bin/env python3
"""新闻爬虫定时任务主程序"""

import os
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
from news_crawler import NewsCrawler

# 加载环境变量
load_dotenv('.env.dev')

def signal_handler(signum, frame):
    """处理中断信号"""
    print("\n收到中断信号，正在停止爬虫...")
    if crawler:
        crawler.stop()
    print("爬虫已停止")
    sys.exit(0)

def main():
    """主函数"""
    global crawler
    
    print("🚀 新闻爬虫定时任务启动")
    print(f"⏰ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查必要的环境变量
    required_vars = ["DASHSCOPE_API_KEY", "DB_HOST", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ 缺少必要的环境变量: {', '.join(missing_vars)}")
        print("请检查 .env.dev 文件")
        return
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 创建爬虫实例
        crawler = NewsCrawler()
        
        print("📰 定时任务已配置：每天凌晨4点执行")
        print("🔄 立即执行一次爬取任务...")
        
        # 启动定时任务并立即执行一次
        crawler.start(run_immediately=True)
        
        print("✅ 定时任务已启动，按 Ctrl+C 停止")
        print("⏱️  下次执行时间：明天凌晨4点")
        
        # 保持程序运行
        while True:
            try:
                import time
                time.sleep(60)  # 每分钟检查一次
            except KeyboardInterrupt:
                break
                
    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"❌ 程序异常: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'crawler' in locals():
            crawler.stop()
        print("程序已退出")

if __name__ == "__main__":
    main()