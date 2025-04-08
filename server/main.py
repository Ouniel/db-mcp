#!/usr/bin/env python3
import os
import sys
import asyncio
import argparse
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv

from .server import DatabaseMCPServer

def main():
    """命令行入口"""
    # 加载环境变量
    load_dotenv('.env')
    load_dotenv('.env.local')
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="数据库MCP服务器")
    
    # 数据库连接参数，可以多次指定
    db_group = parser.add_mutually_exclusive_group()
    db_group.add_argument("--dsn", type=str, action="append", help="数据库连接字符串，可多次指定连接到多个数据库")
    db_group.add_argument("--demo", action="store_true", help="使用演示数据库")
    
    # 传输和服务器参数
    parser.add_argument("--transport", type=str, default="stdio", choices=["stdio", "sse"], help="传输协议")
    parser.add_argument("--port", type=int, default=8080, help="HTTP服务器端口（仅当transport=sse时使用）")
    
    # AI参数
    parser.add_argument("--ai-provider", type=str, default="openai", choices=["openai", "deepseek"], help="AI提供商")
    parser.add_argument("--ai-api-key", type=str, help="AI API密钥")
    parser.add_argument("--ai-model", type=str, help="AI模型")
    parser.add_argument("--ai-api-base", type=str, help="AI API基础URL")
    
    args = parser.parse_args()
    
    # 确保至少有一个DSN或使用演示模式
    if not args.dsn and not args.demo:
        # 尝试从环境变量获取DSN
        dsn_from_env = os.environ.get("DSN")
        if dsn_from_env:
            args.dsn = [dsn.strip() for dsn in dsn_from_env.split(',') if dsn.strip()]
    
    # 初始化服务器
    server = DatabaseMCPServer(
        dsn=args.dsn,
        demo=args.demo,
        ai_provider=args.ai_provider,
        ai_api_key=args.ai_api_key,
        ai_model=args.ai_model,
        ai_api_base=args.ai_api_base
    )
    
    # 运行服务器
    asyncio.run(server.run(transport=args.transport, port=args.port))

if __name__ == "__main__":
    main() 