#!/usr/bin/env python3
import os
import sys
import asyncio
import argparse
from typing import Optional, Dict, Any

def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(description="数据库MCP工具")
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # 服务器子命令
    server_parser = subparsers.add_parser("server", help="启动MCP服务器")
    server_parser.add_argument("--dsn", type=str, help="数据库连接字符串")
    server_parser.add_argument("--demo", action="store_true", help="使用演示数据库")
    server_parser.add_argument("--transport", type=str, default="stdio", choices=["stdio", "sse"], help="传输协议")
    server_parser.add_argument("--port", type=int, default=8080, help="HTTP服务器端口（仅当transport=sse时使用）")
    server_parser.add_argument("--ai-provider", type=str, default="openai", choices=["openai", "deepseek"], help="AI提供商")
    server_parser.add_argument("--ai-api-key", type=str, help="AI API密钥")
    server_parser.add_argument("--ai-model", type=str, help="AI模型")
    server_parser.add_argument("--ai-api-base", type=str, help="AI API基础URL")
    
    # 客户端子命令
    client_parser = subparsers.add_parser("client", help="启动MCP客户端")
    client_parser.add_argument("--config", type=str, help="服务器配置文件路径")
    client_parser.add_argument("--llm-provider", type=str, default="openai", choices=["openai", "deepseek"], help="LLM提供商")
    client_parser.add_argument("--llm-api-key", type=str, help="LLM API密钥")
    client_parser.add_argument("--llm-model", type=str, help="LLM模型")
    client_parser.add_argument("--llm-api-base", type=str, help="LLM API基础URL")
    
    args = parser.parse_args()
    
    if args.command == "server":
        # 导入服务器模块并启动服务器
        from db_mcp.server.main import main as server_main
        sys.argv = [sys.argv[0]] + [f"--{k.replace('_', '-')}={v}" if v is not None else f"--{k.replace('_', '-')}" 
                                  for k, v in vars(args).items() 
                                  if k != "command" and v is not None]
        server_main()
    elif args.command == "client":
        # 导入客户端模块并启动客户端
        from db_mcp.client.main import main as client_main
        sys.argv = [sys.argv[0]] + [f"--{k.replace('_', '-')}={v}" if v is not None else f"--{k.replace('_', '-')}" 
                                  for k, v in vars(args).items() 
                                  if k != "command" and v is not None]
        client_main()
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 