#!/usr/bin/env python3
import os
import sys
import json
import asyncio
import argparse
from typing import Optional, Dict, Any

from dotenv import load_dotenv

from .client import DatabaseMCPClient

def main():
    """命令行入口"""
    # 加载环境变量
    load_dotenv('.env')
    load_dotenv('.env.local')
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="数据库MCP客户端")
    parser.add_argument("--config", type=str, help="服务器配置文件路径")
    parser.add_argument("--llm-provider", type=str, default="openai", choices=["openai", "deepseek"], help="LLM提供商")
    parser.add_argument("--llm-api-key", type=str, help="LLM API密钥")
    parser.add_argument("--llm-model", type=str, help="LLM模型")
    parser.add_argument("--llm-api-base", type=str, help="LLM API基础URL")
    
    args = parser.parse_args()
    
    # 加载服务器配置
    servers = {}
    if args.config:
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if "servers" in config:
                    servers = config["servers"]
                else:
                    servers = config
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)
    else:
        # 使用默认配置
        servers = {
            "db": {
                "command": "python",
                "args": ["-m", "db_mcp.server.main", "--demo", "--transport", "stdio"]
            }
        }
        print("使用默认配置:")
        print(json.dumps(servers, indent=2))
    
    # 初始化客户端
    client = DatabaseMCPClient(
        servers=servers,
        llm_provider=args.llm_provider,
        llm_api_key=args.llm_api_key,
        llm_model=args.llm_model,
        llm_api_base=args.llm_api_base
    )
    
    try:
        # 运行交互式聊天循环
        asyncio.run(client.chat_loop())
    except KeyboardInterrupt:
        print("\n程序被中断")
    finally:
        # 清理资源
        asyncio.run(client.cleanup())

if __name__ == "__main__":
    main() 