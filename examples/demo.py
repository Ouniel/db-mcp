#!/usr/bin/env python3
"""
演示示例：启动演示数据库服务器并连接客户端
"""
import os
import sys
import asyncio
import argparse
import json

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_mcp.server import DatabaseMCPServer
from db_mcp.client import DatabaseMCPClient

async def start_demo_server(demo_mode=True, dsn_list=None):
    """
    启动演示服务器
    :param demo_mode: 是否使用演示数据库
    :param dsn_list: 可选的DSN列表，用于连接多个真实数据库
    """
    # 创建演示服务器
    server = DatabaseMCPServer(demo=demo_mode, dsn=dsn_list)
    
    # 后台运行服务器
    server_task = asyncio.create_task(server.run(transport="stdio"))
    
    # 等待服务器启动
    await asyncio.sleep(1)
    
    return server_task, server

async def start_client(server_configs=None):
    """
    启动客户端
    :param server_configs: 服务器配置，可以指定多个服务器
    """
    # 默认配置 - 连接到演示服务器
    if server_configs is None:
        server_configs = {
            "db": {
                "command": "python",
                "args": ["-m", "db_mcp.server.main", "--demo", "--transport", "stdio"]
            }
        }
    
    # 创建客户端
    client = DatabaseMCPClient(
        servers=server_configs,
        llm_provider="openai"
    )
    
    # 运行客户端
    try:
        await client.chat_loop()
    finally:
        await client.cleanup()

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="DB-MCP演示示例")
    parser.add_argument("--server-only", action="store_true", help="仅启动服务器")
    parser.add_argument("--client-only", action="store_true", help="仅启动客户端")
    parser.add_argument("--multi-db", action="store_true", help="使用多数据库模式")
    parser.add_argument("--with-mongodb", action="store_true", help="添加MongoDB示例")
    
    args = parser.parse_args()
    
    if args.multi_db:
        # 使用SQLite演示多数据库模式
        # 创建临时SQLite数据库和MongoDB示例
        import sqlite3
        import tempfile
        
        # 创建第一个示例数据库（员工管理）
        db1_path = os.path.join(tempfile.gettempdir(), "db1.sqlite")
        conn1 = sqlite3.connect(db1_path)
        conn1.execute("CREATE TABLE IF NOT EXISTS employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL)")
        conn1.execute("INSERT OR IGNORE INTO employees VALUES (1, '张三', '研发部', 10000)")
        conn1.execute("INSERT OR IGNORE INTO employees VALUES (2, '李四', '市场部', 12000)")
        conn1.commit()
        conn1.close()
        
        # 创建第二个示例数据库（产品管理）
        db2_path = os.path.join(tempfile.gettempdir(), "db2.sqlite")
        conn2 = sqlite3.connect(db2_path)
        conn2.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)")
        conn2.execute("INSERT OR IGNORE INTO products VALUES (1, '笔记本电脑', 6999, 50)")
        conn2.execute("INSERT OR IGNORE INTO products VALUES (2, '智能手机', 3999, 100)")
        conn2.commit()
        conn2.close()
        
        dsn_list = [
            f"sqlite:///{db1_path}",
            f"sqlite:///{db2_path}"
        ]
        
        if args.with_mongodb:
            try:
                # 尝试连接MongoDB并创建示例数据
                from motor.motor_asyncio import AsyncIOMotorClient

                # 获取一个临时数据库名
                mongo_db_name = f"demo_db_{os.urandom(4).hex()}"
                mongo_uri = f"mongodb://localhost:27017/{mongo_db_name}"
                
                # 检查MongoDB是否可用
                try:
                    client = AsyncIOMotorClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
                    await client.admin.command('ping')
                    print(f"MongoDB连接成功，创建临时数据库: {mongo_db_name}")
                    
                    # 创建示例数据
                    db = client[mongo_db_name]
                    
                    # 添加一些用户数据
                    users = [
                        {"name": "王五", "email": "wang5@example.com", "age": 28, "vip": True},
                        {"name": "赵六", "email": "zhao6@example.com", "age": 35, "vip": False}
                    ]
                    await db.users.insert_many(users)
                    
                    # 添加一些订单数据
                    orders = [
                        {"order_id": "ORD001", "user": "王五", "items": ["笔记本电脑"], "total": 6999, "status": "已完成"},
                        {"order_id": "ORD002", "user": "赵六", "items": ["智能手机", "保护壳"], "total": 4099, "status": "处理中"}
                    ]
                    await db.orders.insert_many(orders)
                    
                    # 添加MongoDB DSN到列表
                    dsn_list.append(mongo_uri)
                    print(f"MongoDB示例数据创建完成，使用连接: {mongo_uri}")
                    
                except Exception as e:
                    print(f"MongoDB连接失败，跳过MongoDB示例: {e}")
                
            except ImportError:
                print("未安装motor库，跳过MongoDB示例")
        
        print(f"创建了以下示例数据库:\n{json.dumps(dsn_list, indent=2)}")
        
        if args.server_only:
            # 仅启动服务器
            server = DatabaseMCPServer(dsn=dsn_list)
            await server.run(transport="sse", port=8080)
        elif args.client_only:
            # 仅启动客户端
            dsn_args = []
            for dsn in dsn_list:
                dsn_args.extend(["--dsn", dsn])
                
            server_configs = {
                "multi_db": {
                    "command": "python",
                    "args": ["-m", "db_mcp.server.main"] + dsn_args + ["--transport", "stdio"]
                }
            }
            await start_client(server_configs)
        else:
            # 启动客户端连接到多数据库服务器
            dsn_args = []
            for dsn in dsn_list:
                dsn_args.extend(["--dsn", dsn])
                
            server_configs = {
                "multi_db": {
                    "command": "python",
                    "args": ["-m", "db_mcp.server.main"] + dsn_args + ["--transport", "stdio"]
                }
            }
            await start_client(server_configs)
            
    else:
        # 使用标准演示模式
        if args.server_only:
            # 仅启动服务器
            server = DatabaseMCPServer(demo=True)
            await server.run(transport="sse", port=8080)
        elif args.client_only:
            # 仅启动客户端
            await start_client()
        else:
            # 启动服务器和客户端
            print("启动演示服务器和客户端...")
            await start_client()

if __name__ == "__main__":
    asyncio.run(main()) 