import json
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import Tool

from . import DatabaseTool
from ..connectors import DatabaseConnector

class QueryTool(DatabaseTool):
    """数据库查询工具"""
    
    def __init__(self, connector: DatabaseConnector):
        """
        初始化查询工具
        :param connector: 数据库连接器
        """
        super().__init__(connector)
    
    def get_tool(self, tool_name: str = "run_query", description: str = "执行SQL查询并返回结果") -> Tool:
        """
        获取MCP工具
        :param tool_name: 工具名称，默认为run_query
        :param description: 工具描述，默认为"执行SQL查询并返回结果"
        :return: MCP工具对象
        """
        
        async def run_query(sql_query: str) -> str:
            """
            执行SQL查询
            :param sql_query: SQL查询语句
            :return: 查询结果的JSON字符串
            """
            try:
                if not await self.connector.is_connected():
                    await self.connector.connect()
                
                results = await self.connector.execute_query(sql_query)
                
                # 检查是否有错误
                if results and 'error' in results[0]:
                    return json.dumps({'status': 'error', 'message': results[0]['error']})
                
                # 查询成功
                return json.dumps({'status': 'success', 'results': results}, ensure_ascii=False, default=str)
            except Exception as e:
                return json.dumps({'status': 'error', 'message': str(e)})
        
        tool = Tool(
            name=tool_name,
            description=description,
            input_schema={
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": "要执行的SQL查询语句"
                    }
                },
                "required": ["sql_query"]
            },
            function=run_query
        )
        
        return tool 