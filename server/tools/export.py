import csv
import json
import os
import tempfile
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import Tool

from . import DatabaseTool
from ..connectors import DatabaseConnector

class ExportTool(DatabaseTool):
    """数据库导出工具"""
    
    def __init__(self, connector: DatabaseConnector):
        """
        初始化导出工具
        :param connector: 数据库连接器
        """
        super().__init__(connector)
    
    def get_tool(self, tool_name: str = "export_to_csv", description: str = "执行SQL查询并将结果导出为CSV文件") -> Tool:
        """
        获取MCP工具
        :param tool_name: 工具名称，默认为export_to_csv
        :param description: 工具描述，默认为"执行SQL查询并将结果导出为CSV文件"
        :return: MCP工具对象
        """
        
        async def export_to_csv(sql_query: str, output_file: str) -> str:
            """
            执行SQL查询并将结果导出为CSV文件
            :param sql_query: SQL查询语句
            :param output_file: 输出文件路径
            :return: 导出结果的JSON字符串
            """
            try:
                if not await self.connector.is_connected():
                    await self.connector.connect()
                
                results = await self.connector.execute_query(sql_query)
                
                # 检查是否有错误
                if results and 'error' in results[0]:
                    return json.dumps({'status': 'error', 'message': results[0]['error']})
                
                # 没有结果
                if not results:
                    return json.dumps({'status': 'warning', 'message': '查询没有返回结果'})
                
                # 确保输出目录存在
                output_dir = os.path.dirname(output_file)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                
                # 写入CSV文件
                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    if results:
                        # 获取列名
                        fieldnames = results[0].keys()
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(results)
                
                return json.dumps({
                    'status': 'success',
                    'message': f'数据已导出到 {output_file}',
                    'row_count': len(results)
                }, ensure_ascii=False)
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
                    },
                    "output_file": {
                        "type": "string",
                        "description": "导出CSV文件的路径"
                    }
                },
                "required": ["sql_query", "output_file"]
            },
            function=export_to_csv
        )
        
        return tool 