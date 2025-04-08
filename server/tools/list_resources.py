import json
from typing import Any, Dict, List, Optional, Union

from mcp.server.fastmcp import Tool

from . import DatabaseTool
from ..connectors import DatabaseConnector

class ListResourcesTool(DatabaseTool):
    """数据库资源列表工具"""
    
    def __init__(self, connector: DatabaseConnector):
        """
        初始化列表工具
        :param connector: 数据库连接器
        """
        super().__init__(connector)
    
    def get_tool(self, tool_name: str = "list_resources", description: str = "列出数据库中的资源（schemas、tables、columns、indexes、procedures等）") -> Tool:
        """
        获取MCP工具
        :param tool_name: 工具名称，默认为list_resources
        :param description: 工具描述，默认为"列出数据库中的资源（schemas、tables、columns、indexes、procedures等）"
        :return: MCP工具对象
        """
        
        async def list_resources(resource_type: str, schema: Optional[str] = None, table: Optional[str] = None, procedure: Optional[str] = None) -> str:
            """
            列出数据库中的资源
            :param resource_type: 资源类型，可选值：schemas, tables, columns, indexes, procedures, procedure_details
            :param schema: schema名称，获取tables、columns、indexes、procedures时需要
            :param table: 表名，获取columns、indexes时需要
            :param procedure: 存储过程名称，获取procedure_details时需要
            :return: 资源列表的JSON字符串
            """
            try:
                if not await self.connector.is_connected():
                    await self.connector.connect()
                
                result = None
                
                if resource_type == 'schemas':
                    # 获取所有schemas
                    result = await self.connector.get_schemas()
                elif resource_type == 'tables':
                    # 获取指定schema中的所有表
                    if not schema:
                        return json.dumps({'status': 'error', 'message': '获取表列表需要提供schema参数'})
                    result = await self.connector.get_tables(schema)
                elif resource_type == 'columns':
                    # 获取表结构（列信息）
                    if not schema or not table:
                        return json.dumps({'status': 'error', 'message': '获取列信息需要提供schema和table参数'})
                    result = await self.connector.get_table_structure(schema, table)
                elif resource_type == 'indexes':
                    # 获取表的索引
                    if not schema or not table:
                        return json.dumps({'status': 'error', 'message': '获取索引信息需要提供schema和table参数'})
                    result = await self.connector.get_indexes(schema, table)
                elif resource_type == 'procedures':
                    # 获取存储过程
                    if not schema:
                        return json.dumps({'status': 'error', 'message': '获取存储过程列表需要提供schema参数'})
                    result = await self.connector.get_procedures(schema)
                elif resource_type == 'procedure_details':
                    # 获取存储过程详情
                    if not schema or not procedure:
                        return json.dumps({'status': 'error', 'message': '获取存储过程详情需要提供schema和procedure参数'})
                    result = await self.connector.get_procedure_details(schema, procedure)
                else:
                    return json.dumps({
                        'status': 'error',
                        'message': f'不支持的资源类型: {resource_type}，支持的类型: schemas, tables, columns, indexes, procedures, procedure_details'
                    })
                
                return json.dumps({'status': 'success', 'results': result}, ensure_ascii=False, default=str)
            except Exception as e:
                return json.dumps({'status': 'error', 'message': str(e)})
        
        tool = Tool(
            name=tool_name,
            description=description,
            input_schema={
                "type": "object",
                "properties": {
                    "resource_type": {
                        "type": "string",
                        "description": "要列出的资源类型，可选值：schemas, tables, columns, indexes, procedures, procedure_details",
                        "enum": ["schemas", "tables", "columns", "indexes", "procedures", "procedure_details"]
                    },
                    "schema": {
                        "type": "string",
                        "description": "schema名称，获取tables、columns、indexes、procedures时需要"
                    },
                    "table": {
                        "type": "string",
                        "description": "表名，获取columns、indexes时需要"
                    },
                    "procedure": {
                        "type": "string",
                        "description": "存储过程名称，获取procedure_details时需要"
                    }
                },
                "required": ["resource_type"]
            },
            function=list_resources
        )
        
        return tool 