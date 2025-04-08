import json
import os
from typing import Any, Dict, List, Optional, Union, Literal

import openai
import httpx

from mcp.server.fastmcp import Tool

from . import DatabaseTool
from ..connectors import DatabaseConnector

class AiTool(DatabaseTool):
    """人工智能工具，提供SQL生成和数据库解释功能"""
    
    def __init__(self, 
                 connector: DatabaseConnector, 
                 provider: Literal["openai", "deepseek"] = "openai",
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 api_base: Optional[str] = None):
        """
        初始化AI工具
        :param connector: 数据库连接器
        :param provider: API提供商，可选值：openai, deepseek
        :param api_key: API密钥，如果不提供则尝试从环境变量获取
        :param model: 使用的模型，如果不提供则使用默认值
        :param api_base: API基础URL，如果不提供则使用默认值
        """
        super().__init__(connector)
        
        self.provider = provider
        self.model = model
        
        # 设置API密钥
        if provider == "openai":
            self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
            if not self.api_key:
                raise ValueError("未提供OpenAI API密钥，请通过参数或环境变量OPENAI_API_KEY提供")
            self.model = model or "gpt-3.5-turbo"
            self.api_base = api_base or "https://api.openai.com/v1"
        elif provider == "deepseek":
            self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
            if not self.api_key:
                raise ValueError("未提供DeepSeek API密钥，请通过参数或环境变量DEEPSEEK_API_KEY提供")
            self.model = model or "deepseek-chat"
            self.api_base = api_base or "https://api.deepseek.com/v1"
        else:
            raise ValueError(f"不支持的API提供商: {provider}，支持的提供商: openai, deepseek")
    
    async def _get_db_schema_info(self) -> str:
        """获取数据库schema信息，用于AI生成SQL或解释"""
        try:
            # 获取所有schemas
            schemas = await self.connector.get_schemas()
            
            schema_info = []
            
            for schema_obj in schemas:
                schema_name = schema_obj['name']
                schema_info.append(f"Schema: {schema_name}")
                
                # 获取该schema下的所有表
                tables = await self.connector.get_tables(schema_name)
                
                for table_obj in tables:
                    table_name = table_obj['name']
                    schema_info.append(f"  Table: {table_name}")
                    
                    # 获取表结构
                    columns = await self.connector.get_table_structure(schema_name, table_name)
                    
                    for column in columns:
                        column_info = f"    - {column['name']} ({column['type']})"
                        if column['key'] == 'PRI':
                            column_info += " PRIMARY KEY"
                        elif column['key'] == 'UNI':
                            column_info += " UNIQUE"
                        schema_info.append(column_info)
            
            return "\n".join(schema_info)
        except Exception as e:
            return f"获取数据库结构信息错误: {str(e)}"
    
    async def _call_ai_api(self, messages: List[Dict[str, str]]) -> str:
        """调用AI API"""
        if self.provider == "openai":
            # 使用OpenAI API
            if self.api_base != "https://api.openai.com/v1":
                client = openai.OpenAI(api_key=self.api_key, base_url=self.api_base)
            else:
                client = openai.OpenAI(api_key=self.api_key)
            
            response = client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.1
            )
            
            return response.choices[0].message.content
        
        elif self.provider == "deepseek":
            # 使用DeepSeek API
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": 0.1
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_base}/chat/completions",
                    headers=headers,
                    json=payload
                )
                
                response_json = response.json()
                
                if "choices" in response_json and len(response_json["choices"]) > 0:
                    return response_json["choices"][0]["message"]["content"]
                else:
                    raise ValueError(f"API返回错误: {response_json}")
    
    def get_tool(self, db_prefix: str = "") -> List[Tool]:
        """
        获取MCP工具列表
        :param db_prefix: 数据库前缀，用于区分不同的数据库实例
        :return: MCP工具列表
        """
        # 如果提供了数据库前缀，则添加下划线
        prefix = f"{db_prefix}_" if db_prefix else ""
        
        return [
            self._get_generate_sql_tool(f"{prefix}generate_sql", f"在{db_prefix or ''}数据库中使用人工智能根据自然语言描述生成SQL查询语句"),
            self._get_explain_db_tool(f"{prefix}explain_db", f"使用人工智能解释{db_prefix or ''}数据库中的元素（表、列、存储过程等）")
        ]
    
    def _get_generate_sql_tool(self, tool_name: str = "generate_sql", description: str = "使用人工智能根据自然语言描述生成SQL查询语句") -> Tool:
        """获取SQL生成工具"""
        
        async def generate_sql(query: str) -> str:
            """
            使用AI生成SQL查询语句
            :param query: 用自然语言描述的查询需求
            :return: 生成的SQL语句
            """
            try:
                # 获取数据库结构信息
                schema_info = await self._get_db_schema_info()
                
                # 构建提示
                messages = [
                    {"role": "system", "content": f"你是一个SQL专家，可以根据自然语言描述生成SQL查询语句。以下是数据库结构信息:\n\n{schema_info}"},
                    {"role": "user", "content": f"请根据以下描述生成SQL语句，只返回SQL语句本身，不要有任何其他解释或说明:\n\n{query}"}
                ]
                
                # 调用AI API
                response = await self._call_ai_api(messages)
                
                return json.dumps({"status": "success", "query": query, "sql": response.strip()}, ensure_ascii=False)
            except Exception as e:
                return json.dumps({"status": "error", "message": str(e)})
        
        return Tool(
            name=tool_name,
            description=description,
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "用自然语言描述的查询需求"
                    }
                },
                "required": ["query"]
            },
            function=generate_sql
        )
    
    def _get_explain_db_tool(self, tool_name: str = "explain_db", description: str = "使用人工智能解释数据库中的元素（表、列、存储过程等）") -> Tool:
        """获取数据库解释工具"""
        
        async def explain_db(element_type: str, element_name: str, schema: Optional[str] = None) -> str:
            """
            使用AI解释数据库中的元素（表、列、存储过程等）
            :param element_type: 元素类型，可选值：schema, table, column, procedure
            :param element_name: 元素名称
            :param schema: schema名称，获取table、column、procedure的信息时需要
            :return: 解释信息
            """
            try:
                # 获取元素信息
                element_info = ""
                
                if element_type == "schema":
                    # 获取schema下的所有表
                    tables = await self.connector.get_tables(element_name)
                    element_info = f"Schema '{element_name}' 包含的表:\n"
                    for table in tables:
                        element_info += f"- {table['name']}\n"
                
                elif element_type == "table":
                    if not schema:
                        return json.dumps({"status": "error", "message": "获取表信息需要提供schema参数"})
                    
                    # 获取表结构
                    columns = await self.connector.get_table_structure(schema, element_name)
                    element_info = f"表 '{schema}.{element_name}' 的结构:\n"
                    for column in columns:
                        column_info = f"- {column['name']} ({column['type']})"
                        if column['key'] == 'PRI':
                            column_info += " PRIMARY KEY"
                        elif column['key'] == 'UNI':
                            column_info += " UNIQUE"
                        element_info += column_info + "\n"
                
                elif element_type == "column":
                    if not schema:
                        return json.dumps({"status": "error", "message": "获取列信息需要提供schema参数"})
                    
                    # 获取表结构中的列信息
                    table_parts = element_name.split('.')
                    if len(table_parts) != 2:
                        return json.dumps({"status": "error", "message": "列名格式应为'表名.列名'"})
                    
                    table_name, column_name = table_parts
                    columns = await self.connector.get_table_structure(schema, table_name)
                    
                    column_info = None
                    for col in columns:
                        if col['name'] == column_name:
                            column_info = col
                            break
                    
                    if column_info:
                        element_info = f"列 '{schema}.{table_name}.{column_name}':\n"
                        element_info += f"- 类型: {column_info['type']}\n"
                        element_info += f"- 可空: {'是' if column_info['null'] else '否'}\n"
                        element_info += f"- 键类型: {column_info['key'] if column_info['key'] else '无'}\n"
                        element_info += f"- 默认值: {column_info['default'] if column_info['default'] else '无'}\n"
                    else:
                        return json.dumps({"status": "error", "message": f"找不到列 '{schema}.{table_name}.{column_name}'"})
                
                elif element_type == "procedure":
                    if not schema:
                        return json.dumps({"status": "error", "message": "获取存储过程信息需要提供schema参数"})
                    
                    # 获取存储过程详情
                    procedure_details = await self.connector.get_procedure_details(schema, element_name)
                    
                    if 'error' in procedure_details:
                        return json.dumps({"status": "error", "message": procedure_details['error']})
                    
                    element_info = f"存储过程 '{schema}.{element_name}':\n"
                    if 'definition' in procedure_details:
                        element_info += f"定义:\n{procedure_details['definition']}\n"
                
                else:
                    return json.dumps({
                        "status": "error",
                        "message": f"不支持的元素类型: {element_type}，支持的类型: schema, table, column, procedure"
                    })
                
                # 构建提示
                messages = [
                    {"role": "system", "content": "你是一个数据库专家，可以解释数据库中的元素（表、列、存储过程等）的用途和含义。"},
                    {"role": "user", "content": f"请根据以下信息，解释这个数据库元素的可能用途和含义。只需简短解释，不要有任何其他废话。回答中文。\n\n{element_info}"}
                ]
                
                # 调用AI API
                explanation = await self._call_ai_api(messages)
                
                return json.dumps({
                    "status": "success",
                    "element_type": element_type,
                    "element_name": element_name,
                    "schema": schema,
                    "explanation": explanation.strip()
                }, ensure_ascii=False)
            
            except Exception as e:
                return json.dumps({"status": "error", "message": str(e)})
        
        return Tool(
            name=tool_name,
            description=description,
            input_schema={
                "type": "object",
                "properties": {
                    "element_type": {
                        "type": "string",
                        "description": "元素类型",
                        "enum": ["schema", "table", "column", "procedure"]
                    },
                    "element_name": {
                        "type": "string",
                        "description": "元素名称，对于column类型，格式应为'表名.列名'"
                    },
                    "schema": {
                        "type": "string",
                        "description": "schema名称，获取table、column、procedure的信息时需要"
                    }
                },
                "required": ["element_type", "element_name"]
            },
            function=explain_db
        ) 