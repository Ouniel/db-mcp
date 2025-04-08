import os
import json
import asyncio
from typing import Any, Dict, List, Optional, Union, Tuple, Literal
from contextlib import AsyncExitStack

from mcp.client import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .llm import LLMProvider, OpenAIProvider, DeepSeekProvider

class DatabaseMCPClient:
    """数据库MCP客户端，连接到MCP服务器并与LLM交互"""
    
    def __init__(self, 
                 servers: Dict[str, Union[str, Dict[str, Any]]],
                 llm_provider: Literal["openai", "deepseek"] = "openai",
                 llm_api_key: Optional[str] = None,
                 llm_model: Optional[str] = None,
                 llm_api_base: Optional[str] = None):
        """
        初始化客户端
        :param servers: 服务器配置，格式为 {"server_name": "script_path"} 或 {"server_name": {"command": "command", "args": ["arg1", "arg2"]}}
        :param llm_provider: LLM提供商，可选值：openai, deepseek
        :param llm_api_key: LLM API密钥，如果不提供则尝试从环境变量获取
        :param llm_model: 使用的LLM模型，如果不提供则使用默认值
        :param llm_api_base: LLM API基础URL，如果不提供则使用默认值
        """
        self.servers = servers
        self.exit_stack = AsyncExitStack()
        self.sessions = {}
        self.tools_by_session = {}
        self.all_tools = []
        
        # 创建LLM提供商
        if llm_provider == "openai":
            self.llm = OpenAIProvider(api_key=llm_api_key, model=llm_model, api_base=llm_api_base)
        elif llm_provider == "deepseek":
            self.llm = DeepSeekProvider(api_key=llm_api_key, model=llm_model, api_base=llm_api_base)
        else:
            raise ValueError(f"不支持的LLM提供商: {llm_provider}，支持的提供商: openai, deepseek")
    
    async def connect_to_servers(self):
        """连接到所有服务器并获取工具"""
        for server_name, server_config in self.servers.items():
            try:
                # 根据配置启动服务器
                if isinstance(server_config, str):
                    # 使用脚本路径启动服务器
                    script_path = server_config
                    is_python = script_path.endswith('.py')
                    is_js = script_path.endswith('.js')
                    
                    if not (is_python or is_js):
                        raise ValueError(f"服务器脚本必须是.py或.js文件: {script_path}")
                    
                    command = "python" if is_python else "node"
                    server_params = StdioServerParameters(
                        command=command,
                        args=[script_path],
                        env=None
                    )
                else:
                    # 使用命令和参数启动服务器
                    if not isinstance(server_config, dict) or 'command' not in server_config or 'args' not in server_config:
                        raise ValueError(f"服务器配置格式错误: {server_config}")
                    
                    server_params = StdioServerParameters(
                        command=server_config['command'],
                        args=server_config['args'],
                        env=server_config.get('env')
                    )
                
                # 启动服务器并获取会话
                stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
                read_stream, write_stream = stdio_transport
                session = await self.exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
                await session.initialize()
                
                self.sessions[server_name] = session
                
                # 获取工具列表
                resp = await session.list_tools()
                self.tools_by_session[server_name] = resp.tools
                
                # 添加工具到全局工具列表，为每个工具名称添加前缀
                for tool in resp.tools:
                    # 使用OpenAI Function Calling格式
                    function_name = f"{server_name}_{tool.name}"
                    self.all_tools.append({
                        "type": "function",
                        "function": {
                            "name": function_name,
                            "description": tool.description,
                            "parameters": self._convert_schema(tool.input_schema)
                        }
                    })
                
                print(f"连接到服务器: {server_name}")
            except Exception as e:
                print(f"连接到服务器 {server_name} 失败: {e}")
        
        # 打印工具列表
        print("\n可用工具:")
        for tool in self.all_tools:
            print(f"  - {tool['function']['name']}: {tool['function']['description']}")
    
    def _convert_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """将MCP schema转换为OpenAI Function Calling格式"""
        result = {}
        
        if "type" in schema:
            result["type"] = schema["type"]
        
        if "properties" in schema:
            result["properties"] = schema["properties"]
        
        if "required" in schema:
            result["required"] = schema["required"]
        
        return result
    
    async def chat(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        与LLM对话
        :param messages: 消息列表，每个消息是一个字典，包含role和content字段
        :return: 对话结果
        """
        # 第一次调用，直接传递工具列表
        response = await self.llm.chat_completion(messages, self.all_tools)
        
        # 检查是否调用了工具
        if response.get("finish_reason") == "tool_calls" and "tool_calls" in response.get("message", {}):
            # 添加LLM的响应到消息中
            messages.append(response["message"])
            
            # 处理工具调用
            for tool_call in response["message"]["tool_calls"]:
                if tool_call["type"] == "function":
                    function_name = tool_call["function"]["name"]
                    arguments = json.loads(tool_call["function"]["arguments"])
                    
                    # 调用工具并获取结果
                    tool_result = await self._call_mcp_tool(function_name, arguments)
                    
                    # 添加工具结果到消息中
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call["id"],
                        "name": function_name,
                        "content": tool_result
                    })
            
            # 再次调用LLM，不传递工具列表
            return await self.llm.chat_completion(messages)
        
        # 没有工具调用，直接返回结果
        return response
    
    async def _call_mcp_tool(self, tool_full_name: str, arguments: Dict[str, Any]) -> str:
        """
        调用MCP工具
        :param tool_full_name: 工具全名，格式为"server_name_tool_name"
        :param arguments: 工具参数
        :return: 工具调用结果
        """
        try:
            # 拆分服务器名称和工具名称
            parts = tool_full_name.split('_', 1)
            if len(parts) != 2:
                return f"无效的工具名称: {tool_full_name}"
            
            server_name, tool_name = parts
            
            # 获取会话
            session = self.sessions.get(server_name)
            if not session:
                return f"找不到服务器: {server_name}"
            
            # 调用工具
            resp = await session.call_tool(tool_name, arguments)
            
            return resp.content if resp.content else "工具执行无输出"
        except Exception as e:
            return f"工具调用错误: {str(e)}"
    
    async def chat_loop(self):
        """交互式聊天循环"""
        print("\n数据库MCP客户端已启动，输入 'quit' 退出")
        
        # 连接到服务器
        await self.connect_to_servers()
        
        # 初始化消息列表
        messages = []
        
        while True:
            try:
                # 获取用户输入
                user_input = input("\n用户: ")
                if user_input.lower() == 'quit':
                    break
                
                # 添加用户消息
                messages.append({"role": "user", "content": user_input})
                
                # 保持消息历史不超过20条
                messages = messages[-20:]
                
                # 调用LLM
                response = await self.chat(messages)
                
                # 获取回复内容
                content = response.get("message", {}).get("content", "")
                
                # 打印回复
                print(f"\nAI: {content}")
            except Exception as e:
                print(f"\n错误: {e}")
    
    async def cleanup(self):
        """清理资源"""
        await self.exit_stack.aclose()
        print("客户端已关闭") 