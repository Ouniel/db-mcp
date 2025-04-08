import os
from typing import Any, Dict, List, Optional, Union

import openai

from .base import LLMProvider

class OpenAIProvider(LLMProvider):
    """OpenAI LLM提供商"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, api_base: Optional[str] = None):
        """
        初始化OpenAI提供商
        :param api_key: API密钥，如果不提供则尝试从环境变量获取
        :param model: 使用的模型，如果不提供则使用默认值gpt-3.5-turbo
        :param api_base: API基础URL，如果不提供则使用默认值https://api.openai.com/v1
        """
        # 获取API密钥
        api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("未提供OpenAI API密钥，请通过参数或环境变量OPENAI_API_KEY提供")
        
        # 获取模型
        model = model or "gpt-3.5-turbo"
        
        # 获取API基础URL
        api_base = api_base or "https://api.openai.com/v1"
        
        super().__init__(api_key, model, api_base)
        
        # 初始化OpenAI客户端
        if api_base == "https://api.openai.com/v1":
            self.client = openai.OpenAI(api_key=api_key)
        else:
            self.client = openai.OpenAI(api_key=api_key, base_url=api_base)
    
    async def chat_completion(self, 
                              messages: List[Dict[str, str]], 
                              tools: Optional[List[Dict[str, Any]]] = None
                             ) -> Dict[str, Any]:
        """
        对话完成
        :param messages: 消息列表，每个消息是一个字典，包含role和content字段
        :param tools: 工具列表（仅当支持Function Calling时使用）
        :return: 完成结果
        """
        try:
            kwargs = {
                "model": self.model,
                "messages": messages,
                "temperature": 0.7,
            }
            
            if tools:
                kwargs["tools"] = tools
            
            response = self.client.chat.completions.create(**kwargs)
            
            choices = response.choices
            first_choice = choices[0]
            
            result = {
                "id": response.id,
                "model": response.model,
                "created": response.created,
                "message": {
                    "role": first_choice.message.role,
                    "content": first_choice.message.content
                },
                "finish_reason": first_choice.finish_reason
            }
            
            # 如果有工具调用
            if first_choice.finish_reason == "tool_calls" and hasattr(first_choice.message, "tool_calls"):
                tool_calls = []
                for tool_call in first_choice.message.tool_calls:
                    tool_calls.append({
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "function": {
                            "name": tool_call.function.name,
                            "arguments": tool_call.function.arguments
                        }
                    })
                result["message"]["tool_calls"] = tool_calls
            
            return result
        except Exception as e:
            raise Exception(f"OpenAI调用错误: {str(e)}")