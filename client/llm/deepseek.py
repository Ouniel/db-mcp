import os
import json
from typing import Any, Dict, List, Optional, Union

import httpx

from .base import LLMProvider

class DeepSeekProvider(LLMProvider):
    """DeepSeek LLM提供商"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, api_base: Optional[str] = None):
        """
        初始化DeepSeek提供商
        :param api_key: API密钥，如果不提供则尝试从环境变量获取
        :param model: 使用的模型，如果不提供则使用默认值deepseek-chat
        :param api_base: API基础URL，如果不提供则使用默认值https://api.deepseek.com/v1
        """
        # 获取API密钥
        api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("未提供DeepSeek API密钥，请通过参数或环境变量DEEPSEEK_API_KEY提供")
        
        # 获取模型
        model = model or "deepseek-chat"
        
        # 获取API基础URL
        api_base = api_base or "https://api.deepseek.com/v1"
        
        super().__init__(api_key, model, api_base)
    
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
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": 0.7
            }
            
            if tools:
                payload["tools"] = tools
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_base}/chat/completions",
                    headers=headers,
                    json=payload
                )
                
                if response.status_code != 200:
                    raise Exception(f"DeepSeek API错误: {response.status_code} {response.text}")
                
                response_json = response.json()
                
                choices = response_json.get("choices", [])
                if not choices:
                    raise Exception("DeepSeek API返回结果中没有choices字段")
                
                first_choice = choices[0]
                
                result = {
                    "id": response_json.get("id"),
                    "model": response_json.get("model"),
                    "created": response_json.get("created"),
                    "message": first_choice.get("message", {}),
                    "finish_reason": first_choice.get("finish_reason")
                }
                
                return result
            
        except Exception as e:
            raise Exception(f"DeepSeek调用错误: {str(e)}") 