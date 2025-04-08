from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

class LLMProvider(ABC):
    """LLM提供商基类"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None, api_base: Optional[str] = None):
        """
        初始化LLM提供商
        :param api_key: API密钥，如果不提供则尝试从环境变量获取
        :param model: 使用的模型，如果不提供则使用默认值
        :param api_base: API基础URL，如果不提供则使用默认值
        """
        self.api_key = api_key
        self.model = model
        self.api_base = api_base
    
    @abstractmethod
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
        pass 