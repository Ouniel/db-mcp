from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Callable

from mcp.server.fastmcp import Tool

from ..connectors import DatabaseConnector

class DatabaseTool(ABC):
    """数据库工具基类"""
    
    def __init__(self, connector: DatabaseConnector):
        """
        初始化工具
        :param connector: 数据库连接器
        """
        self.connector = connector
    
    @abstractmethod
    def get_tool(self) -> Tool:
        """获取MCP工具"""
        pass

# 导出具体工具类
from .query import QueryTool
from .export import ExportTool
from .list_resources import ListResourcesTool
from .ai import AiTool

__all__ = [
    'DatabaseTool',
    'QueryTool',
    'ExportTool',
    'ListResourcesTool',
    'AiTool'
] 