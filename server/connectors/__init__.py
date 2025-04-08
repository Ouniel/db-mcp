from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

class DatabaseConnector(ABC):
    """数据库连接器的基类，定义所有数据库连接器必须实现的接口"""
    
    @abstractmethod
    async def connect(self) -> bool:
        """建立数据库连接"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """断开数据库连接"""
        pass
    
    @abstractmethod
    async def is_connected(self) -> bool:
        """检查连接状态"""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """执行SQL查询并返回结果"""
        pass
    
    @abstractmethod
    async def get_schemas(self) -> List[Dict[str, Any]]:
        """获取所有schema"""
        pass
    
    @abstractmethod
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """获取指定schema中的所有表"""
        pass
    
    @abstractmethod
    async def get_table_structure(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表结构"""
        pass
    
    @abstractmethod
    async def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表的索引"""
        pass
    
    @abstractmethod
    async def get_procedures(self, schema: str) -> List[Dict[str, Any]]:
        """获取存储过程"""
        pass
    
    @abstractmethod
    async def get_procedure_details(self, schema: str, procedure: str) -> Dict[str, Any]:
        """获取存储过程详情"""
        pass

# 导出具体实现类
from .mysql import MySQLConnector
from .postgresql import PostgreSQLConnector
from .sqlite import SQLiteConnector
from .sqlserver import SQLServerConnector
from .mongodb import MongoDBConnector
from .factory import ConnectorFactory

__all__ = [
    'DatabaseConnector',
    'MySQLConnector',
    'PostgreSQLConnector', 
    'SQLiteConnector',
    'SQLServerConnector',
    'MongoDBConnector',
    'ConnectorFactory'
] 