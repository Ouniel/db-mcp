import asyncio
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union

import aiosqlite

from . import DatabaseConnector

class SQLiteConnector(DatabaseConnector):
    """SQLite数据库连接器"""
    
    def __init__(self, dsn: str):
        """
        初始化SQLite连接器
        :param dsn: 数据库连接字符串，格式：sqlite:///path/to/database.db 或 sqlite::memory:
        """
        self.dsn = dsn
        
        # 解析DSN
        if dsn.startswith('sqlite:///'):
            self.db_path = dsn[10:]  # 去掉 'sqlite:///'
        elif dsn == 'sqlite::memory:':
            self.db_path = ':memory:'
        else:
            raise ValueError("SQLite DSN格式错误，正确格式：sqlite:///path/to/database.db 或 sqlite::memory:")
        
        # 检查文件存在性（内存数据库除外）
        if self.db_path != ':memory:' and not os.path.exists(self.db_path):
            raise FileNotFoundError(f"SQLite数据库文件不存在: {self.db_path}")
        
        self.conn = None
    
    async def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            # 设置行工厂以返回字典
            self.conn.row_factory = self._dict_factory
            return True
        except Exception as e:
            print(f"SQLite连接失败: {e}")
            return False
    
    def _dict_factory(self, cursor, row):
        """将行转换为字典"""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    
    async def disconnect(self) -> bool:
        """断开数据库连接"""
        if self.conn:
            await self.conn.close()
            self.conn = None
            return True
        return False
    
    async def is_connected(self) -> bool:
        """检查连接状态"""
        return self.conn is not None
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """执行SQL查询并返回结果"""
        if not self.conn:
            await self.connect()
        
        try:
            async with self.conn.execute(query, params or []) as cursor:
                result = await cursor.fetchall()
                
                # 对于非查询语句，检查受影响的行数
                if not result and query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                    return [{'affected_rows': cursor.rowcount}]
                
                return result
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_schemas(self) -> List[Dict[str, Any]]:
        """获取所有schema (SQLite没有schema的概念，返回main)"""
        return [{'name': 'main', 'type': 'schema'}]
    
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """获取指定schema中的所有表"""
        query = """
        SELECT 
            name,
            'table' AS type,
            'main' AS schema
        FROM 
            sqlite_master
        WHERE 
            type = 'table' 
            AND name NOT LIKE 'sqlite_%'
        ORDER BY 
            name
        """
        return await self.execute_query(query)
    
    async def get_table_structure(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表结构"""
        # SQLite的PRAGMA使我们可以获取表结构
        query = f"PRAGMA table_info({table})"
        result = await self.execute_query(query)
        
        columns = []
        for row in result:
            column = {
                'name': row['name'],
                'type': row['type'],
                'null': row['notnull'] == 0,
                'key': 'PRI' if row['pk'] == 1 else '',
                'default': row['dflt_value'],
                'extra': ''
            }
            columns.append(column)
        
        return columns
    
    async def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表的索引"""
        query = f"PRAGMA index_list({table})"
        result = await self.execute_query(query)
        
        indexes = []
        for idx in result:
            index_name = idx['name']
            # 获取索引列
            index_detail_query = f"PRAGMA index_info({index_name})"
            index_detail = await self.execute_query(index_detail_query)
            
            columns = []
            for col in index_detail:
                columns.append({
                    'name': col['name'],
                    'order': col['seqno'] + 1  # SQLite从0开始
                })
            
            indexes.append({
                'name': index_name,
                'unique': idx['unique'] == 1,
                'columns': columns
            })
        
        return indexes
    
    async def get_procedures(self, schema: str) -> List[Dict[str, Any]]:
        """获取存储过程（SQLite不支持存储过程，返回空列表）"""
        return []
    
    async def get_procedure_details(self, schema: str, procedure: str) -> Dict[str, Any]:
        """获取存储过程详情（SQLite不支持存储过程）"""
        return {'error': 'SQLite does not support stored procedures'} 