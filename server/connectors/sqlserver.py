import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
import aioodbc

from . import DatabaseConnector

class SQLServerConnector(DatabaseConnector):
    """SQL Server数据库连接器"""
    
    def __init__(self, dsn: str):
        """
        初始化SQL Server连接器
        :param dsn: 数据库连接字符串，格式：sqlserver://user:password@host:port/dbname
        """
        self.dsn = dsn
        
        # 解析DSN
        match = re.match(r'sqlserver://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', dsn)
        if not match:
            raise ValueError("SQL Server DSN格式错误，正确格式：sqlserver://user:password@host:port/dbname")
        
        self.user = match.group(1)
        self.password = match.group(2)
        self.host = match.group(3)
        self.port = match.group(4)
        self.database = match.group(5)
        
        # 创建ODBC连接字符串
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password}"
        )
        
        self.pool = None
    
    async def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.pool = await aioodbc.create_pool(dsn=self.connection_string, autocommit=True)
            return True
        except Exception as e:
            print(f"SQL Server连接失败: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """断开数据库连接"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
            return True
        return False
    
    async def is_connected(self) -> bool:
        """检查连接状态"""
        return self.pool is not None
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """执行SQL查询并返回结果"""
        if not self.pool:
            await self.connect()
        
        try:
            async with self.pool.acquire() as connection:
                async with connection.cursor() as cursor:
                    # SQL Server使用?作为参数占位符
                    if params:
                        # 将命名参数转换为位置参数
                        param_names = re.findall(r':(\w+)', query)
                        for name in param_names:
                            query = query.replace(f':{name}', '?')
                        param_values = [params[name] for name in param_names]
                        await cursor.execute(query, param_values)
                    else:
                        await cursor.execute(query)
                    
                    # 获取列名
                    columns = [column[0] for column in cursor.description] if cursor.description else []
                    
                    # 获取结果
                    if query.strip().upper().startswith(('SELECT', 'EXEC', 'WITH')):
                        rows = await cursor.fetchall()
                        result = []
                        for row in rows:
                            result.append(dict(zip(columns, row)))
                        return result
                    else:
                        # 对于非查询操作，返回影响的行数
                        return [{'affected_rows': cursor.rowcount}]
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_schemas(self) -> List[Dict[str, Any]]:
        """获取所有schema"""
        query = """
        SELECT 
            schema_name AS name,
            'schema' AS type
        FROM 
            information_schema.schemata
        WHERE 
            schema_name NOT IN ('sys', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY 
            schema_name
        """
        return await self.execute_query(query)
    
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """获取指定schema中的所有表"""
        query = """
        SELECT 
            table_name AS name,
            'table' AS type,
            table_schema AS schema
        FROM 
            information_schema.tables
        WHERE 
            table_schema = :schema
            AND table_type = 'BASE TABLE'
        ORDER BY 
            table_name
        """
        return await self.execute_query(query, {'schema': schema})
    
    async def get_table_structure(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表结构"""
        query = """
        SELECT 
            c.COLUMN_NAME AS name,
            c.DATA_TYPE + 
              CASE 
                WHEN c.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar') THEN '(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                WHEN c.DATA_TYPE IN ('decimal', 'numeric') THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
                ELSE ''
              END AS type,
            CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS null,
            CASE 
                WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI'
                WHEN fk.COLUMN_NAME IS NOT NULL THEN 'FOR'
                WHEN c.COLUMN_NAME IN (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                                      WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsUniqueCnst') = 1
                                      AND TABLE_SCHEMA = :schema AND TABLE_NAME = :table) THEN 'UNI'
                ELSE ''
            END AS key,
            c.COLUMN_DEFAULT AS default,
            CASE WHEN c.IS_IDENTITY = 'YES' THEN 'auto_increment' ELSE '' END AS extra
        FROM 
            INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = :schema
                AND tc.TABLE_NAME = :table
        ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
        LEFT JOIN (
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                AND tc.TABLE_SCHEMA = :schema
                AND tc.TABLE_NAME = :table
        ) fk ON c.COLUMN_NAME = fk.COLUMN_NAME
        WHERE 
            c.TABLE_SCHEMA = :schema
            AND c.TABLE_NAME = :table
        ORDER BY 
            c.ORDINAL_POSITION
        """
        return await self.execute_query(query, {'schema': schema, 'table': table})
    
    async def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表的索引"""
        query = """
        SELECT 
            i.name AS name,
            i.is_unique AS unique,
            col.name AS column_name,
            ic.key_ordinal AS key_ordinal
        FROM 
            sys.indexes i
        INNER JOIN 
            sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN 
            sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
        INNER JOIN 
            sys.objects o ON i.object_id = o.object_id
        INNER JOIN 
            sys.schemas s ON o.schema_id = s.schema_id
        WHERE 
            s.name = :schema
            AND o.name = :table
            AND i.is_primary_key = 0
            AND i.is_unique_constraint = 0
        ORDER BY 
            i.name, ic.key_ordinal
        """
        results = await self.execute_query(query, {'schema': schema, 'table': table})
        
        # 处理索引列
        indexes = {}
        for row in results:
            index_name = row['name']
            if index_name not in indexes:
                indexes[index_name] = {
                    'name': index_name,
                    'unique': bool(row['unique']),
                    'columns': []
                }
            
            indexes[index_name]['columns'].append({
                'name': row['column_name'],
                'order': row['key_ordinal']
            })
        
        return list(indexes.values())
    
    async def get_procedures(self, schema: str) -> List[Dict[str, Any]]:
        """获取存储过程"""
        query = """
        SELECT 
            ROUTINE_NAME AS name,
            ROUTINE_TYPE AS type,
            CREATED AS created,
            LAST_ALTERED AS modified
        FROM 
            INFORMATION_SCHEMA.ROUTINES
        WHERE 
            ROUTINE_SCHEMA = :schema
            AND ROUTINE_TYPE = 'PROCEDURE'
        ORDER BY 
            ROUTINE_NAME
        """
        return await self.execute_query(query, {'schema': schema})
    
    async def get_procedure_details(self, schema: str, procedure: str) -> Dict[str, Any]:
        """获取存储过程详情"""
        query = """
        SELECT 
            OBJECT_DEFINITION(OBJECT_ID(:schema_proc)) AS definition,
            :procedure AS name,
            :schema AS schema
        """
        results = await self.execute_query(query, {
            'schema_proc': f'{schema}.{procedure}',
            'procedure': procedure,
            'schema': schema
        })
        
        if results and results[0]['definition']:
            return results[0]
        return {'error': f'Procedure {schema}.{procedure} not found'} 