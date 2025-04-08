import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from . import DatabaseConnector

class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL数据库连接器"""
    
    def __init__(self, dsn: str):
        """
        初始化PostgreSQL连接器
        :param dsn: 数据库连接字符串，格式：postgres://user:password@host:port/dbname?sslmode=disable
        """
        self.dsn = dsn
        self.conn = None
    
    async def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.conn = await asyncpg.connect(self.dsn)
            return True
        except Exception as e:
            print(f"PostgreSQL连接失败: {e}")
            return False
    
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
            # 将命名参数转换为位置参数
            if params:
                # 找出查询中的所有命名参数
                param_names = re.findall(r':(\w+)', query)
                # 替换查询中的命名参数为$1, $2, ...
                for i, name in enumerate(param_names, 1):
                    query = query.replace(f':{name}', f'${i}')
                # 获取参数值
                param_values = [params[name] for name in param_names]
                stmt = await self.conn.prepare(query)
                result = await stmt.fetch(*param_values)
            else:
                result = await self.conn.fetch(query)
            
            # 将查询结果转换为字典列表
            return [dict(row) for row in result]
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_schemas(self) -> List[Dict[str, Any]]:
        """获取所有schema"""
        query = """
        SELECT 
            nspname AS name,
            'schema' AS type
        FROM 
            pg_catalog.pg_namespace
        WHERE 
            nspname NOT LIKE 'pg_%' 
            AND nspname != 'information_schema'
        ORDER BY 
            nspname
        """
        return await self.execute_query(query)
    
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """获取指定schema中的所有表"""
        query = """
        SELECT 
            tablename AS name,
            'table' AS type,
            schemaname AS schema
        FROM 
            pg_catalog.pg_tables
        WHERE 
            schemaname = $1
        ORDER BY 
            tablename
        """
        return await self.execute_query(query, {'schema': schema})
    
    async def get_table_structure(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表结构"""
        query = """
        SELECT 
            a.attname AS name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
            CASE WHEN a.attnotnull = false THEN true ELSE false END AS null,
            CASE 
                WHEN (SELECT COUNT(*) FROM pg_constraint 
                      WHERE conrelid = a.attrelid AND conkey[1] = a.attnum AND contype = 'p') > 0 THEN 'PRI'
                WHEN (SELECT COUNT(*) FROM pg_constraint 
                      WHERE conrelid = a.attrelid AND conkey[1] = a.attnum AND contype = 'u') > 0 THEN 'UNI'
                ELSE ''
            END AS key,
            (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid) 
             FROM pg_catalog.pg_attrdef d
             WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS default,
            '' AS extra
        FROM 
            pg_catalog.pg_attribute a
        JOIN 
            pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN 
            pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE 
            n.nspname = $1
            AND c.relname = $2
            AND a.attnum > 0 
            AND NOT a.attisdropped
        ORDER BY 
            a.attnum
        """
        return await self.execute_query(query, {'schema': schema, 'table': table})
    
    async def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表的索引"""
        query = """
        SELECT 
            i.relname AS name,
            idx.indisunique AS unique,
            array_agg(a.attname ORDER BY array_position(idx.indkey, a.attnum)) AS columns
        FROM 
            pg_catalog.pg_class t
        JOIN 
            pg_catalog.pg_namespace n ON t.relnamespace = n.oid
        JOIN 
            pg_catalog.pg_index idx ON t.oid = idx.indrelid
        JOIN 
            pg_catalog.pg_class i ON idx.indexrelid = i.oid
        JOIN 
            pg_catalog.pg_attribute a ON t.oid = a.attrelid AND a.attnum = ANY(idx.indkey)
        WHERE 
            n.nspname = $1
            AND t.relname = $2
        GROUP BY 
            i.relname, idx.indisunique
        ORDER BY 
            i.relname
        """
        results = await self.execute_query(query, {'schema': schema, 'table': table})
        
        # 处理索引列
        for index in results:
            columns = index.pop('columns')
            index['columns'] = [{'name': col, 'order': i+1} for i, col in enumerate(columns)]
        
        return results
    
    async def get_procedures(self, schema: str) -> List[Dict[str, Any]]:
        """获取存储过程（在PostgreSQL中称为函数）"""
        query = """
        SELECT 
            p.proname AS name,
            CASE p.prokind 
                WHEN 'f' THEN 'FUNCTION'
                WHEN 'p' THEN 'PROCEDURE'
                ELSE p.prokind::text
            END AS type,
            pg_catalog.pg_get_function_arguments(p.oid) AS args,
            pg_catalog.obj_description(p.oid, 'pg_proc') AS comment
        FROM 
            pg_catalog.pg_proc p
        JOIN 
            pg_catalog.pg_namespace n ON p.pronamespace = n.oid
        WHERE 
            n.nspname = $1
        ORDER BY 
            p.proname
        """
        return await self.execute_query(query, {'schema': schema})
    
    async def get_procedure_details(self, schema: str, procedure: str) -> Dict[str, Any]:
        """获取存储过程详情"""
        query = """
        SELECT 
            p.proname AS name,
            n.nspname AS schema,
            CASE p.prokind 
                WHEN 'f' THEN 'FUNCTION'
                WHEN 'p' THEN 'PROCEDURE'
                ELSE p.prokind::text
            END AS type,
            pg_catalog.pg_get_function_arguments(p.oid) AS args,
            pg_catalog.pg_get_function_result(p.oid) AS result_type,
            pg_catalog.pg_get_functiondef(p.oid) AS definition
        FROM 
            pg_catalog.pg_proc p
        JOIN 
            pg_catalog.pg_namespace n ON p.pronamespace = n.oid
        WHERE 
            n.nspname = $1
            AND p.proname = $2
        """
        results = await self.execute_query(query, {'schema': schema, 'procedure': procedure})
        if results:
            return results[0]
        return {'error': f'Procedure {schema}.{procedure} not found'} 