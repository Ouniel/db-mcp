import asyncio
from typing import Any, Dict, List, Optional, Tuple

import aiomysql
from aiomysql.cursors import DictCursor

from . import DatabaseConnector

class MySQLConnector(DatabaseConnector):
    """MySQL数据库连接器"""
    
    def __init__(self, dsn: str):
        """
        初始化MySQL连接器
        :param dsn: 数据库连接字符串，格式：mysql://user:password@host:port/dbname
        """
        self.dsn = dsn
        # 解析DSN
        parts = dsn.replace('mysql://', '').split('@')
        if len(parts) != 2:
            raise ValueError("MySQL DSN格式错误，正确格式：mysql://user:password@host:port/dbname")
        
        user_pass, host_db = parts
        user_parts = user_pass.split(':')
        if len(user_parts) != 2:
            raise ValueError("MySQL DSN中用户名密码格式错误")
        self.user, self.password = user_parts
        
        host_parts = host_db.split('/')
        if len(host_parts) < 2:
            raise ValueError("MySQL DSN中主机和数据库格式错误")
        
        host_port = host_parts[0].split(':')
        if len(host_port) == 2:
            self.host, port_str = host_port
            self.port = int(port_str)
        else:
            self.host = host_port[0]
            self.port = 3306
        
        self.database = host_parts[1]
        
        # 连接池
        self.pool = None
    
    async def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.database,
                autocommit=True,
                cursorclass=DictCursor
            )
            return True
        except Exception as e:
            print(f"MySQL连接失败: {e}")
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
        
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    if params:
                        await cursor.execute(query, params)
                    else:
                        await cursor.execute(query)
                    
                    if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN')):
                        return await cursor.fetchall()
                    else:
                        # 对于非查询操作，返回影响的行数
                        return [{'affected_rows': cursor.rowcount}]
                except Exception as e:
                    return [{'error': str(e)}]
    
    async def get_schemas(self) -> List[Dict[str, Any]]:
        """获取所有schema（在MySQL中就是数据库）"""
        query = "SHOW DATABASES"
        results = await self.execute_query(query)
        return [{'name': row['Database'], 'type': 'schema'} for row in results]
    
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """获取指定schema中的所有表"""
        query = f"SHOW TABLES FROM `{schema}`"
        results = await self.execute_query(query)
        table_name_key = f"Tables_in_{schema}"
        return [{'name': row[table_name_key], 'type': 'table', 'schema': schema} for row in results]
    
    async def get_table_structure(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表结构"""
        query = f"DESCRIBE `{schema}`.`{table}`"
        results = await self.execute_query(query)
        
        columns = []
        for row in results:
            column = {
                'name': row['Field'],
                'type': row['Type'],
                'null': row['Null'] == 'YES',
                'key': row['Key'],
                'default': row['Default'],
                'extra': row['Extra']
            }
            columns.append(column)
        
        return columns
    
    async def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表的索引"""
        query = f"SHOW INDEX FROM `{schema}`.`{table}`"
        results = await self.execute_query(query)
        
        indexes = {}
        for row in results:
            index_name = row['Key_name']
            if index_name not in indexes:
                indexes[index_name] = {
                    'name': index_name,
                    'unique': not row['Non_unique'],
                    'columns': []
                }
            
            indexes[index_name]['columns'].append({
                'name': row['Column_name'],
                'order': row['Seq_in_index']
            })
        
        return list(indexes.values())
    
    async def get_procedures(self, schema: str) -> List[Dict[str, Any]]:
        """获取存储过程"""
        query = f"SHOW PROCEDURE STATUS WHERE Db = '{schema}'"
        results = await self.execute_query(query)
        
        procedures = []
        for row in results:
            procedure = {
                'name': row['Name'],
                'type': 'PROCEDURE',
                'created': row['Created'],
                'modified': row['Modified'],
                'security_type': row['Security_type'],
                'comment': row['Comment']
            }
            procedures.append(procedure)
        
        return procedures
    
    async def get_procedure_details(self, schema: str, procedure: str) -> Dict[str, Any]:
        """获取存储过程详情"""
        query = f"SHOW CREATE PROCEDURE `{schema}`.`{procedure}`"
        results = await self.execute_query(query)
        
        if results:
            return {
                'name': procedure,
                'schema': schema,
                'definition': results[0]['Create Procedure']
            }
        
        return {'error': f'Procedure {schema}.{procedure} not found'} 