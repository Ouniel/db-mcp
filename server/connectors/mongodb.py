import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse, parse_qs

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from . import DatabaseConnector

class MongoDBConnector(DatabaseConnector):
    """MongoDB数据库连接器"""
    
    def __init__(self, dsn: str):
        """
        初始化MongoDB连接器
        :param dsn: 数据库连接字符串，格式：mongodb://user:password@host:port/dbname
        """
        self.dsn = dsn
        self.client = None
        self.db = None
        
        # 解析DSN
        parsed_url = urlparse(dsn)
        if parsed_url.scheme != 'mongodb':
            raise ValueError("MongoDB DSN格式错误，正确格式：mongodb://user:password@host:port/dbname")
        
        # 获取数据库名称
        path = parsed_url.path.strip('/')
        if not path:
            raise ValueError("MongoDB DSN中未指定数据库名称")
        
        self.database_name = path
    
    async def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.client = AsyncIOMotorClient(self.dsn)
            self.db = self.client[self.database_name]
            # 检查连接是否有效
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            print(f"MongoDB连接失败: {e}")
            return False
    
    async def disconnect(self) -> bool:
        """断开数据库连接"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            return True
        return False
    
    async def is_connected(self) -> bool:
        """检查连接状态"""
        if not self.client:
            return False
        try:
            # 尝试执行一个简单的命令来检查连接状态
            await self.client.admin.command('ping')
            return True
        except:
            return False
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        执行MongoDB查询
        注意：在MongoDB中，查询是JSON格式，我们假设query是一个JSON字符串或特殊格式的查询语法
        params可能包含集合名称和其他参数
        """
        if not self.client or not self.db:
            await self.connect()
        
        try:
            import json
            
            # 如果query是JSON字符串，解析它
            try:
                query_obj = json.loads(query)
            except json.JSONDecodeError:
                # 如果不是有效的JSON，尝试解析为特殊语法
                # 格式: COLLECTION_NAME.OPERATION(QUERY_JSON)
                match = re.match(r'(\w+)\.(find|findOne|aggregate|count|distinct)\((.*)\)', query)
                if not match:
                    return [{'error': f"无效的MongoDB查询格式: {query}"}]
                
                collection_name = match.group(1)
                operation = match.group(2)
                try:
                    query_args = json.loads(match.group(3))
                except json.JSONDecodeError:
                    return [{'error': f"无效的查询参数JSON: {match.group(3)}"}]
                
                collection = self.db[collection_name]
                result = []
                
                if operation == 'find':
                    filter_dict = query_args.get('filter', {})
                    projection = query_args.get('projection', None)
                    limit = query_args.get('limit', 0)
                    skip = query_args.get('skip', 0)
                    sort = query_args.get('sort', None)
                    
                    cursor = collection.find(filter_dict, projection)
                    if skip:
                        cursor = cursor.skip(skip)
                    if limit:
                        cursor = cursor.limit(limit)
                    if sort:
                        cursor = cursor.sort(sort)
                        
                    async for document in cursor:
                        # 将ObjectId转换为字符串
                        document['_id'] = str(document['_id'])
                        result.append(document)
                
                elif operation == 'findOne':
                    filter_dict = query_args.get('filter', {})
                    projection = query_args.get('projection', None)
                    
                    document = await collection.find_one(filter_dict, projection)
                    if document:
                        document['_id'] = str(document['_id'])
                        result.append(document)
                
                elif operation == 'aggregate':
                    pipeline = query_args if isinstance(query_args, list) else query_args.get('pipeline', [])
                    
                    async for document in collection.aggregate(pipeline):
                        if '_id' in document:
                            document['_id'] = str(document['_id'])
                        result.append(document)
                
                elif operation == 'count':
                    filter_dict = query_args.get('filter', {})
                    count = await collection.count_documents(filter_dict)
                    result.append({'count': count})
                
                elif operation == 'distinct':
                    field = query_args.get('field', '')
                    filter_dict = query_args.get('filter', {})
                    
                    values = await collection.distinct(field, filter_dict)
                    result.append({'values': values})
                
                return result
            
            # 如果是JSON对象，解析格式为{collection: '集合名', operation: '操作', query: {查询}}
            collection_name = query_obj.get('collection', '')
            operation = query_obj.get('operation', 'find')
            query_filter = query_obj.get('filter', {})
            options = query_obj.get('options', {})
            
            if not collection_name:
                return [{'error': "必须指定集合名称"}]
            
            collection = self.db[collection_name]
            result = []
            
            if operation == 'find':
                projection = options.get('projection', None)
                limit = options.get('limit', 0)
                skip = options.get('skip', 0)
                sort = options.get('sort', None)
                
                cursor = collection.find(query_filter, projection)
                if skip:
                    cursor = cursor.skip(skip)
                if limit:
                    cursor = cursor.limit(limit)
                if sort:
                    cursor = cursor.sort(sort)
                    
                async for document in cursor:
                    # 将ObjectId转换为字符串
                    document['_id'] = str(document['_id'])
                    result.append(document)
            
            elif operation == 'findOne':
                projection = options.get('projection', None)
                
                document = await collection.find_one(query_filter, projection)
                if document:
                    document['_id'] = str(document['_id'])
                    result.append(document)
            
            elif operation == 'aggregate':
                pipeline = query_obj.get('pipeline', [])
                
                async for document in collection.aggregate(pipeline):
                    if '_id' in document:
                        document['_id'] = str(document['_id'])
                    result.append(document)
            
            elif operation == 'count':
                count = await collection.count_documents(query_filter)
                result.append({'count': count})
            
            elif operation == 'insert':
                documents = query_obj.get('documents', [])
                if not isinstance(documents, list):
                    documents = [documents]
                
                if len(documents) == 1:
                    inserted = await collection.insert_one(documents[0])
                    result.append({'inserted_id': str(inserted.inserted_id)})
                else:
                    inserted = await collection.insert_many(documents)
                    result.append({'inserted_ids': [str(id) for id in inserted.inserted_ids]})
            
            elif operation == 'update':
                update_data = query_obj.get('update', {})
                if not update_data:
                    return [{'error': "更新操作必须提供update字段"}]
                
                many = options.get('many', False)
                upsert = options.get('upsert', False)
                
                if many:
                    updated = await collection.update_many(query_filter, update_data, upsert=upsert)
                    result.append({
                        'matched_count': updated.matched_count,
                        'modified_count': updated.modified_count,
                        'upserted_id': str(updated.upserted_id) if updated.upserted_id else None
                    })
                else:
                    updated = await collection.update_one(query_filter, update_data, upsert=upsert)
                    result.append({
                        'matched_count': updated.matched_count,
                        'modified_count': updated.modified_count,
                        'upserted_id': str(updated.upserted_id) if updated.upserted_id else None
                    })
            
            elif operation == 'delete':
                many = options.get('many', False)
                
                if many:
                    deleted = await collection.delete_many(query_filter)
                    result.append({'deleted_count': deleted.deleted_count})
                else:
                    deleted = await collection.delete_one(query_filter)
                    result.append({'deleted_count': deleted.deleted_count})
            
            else:
                return [{'error': f"不支持的操作: {operation}"}]
            
            return result
            
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_schemas(self) -> List[Dict[str, Any]]:
        """获取所有schema (在MongoDB中就是数据库)"""
        if not self.client:
            await self.connect()
        
        try:
            db_names = await self.client.list_database_names()
            # 过滤掉系统数据库
            db_names = [db for db in db_names if db not in ['admin', 'local', 'config']]
            return [{'name': db, 'type': 'database'} for db in db_names]
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """获取指定schema中的所有表 (在MongoDB中就是集合)"""
        if not self.client:
            await self.connect()
        
        try:
            # 如果指定的是当前数据库，使用self.db，否则获取指定数据库
            if schema == self.database_name:
                db = self.db
            else:
                db = self.client[schema]
            
            collection_names = await db.list_collection_names()
            return [{'name': coll, 'type': 'collection', 'schema': schema} for coll in collection_names]
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_table_structure(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """
        获取表结构 (在MongoDB中通过检查一个文档的结构来模拟)
        在MongoDB中这个操作比较特殊，我们会尝试获取集合的一个文档，然后分析它的结构
        """
        if not self.client:
            await self.connect()
        
        try:
            # 如果指定的是当前数据库，使用self.db，否则获取指定数据库
            if schema == self.database_name:
                db = self.db
            else:
                db = self.client[schema]
            
            collection = db[table]
            # 获取第一个文档来分析结构
            document = await collection.find_one()
            
            if not document:
                return [{'name': '_id', 'type': 'ObjectId', 'key': 'PRI'}]
            
            # 从文档中提取字段信息
            fields = []
            for field_name, value in document.items():
                field_type = type(value).__name__
                is_primary = field_name == '_id'
                
                fields.append({
                    'name': field_name,
                    'type': field_type,
                    'key': 'PRI' if is_primary else '',
                    'null': True,  # MongoDB字段默认允许为空
                    'default': None
                })
            
            return fields
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """获取表的索引"""
        if not self.client:
            await self.connect()
        
        try:
            # 如果指定的是当前数据库，使用self.db，否则获取指定数据库
            if schema == self.database_name:
                db = self.db
            else:
                db = self.client[schema]
            
            collection = db[table]
            # 获取索引信息
            indexes = await collection.index_information()
            
            result = []
            for index_name, index_info in indexes.items():
                keys = index_info['key']
                unique = 'unique' in index_info and index_info['unique']
                
                result.append({
                    'name': index_name,
                    'fields': [k[0] for k in keys],
                    'unique': unique,
                    'type': 'index'
                })
            
            return result
        except Exception as e:
            return [{'error': str(e)}]
    
    async def get_procedures(self, schema: str) -> List[Dict[str, Any]]:
        """
        获取存储过程 (MongoDB没有传统的存储过程，返回空列表)
        在MongoDB中可以考虑返回服务器脚本或函数，但这里简化处理
        """
        return []
    
    async def get_procedure_details(self, schema: str, procedure: str) -> Dict[str, Any]:
        """
        获取存储过程详情 (MongoDB没有传统的存储过程)
        """
        return {'error': 'MongoDB不支持传统的存储过程'} 