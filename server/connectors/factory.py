from typing import Optional, Dict, Any
from urllib.parse import urlparse

from . import (
    DatabaseConnector,
    MySQLConnector,
    PostgreSQLConnector,
    SQLiteConnector,
    SQLServerConnector,
    MongoDBConnector
)

class ConnectorFactory:
    """数据库连接器工厂，根据DSN创建合适的数据库连接器"""
    
    @staticmethod
    def create_connector(dsn: str) -> DatabaseConnector:
        """
        根据DSN创建合适的数据库连接器
        :param dsn: 数据库连接字符串
        :return: 数据库连接器实例
        """
        if not dsn:
            raise ValueError("数据库连接字符串不能为空")
        
        # 解析DSN的协议部分
        parsed_url = urlparse(dsn)
        scheme = parsed_url.scheme.lower()
        
        # 根据协议选择连接器
        if scheme == 'mysql':
            return MySQLConnector(dsn)
        elif scheme == 'postgres':
            return PostgreSQLConnector(dsn)
        elif scheme == 'sqlite':
            return SQLiteConnector(dsn)
        elif scheme == 'sqlserver':
            return SQLServerConnector(dsn)
        elif scheme == 'mongodb':
            return MongoDBConnector(dsn)
        else:
            raise ValueError(f"不支持的数据库类型: {scheme}")
    
    @staticmethod
    async def create_demo_connector() -> DatabaseConnector:
        """
        创建演示用的SQLite内存数据库连接器，并初始化示例数据
        :return: SQLite连接器
        """
        connector = SQLiteConnector("sqlite::memory:")
        await connector.connect()
        
        # 创建示例表结构
        await connector.execute_query("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE,
            hire_date TEXT,
            department_id INTEGER,
            salary REAL
        )
        """)
        
        await connector.execute_query("""
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT
        )
        """)
        
        # 插入示例数据
        await connector.execute_query("""
        INSERT INTO departments (id, name, location) VALUES
        (1, 'Engineering', 'Building A'),
        (2, 'Sales', 'Building B'),
        (3, 'Marketing', 'Building B'),
        (4, 'HR', 'Building C')
        """)
        
        await connector.execute_query("""
        INSERT INTO employees (id, first_name, last_name, email, hire_date, department_id, salary) VALUES
        (1, 'John', 'Doe', 'john.doe@example.com', '2020-01-15', 1, 75000),
        (2, 'Jane', 'Smith', 'jane.smith@example.com', '2019-05-20', 2, 82000),
        (3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2021-03-10', 1, 68000),
        (4, 'Alice', 'Williams', 'alice.williams@example.com', '2018-11-05', 3, 79000),
        (5, 'Charlie', 'Brown', 'charlie.brown@example.com', '2022-02-28', 4, 62000)
        """)
        
        # 创建索引
        await connector.execute_query("CREATE INDEX idx_employee_dept ON employees(department_id)")
        await connector.execute_query("CREATE INDEX idx_employee_name ON employees(last_name, first_name)")
        
        return connector 