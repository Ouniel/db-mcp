# DB-MCP: 通用数据库MCP服务器和客户端

DB-MCP是一个基于Model Context Protocol (MCP)的通用数据库服务器和客户端，支持连接多种数据库，包括MySQL、PostgreSQL、SQLite、SQL Server和MongoDB。它允许使用OpenAI或DeepSeek API与数据库进行自然语言交互。

## 功能特点

- 支持多种数据库：MySQL、PostgreSQL、SQLite、SQL Server、MongoDB
- 支持同时连接多个不同类型的数据库，并在一个会话中进行跨库操作
- 支持多种MCP传输协议：stdio、sse
- 支持多种LLM提供商：OpenAI、DeepSeek
- 内置演示数据库，方便快速入门
- 提供数据库浏览、查询、导出等功能
- 支持AI驱动的SQL生成和数据库解释

## 系统架构

```
+---------------------+                   +------------------+
|                     |                   |                  |
|   客户端程序         |                   |   MCP服务器       |
|  +--------------+   |                   |                  |
|  | MCP客户端组件 |<--+--- MCP协议 ------>|                  |
|  +--------------+   |                   |                  |
|         |           |                   |                  |
|         v           |                   |                  |
|  +--------------+   |                   |                  |
|  |  LLM API交互  |   |                   |                  |
|  +--------------+   |                   |                  |
|                     |                   |       |          |
+---------------------+                   +-------+----------+
                                                  |
                                                  v
                                          +-----------------+
                                          |                 |
                                          |  各种数据库      |
                                          |  MySQL          |
                                          |  PostgreSQL     |
                                          |  SQLite         |
                                          |  SQL Server     |
                                          |  MongoDB        |
                                          |                 |
                                          +-----------------+
```

## 安装

### 要求

- Python 3.8+
- 数据库驱动器（根据需要）：
  - MySQL: aiomysql
  - PostgreSQL: asyncpg
  - SQLite: aiosqlite
  - SQL Server: pyodbc, aioodbc
- 其他依赖：
  - mcp: MCP协议
  - openai: OpenAI API
  - httpx: HTTP客户端
  - python-dotenv: 环境变量

### 安装步骤

1. 克隆仓库
   ```
   git clone https://github.com/yourusername/db-mcp.git
   cd db-mcp
   ```

2. 安装依赖
   ```
   pip install -r requirements.txt
   ```

3. 配置环境变量
   ```
   cp config/example.env .env
   # 编辑.env文件，设置数据库连接字符串和API密钥
   ```

## 使用方法

### 服务器模式

启动服务器模式，连接到数据库并提供MCP服务：

```bash
# 使用演示数据库（SQLite内存数据库）
python main.py server --demo

# 连接到MySQL数据库
python main.py server --dsn "mysql://user:password@localhost:3306/dbname"

# 连接到PostgreSQL数据库
python main.py server --dsn "postgres://user:password@localhost:5432/dbname?sslmode=disable"

# 同时连接多个数据库
python main.py server --dsn "mysql://user:password@localhost:3306/dbname" --dsn "postgres://user:password@localhost:5432/dbname" --dsn "sqlite:///path/to/database.db"

# 使用SSE传输协议（适用于Cursor等客户端）
python main.py server --demo --transport sse --port 8080

# 指定AI提供商
python main.py server --demo --ai-provider deepseek --ai-api-key "your-api-key"
```

### 客户端模式

启动客户端模式，连接到MCP服务器并与LLM交互：

```bash
# 使用默认配置（连接到演示服务器）
python main.py client

# 使用配置文件
python main.py client --config config/example.json

# 指定LLM提供商
python main.py client --llm-provider deepseek --llm-api-key "your-api-key"
```

## 数据库连接字符串格式

各数据库连接字符串格式如下：

### MySQL
```
mysql://user:password@localhost:3306/dbname
```

### PostgreSQL
```
postgres://user:password@localhost:5432/dbname?sslmode=disable
```

### SQLite
```
sqlite:///path/to/database.db
sqlite::memory:  # 内存数据库
```

### SQL Server
```
sqlserver://user:password@localhost:1433/dbname
```

### MongoDB
```
mongodb://user:password@localhost:27017/dbname
```

## 示例查询

一旦连接到客户端，您可以使用自然语言与数据库交互：

```
用户: 列出所有表
AI: [列出数据库中的所有表]

用户: employees表有哪些字段？
AI: [显示employees表的结构]

用户: 查询工资最高的3名员工
AI: [执行查询并展示结果]

用户: 生成一个SQL查询，找出每个部门的平均工资
AI: [生成SQL并执行]

# 多数据库场景
用户: mysql_dbname数据库中有哪些表？
AI: [列出MySQL数据库中的所有表]

用户: 从postgres_dbname数据库的users表中查询最新注册的10名用户
AI: [执行PostgreSQL数据库查询并显示结果]

用户: 在mongodb_database中找出所有products集合中价格高于100的商品
AI: [执行MongoDB查询并显示结果]

用户: 将mysql_dbname.customers表中的VIP客户与sqlite_db.orders表中的订单数据进行比较分析
AI: [生成跨数据库的分析结果]
```