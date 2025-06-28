# 🚀 DB-MCP: 通用数据库MCP服务器与客户端

<p align="center">
  <img alt="Python版本" src="https://img.shields.io/badge/Python-3.8%2B-blue">
  <img alt="数据库支持" src="https://img.shields.io/badge/数据库-MySQL%2C%20PostgreSQL%2C%20SQLite%2C%20SQL%20Server%2C%20MongoDB-green">
  <img alt="AI集成" src="https://img.shields.io/badge/AI-OpenAI%2FDeepSeek-orange">
</p>

> 使用自然语言与多种数据库交互的通用接口，基于Model Context Protocol (MCP)构建

DB-MCP是一个创新的数据库交互框架，支持通过自然语言与多种数据库（MySQL、PostgreSQL、SQLite、SQL Server、MongoDB）进行交互。它集成了大型语言模型（LLM）能力，使非技术人员也能轻松执行复杂的数据库操作。

<br/>

## ✨ 核心功能

- **多数据库统一接口**：同时连接不同类型数据库并执行跨库操作
- **AI驱动查询**：使用自然语言生成SQL语句并解释结果
- **灵活传输协议**：支持stdio、sse等多种MCP传输协议
- **多LLM提供商**：兼容OpenAI和DeepSeek等AI服务
- **开箱即用**：内置演示数据库，快速上手体验
- **丰富工具集**：数据库浏览、查询执行、数据导出一体化

<br/>

## 🧩 系统架构

```mermaid
graph LR
    subgraph 客户端
        C[用户界面] --> MCP[MCP客户端]
        MCP --> LLM[LLM API交互]
    end
    
    subgraph 服务器
        MCP --> S[MCP服务器]
        S --> DB1[(MySQL)]
        S --> DB2[(PostgreSQL)]
        S --> DB3[(SQLite)]
        S --> DB4[(SQL Server)]
        S --> DB5[(MongoDB)]
    end
    
    LLM -.-> AI[OpenAI/DeepSeek]
    
    客户端 -- MCP协议 --> 服务器
```

<br/>

## ⚙️ 安装指南

### 前置要求
- Python 3.8+
- 数据库驱动（按需安装）：
  - MySQL: `aiomysql`
  - PostgreSQL: `asyncpg`
  - SQLite: `aiosqlite`
  - SQL Server: `pyodbc`, `aioodbc`

### 快速安装
```bash
# 1. 克隆仓库
git clone https://github.com/yourusername/db-mcp.git
cd db-mcp

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置环境
cp config/example.env .env
# 编辑.env文件设置API密钥和数据库连接
```

<br/>

## 🚀 使用指南

### 服务器模式
启动MCP服务器连接数据库：
```bash
# 使用演示数据库（快速体验）
python main.py server --demo

# 连接MySQL数据库
python main.py server --dsn "mysql://user:password@localhost:3306/dbname"

# 多数据库连接（跨库操作）
python main.py server \
  --dsn "mysql://user:password@localhost:3306/mysql_db" \
  --dsn "postgres://user:password@localhost:5432/postgres_db" \
  --dsn "sqlite:///data/app.db"

# 使用SSE协议（Web应用集成）
python main.py server --transport sse --port 8080

# 指定AI提供商
python main.py server --ai-provider deepseek --ai-api-key "your-api-key"
```

### 客户端模式
连接MCP服务器进行交互：
```bash
# 基础客户端
python main.py client

# 自定义配置
python main.py client --config config/prod.json

# 指定LLM提供商
python main.py client --llm-provider openai --llm-api-key "sk-..."
```

<br/>

## 🔌 数据库连接格式

| 数据库类型 | 连接字符串格式 | 示例 |
|------------|----------------|------|
| **MySQL** | `mysql://user:password@host:port/dbname` | `mysql://admin:pass123@localhost:3306/employees` |
| **PostgreSQL** | `postgres://user:password@host:port/dbname?param=value` | `postgres://user:pass@db.example.com:5432/sales?sslmode=require` |
| **SQLite** | `sqlite:///path/to/file.db` | `sqlite:///data/app.db` |
| **SQL Server** | `sqlserver://user:password@host:port/dbname` | `sqlserver://sa:Password!@sqlserver:1433/master` |
| **MongoDB** | `mongodb://user:password@host:port/dbname` | `mongodb://mongo_user:pass@mongo:27017/logs` |

<br/>

## 💬 自然语言交互示例

```plaintext
用户: 列出所有产品表
AI: [显示所有数据库中的产品表]

用户: mysql_db中的customers表结构是什么？
AI: [展示MySQL customers表结构]

用户: 查询价格最高的5个电子产品
AI: [执行查询并展示结果]

用户: 生成SQL查询纽约客户的最近订单
AI: 
SELECT * FROM orders 
WHERE customer_id IN (
  SELECT id FROM customers 
  WHERE city = 'New York'
)
ORDER BY order_date DESC
LIMIT 10;

用户: 比较mysql_db和postgres_db中的用户数量
AI: 
MySQL用户数: 1,245
PostgreSQL用户数: 987
差异: +258 (26.1%)

用户: 将mongodb中的产品数据导入SQLite
AI: [执行数据迁移操作]
```

<br/>

## 🛡️ 安全与最佳实践

<details>
<summary><b>安全注意事项</b></summary>

1. **API密钥保护**：
   - 永远不要将API密钥提交到版本控制系统
   - 使用`.env`文件管理敏感信息
   - 定期轮换API密钥

2. **数据库权限**：
   ```sql
   -- 创建专用数据库用户
   CREATE USER 'mcp_user'@'localhost' IDENTIFIED BY 'strong_password';
   
   -- 限制权限
   GRANT SELECT ON employees.* TO 'mcp_user'@'localhost';
   ```

3. **生产环境配置**：
   - 使用SSL/TLS加密数据库连接
   - 启用防火墙限制访问IP
   - 定期审计查询日志
</details>

<details>
<summary><b>性能优化</b></summary>

1. **查询限制**：
   ```bash
   # 设置最大返回行数
   python main.py server --max-rows 100
   ```

2. **缓存策略**：
   - 启用查询结果缓存
   - 设置合理的缓存过期时间

3. **连接池配置**：
   ```python
   # 在.env文件中配置
   DB_MAX_CONNECTIONS=10
   DB_TIMEOUT=30
   ```
</details>

<br/>

## ⚠️ 免责声明

**使用本工具前请务必阅读并同意以下条款：**

1. **数据安全**：确保您拥有操作目标数据库的合法权限
2. **AI风险**：LLM生成的查询语句需人工审核后再执行
3. **生产环境**：不建议直接在生产数据库使用AI生成查询
4. **敏感数据**：避免处理未脱敏的个人身份信息(PII)
5. **法律合规**：遵守当地数据保护法规(GDPR, CCPA等)

开发者不对以下情况负责：
- 因AI生成查询导致的数据损坏
- 未授权访问造成的法律问题
- 敏感信息泄露导致的后果
- 违反服务条款的API使用

<br/>



---
**用自然语言解锁数据库能力** - 让数据交互更直观自然 💬
