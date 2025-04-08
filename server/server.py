import os
import logging
from typing import Any, Dict, List, Optional, Tuple, Union, Literal

from mcp.server.fastmcp import FastMCP

from .connectors import DatabaseConnector, ConnectorFactory
from .tools import QueryTool, ExportTool, ListResourcesTool, AiTool

class DatabaseMCPServer:
    """数据库MCP服务器，整合连接器和工具"""
    
    def __init__(self, 
                 dsn: Optional[Union[str, List[str]]] = None,
                 demo: bool = False,
                 ai_provider: Literal["openai", "deepseek"] = "openai",
                 ai_api_key: Optional[str] = None,
                 ai_model: Optional[str] = None,
                 ai_api_base: Optional[str] = None):
        """
        初始化服务器
        :param dsn: 单个数据库连接字符串或数据库连接字符串列表，如果不提供则尝试从环境变量获取
        :param demo: 是否使用演示数据库
        :param ai_provider: AI提供商，可选值：openai, deepseek
        :param ai_api_key: AI API密钥，如果不提供则尝试从环境变量获取
        :param ai_model: 使用的AI模型，如果不提供则使用默认值
        :param ai_api_base: AI API基础URL，如果不提供则使用默认值
        """
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("DatabaseMCPServer")
        
        # 获取DSN列表
        if dsn is None:
            env_dsn = os.environ.get("DSN")
            if env_dsn:
                # 环境变量中可以用逗号分隔多个DSN
                self.dsn_list = [d.strip() for d in env_dsn.split(',') if d.strip()]
            else:
                self.dsn_list = []
        elif isinstance(dsn, str):
            self.dsn_list = [dsn]
        elif isinstance(dsn, list):
            self.dsn_list = dsn
        else:
            raise ValueError("DSN必须是字符串或字符串列表")
        
        self.demo = demo
        
        # 如果既没有提供DSN，也不是演示模式，则报错
        if not self.dsn_list and not self.demo:
            raise ValueError("必须提供DSN或使用演示模式")
        
        # AI设置
        self.ai_provider = ai_provider
        self.ai_api_key = ai_api_key
        self.ai_model = ai_model
        self.ai_api_base = ai_api_base
        
        # MCP服务器
        self.mcp = FastMCP("DatabaseMCP")
        
        # 数据库连接器字典，键为数据库名称，值为连接器实例
        self.connectors = {}
        
        # 工具列表
        self.tools = []
    
    async def setup(self):
        """
        设置服务器，创建连接器和工具
        """
        try:
            # 创建数据库连接器
            if self.demo:
                self.logger.info("使用演示数据库")
                demo_connector = await ConnectorFactory.create_demo_connector()
                self.connectors["demo"] = demo_connector
            
            # 创建所有指定的数据库连接器
            for dsn in self.dsn_list:
                try:
                    # 从DSN中提取数据库类型和可能的数据库名作为标识符
                    db_type = dsn.split('://')[0] if '://' in dsn else 'unknown'
                    db_name = dsn.split('/')[-1] if '/' in dsn else f"db_{len(self.connectors)}"
                    connector_id = f"{db_type}_{db_name}"
                    
                    self.logger.info(f"连接到数据库: {dsn}")
                    connector = ConnectorFactory.create_connector(dsn)
                    await connector.connect()
                    
                    # 保存连接器
                    self.connectors[connector_id] = connector
                except Exception as e:
                    self.logger.error(f"连接到数据库 {dsn} 失败: {e}")
            
            # 为每个连接器创建工具
            if self.connectors:
                await self._setup_tools()
            else:
                self.logger.error("没有可用的数据库连接器")
                raise ValueError("没有可用的数据库连接器")
            
        except Exception as e:
            self.logger.error(f"设置服务器失败: {e}")
            raise
    
    async def _setup_tools(self):
        """
        为每个数据库连接器设置工具
        """
        try:
            for db_id, connector in self.connectors.items():
                # 查询工具
                query_tool = QueryTool(connector)
                self.mcp.add_tool(query_tool.get_tool(tool_name=f"{db_id}_run_query", 
                                                     description=f"在{db_id}数据库中执行SQL查询并返回结果"))
                
                # 导出工具
                export_tool = ExportTool(connector)
                self.mcp.add_tool(export_tool.get_tool(tool_name=f"{db_id}_export_to_csv",
                                                     description=f"在{db_id}数据库中执行SQL查询并将结果导出为CSV文件"))
                
                # 列表工具
                list_tool = ListResourcesTool(connector)
                self.mcp.add_tool(list_tool.get_tool(tool_name=f"{db_id}_list_resources",
                                                   description=f"列出{db_id}数据库中的资源（schemas、tables、columns、indexes、procedures等）"))
                
                # AI工具
                if (self.ai_provider == "openai" and os.environ.get("OPENAI_API_KEY")) or \
                   (self.ai_provider == "deepseek" and os.environ.get("DEEPSEEK_API_KEY")) or \
                   self.ai_api_key:
                    try:
                        ai_tool = AiTool(
                            connector=connector,
                            provider=self.ai_provider,
                            api_key=self.ai_api_key,
                            model=self.ai_model,
                            api_base=self.ai_api_base
                        )
                        # AiTool返回的是工具列表
                        for tool in ai_tool.get_tool(db_prefix=db_id):
                            self.mcp.add_tool(tool)
                        self.logger.info(f"已为{db_id}数据库添加AI工具 (使用 {self.ai_provider})")
                    except Exception as e:
                        self.logger.warning(f"为{db_id}数据库添加AI工具失败: {e}")
                else:
                    self.logger.warning(f"未提供{self.ai_provider}的API密钥，AI功能不可用")
                
                self.logger.info(f"已为{db_id}数据库添加工具")
                
        except Exception as e:
            self.logger.error(f"设置工具失败: {e}")
            raise
    
    async def run(self, transport: str = "stdio", port: int = 8080):
        """
        运行MCP服务器
        :param transport: 传输协议，可选值：stdio, sse
        :param port: HTTP服务器端口（仅当transport=sse时使用）
        """
        await self.setup()
        
        if transport == "stdio":
            self.logger.info("使用stdio传输协议")
            await self.mcp.run(transport="stdio")
        elif transport == "sse":
            self.logger.info(f"使用SSE传输协议，端口: {port}")
            await self.mcp.run(transport="sse", port=port)
        else:
            raise ValueError(f"不支持的传输协议: {transport}，支持的协议: stdio, sse")
    
    async def shutdown(self):
        """
        关闭服务器，断开所有数据库连接
        """
        for db_id, connector in self.connectors.items():
            try:
                await connector.disconnect()
                self.logger.info(f"已断开{db_id}数据库连接")
            except Exception as e:
                self.logger.error(f"断开{db_id}数据库连接失败: {e}")
        
        self.logger.info("服务器已关闭") 