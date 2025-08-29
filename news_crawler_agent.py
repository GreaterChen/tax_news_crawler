"""基于 LangChain 的智能新闻爬虫 Agent"""
import os
import json
import re
import logging
from typing import List, Dict, Callable, Any
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random
from functools import wraps
from sqlalchemy import create_engine, text
from langchain_openai import ChatOpenAI
from langchain_community.chat_models import ChatTongyi
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from langchain_core.runnables import RunnablePassthrough
import urllib3
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('.env.dev')

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置logger
logger = logging.getLogger(__name__)

def retry_decorator(max_retries=3, initial_delay=2, backoff_factor=2, exceptions=(Exception,)):
    """重试装饰器
    
    Args:
        max_retries: 最大重试次数
        initial_delay: 初始延迟时间(秒)
        backoff_factor: 退避因子，用于计算指数退避
        exceptions: 需要重试的异常类型
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"最大重试次数已用尽，最终异常: {str(e)}")
                        raise
                    
                    # 添加一些随机性避免同时请求
                    jitter = random.uniform(0, 0.5) * delay
                    sleep_time = delay + jitter
                    
                    logger.warning(f"请求失败，进行第 {retries} 次重试，等待 {sleep_time:.2f} 秒... 异常: {str(e)}")
                    time.sleep(sleep_time)
                    
                    # 指数退避
                    delay *= backoff_factor
        return wrapper
    return decorator

class NewsURLs(BaseModel):
    """新闻URL列表输出模型"""
    urls: List[str] = Field(description="新闻文章的URL列表")

class NewsContent(BaseModel):
    """新闻内容输出模型"""
    title: str = Field(description="新闻标题")
    summary: str = Field(description="新闻摘要，不超过4句话")
    tags: List[str] = Field(description="新闻标签列表")
    publish_date: str = Field(description="发布日期")
    is_relevant: bool = Field(description="是否为相关新闻")

class NewsCrawlerAgent:
    """智能新闻爬虫 Agent"""
    
    def __init__(self, db_url: str = None, tongyi_api_key: str = None):
        """初始化爬虫 Agent
        
        Args:
            db_url: 数据库连接URL，如果为None则从环境变量获取
            tongyi_api_key: AI API密钥，如果为None则从环境变量获取
        """
        # 从环境变量获取数据库配置
        if db_url is None:
            db_type = os.getenv("DB_TYPE", "mysql")
            db_host = os.getenv("DB_HOST", "127.0.0.1")
            db_port = os.getenv("DB_PORT", "3306")
            db_username = os.getenv("DB_USERNAME", "root")
            db_password = os.getenv("DB_PASSWORD")
            db_database = os.getenv("DB_DATABASE")
            
            if db_type == "mysql":
                db_url = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}"
            elif db_type == "postgresql":
                db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}"
            else:
                raise ValueError(f"不支持的数据库类型: {db_type}")
                
        self.engine = create_engine(db_url)
        
        # 从环境变量获取API密钥
        if tongyi_api_key is None:
            tongyi_api_key = os.getenv("DASHSCOPE_API_KEY")
            
        self.llm = ChatTongyi(
            model="qwen-plus-latest",
            api_key=tongyi_api_key
        )
        
        # 初始化URL提取的提示模板
        self.url_extract_prompt = ChatPromptTemplate.from_messages([
            ("system", """你是一个专业的网页分析助手。你的任务是从给定的HTML内容中提取所有新闻文章的URL。
            请分析HTML结构，找出所有新闻链接。通常这些链接会在新闻列表、文章卡片或类似的容器中。
            只返回新闻文章的URL，不要包含其他类型的链接（如导航、广告等）。
            请只返回以下格式的 JSON：
            {{
            "urls": ["url1", "url2", "url3", ..., "url10"]
            }}
            """),
            ("human", "{html_content}")
        ])
        
        # 英文内容过滤和提取的提示模板
        self.english_filter_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a professional news content filter and summarizer. Your task is to analyze news articles and filter for tax-related content.

Please filter out any unrelated news articles. Only include content related to tax Legislation, taxation Policy, HKICPA, or ACCA.

For each relevant article, provide:
1. A concise summary in English, no longer than 4 sentences, written in an objective and neutral tone
2. Tag each summary with one or more of the following categories: Legislation, Policy, HKICPA, ACCA
3. The publish date in YYYY-MM-DD format (extract from the HTML if clearly available, otherwise leave empty "")
4. Whether the article is relevant (true/false)

If the article is not relevant to tax matters, set is_relevant to false.

IMPORTANT: You must return ONLY valid JSON format. Do not include any additional text, explanation, or markdown formatting.

{{
    "title": "Article title",
    "summary": "Concise summary in English (max 4 sentences)",
    "tags": ["Legislation", "Policy", "HKICPA", "ACCA"],
    "publish_date": "",
    "is_relevant": true
}}
            """),
            ("human", "{html_content}")
        ])
        
        # 繁体中文内容过滤和提取的提示模板
        self.traditional_chinese_filter_prompt = ChatPromptTemplate.from_messages([
            ("system", """你是一個專業的新聞內容過濾和摘要助手。你的任務是分析新聞文章並篩選出稅務相關內容。

請過濾掉任何無關的新聞文章。只包含與稅務立法、稅務政策、香港會計師公會(HKICPA)或特許公認會計師公會(ACCA)相關的內容。

對於每篇相關文章，請提供：
1. 繁體中文的簡潔摘要，不超過4句話，以客觀中性的語調撰寫
2. 為每個摘要標記以下一個或多個類別：立法、政策、HKICPA、ACCA
3. 發佈日期格式為 YYYY-MM-DD（如果HTML中有明確信息則提取，否則留空 ""）
4. 文章是否相關（true/false）

如果文章與稅務事項無關，請設置 is_relevant 為 false。

重要：必須只返回有效的 JSON 格式，不要包含任何額外文字、解釋或 markdown 格式。

{{
    "title": "文章標題",
    "summary": "繁體中文簡潔摘要（最多4句話）",
    "tags": ["立法", "政策", "HKICPA", "ACCA"],
    "publish_date": "",
    "is_relevant": true
}}
            """),
            ("human", "{html_content}")
        ])
        
        # 简体中文内容过滤和提取的提示模板
        self.simplified_chinese_filter_prompt = ChatPromptTemplate.from_messages([
            ("system", """你是一个专业的新闻内容过滤和摘要助手。你的任务是分析新闻文章并筛选出税务相关内容。

请过滤掉任何无关的新闻文章。只包含与税务立法、税务政策、香港会计师公会(HKICPA)或特许公认会计师公会(ACCA)相关的内容。

对于每篇相关文章，请提供：
1. 简体中文的简洁摘要，不超过4句话，以客观中性的语调撰写
2. 为每个摘要标记以下一个或多个类别：立法、政策、HKICPA、ACCA
3. 发布日期格式为 YYYY-MM-DD（如果HTML中有明确信息则提取，否则留空 ""）
4. 文章是否相关（true/false）

如果文章与税务事项无关，请设置 is_relevant 为 false。

重要：必须只返回有效的 JSON 格式，不要包含任何额外文字、解释或 markdown 格式。

{{
    "title": "文章标题",
    "summary": "简体中文简洁摘要（最多4句话）",
    "tags": ["立法", "政策", "HKICPA", "ACCA"],
    "publish_date": "",
    "is_relevant": true
}}
            """),
            ("human", "{html_content}")
        ])
        
        # 设置输出解析器
        self.url_parser = JsonOutputParser(pydantic_schema=NewsURLs)
        self.content_parser = JsonOutputParser(pydantic_schema=NewsContent)
        
        # 构建处理链
        self.url_chain = (
            {"html_content": RunnablePassthrough()} 
            | self.url_extract_prompt 
            | self.llm 
            | self.url_parser
        )
        
        # 构建不同语言的内容处理链
        self.english_chain = (
            {"html_content": RunnablePassthrough()} 
            | self.english_filter_prompt 
            | self.llm 
            | self.content_parser
        )
        
        self.traditional_chinese_chain = (
            {"html_content": RunnablePassthrough()} 
            | self.traditional_chinese_filter_prompt 
            | self.llm 
            | self.content_parser
        )
        
        self.simplified_chinese_chain = (
            {"html_content": RunnablePassthrough()} 
            | self.simplified_chinese_filter_prompt 
            | self.llm 
            | self.content_parser
        )
    
    def get_content_chain_by_language(self, language: str):
        """根据语言获取对应的内容处理链"""
        language_chains = {
            'en': self.english_chain,                    # 英文
            'zh-hk': self.traditional_chinese_chain,     # 繁体中文（香港）
            'zh': self.simplified_chinese_chain          # 简体中文
        }
        return language_chains.get(language, self.simplified_chinese_chain)
    
    @retry_decorator(max_retries=3, exceptions=(requests.RequestException, requests.Timeout, ConnectionError))
    def fetch_html(self, url: str) -> str:
        """同步获取网页HTML内容，失败时会自动重试"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        response = requests.get(url, timeout=30, headers=headers, verify=False)
        if response.status_code == 200:
            response.encoding = response.apparent_encoding or 'utf-8'
            return response.text
        else:
            logger.error(f"获取页面失败 {url}: HTTP {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            logger.debug(f"Response content: {response.text[:500]}...")
            raise requests.RequestException(f"HTTP错误 {response.status_code}")
            
    def normalize_urls(self, urls: List[str], base_url: str) -> List[str]:
        """标准化URL列表，将相对路径转换为绝对路径"""
        normalized_urls = []
        
        for url in urls:
            if not url:
                continue
                
            # 如果已经是完整URL，直接使用
            if url.startswith(('http://', 'https://')):
                normalized_urls.append(url)
            else:
                # 使用urljoin处理相对路径
                full_url = urljoin(base_url, url)
                normalized_urls.append(full_url)
                
        return normalized_urls
    
    def extract_base_url(self, url: str) -> str:
        """从完整URL中提取基础URL"""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def extract_news_urls(self, html_content: str, source_url: str) -> List[str]:
        """使用LLM提取新闻URL列表，并处理相对路径"""
        try:
            result = self.url_chain.invoke(html_content)
            raw_urls = result['urls']
            
            # 提取基础URL
            base_url = self.extract_base_url(source_url)
            
            # 标准化URL
            normalized_urls = self.normalize_urls(raw_urls, base_url)
            
            logger.info(f"提取到 {len(raw_urls)} 个URL，标准化后 {len(normalized_urls)} 个")
            if len(raw_urls) > 0 and len(normalized_urls) > 0:
                logger.debug(f"示例URL转换: {raw_urls[0]} -> {normalized_urls[0]}")
            
            return normalized_urls
            
        except Exception as e:
            logger.error(f"提取URL失败: {str(e)}")
            return []
            
    def clean_json_response(self, response_text: str) -> str:
        """清理LLM响应，提取有效的JSON部分"""
        # 移除可能的markdown格式
        response_text = re.sub(r'```json\s*', '', response_text)
        response_text = re.sub(r'```\s*$', '', response_text)
        
        # 寻找JSON对象
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            return json_match.group(0)
        
        return response_text.strip()
    
    def get_valid_tags_by_language(self, language: str) -> List[str]:
        """根据语言获取有效的标签列表"""
        tag_mappings = {
            'en': ['Legislation', 'Policy', 'HKICPA', 'ACCA'],
            'zh-hk': ['立法', '政策', 'HKICPA', 'ACCA'],  # 繁体中文（香港）
            'zh': ['立法', '政策', 'HKICPA', 'ACCA']      # 简体中文
        }
        return tag_mappings.get(language, ['立法', '政策', 'HKICPA', 'ACCA'])
    
    def validate_tags(self, tags: List[str], language: str) -> List[str]:
        """验证和过滤标签，只保留有效的标签"""
        valid_tags = self.get_valid_tags_by_language(language)
        valid_tags_lower = [tag.lower() for tag in valid_tags]
        
        filtered_tags = []
        for tag in tags:
            if not tag:
                continue
                
            tag_clean = tag.strip()
            
            # 精确匹配
            if tag_clean in valid_tags:
                filtered_tags.append(tag_clean)
                continue
            
            # 不区分大小写匹配
            tag_lower = tag_clean.lower()
            for i, valid_tag_lower in enumerate(valid_tags_lower):
                if tag_lower == valid_tag_lower:
                    filtered_tags.append(valid_tags[i])  # 使用原始大小写
                    break
        
        # 去重并保持顺序
        seen = set()
        unique_tags = []
        for tag in filtered_tags:
            if tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)
        
        if len(unique_tags) != len(tags):
            logger.debug(f"标签过滤: 原始标签 {tags} -> 有效标签 {unique_tags}")
        
        return unique_tags
    
    def validate_and_fix_result(self, result: Dict, language: str = 'zh') -> Dict:
        """验证和修复提取结果"""
        # 确保必要字段存在
        validated_result = {
            "title": result.get("title", ""),
            "summary": result.get("summary", ""),
            "tags": result.get("tags", []),
            "publish_date": result.get("publish_date", ""),
            "is_relevant": result.get("is_relevant", False)
        }
        
        # 确保tags是列表
        if not isinstance(validated_result["tags"], list):
            if isinstance(validated_result["tags"], str):
                validated_result["tags"] = [validated_result["tags"]]
            else:
                validated_result["tags"] = []
        
        # 验证和过滤标签
        validated_result["tags"] = self.validate_tags(validated_result["tags"], language)
        
        # 确保is_relevant是布尔值
        if not isinstance(validated_result["is_relevant"], bool):
            validated_result["is_relevant"] = str(validated_result["is_relevant"]).lower() in ['true', '1', 'yes']
        
        return validated_result
    
    def extract_news_content(self, html_content: str, language: str = 'zh') -> Dict:
        """使用LLM提取和过滤新闻内容，带有重试和容错机制"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 根据语言选择对应的处理链
                content_chain = self.get_content_chain_by_language(language)
                
                # 尝试使用 LangChain 处理链
                try:
                    result = content_chain.invoke(html_content)
                    result = self.validate_and_fix_result(result, language)
                except Exception as chain_error:
                    logger.warning(f"LangChain处理失败 (尝试 {attempt + 1}/{max_retries}): {str(chain_error)}")
                    
                    # 如果LangChain失败，尝试直接调用LLM
                    try:
                        # 获取对应语言的prompt模板
                        if language == 'en':
                            prompt_template = self.english_filter_prompt
                        elif language == 'zh-hk':
                            prompt_template = self.traditional_chinese_filter_prompt
                        else:  # zh 默认简体中文
                            prompt_template = self.simplified_chinese_filter_prompt
                        
                        formatted_prompt = prompt_template.format(html_content=html_content)
                        
                        # 直接调用LLM
                        raw_response = self.llm.invoke(formatted_prompt)
                        response_text = raw_response.content if hasattr(raw_response, 'content') else str(raw_response)
                        
                        # 清理响应文本
                        cleaned_response = self.clean_json_response(response_text)
                        
                        # 尝试解析JSON
                        result = json.loads(cleaned_response)
                        result = self.validate_and_fix_result(result, language)
                        
                    except Exception as direct_error:
                        logger.warning(f"直接LLM调用也失败 (尝试 {attempt + 1}/{max_retries}): {str(direct_error)}")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)  # 指数退避
                            continue
                        else:
                            logger.error("所有重试都失败了")
                            return None
                
                # 检查必要字段
                if not result.get('title') or not result.get('summary'):
                    logger.warning(f"提取内容缺少必要字段 (尝试 {attempt + 1}/{max_retries}): {result.keys()}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        return None
                
                # 如果不相关，返回None
                if not result.get('is_relevant', False):
                    logger.debug(f"新闻不相关，已过滤: {result.get('title', 'Unknown')}")
                    return None
                
                # 如果没有有效标签，也视为不相关
                if not result.get('tags') or len(result.get('tags', [])) == 0:
                    logger.debug(f"新闻没有有效标签，已过滤: {result.get('title', 'Unknown')}")
                    return None
                    
                # 处理可能缺失的发布日期
                if not result.get('publish_date'):
                    logger.debug("未提取到发布日期，将在保存时使用当前日期")
                    
                return {
                    "title": result['title'],
                    "summary": result['summary'],
                    "tags": result.get('tags', []),
                    "publish_date": result.get('publish_date', '')
                }
                
            except Exception as e:
                logger.error(f"提取内容失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                    continue
                else:
                    logger.error("所有重试都失败了")
                    return None
        
        return None
            
    def check_urls_exist(self, urls: List[str]) -> List[str]:
        """批量检查URL是否已存在于数据库中，返回不存在的URL列表"""
        if not urls:
            return []
            
        existing_url_set = set()
        
        # 分批处理，避免SQL查询过长（每批最多50个URL）
        batch_size = 50
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            
            # 构建查询语句
            placeholders = ", ".join([f":url_{j}" for j in range(len(batch_urls))])
            check_query = text(f"""
                SELECT url FROM news 
                WHERE url IN ({placeholders})
            """)
            
            # 构建参数字典
            params = {f"url_{j}": url for j, url in enumerate(batch_urls)}
            
            try:
                with self.engine.connect() as conn:
                    existing_urls = conn.execute(check_query, params).fetchall()
                    existing_url_set.update(row[0] for row in existing_urls)
            except Exception as e:
                logger.error(f"检查URL批次失败: {str(e)}")
                continue
            
        # 返回不存在的URL
        new_urls = [url for url in urls if url not in existing_url_set]
        
        logger.info(f"URL检查结果: 总共{len(urls)}个, 已存在{len(existing_url_set)}个, 新增{len(new_urls)}个")
        return new_urls

    def save_to_db(self, news_data: Dict, source: Dict):
        """保存新闻到数据库"""
        if not news_data:
            return
            
        # 如果没有发布日期，使用当前日期
        if not news_data.get("publish_date"):
            current_date = datetime.now().strftime("%Y-%m-%d")
            news_data["publish_date"] = current_date
            logger.debug(f"未找到发布日期，使用当前日期: {current_date}")
        
        # 将标签转换为字符串
        tags_str = ", ".join(news_data.get("tags", []))
            
        data = {
            "language": source["language"],
            "source": source["source"],
            "date": news_data["publish_date"],
            "content": news_data["summary"],  # 使用摘要而不是完整内容
            "url": source["url"],
            "title": news_data["title"],
            "tags": tags_str
        }
        
        # 直接插入数据（因为URL已经在爬取前检查过了）
        insert_query = text("""
            INSERT INTO news (language, source, date, content, url, title, news_type)
            VALUES (:language, :source, :date, :content, :url, :title, :tags)
        """)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(insert_query, data)
                conn.commit()
                logger.info(f"成功保存新闻: {data['title']} - 标签: {tags_str}")
        except Exception as e:
            logger.error(f"保存新闻失败 {data['url']}: {str(e)}")
                
    def process_news_url(self, url: str, source: Dict):
        """处理单个新闻URL"""
        try:
            html_content = self.fetch_html(url)
            if html_content:
                news_data = self.extract_news_content(html_content, source["language"])
                if news_data:   # 只有相关新闻才会保存
                    source_with_url = dict(source)
                    source_with_url["url"] = url
                    self.save_to_db(news_data, source_with_url)
        except Exception as e:
            logger.error(f"处理新闻URL失败 {url}: {str(e)}")
                
    def crawl_news(self, source: Dict):
        """爬取单个来源的新闻
        
        Args:
            source: 包含url、language等信息的字典
        """
        try:
            # 获取主页HTML
            html_content = self.fetch_html(source["url"])
            if not html_content:    # TODO 出错处理
                return
                
            # 提取新闻URL列表，并处理相对路径
            news_urls = self.extract_news_urls(html_content, source["url"])
            if not news_urls:
                logger.warning(f"未提取到新闻URL: {source['source']}")
                return
                
            # 检查哪些URL是新的（未爬取过的）
            new_urls = self.check_urls_exist(news_urls)
            if not new_urls:
                logger.info(f"所有新闻URL都已存在，跳过: {source['source']}")
                return
                
            logger.info(f"开始处理 {len(new_urls)} 个新URL，来源: {source['source']}")
            
            # 串行处理新的新闻URL
            for url in new_urls:
                try:
                    self.process_news_url(url, source)
                    # 添加短暂延迟，避免请求过于频繁
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"处理新闻URL失败 {url}: {str(e)}")
                    continue
                
        except Exception as e:
            logger.error(f"爬取失败 {source['source']}: {str(e)}")
            
    def get_source_list(self) -> List[Dict]:
        """从数据库获取需要爬取的网站列表"""
        query = text("""
            SELECT url, language, source_name, info 
            FROM news_sources 
            WHERE is_active = true
        """)
        
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            
        return [
            {
                "url": row[0],
                "language": row[1],
                "source": row[2],
                "info": row[3]
            }
            for row in results
        ]
        
    def crawl_all(self):
        """爬取所有来源的新闻"""
        sources = self.get_source_list()
        logger.info(f"找到 {len(sources)} 个新闻源需要处理")
        
        for i, source in enumerate(sources, 1):
            try:
                logger.info(f"[{i}/{len(sources)}] 开始处理新闻源: {source['source']}")
                self.crawl_news(source)
                logger.info(f"[{i}/{len(sources)}] 完成处理新闻源: {source['source']}")
            except Exception as e:
                logger.error(f"爬取来源失败 {source['source']}: {str(e)}")
                continue


def main():
    """主函数，用于测试爬虫功能"""
    import os
    
    # 检查必要的环境变量
    api_key = os.getenv("DASHSCOPE_API_KEY")
    
    if not api_key:
        logger.error("❌ 错误: 请设置环境变量 DASHSCOPE_API_KEY")
        logger.error("示例: export DASHSCOPE_API_KEY='your_api_key_here'")
        return
    
    # 检查数据库配置
    db_type = os.getenv("DB_TYPE", "mysql")
    db_host = os.getenv("DB_HOST")
    db_database = os.getenv("DB_DATABASE")
    
    if not db_host or not db_database:
        logger.error("❌ 错误: 请确保设置了数据库环境变量")
        logger.error("需要的变量: DB_TYPE, DB_HOST, DB_DATABASE, DB_USERNAME, DB_PASSWORD")
        return
    
    logger.info("🚀 开始初始化新闻爬虫...")
    logger.info(f"📊 使用数据库: {db_type}://{db_host}/{db_database}")
    logger.info(f"🤖 使用模型: qwen-plus-latest")
    
    try:
        # 创建爬虫实例，自动从环境变量读取配置
        agent = NewsCrawlerAgent()
        
        logger.info("✅ 爬虫初始化成功！")
        logger.info("📰 开始执行新闻爬取任务...")
        logger.info("=" * 50)
        
        start_time = datetime.now()
        
        # 执行爬取
        agent.crawl_all()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 50)
        logger.info(f"🎉 爬取任务完成!")
        logger.info(f"⏱️  总耗时: {duration}")
        logger.info(f"📅 完成时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except KeyboardInterrupt:
        logger.info("⚠️  用户中断了爬取任务")
    except Exception as e:
        logger.error(f"❌ 爬取过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 