# -*- coding: utf-8 -*-
"""
SellerSprite (卖家精灵) 产品数据爬虫

使用 Playwright 进行浏览器自动化 + API 拦截，
从卖家精灵产品调研页面爬取 Amazon Automotive 分类产品数据。

使用方法:
    1. 在 config.py 中填写账号密码
    2. 运行: python sellersprite_spider.py
    3. 首次运行会打开浏览器让你确认登录
    4. 数据保存在 data/automotive/ 目录下
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from urllib.parse import urlencode, quote, urlparse, parse_qs, urlunparse

import pandas as pd
from playwright.async_api import async_playwright, Page, BrowserContext

import config

# ============================================================
# 日志设置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "spider.log"),
            encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("SellerSprite")


class SellerSpriteSpider:
    """卖家精灵产品调研数据爬虫"""

    def __init__(self):
        self.all_products = []          # 所有抓取到的产品数据
        self.api_responses = []         # 原始 API 响应
        self.api_endpoint = None        # 发现的 API 端点
        self.total_records = 0          # 总记录数
        self.current_page = 0           # 当前页码
        self.context: BrowserContext = None
        self.page: Page = None
        self.seen_ids = set()           # 实时去重追踪（产品 ID 集合）
        self.new_items_in_last_page = 0 # 上一页新增的非重复项数
        self._last_probe_total = 0     # 最近一次探测到的 total 值
        self._saved_count = 0          # 已保存到文件的去重数据条数
        self._batch_index = 0          # 当前批次文件编号

    # --------------------------------------------------------
    # 启动入口
    # --------------------------------------------------------
    async def run(self):
        """主运行流程"""
        logger.info("=" * 60)
        logger.info("SellerSprite 产品数据爬虫启动")
        logger.info(f"目标分类: {config.CATEGORY_NAME} (节点ID: {config.CATEGORY_NODE_ID})")
        logger.info(f"目标数据量: {config.MAX_PRODUCTS} 条")
        logger.info(f"每页: {config.PAGE_SIZE} 条")
        logger.info("=" * 60)

        # 创建输出目录
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        os.makedirs(config.BROWSER_STATE_DIR, exist_ok=True)

        async with async_playwright() as pw:
            # 尝试复用已保存的浏览器状态（免重复登录）
            state_file = os.path.join(config.BROWSER_STATE_DIR, "state.json")
            browser = await pw.chromium.launch(
                headless=config.HEADLESS,
                args=["--disable-blink-features=AutomationControlled"],
            )

            if os.path.exists(state_file):
                logger.info("发现已保存的登录状态，尝试复用...")
                self.context = await browser.new_context(
                    storage_state=state_file,
                    viewport={"width": 1920, "height": 1080},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                )
            else:
                self.context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                )

            self.page = await self.context.new_page()

            try:
                # Step 1: 登录
                await self._login(state_file)

                # Step 2: 注册 API 拦截器
                self._setup_api_interceptor()
                await self._setup_request_modifier()

                # Step 3: 爬取所有数据
                await self._crawl_all_data()

                # Step 4: 最终保存
                self._save_data()

                unique_count = len(self._deduplicate(self.all_products))
                logger.info("=" * 60)
                logger.info(f"爬取完成！共获取 {unique_count} 条唯一产品数据")
                logger.info("=" * 60)

            except KeyboardInterrupt:
                logger.warning("用户手动中断 (Ctrl+C)")
                if self.all_products:
                    logger.info(f"正在保存已获取的 {len(self.all_products)} 条数据...")
                    self._save_data(suffix="_interrupted")
                    logger.info("数据已保存！")
            except Exception as e:
                logger.error(f"爬取过程中出错: {e}", exc_info=True)
                # 即使出错也保存已获取的数据
                if self.all_products:
                    logger.info(f"保存已获取的 {len(self.all_products)} 条数据...")
                    self._save_data(suffix="_error")
            finally:
                # 保存浏览器状态
                try:
                    await self.context.storage_state(path=state_file)
                    logger.info("浏览器登录状态已保存")
                except Exception:
                    pass
                await browser.close()

    # --------------------------------------------------------
    # Step 1: 登录
    # --------------------------------------------------------
    async def _login(self, state_file: str):
        """登录卖家精灵"""
        # 先访问首页检查是否已登录
        logger.info("检查登录状态...")
        try:
            await self.page.goto(config.BASE_URL + "/v3/dashboard",
                                 timeout=config.PAGE_TIMEOUT,
                                 wait_until="domcontentloaded")
        except Exception:
            # 可能被重定向，忽略导航中断错误
            pass
        await asyncio.sleep(3)
        # 等待所有重定向完成
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        current_url = self.page.url
        logger.info(f"当前页面: {current_url}")

        # 如果被重定向到登录页面，说明需要登录
        if "login" in current_url.lower() or "sign" in current_url.lower():
            logger.info("需要登录，开始登录流程...")
            await self._do_login()
        elif "welcome" in current_url.lower():
            # 被重定向到 welcome 页面，可能需要登录
            logger.info("被重定向到 welcome 页面，检查登录状态...")
            login_btn = await self.page.query_selector(
                'a[href*="login"], button:has-text("登录"), button:has-text("Login"), button:has-text("Sign In"), a:has-text("登录"), a:has-text("Login")'
            )
            if login_btn:
                logger.info("检测到登录入口，需要登录...")
                await self._do_login()
            else:
                logger.info("✓ 已经处于登录状态 (welcome 页面)")
        else:
            # 检查页面内容是否有登录按钮
            login_btn = await self.page.query_selector(
                'a[href*="login"], button:has-text("登录"), button:has-text("Login"), button:has-text("Sign In")'
            )
            if login_btn:
                logger.info("检测到登录按钮，需要登录...")
                await self._do_login()
            else:
                logger.info("✓ 已经处于登录状态")

    async def _do_login(self):
        """执行登录操作"""
        # 确定实际登录页 URL - SellerSprite 可能有多种登录入口
        login_urls = [
            config.BASE_URL + "/cn/w/user/login",
            config.BASE_URL + "/v3/login",
            config.LOGIN_URL,
        ]

        if not config.SELLERSPRITE_USERNAME or not config.SELLERSPRITE_PASSWORD:
            logger.error("请先在 config.py 中填写账号密码！")
            logger.info("等待你手动登录...")
            # 如果当前已经在登录页面，就不用再导航了
            if "login" not in self.page.url.lower():
                try:
                    await self.page.goto(login_urls[0], timeout=config.PAGE_TIMEOUT,
                                         wait_until="domcontentloaded")
                except Exception:
                    pass
                await asyncio.sleep(2)
            logger.info("请在浏览器中手动完成登录，登录成功后按 Enter 继续...")
            input(">>> 请在浏览器中完成登录，然后按 Enter 键继续...")
            await asyncio.sleep(2)
            return

        # 导航到登录页 - 如果当前已在登录页就不用导航了
        if "login" not in self.page.url.lower():
            for login_url in login_urls:
                try:
                    logger.info(f"尝试访问登录页: {login_url}")
                    await self.page.goto(login_url, timeout=config.PAGE_TIMEOUT,
                                         wait_until="domcontentloaded")
                    await asyncio.sleep(2)
                    try:
                        await self.page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass
                    # 检查是否成功到达登录页
                    if "login" in self.page.url.lower():
                        break
                except Exception as e:
                    logger.debug(f"  导航到 {login_url} 失败: {e}")
                    continue
        
        await asyncio.sleep(2)
        logger.info(f"当前登录页面: {self.page.url}")

        # 尝试多种登录表单选择器
        username_selectors = [
            'input[name="username"]',
            'input[name="email"]',
            'input[type="email"]',
            'input[placeholder*="邮箱"]',
            'input[placeholder*="email"]',
            'input[placeholder*="Email"]',
            'input[placeholder*="用户名"]',
            'input[placeholder*="账号"]',
            '.el-input__inner[type="text"]',
        ]

        password_selectors = [
            'input[name="password"]',
            'input[type="password"]',
            'input[placeholder*="密码"]',
            'input[placeholder*="password"]',
            'input[placeholder*="Password"]',
            '.el-input__inner[type="password"]',
        ]

        login_btn_selectors = [
            'button[type="submit"]',
            'button:has-text("登录")',
            'button:has-text("Login")',
            'button:has-text("Sign In")',
            'button:has-text("Log In")',
            '.login-btn',
            '.el-button--primary',
        ]

        # 填写用户名（遍历所有匹配元素，找第一个可见的）
        username_filled = False
        for sel in username_selectors:
            elems = await self.page.query_selector_all(sel)
            for elem in elems:
                if await elem.is_visible():
                    await elem.click()
                    await elem.fill("")
                    await elem.type(config.SELLERSPRITE_USERNAME, delay=50)
                    logger.info(f"已填写用户名 (选择器: {sel})")
                    username_filled = True
                    break
            if username_filled:
                break

        if not username_filled:
            logger.warning("未找到可见的用户名输入框，请手动登录")
            input(">>> 请在浏览器中完成登录，然后按 Enter 键继续...")
            return

        await asyncio.sleep(0.5)

        # 填写密码（遍历所有匹配元素，找第一个可见的）
        password_filled = False
        for sel in password_selectors:
            elems = await self.page.query_selector_all(sel)
            for elem in elems:
                if await elem.is_visible():
                    await elem.click()
                    await elem.fill("")
                    await elem.type(config.SELLERSPRITE_PASSWORD, delay=50)
                    logger.info(f"已填写密码 (选择器: {sel})")
                    password_filled = True
                    break
            if password_filled:
                break

        if not password_filled:
            logger.warning("未找到可见的密码输入框，请手动登录")
            input(">>> 请在浏览器中完成登录，然后按 Enter 键继续...")
            return

        await asyncio.sleep(0.5)

        # 点击登录按钮
        login_clicked = False
        for sel in login_btn_selectors:
            elem = await self.page.query_selector(sel)
            if elem and await elem.is_visible():
                await elem.click()
                logger.info(f"已点击登录按钮 (选择器: {sel})")
                login_clicked = True
                break

        if not login_clicked:
            logger.warning("未找到登录按钮，请手动点击登录")
            input(">>> 请在浏览器中点击登录按钮，然后按 Enter 键继续...")
            return

        # 等待登录完成 - 登录后可能有多次重定向
        logger.info("等待登录完成...")
        await asyncio.sleep(5)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        # 再等一下确保重定向链完成
        await asyncio.sleep(3)

        # 检查是否有验证码
        captcha = await self.page.query_selector(
            '.captcha, .verify, [class*="captcha"], [class*="slider"], [class*="verify"]'
        )
        if captcha:
            logger.warning("检测到验证码！请手动完成验证...")
            input(">>> 请完成验证码后按 Enter 键继续...")
            await asyncio.sleep(2)

        # 确认登录成功
        current_url = self.page.url
        if "login" not in current_url.lower():
            logger.info("✓ 登录成功！")
        else:
            logger.warning("登录可能未成功，请检查浏览器")
            input(">>> 确认登录状态后按 Enter 键继续...")

    # --------------------------------------------------------
    # Step 2: API 拦截器
    # --------------------------------------------------------
    def _setup_api_interceptor(self):
        """注册网络请求拦截器，捕获 API 返回的产品数据"""

        async def on_response(response):
            """拦截 API 响应"""
            url = response.url
            # 匹配产品调研相关的 API 端点
            api_patterns = [
                "/v3/product/research",
                "/v3/api/product",
                "/product-research/page",
                "/product/page",
                "/api/product/research",
                "product-research",
            ]

            is_api = any(p in url for p in api_patterns)
            if not is_api:
                return

            # 只处理成功的 JSON 响应
            if response.status != 200:
                return

            content_type = response.headers.get("content-type", "")
            if "json" not in content_type and "javascript" not in content_type:
                return

            try:
                body = await response.json()
                # SellerSprite API 通常返回 {code: 0, data: {items: [...], total: N}}
                # 尝试多种数据结构
                data = None
                items = None
                total = None

                if isinstance(body, dict):
                    # 记录 API 端点
                    if not self.api_endpoint:
                        self.api_endpoint = url.split("?")[0]
                        logger.info(f"✓ 发现 API 端点: {self.api_endpoint}")

                    # 尝试提取数据 - 常见结构
                    data = body.get("data", body)

                    if isinstance(data, dict):
                        # {data: {items: [...], total: N}}
                        items = (
                            data.get("items")
                            or data.get("records")
                            or data.get("list")
                            or data.get("rows")
                            or data.get("content")
                            or data.get("products")
                        )
                        total = (
                            data.get("total")
                            or data.get("totalCount")
                            or data.get("totalElements")
                            or data.get("count")
                        )
                    elif isinstance(data, list):
                        items = data

                    if items and isinstance(items, list) and len(items) > 0:
                        # 实时去重统计
                        new_count = 0
                        id_fields = ["asin", "ASIN", "productAsin", "product_asin", "id", "productId"]
                        for item in items:
                            item_id = None
                            for f in id_fields:
                                if f in item:
                                    item_id = item.get(f)
                                    break
                            if item_id and item_id not in self.seen_ids:
                                self.seen_ids.add(item_id)
                                new_count += 1
                            elif not item_id:
                                new_count += 1  # 无 ID 字段的按新数据计

                        self.new_items_in_last_page = new_count

                        logger.info(
                            f"  API 拦截成功: {len(items)} 条产品数据 "
                            f"(新增 {new_count}, 重复 {len(items)-new_count}, "
                            f"total={total}, URL片段=...{url[-80:]})"
                        )
                        if total:
                            self.total_records = total
                        self.api_responses.append({
                            "url": url,
                            "items": items,
                            "total": total,
                            "timestamp": datetime.now().isoformat(),
                        })
                        self.all_products.extend(items)

                        # 记录第一条数据的字段，帮助了解数据结构
                        if len(self.all_products) == len(items):
                            sample = items[0]
                            logger.info(f"  数据字段 ({len(sample)} 个): {list(sample.keys())}")
                    else:
                        # 即使没有 items，也记录 API 结构以便调试
                        logger.debug(f"  API 响应无产品数据，结构: {list(body.keys()) if isinstance(body, dict) else type(body)}")

            except Exception as e:
                logger.debug(f"  解析 API 响应失败: {e}")

        self.page.on("response", on_response)
        logger.info("✓ API 拦截器已注册")

    # --------------------------------------------------------
    # Step 2.5: 请求修改器 — 强制正确的 API 请求参数
    # --------------------------------------------------------
    async def _setup_request_modifier(self):
        """
        注册请求拦截器，强制修改发出的产品调研 API 请求中的 size 和 page 参数。
        解决 SPA 前端忽略 URL 参数、默认使用小页面大小 (如 20) 的问题。
        """
        spider = self

        async def handle_route(route):
            request = route.request
            url = request.url

            # 只修改主数据接口 /v3/api/product-research
            # 不修改辅助接口（如 /nodes、/support_new_station）
            parsed_url = urlparse(url)
            if parsed_url.path != '/v3/api/product-research':
                await route.continue_()
                return

            target_page = spider.current_page if spider.current_page > 0 else 1
            target_size = config.PAGE_SIZE

            try:
                # 处理 POST 请求（JSON body）
                if request.method == 'POST' and request.post_data:
                    try:
                        body = json.loads(request.post_data)
                        old_size = body.get('size', '?')
                        old_page = body.get('page', '?')
                        body['size'] = target_size
                        body['page'] = target_page
                        logger.info(
                            f"  [请求拦截] POST: size {old_size}→{target_size}, "
                            f"page {old_page}→{target_page}"
                        )
                        await route.continue_(post_data=json.dumps(body))
                        return
                    except (json.JSONDecodeError, TypeError):
                        pass

                # 处理 GET 请求 — 修改 URL 查询参数
                params = parse_qs(parsed_url.query, keep_blank_values=True)
                if 'size' in params or 'page' in params:
                    old_size = params.get('size', ['?'])[0]
                    old_page = params.get('page', ['?'])[0]
                    params['size'] = [str(target_size)]
                    params['page'] = [str(target_page)]
                    new_query = urlencode(params, doseq=True)
                    new_url = urlunparse(parsed_url._replace(query=new_query))
                    logger.info(
                        f"  [请求拦截] GET: size {old_size}→{target_size}, "
                        f"page {old_page}→{target_page}"
                    )
                    await route.continue_(url=new_url)
                    return
            except Exception as e:
                logger.debug(f"  请求拦截处理异常: {e}")

            await route.continue_()

        await self.page.route(
            re.compile(r'.*/v3/api/product-research.*'),
            handle_route
        )
        logger.info("✓ API 请求修改器已注册（强制 size=%d）", config.PAGE_SIZE)

    # --------------------------------------------------------
    # URL 构建辅助方法
    # --------------------------------------------------------
    def _build_research_url(self, min_sales=None, max_sales=None, page=1):
        """构建产品调研页面的 URL"""
        url = (
            f"{config.PRODUCT_RESEARCH_URL}?"
            f"market={config.MARKET}&page={page}&size={config.PAGE_SIZE}"
            f"&symbolFlag=false&monthName=bsr_sales_nearly&selectType=2"
            f"&filterSub=false&weightUnit=g"
            f"&order%5Bfield%5D={config.ORDER_FIELD}"
            f"&order%5Bdesc%5D={'true' if config.ORDER_DESC else 'false'}"
            f"&productTags=%5B%5D"
            f"&nodeIdPaths=%5B%22{config.CATEGORY_NODE_ID}%22%5D"
            f"&sellerTypes=%5B%5D&eligibility=%5B%5D"
            f"&pkgDimensionTypeList=%5B%5D&sellerNationList=%5B%5D"
            f"&lowPrice=N"
        )
        if min_sales is not None:
            url += f"&minSales={min_sales}"
        if max_sales is not None:
            url += f"&maxSales={max_sales}"
        return url

    # --------------------------------------------------------
    # Step 3: 导航到产品调研页面
    # --------------------------------------------------------
    async def _navigate_to_product_research(self, min_sales=None, max_sales=None):
        """导航到带有筛选条件的产品调研页面

        Args:
            min_sales: 最小销量（默认使用 config.MIN_SALES）
            max_sales: 最大销量（默认使用 config.MAX_SALES）
        """
        if min_sales is None:
            min_sales = config.MIN_SALES
        if max_sales is None:
            max_sales = getattr(config, 'MAX_SALES', None)

        url = self._build_research_url(min_sales=min_sales, max_sales=max_sales)

        logger.info(f"导航到产品调研页面 (销量: {min_sales}~{max_sales})...")
        logger.info(f"URL: {url[:150]}...")

        # 使用 domcontentloaded 而非 load，避免被重定向打断
        max_nav_retries = 3
        for attempt in range(max_nav_retries):
            try:
                await self.page.goto(url, timeout=config.PAGE_TIMEOUT,
                                     wait_until="domcontentloaded")
                break
            except Exception as e:
                error_msg = str(e)
                if "interrupted by another navigation" in error_msg:
                    logger.info(f"  导航被重定向打断 (尝试 {attempt+1}/{max_nav_retries})，等待重定向完成...")
                    await asyncio.sleep(3)
                    try:
                        await self.page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass

                    current = self.page.url
                    logger.info(f"  重定向后的页面: {current}")

                    if "product-research" in current:
                        logger.info("  ✓ 已到达产品调研页面")
                        break
                    elif "login" in current.lower():
                        logger.warning("  被重定向到登录页面，重新登录...")
                        await self._do_login()
                        continue
                    elif "welcome" in current.lower() or "v2" in current.lower():
                        logger.info("  在 welcome 页面，重新尝试导航...")
                        await asyncio.sleep(2)
                        continue
                    else:
                        logger.info(f"  在未知页面: {current}，重新尝试...")
                        continue
                else:
                    raise

        # 等待页面稳定
        await asyncio.sleep(3)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        current_url = self.page.url
        logger.info(f"✓ 页面已加载: {current_url[:100]}")

        # 如果最终不在产品调研页面，尝试手动导航
        if "product-research" not in current_url:
            logger.warning(f"未到达产品调研页面，当前: {current_url}")
            logger.info("请手动在浏览器中导航到产品调研页面...")
            logger.info(f"目标URL: {url[:100]}...")
            input(">>> 到达产品调研页面后按 Enter 键继续...")
            await asyncio.sleep(2)

    # --------------------------------------------------------
    # Step 4: 等待首页数据
    # --------------------------------------------------------
    async def _wait_for_first_page_data(self, before_count=None):
        """等待首页数据加载完成"""
        logger.info("等待首页数据加载...")

        # 等待数据表格出现
        table_selectors = [
            ".el-table",
            "table",
            ".product-list",
            ".research-table",
            "[class*='table']",
            "[class*='product']",
        ]

        table_found = False
        for sel in table_selectors:
            try:
                await self.page.wait_for_selector(sel, timeout=15000)
                logger.info(f"  找到数据表格: {sel}")
                table_found = True
                break
            except Exception:
                continue

        if not table_found:
            logger.warning("未找到数据表格元素，尝试等待更长时间...")
            await asyncio.sleep(5)

        # 等待 API 拦截器捕获数据
        if before_count is None:
            before_count = len(self.all_products)
        max_wait = 30
        waited = 0
        while len(self.all_products) == before_count and waited < max_wait:
            await asyncio.sleep(1)
            waited += 1

        if len(self.all_products) > before_count:
            new_count = len(self.all_products) - before_count
            logger.info(f"✓ 首页数据已获取: {new_count} 条")
            if self.total_records:
                total_pages = (self.total_records + config.PAGE_SIZE - 1) // config.PAGE_SIZE
                logger.info(f"  总记录数: {self.total_records}, 总页数: {total_pages}")
        else:
            logger.warning("首页数据未被拦截到")
            logger.info("可能需要检查 API 端点模式...")
            # 打印页面上的所有请求URL帮助调试
            logger.info("尝试从页面 DOM 直接抓取数据...")
            await self._try_dom_extraction()

    # --------------------------------------------------------
    # 自动分片：探测区间数据量
    # --------------------------------------------------------
    async def _probe_range(self, min_sales, max_sales):
        """探测指定销量区间的总记录数

        Returns:
            该区间的总记录数 (int)，探测失败返回 0
        """
        logger.info(f"  [探测] 区间 [{min_sales}, {max_sales}] 的数据量...")

        # 保存当前状态
        before_count = len(self.all_products)

        # 导航到该区间的第1页
        await self._navigate_to_product_research(min_sales=min_sales, max_sales=max_sales)

        # 等待 API 响应以获取 total
        max_wait = 30
        waited = 0
        while len(self.all_products) == before_count and waited < max_wait:
            await asyncio.sleep(1)
            waited += 1

        # 获取 total_records（API 拦截器会更新此值）
        probed_total = self.total_records

        if len(self.all_products) == before_count:
            logger.info(f"  [探测] 区间 [{min_sales}, {max_sales}]: 0 条")
            return 0

        logger.info(f"  [探测] 区间 [{min_sales}, {max_sales}]: 共 {probed_total} 条")
        return probed_total

    # --------------------------------------------------------
    # 自动分片：递归二分生成安全区间
    # --------------------------------------------------------
    async def _generate_slices(self, min_sales, max_sales, depth=0):
        """递归二分产生所有安全区间（每个区间 <= MAX_ITEMS_PER_QUERY 条）

        Returns:
            安全区间列表 [(min1, max1, total1), ...]
        """
        indent = "  " * (depth + 1)
        max_items = config.MAX_ITEMS_PER_QUERY

        # 防御：区间太小无法再分
        if min_sales >= max_sales:
            total = await self._probe_range(min_sales, max_sales)
            if total > 0:
                logger.info(f"{indent}[分片] 最小区间 [{min_sales}, {max_sales}]: {total} 条")
                return [(min_sales, max_sales, total)]
            return []

        # 探测当前区间的总数
        total = await self._probe_range(min_sales, max_sales)

        if total <= 0:
            logger.info(f"{indent}[分片] 区间 [{min_sales}, {max_sales}] 无数据，跳过")
            return []

        if total <= max_items:
            logger.info(f"{indent}[OK] 区间 [{min_sales}, {max_sales}]: {total} 条 <= {max_items}")
            return [(min_sales, max_sales, total)]

        # 需要拆分
        if max_sales - min_sales < 1:
            logger.warning(
                f"{indent}[!] 区间 [{min_sales}, {max_sales}] 有 {total} 条"
                f"但无法再分！强制爬取前 {max_items} 条"
            )
            return [(min_sales, max_sales, total)]

        mid = (min_sales + max_sales) // 2
        logger.info(
            f"{indent}[拆分] 区间 [{min_sales}, {max_sales}]: {total} 条 > {max_items}，"
            f"二分为 [{min_sales}, {mid}] + [{mid+1}, {max_sales}]"
        )

        # 递归处理左右半区间
        left_slices = await self._generate_slices(min_sales, mid, depth + 1)
        right_slices = await self._generate_slices(mid + 1, max_sales, depth + 1)

        return left_slices + right_slices

    # --------------------------------------------------------
    # 自动分片：爬取单个安全区间
    # --------------------------------------------------------
    async def _crawl_single_range(self, min_sales, max_sales, range_total, slice_idx, total_slices):
        """爬取单个销量区间的所有页数据"""
        logger.info("=" * 60)
        logger.info(
            f"[分片 {slice_idx}/{total_slices}] "
            f"销量 [{min_sales}, {max_sales}], 预计 {range_total} 条"
        )
        logger.info("=" * 60)

        before_count = len(self.all_products)

        # 导航到该区间
        await self._navigate_to_product_research(min_sales=min_sales, max_sales=max_sales)
        await self._wait_for_first_page_data(before_count)

        first_page_new = len(self.all_products) - before_count
        if first_page_new == 0:
            logger.warning(f"  分片 [{min_sales}, {max_sales}] 首页无数据，跳过")
            return

        # 计算该区间需要翻多少页
        total_pages = min(
            (range_total + config.PAGE_SIZE - 1) // config.PAGE_SIZE,
            config.MAX_PAGES_PER_QUERY
        )
        logger.info(f"  该区间需翻 {total_pages} 页")

        self.current_page = 1
        consecutive_dup_pages = 0

        while self.current_page < total_pages:
            self.current_page += 1

            delay = random.uniform(config.PAGE_DELAY_MIN, config.PAGE_DELAY_MAX)
            logger.info(
                f"  [{slice_idx}/{total_slices}] 页 {self.current_page}/{total_pages} | "
                f"总计: {len(self.all_products)} 条 (唯一: {len(self.seen_ids)}) | "
                f"等待 {delay:.1f}s..."
            )
            await asyncio.sleep(delay)

            self.new_items_in_last_page = 0
            success = await self._go_to_next_page()
            if not success:
                logger.warning(f"翻页失败，重试...")
                retry_count = 0
                while retry_count < config.MAX_RETRIES and not success:
                    retry_count += 1
                    await asyncio.sleep(3)
                    success = await self._go_to_next_page()

                if not success:
                    logger.error(f"翻页失败 {config.MAX_RETRIES} 次，跳过该区间剩余页")
                    break

            if success and self.new_items_in_last_page == 0:
                consecutive_dup_pages += 1
                logger.warning(
                    f"  第 {self.current_page} 页全部为重复数据 "
                    f"(连续 {consecutive_dup_pages} 页)"
                )
                if consecutive_dup_pages >= 3:
                    logger.info("  连续 3 页全部重复，提前结束该区间")
                    break
            else:
                consecutive_dup_pages = 0

            # 检查是否需要保存一批
            self._check_and_save_batch()

            if len(self.seen_ids) >= config.MAX_PRODUCTS:
                logger.info(f"已达到目标数据量 {config.MAX_PRODUCTS}，停止")
                return

        # 区间结束时也检查一次
        self._check_and_save_batch()

        range_new = len(self.all_products) - before_count
        logger.info(
            f"  [完成] 分片 [{min_sales}, {max_sales}]: "
            f"本区间获取 {range_new} 条"
        )

    # --------------------------------------------------------
    # 主爬取流程
    # --------------------------------------------------------
    async def _crawl_all_data(self):
        """主爬取入口：支持自动分片或传统翻页"""

        if getattr(config, 'AUTO_SLICE', False):
            await self._crawl_with_auto_slice()
        else:
            await self._crawl_fixed_range()

    async def _crawl_with_auto_slice(self):
        """自动分片模式：递归二分销量区间，逐片爬取"""
        min_sales = config.MIN_SALES
        max_sales = config.MAX_SALES

        logger.info("=" * 60)
        logger.info("AUTO_SLICE 自动分片模式启动")
        logger.info(f"   销量范围: [{min_sales}, {max_sales}]")
        logger.info(f"   每页: {config.PAGE_SIZE} 条, 单查询上限: {config.MAX_ITEMS_PER_QUERY} 条")
        logger.info("=" * 60)

        # Phase 1: 生成所有安全分片
        logger.info("")
        logger.info("Phase 1: 探测并生成分片...")
        slices = await self._generate_slices(min_sales, max_sales)

        if not slices:
            logger.error("未生成任何分片，无法爬取")
            return

        total_estimated = sum(s[2] for s in slices)
        logger.info(f"")
        logger.info(f"分片计划:")
        logger.info(f"   共 {len(slices)} 个分片, 预计 {total_estimated} 条数据")
        for i, (s_min, s_max, s_total) in enumerate(slices, 1):
            pages = min(
                (s_total + config.PAGE_SIZE - 1) // config.PAGE_SIZE,
                config.MAX_PAGES_PER_QUERY
            )
            logger.info(f"   [{i}] 销量 [{s_min}, {s_max}]: ~{s_total} 条, {pages} 页")

        # Phase 2: 逐个分片爬取
        logger.info(f"")
        logger.info(f"Phase 2: 开始逐片爬取...")
        for i, (s_min, s_max, s_total) in enumerate(slices, 1):
            await self._crawl_single_range(s_min, s_max, s_total, i, len(slices))

            # 检查是否需要保存一批
            self._check_and_save_batch()

            # 检查是否已达总目标
            if len(self.seen_ids) >= config.MAX_PRODUCTS:
                logger.info(f"已达到目标数据量 {config.MAX_PRODUCTS}，停止所有分片")
                break

            # 分片间额外延迟
            if i < len(slices):
                slice_delay = random.uniform(3, 6)
                logger.info(f"")
                logger.info(f"分片间延迟 {slice_delay:.1f}s...")
                logger.info(f"")
                await asyncio.sleep(slice_delay)

        logger.info(f"")
        logger.info(f"所有分片爬取完成！总计: {len(self.all_products)} 条 (唯一: {len(self.seen_ids)})")

    async def _crawl_fixed_range(self):
        """传统固定区间翻页爬取（AUTO_SLICE=False 时使用）"""
        # 记录导航前的数据量（用于检测首页新数据）
        before_nav_count = len(self.all_products)

        # 导航到过滤的产品调研页面
        await self._navigate_to_product_research()

        # 等待首页数据（传入导航前的计数，避免时序问题）
        await self._wait_for_first_page_data(before_nav_count)

        if not self.all_products:
            logger.error("未获取到首页数据，无法继续翻页")
            return

        target = min(config.MAX_PRODUCTS, self.total_records or config.MAX_PRODUCTS)
        total_pages = (target + config.PAGE_SIZE - 1) // config.PAGE_SIZE
        logger.info(f"开始翻页爬取: API 报告总页数 {total_pages}")

        self.current_page = 1  # 首页已爬取
        consecutive_dup_pages = 0  # 连续全重复页计数

        while self.current_page < total_pages:
            self.current_page += 1

            # 随机延迟
            delay = random.uniform(config.PAGE_DELAY_MIN, config.PAGE_DELAY_MAX)
            logger.info(
                f"[{self.current_page}/{total_pages}] "
                f"已获取 {len(self.all_products)} 条 (唯一: {len(self.seen_ids)}), "
                f"等待 {delay:.1f}s 后翻页..."
            )
            await asyncio.sleep(delay)

            # 翻页
            self.new_items_in_last_page = 0
            success = await self._go_to_next_page()
            if not success:
                logger.warning(f"翻页失败，重试...")
                retry_count = 0
                while retry_count < config.MAX_RETRIES and not success:
                    retry_count += 1
                    await asyncio.sleep(3)
                    success = await self._go_to_next_page()

                if not success:
                    logger.error(f"翻页失败 {config.MAX_RETRIES} 次，停止爬取")
                    break

            # 检测重复数据 - 如果连续 3 页无新数据则提前停止
            if success and self.new_items_in_last_page == 0:
                consecutive_dup_pages += 1
                logger.warning(
                    f"  第 {self.current_page} 页全部为重复数据 "
                    f"(连续 {consecutive_dup_pages} 页)"
                )
                if consecutive_dup_pages >= 3:
                    logger.info("  连续 3 页全部重复，提前停止爬取")
                    break
            else:
                consecutive_dup_pages = 0

            # 检查是否需要保存一批
            self._check_and_save_batch()

            # 检查是否已达总目标
            if len(self.seen_ids) >= config.MAX_PRODUCTS:
                logger.info(f"已达到目标数据量 {config.MAX_PRODUCTS}，停止爬取")
                break

    async def _go_to_next_page(self) -> bool:
        """点击下一页或通过 URL 翻页"""
        before_count = len(self.all_products)

        # 方法1: 尝试点击"下一页"按钮
        next_btn_selectors = [
            ".el-pagination .btn-next:not(.disabled):not([disabled])",
            "button.btn-next",
            ".pagination .next",
            'button:has-text("下一页")',
            'button:has-text("Next")',
            ".el-pager li.number + li.number",  # Element UI 分页
        ]

        clicked = False
        for sel in next_btn_selectors:
            try:
                btn = await self.page.query_selector(sel)
                if btn and await btn.is_enabled():
                    await btn.click()
                    clicked = True
                    logger.debug(f"  点击下一页按钮: {sel}")
                    break
            except Exception:
                continue

        if not clicked:
            # 方法2: 直接修改 URL 中的 page 参数
            logger.debug("  按钮点击失败，尝试通过 URL 翻页...")
            current_url = self.page.url
            new_url = re.sub(r'page=\d+', f'page={self.current_page}', current_url)
            if new_url == current_url:
                new_url = current_url + f"&page={self.current_page}"
            try:
                await self.page.goto(new_url, timeout=config.PAGE_TIMEOUT,
                                     wait_until="domcontentloaded")
            except Exception as e:
                if "interrupted by another navigation" in str(e):
                    logger.debug("  URL翻页导航被重定向打断，等待完成...")
                    await asyncio.sleep(3)
                else:
                    raise

        # 等待新数据加载（networkidle 在 SPA 页面上可能永远无法达到，短超时即可）
        try:
            await self.page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass

        # 等待 API 拦截器捕获数据
        max_wait = 20
        waited = 0
        while len(self.all_products) == before_count and waited < max_wait:
            await asyncio.sleep(1)
            waited += 1

        new_count = len(self.all_products) - before_count
        if new_count > 0:
            logger.info(f"  ✓ 第 {self.current_page} 页获取 {new_count} 条数据")
            return True
        else:
            logger.warning(f"  ✗ 第 {self.current_page} 页未获取到新数据")
            return False

    # --------------------------------------------------------
    # DOM 直接抓取 (备用方案)
    # --------------------------------------------------------
    async def _try_dom_extraction(self):
        """从页面 DOM 直接提取数据（API 拦截失败时的备用方案）"""
        logger.info("尝试从 DOM 提取数据...")

        # 检测页面内容
        page_content = await self.page.content()
        logger.info(f"  页面内容长度: {len(page_content)} 字符")

        # 尝试从 Element UI 表格提取
        rows = await self.page.query_selector_all(".el-table__row")
        if rows:
            logger.info(f"  找到 {len(rows)} 行表格数据")
            for row in rows:
                cells = await row.query_selector_all(".el-table__cell, td")
                row_data = {}
                for i, cell in enumerate(cells):
                    text = await cell.inner_text()
                    row_data[f"col_{i}"] = text.strip()
                if row_data:
                    self.all_products.append(row_data)

        if not self.all_products:
            logger.info("  DOM 提取也未找到数据，请检查页面状态")
            # 截图保存以便调试
            screenshot_path = os.path.join(config.OUTPUT_DIR, "debug_screenshot.png")
            await self.page.screenshot(path=screenshot_path, full_page=True)
            logger.info(f"  已保存调试截图: {screenshot_path}")

    # --------------------------------------------------------
    # Step 6: 保存数据（分批存储）
    # --------------------------------------------------------
    def _check_and_save_batch(self):
        """检查是否该保存一批数据了（每 SAVE_BATCH_SIZE 条触发一次）"""
        batch_size = getattr(config, 'SAVE_BATCH_SIZE', 2000)
        unique_products = self._deduplicate(self.all_products)
        unsaved_count = len(unique_products) - self._saved_count

        if unsaved_count >= batch_size:
            # 取出未保存的部分
            unsaved_items = unique_products[self._saved_count:]
            # 可能超出 batch_size，按 batch_size 分批保存
            while len(unsaved_items) >= batch_size:
                batch = unsaved_items[:batch_size]
                unsaved_items = unsaved_items[batch_size:]
                self._batch_index += 1
                self._save_batch(batch, self._batch_index)
                self._saved_count += len(batch)

    def _save_batch(self, items, batch_index):
        """保存一批数据到独立文件

        Args:
            items: 要保存的产品列表（已去重）
            batch_index: 批次编号
        """
        suffix = f"_part{batch_index}"
        logger.info(f"保存第 {batch_index} 批: {len(items)} 条")

        # 保存 JSON
        json_path = os.path.join(
            config.OUTPUT_DIR,
            f"{config.OUTPUT_FILENAME}{suffix}.json"
        )
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "meta": {
                        "scrape_time": datetime.now().isoformat(),
                        "market": config.MARKET,
                        "category": config.CATEGORY_NAME,
                        "category_node_id": config.CATEGORY_NODE_ID,
                        "batch_index": batch_index,
                        "batch_count": len(items),
                        "total_scraped_so_far": self._saved_count + len(items),
                        "api_endpoint": self.api_endpoint,
                    },
                    "products": items,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info(f"  JSON 已保存: {json_path}")

        # 保存 Excel
        try:
            df = pd.json_normalize(items, sep="_")
            excel_path = os.path.join(
                config.OUTPUT_DIR,
                f"{config.OUTPUT_FILENAME}{suffix}.xlsx"
            )
            df.to_excel(excel_path, index=False, engine="openpyxl")
            logger.info(f"  Excel 已保存: {excel_path} ({len(df)} 行, {len(df.columns)} 列)")
        except Exception as e:
            logger.error(f"  保存 Excel 失败: {e}")
            try:
                df = pd.DataFrame(items)
                excel_path = os.path.join(
                    config.OUTPUT_DIR,
                    f"{config.OUTPUT_FILENAME}{suffix}_simple.xlsx"
                )
                df.to_excel(excel_path, index=False, engine="openpyxl")
                logger.info(f"  简化 Excel 已保存: {excel_path}")
            except Exception as e2:
                logger.error(f"  简化保存也失败: {e2}")

    def _save_data(self, suffix=""):
        """保存所有未保存的数据（结束时或中断时调用）"""
        if not self.all_products:
            logger.warning("没有数据可保存")
            return

        unique_products = self._deduplicate(self.all_products)
        logger.info(f"去重: {len(self.all_products)} -> {len(unique_products)} 条")

        # 取出还没保存过的部分
        unsaved_items = unique_products[self._saved_count:]

        if not unsaved_items:
            logger.info("所有数据已在之前的批次中保存完毕，无需再保存")
            return

        # 按 batch_size 分批保存剩余数据
        batch_size = getattr(config, 'SAVE_BATCH_SIZE', 2000)
        while len(unsaved_items) > 0:
            batch = unsaved_items[:batch_size]
            unsaved_items = unsaved_items[batch_size:]
            self._batch_index += 1
            self._save_batch(batch, self._batch_index)
            self._saved_count += len(batch)

        logger.info(
            f"全部保存完成: 共 {self._batch_index} 个文件, "
            f"{self._saved_count} 条唯一数据"
        )

    def _deduplicate(self, products: list) -> list:
        """根据 ASIN 等唯一标识去重"""
        seen = set()
        unique = []

        # 尝试找到唯一标识字段
        id_fields = ["asin", "ASIN", "productAsin", "product_asin", "id", "productId"]

        id_field = None
        if products:
            sample = products[0]
            for f in id_fields:
                if f in sample:
                    id_field = f
                    break

        if id_field:
            for p in products:
                pid = p.get(id_field)
                if pid and pid not in seen:
                    seen.add(pid)
                    unique.append(p)
            return unique
        else:
            # 无唯一标识，用 JSON 哈希去重
            for p in products:
                key = json.dumps(p, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    unique.append(p)
            return unique


# ============================================================
# 主程序入口
# ============================================================
async def main():
    spider = SellerSpriteSpider()
    await spider.run()


if __name__ == "__main__":
    asyncio.run(main())
