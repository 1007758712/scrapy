# -*- coding: utf-8 -*-
"""
FastMoss 汽车与摩托车板块 商品数据爬虫

使用 Playwright 浏览器自动化 + API 拦截 + DOM 解析，
爬取 FastMoss TikTok 商品搜索页面的全部商品数据。

使用方法:
    1. 在 config.py 中填写手机号和密码
    2. 运行: python fastmoss_spider.py
    3. 首次运行会打开浏览器，可能需要手动过验证码
    4. 数据保存在 data/automotive/ 目录下

策略:
    - 优先通过 API 响应拦截获取结构化 JSON 数据
    - 如 API 拦截失败，回退到 DOM 表格解析
    - 不按子分类拆分，直接爬取 l1_cid=23 全量数据
    - 如果总量超过 500 页（PAGE_SIZE*500 条），则自动切换为按子分类分别爬取
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
import time
import requests as http_requests  # 避免与其他变量冲突
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import pandas as pd
from playwright.async_api import async_playwright, Page, BrowserContext
from playwright_stealth import Stealth

import config

# ============================================================
# 日志设置
# ============================================================
# 解决 Windows GBK 编码问题：强制 stdout 使用 utf-8
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "spider.log"),
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("FastMoss")


class FastMossSpider:
    """FastMoss 商品搜索数据爬虫"""

    def __init__(self):
        self.all_products = []          # 当前批次抓取到的商品数据
        self.total_products = []        # 所有批次累计的商品数据
        self.api_data_received = False  # 标记 API 拦截器是否收到数据
        self.latest_api_items = []      # 最近一次 API 拦截到的数据
        self.total_records = 0          # API 报告的总记录数
        self.current_page = 0           # 当前页码
        self.seen_ids = set()           # 去重用的 ID 集合
        self.context: BrowserContext = None
        self.page: Page = None
        self.discovered_subcategories = []  # 运行时发现的子分类

    # ============================================================
    # 主入口
    # ============================================================
    async def run(self):
        """主运行流程"""
        logger.info("=" * 60)
        logger.info("FastMoss 汽车与摩托车板块 商品数据爬虫启动")
        logger.info(f"目标分类: {config.L1_NAME} (l1_cid={config.L1_CID})")
        logger.info(f"区域: {config.REGION}")
        logger.info(f"每页: {config.PAGE_SIZE} 条")
        logger.info("=" * 60)

        # 创建输出目录
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        os.makedirs(config.BROWSER_STATE_DIR, exist_ok=True)

        async with async_playwright() as pw:
            state_file = os.path.join(config.BROWSER_STATE_DIR, "fastmoss_state.json")

            # ----------------------------------------------------------
            # 使用持久化上下文 (user-data-dir) 代替 storage_state
            # 这种方式更接近真实 Chrome，更难被反爬检测
            # ----------------------------------------------------------
            user_data_dir = os.path.join(config.BROWSER_STATE_DIR, "chrome_profile")
            os.makedirs(user_data_dir, exist_ok=True)

            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--disable-component-update",
                "--no-first-run",
                "--no-default-browser-check",
                "--window-size=1920,1080",
            ]

            self.context = await pw.chromium.launch_persistent_context(
                user_data_dir,
                headless=config.HEADLESS,
                channel="chrome",  # 使用系统安装的真实 Chrome（TLS 指纹与真实浏览器一致）
                args=launch_args,
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/136.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                timezone_id="Asia/Shanghai",
                ignore_https_errors=True,
            )

            # 应用 playwright-stealth 反检测（覆盖 webdriver/plugins/WebGL 等指纹）
            stealth = Stealth(
                chrome_runtime=True,  # 默认关闭，开启以伪装 chrome.runtime
                navigator_languages_override=("zh-CN", "zh"),
                init_scripts_only=False,
            )
            await stealth.apply_stealth_async(self.context)
            logger.info("[OK] playwright-stealth 反检测已应用")

            # 获取或创建页面
            pages = self.context.pages
            if pages:
                self.page = pages[0]
            else:
                self.page = await self.context.new_page()

            try:
                # Step 1: 登录
                await self._login()

                # Step 2: 探索子分类（如果开启自动发现）
                if config.AUTO_DISCOVER_SUBCATEGORIES:
                    await self._discover_subcategories()

                # Step 3: 注册 API 拦截器 + 用页面导航爬取
                self._setup_api_interceptor()
                await self._crawl_category_via_navigation()

                # Step 5: 保存最终数据
                self._save_final_data()

                logger.info("=" * 60)
                logger.info(f"爬取完成！共获取 {len(self.total_products)} 条唯一商品数据")
                logger.info("=" * 60)

            except Exception as e:
                logger.error(f"爬取过程中出错: {e}", exc_info=True)
                if self.total_products:
                    logger.info(f"紧急保存已获取的 {len(self.total_products)} 条数据...")
                    self._save_final_data()
            finally:
                # 持久化上下文会自动保存 cookies/localStorage 到 user_data_dir
                logger.info("浏览器状态已自动保存到持久化目录")
                await self.context.close()

    # ============================================================
    # 登录
    # ============================================================
    async def _login(self):
        """登录 FastMoss"""
        logger.info("检查登录状态...")

        # 访问商品搜索页检测是否已登录
        try:
            await self.page.goto(
                config.SEARCH_URL + f"?region={config.REGION}",
                timeout=config.PAGE_TIMEOUT,
                wait_until="domcontentloaded",
            )
        except Exception:
            pass
        await asyncio.sleep(3)

        # 等待页面稳定
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # 检查是否已登录 — 看右上角是否显示用户信息而非「登录/注册」
        login_btn = await self.page.query_selector(
            'text="登录/注册", text="登录", a:has-text("登录/注册")'
        )
        # 更精确：检查是否有 FM 开头的用户 ID 显示
        user_info = await self.page.query_selector(
            '[class*="user"], [class*="avatar"], [class*="member"]'
        )
        page_text = await self.page.inner_text("body")

        if "FM" in page_text and ("旗舰版" in page_text or "会员" in page_text or "免费版" in page_text):
            logger.info("[OK] 已登录（检测到用户信息）")
            return

        logger.info("需要登录...")
        await self._do_login()

    async def _do_login(self):
        """执行登录 — FastMoss 使用模态弹窗登录（非页面导航）"""
        # 确保在正确的页面上（商品搜索页）
        current_url = self.page.url
        if "e-commerce/search" not in current_url:
            try:
                await self.page.goto(
                    config.SEARCH_URL + f"?region={config.REGION}",
                    timeout=config.PAGE_TIMEOUT,
                    wait_until="domcontentloaded",
                )
                await asyncio.sleep(3)
            except Exception:
                pass

        # 调试截图：登录前的页面状态
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        debug_path = os.path.join(config.OUTPUT_DIR, "debug_before_login.png")
        await self.page.screenshot(path=debug_path)
        logger.info(f"登录前截图已保存: {debug_path}")

        # ---- 打开登录弹窗 ----
        # 方法1: 点击页面右上角的「登录/注册」按钮
        # 注意: 页面上可能有多个「登录/注册」元素（侧边栏 vs 顶部），
        # 侧边栏的是 <a> 链接（触发导航），顶部的触发弹窗
        # 优先使用 JavaScript 方式触发弹窗，避免意外导航
        opened = False
        
        # 尝试点击顶部导航栏的登录按钮（通常在右上角，有特定样式）
        header_login_selectors = [
            'header >> text="登录/注册"',
            'nav >> text="登录/注册"',
            '.header >> text="登录/注册"',
            '[class*="header"] >> text="登录/注册"',
            '[class*="nav"] >> text="登录/注册"',
        ]
        for sel in header_login_selectors:
            try:
                btn = await self.page.query_selector(sel)
                if btn and await btn.is_visible():
                    await btn.click()
                    logger.info(f"点击了顶部「登录/注册」按钮 ({sel})")
                    opened = True
                    break
            except Exception:
                continue
        
        if not opened:
            # 回退：直接用 JavaScript 查找并点击最合适的登录按钮
            try:
                await self.page.evaluate("""
                    () => {
                        const btns = document.querySelectorAll('a, button, span, div');
                        for (const btn of btns) {
                            if (btn.textContent.trim() === '登录/注册' && 
                                btn.offsetHeight > 0 && 
                                btn.getBoundingClientRect().top < 100) {
                                btn.click();
                                return true;
                            }
                        }
                        // 如果没有在顶部找到，点击任意一个
                        for (const btn of btns) {
                            if (btn.textContent.trim() === '登录/注册' && btn.offsetHeight > 0) {
                                btn.click();
                                return true;
                            }
                        }
                        return false;
                    }
                """)
                logger.info("通过 JS 点击了「登录/注册」按钮")
                opened = True
            except Exception as e:
                logger.debug(f"JS 点击失败: {e}")

        # 等待弹窗出现
        await asyncio.sleep(2)

        # ---- Step 1: 切换到「手机号登录/注册」tab ----
        # 弹窗默认显示微信二维码，需要切换到手机号 tab
        try:
            phone_tab = await self.page.wait_for_selector(
                'text="手机号登录/注册"', timeout=5000
            )
            if phone_tab and await phone_tab.is_visible():
                await phone_tab.click()
                logger.info("切换到「手机号登录/注册」tab")
        except Exception:
            logger.debug("未找到手机号tab，可能已在手机号模式")
        await asyncio.sleep(2)  # 等待 tab 内容完全渲染

        # ---- Step 2: 切换到「密码登录」模式 ----
        # 手机号 tab 默认是验证码模式，需要点击「密码登录」切换
        pw_switched = False
        # 方法1: wait_for_selector
        try:
            pw_login = await self.page.wait_for_selector(
                'text="密码登录"', timeout=8000
            )
            if pw_login and await pw_login.is_visible():
                await pw_login.click()
                logger.info("切换到「密码登录」模式")
                pw_switched = True
        except Exception:
            pass

        # 方法2: JS 查找并点击
        if not pw_switched:
            try:
                result = await self.page.evaluate("""
                    () => {
                        const els = document.querySelectorAll('a, span, div, p');
                        for (const el of els) {
                            if (el.textContent.trim() === '密码登录' && el.offsetHeight > 0) {
                                el.click();
                                return true;
                            }
                        }
                        return false;
                    }
                """)
                if result:
                    logger.info("通过 JS 切换到「密码登录」模式")
                    pw_switched = True
                else:
                    logger.warning("未找到「密码登录」链接")
            except Exception:
                pass
        await asyncio.sleep(1.5)

        # 调试截图：登录表单状态
        debug_path2 = os.path.join(config.OUTPUT_DIR, "debug_login_form.png")
        await self.page.screenshot(path=debug_path2)
        logger.info(f"登录表单截图已保存: {debug_path2}")

        # ---- Step 3: 填写手机号 ----
        # 密码模式 placeholder: 「输入您的手机号码」
        # 验证码模式 placeholder 也可能是: 「输入您的手机号码」或 「请输入手机号」
        phone_input = None
        phone_selectors = [
            'input[placeholder="输入您的手机号码"]',
            'input[placeholder*="手机号码"]',
            'input[placeholder*="手机号"]',
            'input[placeholder*="手机"]',
            'input[placeholder*="phone"]',
            'input[type="tel"]',
            'input[type="number"]',
        ]
        for sel in phone_selectors:
            elem = await self.page.query_selector(sel)
            if elem and await elem.is_visible():
                phone_input = elem
                logger.info(f"找到手机号输入框: {sel}")
                break

        # 方法2: 通过 JS 在弹窗内查找 input
        if not phone_input:
            try:
                phone_input = await self.page.evaluate_handle("""
                    () => {
                        // 查找 modal/dialog 中的所有 input
                        const modals = document.querySelectorAll(
                            '.ant-modal, [class*="modal"], [class*="dialog"], [class*="popup"], [role="dialog"]'
                        );
                        for (const modal of modals) {
                            const inputs = modal.querySelectorAll('input');
                            for (const input of inputs) {
                                if (input.offsetHeight > 0 && 
                                    (input.type === 'text' || input.type === 'tel' || input.type === 'number' || input.type === '')) {
                                    if (input.type !== 'password') {
                                        return input;
                                    }
                                }
                            }
                        }
                        // 备用：查找所有可见的非密码 input
                        const allInputs = document.querySelectorAll('input:not([type="password"]):not([type="hidden"])');
                        for (const input of allInputs) {
                            if (input.offsetHeight > 0 && input.placeholder && 
                                (input.placeholder.includes('手机') || input.placeholder.includes('phone'))) {
                                return input;
                            }
                        }
                        return null;
                    }
                """)
                if phone_input:
                    phone_input = phone_input.as_element()
                    if phone_input:
                        logger.info("通过 JS 在弹窗中找到手机号输入框")
            except Exception as e:
                logger.debug(f"JS 查找手机号输入框异常: {e}")

        if phone_input:
            await phone_input.click(click_count=3)  # 全选以清空
            await asyncio.sleep(0.2)
            await phone_input.fill(config.FASTMOSS_PHONE)
            logger.info(f"已填写手机号: {config.FASTMOSS_PHONE}")
        else:
            logger.warning("未找到手机号输入框，请在浏览器中手动填写")
            input(">>> 请手动填写手机号后按 Enter 继续...")

        await asyncio.sleep(0.5)

        # ---- Step 4: 填写密码 ----
        # 实际 placeholder 是「输入密码」
        pw_input = None
        pw_selectors = [
            'input[placeholder="输入密码"]',
            'input[type="password"]',
            'input[placeholder*="密码"]',
        ]
        for sel in pw_selectors:
            elem = await self.page.query_selector(sel)
            if elem and await elem.is_visible():
                pw_input = elem
                logger.debug(f"找到密码输入框: {sel}")
                break

        if pw_input:
            await pw_input.click(click_count=3)
            await asyncio.sleep(0.2)
            await pw_input.fill(config.FASTMOSS_PASSWORD)
            logger.info("已填写密码")
        else:
            logger.warning("未找到密码输入框，请在浏览器中手动填写")
            input(">>> 请手动填写密码后按 Enter 继续...")

        await asyncio.sleep(0.5)

        # ---- Step 5: 点击「注册/登录」提交按钮 ----
        submit_selectors = [
            'button:has-text("注册/登录")',
            'button:has-text("登录")',
            'button[type="submit"]',
        ]
        for sel in submit_selectors:
            btns = await self.page.query_selector_all(sel)
            for btn in btns:
                if await btn.is_visible():
                    await btn.click()
                    logger.info("点击了「注册/登录」按钮")
                    break
            else:
                continue
            break

        # ---- Step 6: 等待登录完成（可能出现验证码） ----
        await asyncio.sleep(3)
        await self._check_and_handle_captcha("登录时")

        # 等待弹窗关闭和页面更新
        await asyncio.sleep(3)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        await asyncio.sleep(2)

        # ---- Step 7: 确认登录状态 ----
        page_text = await self.page.inner_text("body")
        if "FM" in page_text and ("旗舰版" in page_text or "会员" in page_text or "免费" in page_text):
            logger.info("[OK] 登录成功！")
        else:
            logger.warning("登录可能未成功，请在浏览器中手动确认")
            input(">>> 请在浏览器中完成登录后按 Enter 继续...")
            await asyncio.sleep(2)

    async def _check_and_handle_captcha(self, context=""):
        """检查并处理验证码（需要手动完成）"""
        await asyncio.sleep(1)

        # 检测滑块验证码
        captcha_selectors = [
            'text="Slide to complete the puzzle"',
            'text="滑动完成拼图"',
            '[class*="captcha"]',
            '[class*="verify"]',
            '[class*="slider"]',
            '[class*="puzzle"]',
            'div:has-text("Slide to complete")',
            'div:has-text("滑动")',
        ]

        for sel in captcha_selectors:
            try:
                elem = await self.page.query_selector(sel)
                if elem and await elem.is_visible():
                    logger.warning(f"[!] 检测到验证码 ({context})，请在浏览器中手动完成!")
                    input(">>> 完成验证码后按 Enter 继续...")
                    await asyncio.sleep(2)
                    return True
            except Exception:
                continue

        return False

    # ============================================================
    # 子分类自动发现
    # ============================================================
    async def _discover_subcategories(self):
        """从页面中自动发现所有子分类"""
        logger.info("正在自动发现子分类...")

        # 导航到汽车与摩托车分类（不指定子分类）
        url = f"{config.SEARCH_URL}?region={config.REGION}&page=1&l1_cid={config.L1_CID}&pagesize={config.PAGE_SIZE}"
        try:
            await self.page.goto(url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
        except Exception:
            pass
        await asyncio.sleep(3)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # 检查验证码
        await self._check_and_handle_captcha("子分类发现")

        # 方法1: 尝试从页面 DOM 提取子分类
        # FastMoss 的子分类通常在悬停展开的下拉菜单中
        # 先点击「汽车与摩托车」分类按钮来展开子分类
        category_btn = await self.page.query_selector(
            'text="汽车与摩托车"'
        )
        if category_btn and await category_btn.is_visible():
            # 悬停触发子分类菜单
            await category_btn.hover()
            await asyncio.sleep(1)

            # 尝试获取子分类菜单项
            # Ant Design Dropdown 通常在 body 末尾渲染
            subcategory_selectors = [
                '.ant-dropdown-menu-item',
                '.ant-dropdown li',
                '[class*="subcategory"] li',
                '[class*="sub-menu"] li',
                '[class*="dropdown"] a',
                '.category-popup li',
                '.category-dropdown li',
            ]

            for sel in subcategory_selectors:
                items = await self.page.query_selector_all(sel)
                if items and len(items) > 1:
                    logger.info(f"通过选择器 {sel} 找到 {len(items)} 个子分类项")
                    for item in items:
                        text = (await item.inner_text()).strip()
                        if text and text not in ("全部", "更多", "展开"):
                            logger.info(f"  发现子分类: {text}")
                    break

        # 方法2: 从 URL 变化中提取 l2_cid
        # 在页面中查找所有包含 l2_cid 的链接
        links = await self.page.query_selector_all('a[href*="l2_cid"]')
        for link in links:
            href = await link.get_attribute("href")
            text = (await link.inner_text()).strip()
            if href and "l2_cid=" in href:
                match = re.search(r'l2_cid=(\d+)', href)
                if match:
                    cid = int(match.group(1))
                    if not any(s["l2_cid"] == cid for s in self.discovered_subcategories):
                        self.discovered_subcategories.append({"l2_cid": cid, "name": text})
                        logger.info(f"  从链接发现子分类: {text} (l2_cid={cid})")

        # 方法3: 试图点击展开更多子分类
        expand_btns = await self.page.query_selector_all(
            'text="展开", text="更多", span:has-text("展开"), [class*="expand"]'
        )
        for btn in expand_btns:
            if await btn.is_visible():
                await btn.click()
                await asyncio.sleep(1)
                break

        # 再次尝试从页面获取
        # 获取所有 radio-button 样式的分类选项
        radio_buttons = await self.page.query_selector_all(
            '.ant-radio-button-wrapper, [class*="category-item"], [class*="tag"]'
        )
        for rb in radio_buttons:
            text = (await rb.inner_text()).strip()
            data_attrs = await rb.evaluate('el => JSON.stringify(el.dataset)')
            # 记录日志
            if text and "汽车" not in text and text not in ("全部", "在售", "下架"):
                logger.debug(f"  发现标签: {text}")

        # 合并已知子分类和发现的子分类
        all_subcategories = list(config.SUBCATEGORIES)
        for disc in self.discovered_subcategories:
            if not any(s["l2_cid"] == disc["l2_cid"] for s in all_subcategories):
                all_subcategories.append(disc)

        if len(all_subcategories) > len(config.SUBCATEGORIES):
            logger.info(
                f"[OK] 自动发现了额外的子分类！"
                f"配置中 {len(config.SUBCATEGORIES)} 个 → 总共 {len(all_subcategories)} 个"
            )
        else:
            logger.info(f"未发现额外子分类，使用配置中的 {len(config.SUBCATEGORIES)} 个")

        self.discovered_subcategories = all_subcategories
        logger.info(f"子分类列表 ({len(self.discovered_subcategories)} 个):")
        for sc in self.discovered_subcategories:
            logger.info(f"  - {sc['name']} (l2_cid={sc['l2_cid']})")

    # ============================================================
    # 页面导航 + 拦截器爬取（核心策略）
    # 页面JS会自动重试并生成 fm-sign，我们只需等待拦截成功响应
    # ============================================================
    async def _crawl_category_via_navigation(self):
        """导航到每个子分类页面，等待页面JS自动发起API请求"""
        subcategories = self.discovered_subcategories or config.SUBCATEGORIES
        logger.info(f"将按 {len(subcategories)} 个子分类分别爬取 (页面导航+拦截器)")

        for i, sc in enumerate(subcategories):
            logger.info(f"\n{'=' * 40}")
            logger.info(f"[{i + 1}/{len(subcategories)}] 子分类: {sc['name']} (l2_cid={sc['l2_cid']})")
            logger.info(f"{'=' * 40}")

            # 重置当前批次
            self.all_products = []
            self.api_data_received = False
            self.latest_api_items = []
            self.total_records = 0
            self.current_page = 0

            await self._crawl_subcategory_pages(sc)

            # 保存当前子分类数据
            if self.all_products:
                self._save_subcategory_data(sc["name"])
                self.total_products.extend(self.all_products)
                logger.info(
                    f"[OK] [{sc['name']}] 完成: {len(self.all_products)} 条, "
                    f"总累计 {len(self.total_products)} 条"
                )
            else:
                logger.warning(f"[{sc['name']}] 未获取到数据")

            if i < len(subcategories) - 1:
                delay = config.CATEGORY_DELAY
                logger.info(f"等待 {delay}s 后切换到下一个子分类...")
                await asyncio.sleep(delay)

    async def _crawl_subcategory_pages(self, subcategory: dict):
        """爬取单个子分类的所有页面 — 导航+等待拦截"""
        l2_cid = subcategory["l2_cid"]
        name = subcategory["name"]

        # 第1页
        page_url = (
            f"https://www.fastmoss.com/zh/e-commerce/search"
            f"?region={config.REGION}&page=1"
            f"&l1_cid={config.L1_CID}&pagesize={config.PAGE_SIZE}"
            f"&l2_cid={l2_cid}"
        )
        logger.info(f"导航到: {page_url[:90]}...")
        try:
            await self.page.goto(page_url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
        except Exception:
            pass

        # 等待页面JS重试并成功获取数据（诊断显示需要约30秒）
        self.api_data_received = False
        self.latest_api_items = []
        waited = 0
        max_wait = 45  # 等待最多45秒
        while not self.api_data_received and waited < max_wait:
            await asyncio.sleep(1)
            waited += 1
            if waited % 10 == 0:
                logger.info(f"  等待数据中... ({waited}s)")

        if not self.api_data_received:
            logger.warning(f"  等待 {max_wait}s 后仍未拦截到数据")
            # 截图调试
            debug_path = os.path.join(config.OUTPUT_DIR, f"debug_{name}.png")
            await self.page.screenshot(path=debug_path, full_page=True)
            return

        # 计算总页数
        total_pages = 1
        if self.total_records:
            total_pages = min(
                (self.total_records + config.PAGE_SIZE - 1) // config.PAGE_SIZE,
                config.MAX_PAGES,
            )
        logger.info(
            f"  首页获取 {len(self.all_products)} 条, "
            f"总记录 {self.total_records}, 总页数 {total_pages}"
        )

        # 翻页
        for page_num in range(2, total_pages + 1):
            delay = random.uniform(config.PAGE_DELAY_MIN, config.PAGE_DELAY_MAX)
            await asyncio.sleep(delay)

            # 通过URL导航翻页
            next_url = (
                f"https://www.fastmoss.com/zh/e-commerce/search"
                f"?region={config.REGION}&page={page_num}"
                f"&l1_cid={config.L1_CID}&pagesize={config.PAGE_SIZE}"
                f"&l2_cid={l2_cid}"
            )

            self.api_data_received = False
            self.latest_api_items = []
            before_count = len(self.all_products)

            try:
                await self.page.goto(next_url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
            except Exception:
                pass

            # 等待数据
            waited = 0
            while not self.api_data_received and waited < max_wait:
                await asyncio.sleep(1)
                waited += 1

            new_count = len(self.all_products) - before_count
            logger.info(
                f"  第 {page_num}/{total_pages} 页: "
                f"新增 {new_count} 条, 累计 {len(self.all_products)} 条 "
                f"(等待 {waited}s)"
            )

            if new_count == 0:
                logger.info(f"  第 {page_num} 页无新数据，停止翻页")
                break

            # 检查点
            if len(self.all_products) % (config.PAGE_SIZE * 20) == 0:
                self._save_checkpoint(f"{name}_p{page_num}")

    # ============================================================
    # 浏览器内 fetch/XHR 调 API（备用方案）
    # ============================================================
    async def _crawl_via_browser_fetch(self):
        """在浏览器页面内用 fetch() 调 API，自动携带签名"""
        subcategories = self.discovered_subcategories or config.SUBCATEGORIES
        logger.info(f"将按 {len(subcategories)} 个子分类分别爬取 (浏览器内 fetch)")

        for i, sc in enumerate(subcategories):
            logger.info(f"\n{'=' * 40}")
            logger.info(f"[{i + 1}/{len(subcategories)}] 子分类: {sc['name']} (l2_cid={sc['l2_cid']})")
            logger.info(f"{'=' * 40}")

            # 重置当前批次
            self.all_products = []
            self.seen_ids = set()
            self.total_records = 0

            await self._crawl_subcategory_fetch(sc)

            # 保存当前子分类数据
            if self.all_products:
                self._save_subcategory_data(sc["name"])
                self.total_products.extend(self.all_products)
                logger.info(
                    f"[OK] [{sc['name']}] 完成: {len(self.all_products)} 条, "
                    f"总累计 {len(self.total_products)} 条"
                )
            else:
                logger.warning(f"[{sc['name']}] 未获取到数据")

            # 切换间隔
            if i < len(subcategories) - 1:
                delay = config.CATEGORY_DELAY
                logger.info(f"等待 {delay}s 后切换到下一个子分类...")
                await asyncio.sleep(delay)

    async def _crawl_subcategory_fetch(self, subcategory: dict):
        """在浏览器内 fetch 爬取单个子分类"""
        l2_cid = subcategory["l2_cid"]
        name = subcategory["name"]

        # 先导航到该子分类的搜索页（让页面JS加载签名拦截器）
        page_url = (
            f"https://www.fastmoss.com/zh/e-commerce/search"
            f"?region={config.REGION}&page=1"
            f"&l1_cid={config.L1_CID}&pagesize={config.PAGE_SIZE}"
            f"&l2_cid={l2_cid}"
        )
        logger.info(f"导航到搜索页: {page_url[:80]}...")
        try:
            await self.page.goto(page_url, timeout=config.PAGE_TIMEOUT, wait_until="domcontentloaded")
        except Exception:
            pass
        await asyncio.sleep(5)  # 等待页面JS完全初始化

        # 逐页爬取
        page_num = 1
        max_pages = config.MAX_PAGES
        retry_count = 0

        while page_num <= max_pages:
            _time = int(time.time())
            cnonce = random.randint(10000000, 99999999)
            api_path = (
                f"/api/goods/V2/search"
                f"?page={page_num}&pagesize={config.PAGE_SIZE}"
                f"&order=2,2&region={config.REGION}"
                f"&l1_cid={config.L1_CID}&l2_cid={l2_cid}"
                f"&_time={_time}&cnonce={cnonce}"
            )

            # 在浏览器页面内执行 XMLHttpRequest（触发 axios 拦截器自动添加 fm-sign）
            try:
                result = await self.page.evaluate(
                    """(apiPath) => {
                        return new Promise((resolve) => {
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', apiPath, true);
                            xhr.setRequestHeader('Accept', 'application/json, text/plain, */*');
                            xhr.onload = function() {
                                try {
                                    resolve({ ok: true, body: JSON.parse(xhr.responseText) });
                                } catch(e) {
                                    resolve({ ok: false, error: 'JSON parse error: ' + e.message });
                                }
                            };
                            xhr.onerror = function() {
                                resolve({ ok: false, error: 'XHR network error' });
                            };
                            xhr.ontimeout = function() {
                                resolve({ ok: false, error: 'XHR timeout' });
                            };
                            xhr.timeout = 30000;
                            xhr.send();
                        });
                    }""",
                    api_path,
                )
            except Exception as e:
                logger.error(f"  浏览器 fetch 失败 (page={page_num}): {e}")
                retry_count += 1
                if retry_count >= config.MAX_RETRIES:
                    break
                await asyncio.sleep(3)
                continue

            if not result.get("ok"):
                logger.error(f"  fetch 错误: {result.get('error')}")
                retry_count += 1
                if retry_count >= config.MAX_RETRIES:
                    break
                await asyncio.sleep(2)
                continue

            body = result["body"]
            code = body.get("code")

            if code != 200:
                logger.warning(f"  API 返回 code={code} (page={page_num})")
                retry_count += 1
                if retry_count >= config.MAX_RETRIES:
                    logger.error(f"  连续 {config.MAX_RETRIES} 次非200响应，停止")
                    break
                await asyncio.sleep(2)
                continue

            retry_count = 0

            data = body.get("data", {})
            items = data.get("product_list", [])
            total = data.get("total", data.get("total_cnt", 0))

            if not items:
                logger.info(f"  第 {page_num} 页无数据，爬取结束")
                break

            # 更新总记录数
            if total and not self.total_records:
                self.total_records = total
                max_pages = min(
                    (total + config.PAGE_SIZE - 1) // config.PAGE_SIZE,
                    config.MAX_PAGES,
                )
                logger.info(f"  总记录: {total}, 总页数: {max_pages}")

            # 去重入库
            new_count = 0
            for item in items:
                item_id = str(item.get("product_id", item.get("id", "")))
                if item_id and item_id not in self.seen_ids:
                    self.seen_ids.add(item_id)
                    self.all_products.append(item)
                    new_count += 1

            # 首页输出字段信息
            if page_num == 1 and items:
                sample_keys = list(items[0].keys())
                logger.info(f"  数据字段 ({len(sample_keys)} 个): {sample_keys[:15]}...")

            logger.info(
                f"  第 {page_num}/{max_pages} 页: "
                f"获取 {len(items)} 条, 新增 {new_count} 条, "
                f"累计 {len(self.all_products)} 条"
            )

            # 检查点保存
            if len(self.all_products) % (config.PAGE_SIZE * 50) == 0 and self.all_products:
                self._save_checkpoint(f"{name}_p{page_num}")

            page_num += 1

            # 翻页延迟
            if page_num <= max_pages:
                delay = random.uniform(config.PAGE_DELAY_MIN, config.PAGE_DELAY_MAX)
                await asyncio.sleep(delay)

    # ============================================================
    # HTTP requests 直接调 API（备用方案，需要 fm-sign）
    # ============================================================
    async def _build_http_session(self):
        """从浏览器提取 cookies，构建 requests.Session"""
        cookies = await self.context.cookies()

        self.session = http_requests.Session()
        for c in cookies:
            if "fastmoss" in c.get("domain", ""):
                self.session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": "https://www.fastmoss.com",
            "lang": "ZH_CN",
            "source": "pc",
            "region": config.REGION,
        })

        logger.info(f"[OK] HTTP 会话已构建 (cookies: {len(self.session.cookies)} 个)")

    def _crawl_via_api(self):
        """用 requests 直接调 FastMoss API 爬取数据"""
        subcategories = self.discovered_subcategories or config.SUBCATEGORIES
        logger.info(f"将按 {len(subcategories)} 个子分类分别爬取 (HTTP 直接调 API)")

        for i, sc in enumerate(subcategories):
            logger.info(f"\n{'=' * 40}")
            logger.info(f"[{i + 1}/{len(subcategories)}] 子分类: {sc['name']} (l2_cid={sc['l2_cid']})")
            logger.info(f"{'=' * 40}")

            # 重置当前批次
            self.all_products = []
            self.seen_ids = set()
            self.total_records = 0

            self._crawl_subcategory_api(sc)

            # 保存当前子分类数据
            if self.all_products:
                self._save_subcategory_data(sc["name"])
                self.total_products.extend(self.all_products)
                logger.info(
                    f"[OK] [{sc['name']}] 完成: {len(self.all_products)} 条, "
                    f"总累计 {len(self.total_products)} 条"
                )
            else:
                logger.warning(f"[{sc['name']}] 未获取到数据")

            # 切换间隔
            if i < len(subcategories) - 1:
                delay = config.CATEGORY_DELAY
                logger.info(f"等待 {delay}s 后切换到下一个子分类...")
                time.sleep(delay)

    def _crawl_subcategory_api(self, subcategory: dict):
        """爬取单个子分类的所有页面"""
        l2_cid = subcategory["l2_cid"]
        name = subcategory["name"]
        page_num = 1
        max_pages = config.MAX_PAGES
        retry_count = 0

        while page_num <= max_pages:
            # 构造 API URL
            _time = int(time.time())
            cnonce = random.randint(10000000, 99999999)
            api_url = (
                f"https://www.fastmoss.com/api/goods/V2/search"
                f"?page={page_num}&pagesize={config.PAGE_SIZE}"
                f"&order=2,2&region={config.REGION}"
                f"&l1_cid={config.L1_CID}&l2_cid={l2_cid}"
                f"&_time={_time}&cnonce={cnonce}"
            )

            referer = (
                f"https://www.fastmoss.com/zh/e-commerce/search"
                f"?region={config.REGION}&page={page_num}"
                f"&l1_cid={config.L1_CID}&pagesize={config.PAGE_SIZE}"
                f"&l2_cid={l2_cid}"
            )

            try:
                resp = self.session.get(
                    api_url,
                    headers={"Referer": referer},
                    timeout=30,
                )
                body = resp.json()
            except Exception as e:
                logger.error(f"  API 请求失败 (page={page_num}): {e}")
                retry_count += 1
                if retry_count >= config.MAX_RETRIES:
                    logger.error(f"  超过最大重试次数，停止爬取 [{name}]")
                    break
                time.sleep(3)
                continue

            code = body.get("code")
            if code != 200:
                logger.warning(f"  API 返回 code={code} (page={page_num})")
                retry_count += 1
                if retry_count >= config.MAX_RETRIES:
                    logger.error(f"  连续 {config.MAX_RETRIES} 次非200响应，停止")
                    break
                time.sleep(2)
                continue

            retry_count = 0  # 成功则重置重试计数

            data = body.get("data", {})
            items = data.get("product_list", [])
            total = data.get("total", data.get("total_cnt", 0))

            if not items:
                logger.info(f"  第 {page_num} 页无数据，爬取结束")
                break

            # 更新总记录数
            if total and not self.total_records:
                self.total_records = total
                max_pages = min(
                    (total + config.PAGE_SIZE - 1) // config.PAGE_SIZE,
                    config.MAX_PAGES,
                )
                logger.info(f"  总记录: {total}, 总页数: {max_pages}")

            # 去重入库
            new_count = 0
            for item in items:
                item_id = str(item.get("product_id", item.get("id", "")))
                if item_id and item_id not in self.seen_ids:
                    self.seen_ids.add(item_id)
                    self.all_products.append(item)
                    new_count += 1

            # 首页输出字段信息
            if page_num == 1 and items:
                sample_keys = list(items[0].keys())
                logger.info(f"  数据字段 ({len(sample_keys)} 个): {sample_keys[:15]}...")

            logger.info(
                f"  第 {page_num}/{max_pages} 页: "
                f"获取 {len(items)} 条, 新增 {new_count} 条, "
                f"累计 {len(self.all_products)} 条"
            )

            # 检查点保存
            if len(self.all_products) % (config.PAGE_SIZE * 50) == 0 and self.all_products:
                self._save_checkpoint(f"{name}_p{page_num}")

            page_num += 1

            # 翻页延迟
            if page_num <= max_pages:
                delay = random.uniform(config.PAGE_DELAY_MIN, config.PAGE_DELAY_MAX)
                time.sleep(delay)

    # ============================================================
    # API 拦截器（保留作为备用）
    # ============================================================
    def _setup_api_interceptor(self):
        """注册 API 响应拦截器"""

        spider = self

        async def on_response(response):
            url = response.url

            # 只关注 JSON 响应
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type:
                return

            if response.status != 200:
                return

            # 记录所有 JSON API 调用（调试用）
            logger.debug(f"  [API] JSON响应: {url[-100:]}")

            try:
                body = await response.json()

                if not isinstance(body, dict):
                    return

                # FastMoss API 通常返回: {code: 0/200, data: {list: [...], total: N}}
                data = body.get("data", body)

                items = None
                total = None

                if isinstance(data, dict):
                    for key in ["product_list", "list", "items", "records", "rows", "content", "products", "goods"]:
                        if key in data and isinstance(data[key], list):
                            items = data[key]
                            break

                    for key in ["total", "total_cnt", "result_cnt", "totalCount", "totalElements", "count", "totalNum"]:
                        if key in data:
                            total = data[key]
                            break

                elif isinstance(data, list):
                    items = data

                if not items or len(items) == 0:
                    # 对搜索 API 响应做特殊日志，便于诊断
                    if "search" in url.lower() or "goods" in url.lower():
                        logger.info(
                            f"  [API] 搜索结果为空: URL=...{url[-80:]}, "
                            f"code={body.get('code', '?')}, "
                            f"data_type={type(data).__name__}, "
                            f"data_keys={list(data.keys()) if isinstance(data, dict) else 'N/A'}, "
                            f"total={total}"
                        )
                    return

                sample = items[0]
                if not isinstance(sample, dict):
                    return

                sample_keys = set(k.lower() for k in sample.keys())

                # --- 严格的商品数据验证 ---
                # 必须包含**至少 2 个**以下强特征字段（排除国家、配置等非商品数据）
                strong_product_indicators = [
                    "price", "sales", "gmv", "shopname", "shop_name",
                    "title", "product_name", "goodsname", "goods_name",
                    "commission", "creator", "category",
                    "seven_day_sales", "7d_sales", "total_sales",
                    "product_id", "goods_id", "productid", "goodsid",
                    "product_url", "goods_url", "image", "img",
                    "sold_count", "sale_amount", "trend",  # FastMoss V2 字段
                    "inc_sold_count", "shop", "author",
                ]
                match_count = 0
                for indicator in strong_product_indicators:
                    if indicator in sample_keys or any(indicator in k for k in sample_keys):
                        match_count += 1

                if match_count < 2:
                    # 不是商品数据，跳过
                    logger.debug(
                        f"  [API] 跳过非商品数据: {len(items)} 条, "
                        f"字段={list(sample.keys())[:5]}..., 匹配={match_count}"
                    )
                    return

                logger.info(
                    f"  [OK] API 拦截: {len(items)} 条商品数据 "
                    f"(total={total}, URL=...{url[-80:]})"
                )

                if total:
                    spider.total_records = total

                # 去重并存储
                new_count = 0
                id_fields = [
                    "product_id", "goods_id", "id", "productId",
                    "goodsId", "itemId", "asin",
                ]
                for item in items:
                    item_id = None
                    for f in id_fields:
                        v = item.get(f)
                        if v:
                            item_id = str(v)
                            break

                    if item_id and item_id not in spider.seen_ids:
                        spider.seen_ids.add(item_id)
                        new_count += 1
                    elif not item_id:
                        new_count += 1

                spider.latest_api_items = items
                spider.all_products.extend(items)
                spider.api_data_received = True

                # 首次输出数据字段
                if len(spider.all_products) == len(items):
                    logger.info(f"  数据字段 ({len(sample)} 个): {list(sample.keys())}")

                logger.info(
                    f"  新增 {new_count} 条, 重复 {len(items) - new_count} 条, "
                    f"累计 {len(spider.all_products)} 条"
                )

            except Exception as e:
                logger.debug(f"  API 解析异常: {e}")

        self.page.on("response", on_response)
        logger.info("[OK] API 拦截器已注册")

    # ============================================================
    # 主爬取流程
    # ============================================================
    async def _crawl_category(self):
        """
        爬取策略:
        FastMoss 必须指定子分类（l2_cid）才能返回数据，
        所以直接按子分类逐个爬取。
        """
        subcategories = self.discovered_subcategories or config.SUBCATEGORIES
        logger.info(f"将按 {len(subcategories)} 个子分类分别爬取")

        for i, sc in enumerate(subcategories):
            logger.info(f"\n{'=' * 40}")
            logger.info(f"[{i + 1}/{len(subcategories)}] 子分类: {sc['name']} (l2_cid={sc['l2_cid']})")
            logger.info(f"{'=' * 40}")

            # 重置当前批次数据
            self.all_products = []
            self.api_data_received = False
            self.current_page = 0
            self.total_records = 0

            url = self._build_search_url(page=1, l2_cid=sc["l2_cid"])
            await self._crawl_pages(url, label=sc["name"])

            # 保存当前子分类数据
            if self.all_products:
                self._save_subcategory_data(sc["name"])
                self.total_products.extend(self.all_products)
                logger.info(
                    f"[OK] [{sc['name']}] 完成: {len(self.all_products)} 条, "
                    f"总累计 {len(self.total_products)} 条"
                )
            else:
                logger.warning(f"[{sc['name']}] 未获取到数据")

            # 切换子分类间隔
            if i < len(subcategories) - 1:
                delay = config.CATEGORY_DELAY
                logger.info(f"等待 {delay}s 后切换到下一个子分类...")
                await asyncio.sleep(delay)

    async def _crawl_pages(self, start_url: str, label: str = ""):
        """爬取所有页面"""
        # 导航到第一页
        logger.info(f"导航到: {start_url[:100]}...")
        try:
            await self.page.goto(
                start_url,
                timeout=config.PAGE_TIMEOUT,
                wait_until="domcontentloaded",
            )
        except Exception as e:
            if "interrupted" in str(e):
                await asyncio.sleep(3)
            else:
                raise
        await asyncio.sleep(3)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # 检查验证码
        await self._check_and_handle_captcha(f"首页加载 - {label}")

        # 等待数据加载
        await self._wait_for_data()

        if not self.api_data_received:
            # API 拦截失败，尝试 DOM 解析
            logger.info("API 拦截未获取数据，尝试 DOM 解析...")
            await self._parse_from_dom()

        if not self.all_products:
            logger.warning(f"未获取到 [{label}] 的首页数据")

            # 截图调试
            debug_path = os.path.join(config.OUTPUT_DIR, f"debug_{label}.png")
            await self.page.screenshot(path=debug_path, full_page=True)
            logger.info(f"  调试截图已保存: {debug_path}")
            return

        # 计算总页数
        total_pages = 1
        if self.total_records:
            total_pages = min(
                (self.total_records + config.PAGE_SIZE - 1) // config.PAGE_SIZE,
                config.MAX_PAGES,
            )
        else:
            # 尝试从分页器获取总页数
            total_pages = await self._get_total_pages_from_dom()

        logger.info(
            f"[{label}] 首页获取 {len(self.all_products)} 条, "
            f"总记录 {self.total_records}, 总页数 {total_pages}"
        )

        # 翻页
        self.current_page = 1
        consecutive_empty = 0

        while self.current_page < total_pages:
            self.current_page += 1

            # 随机延迟
            delay = random.uniform(config.PAGE_DELAY_MIN, config.PAGE_DELAY_MAX)
            logger.info(
                f"  [{label}] [{self.current_page}/{total_pages}] "
                f"累计 {len(self.all_products)} 条, 等待 {delay:.1f}s..."
            )
            await asyncio.sleep(delay)

            # 翻页前记录数据量
            before_count = len(self.all_products)
            self.api_data_received = False

            # 执行翻页
            success = await self._go_to_page(self.current_page)

            if not success:
                # 重试
                for retry in range(config.MAX_RETRIES):
                    logger.warning(f"  翻页重试 {retry + 1}/{config.MAX_RETRIES}...")
                    await asyncio.sleep(3)
                    success = await self._go_to_page(self.current_page)
                    if success:
                        break

                if not success:
                    logger.error(f"  翻页失败，停止 [{label}] 的爬取")
                    break

            # 检查是否有新数据
            new_count = len(self.all_products) - before_count
            if new_count == 0:
                consecutive_empty += 1
                logger.warning(
                    f"  [!] 第 {self.current_page} 页无新数据 "
                    f"(连续 {consecutive_empty} 页)"
                )
                if consecutive_empty >= 3:
                    logger.info("  连续 3 页无新数据，停止翻页")
                    break
            else:
                consecutive_empty = 0

            # 每 50 页保存一次检查点
            if self.current_page % 50 == 0:
                self._save_checkpoint(f"{label}_page{self.current_page}")

    async def _go_to_page(self, page_num: int) -> bool:
        """翻到指定页"""
        before_count = len(self.all_products)
        self.api_data_received = False

        # 方法1: 通过 URL 参数直接翻页
        current_url = self.page.url
        # 替换或添加 page 参数
        parsed = urlparse(current_url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["page"] = [str(page_num)]
        new_query = urlencode(params, doseq=True)
        new_url = urlunparse(parsed._replace(query=new_query))

        try:
            await self.page.goto(
                new_url,
                timeout=config.PAGE_TIMEOUT,
                wait_until="domcontentloaded",
            )
        except Exception as e:
            if "interrupted" in str(e):
                await asyncio.sleep(3)
            else:
                logger.debug(f"  URL 翻页失败: {e}")
                # 方法2: 点击翻页按钮
                return await self._click_next_page()

        await asyncio.sleep(2)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # 检查验证码
        had_captcha = await self._check_and_handle_captcha(f"翻页 {page_num}")

        # 等待 API 数据
        success = await self._wait_for_data()

        if not success:
            # 回退到 DOM 解析
            await self._parse_from_dom()

        return len(self.all_products) > before_count

    async def _click_next_page(self) -> bool:
        """点击下一页按钮"""
        before_count = len(self.all_products)

        next_selectors = [
            '.ant-pagination-next:not(.ant-pagination-disabled)',
            'li.ant-pagination-next button',
            'button:has-text(">")',
            'a:has-text("下一页")',
            '.pagination-next',
        ]

        for sel in next_selectors:
            try:
                btn = await self.page.query_selector(sel)
                if btn and await btn.is_enabled():
                    await btn.click()
                    await asyncio.sleep(2)
                    try:
                        await self.page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                    await self._wait_for_data()
                    return len(self.all_products) > before_count
            except Exception:
                continue

        return False

    async def _wait_for_data(self) -> bool:
        """等待 API 拦截器收到数据"""
        waited = 0
        while not self.api_data_received and waited < config.DATA_WAIT_TIMEOUT:
            await asyncio.sleep(1)
            waited += 1

        return self.api_data_received

    # ============================================================
    # DOM 解析（备用方案）
    # ============================================================
    async def _parse_from_dom(self):
        """从页面 DOM 直接提取表格数据"""
        logger.info("尝试从 DOM 表格提取数据...")

        # Ant Design Table 的行
        rows = await self.page.query_selector_all(
            '.ant-table-tbody tr, .ant-table-row, table tbody tr'
        )

        if not rows:
            logger.debug("  未找到表格行")
            return

        logger.info(f"  找到 {len(rows)} 行表格数据")
        new_items = []

        for row in rows:
            try:
                cells = await row.query_selector_all('td, .ant-table-cell')
                if len(cells) < 5:
                    continue

                item = {}

                # 第1列: 商品信息
                product_cell = cells[0]
                # 商品名
                title_el = await product_cell.query_selector(
                    'a, [class*="title"], [class*="name"], span'
                )
                if title_el:
                    item["title"] = (await title_el.inner_text()).strip()

                # 价格
                price_el = await product_cell.query_selector(
                    '[class*="price"], span:has-text("$")'
                )
                if price_el:
                    item["price"] = (await price_el.inner_text()).strip()

                # 商品链接
                link_el = await product_cell.query_selector('a[href]')
                if link_el:
                    item["url"] = await link_el.get_attribute("href")

                # 第2列: 所属店铺
                if len(cells) > 1:
                    shop_el = cells[1]
                    item["shop"] = (await shop_el.inner_text()).strip()

                # 第3列: 达人出单率
                if len(cells) > 2:
                    item["creator_rate"] = (await cells[2].inner_text()).strip()

                # 跳过第4列（趋势图）

                # 第5列: 近7天销量
                if len(cells) > 4:
                    item["sales_7d"] = (await cells[4].inner_text()).strip()

                # 第6列: 近7天销售额
                if len(cells) > 5:
                    item["gmv_7d"] = (await cells[5].inner_text()).strip()

                # 第7列: 总销量
                if len(cells) > 6:
                    item["total_sales"] = (await cells[6].inner_text()).strip()

                # 第8列: 总销售额
                if len(cells) > 7:
                    item["total_gmv"] = (await cells[7].inner_text()).strip()

                # 第9列: 关联达人
                if len(cells) > 8:
                    item["creators"] = (await cells[8].inner_text()).strip()

                if item.get("title"):
                    new_items.append(item)

            except Exception as e:
                logger.debug(f"  解析行失败: {e}")
                continue

        if new_items:
            self.all_products.extend(new_items)
            logger.info(f"  [OK] DOM 提取了 {len(new_items)} 条数据")
        else:
            logger.info("  DOM 未提取到有效数据")

    async def _get_total_pages_from_dom(self) -> int:
        """从分页器获取总页数"""
        try:
            # Ant Design Pagination
            # 查找最后一个页码
            page_items = await self.page.query_selector_all(
                '.ant-pagination-item, li[class*="pagination"]'
            )
            if page_items:
                last_page_text = await page_items[-1].inner_text()
                match = re.search(r'\d+', last_page_text)
                if match:
                    total = int(match.group())
                    logger.info(f"  从分页器获取总页数: {total}")
                    return min(total, config.MAX_PAGES)

            # 尝试获取 "共 XXX 条" 文字
            total_text_el = await self.page.query_selector(
                '.ant-pagination-total-text, [class*="total"]'
            )
            if total_text_el:
                text = await total_text_el.inner_text()
                match = re.search(r'(\d+)', text)
                if match:
                    total_items = int(match.group(1))
                    total_pages = (total_items + config.PAGE_SIZE - 1) // config.PAGE_SIZE
                    return min(total_pages, config.MAX_PAGES)

            # 查找 "... 500" 模式（FastMoss 最大 500 页）
            last_li = await self.page.query_selector(
                '.ant-pagination li:last-child, .ant-pagination li:nth-last-child(2)'
            )
            if last_li:
                text = await last_li.inner_text()
                if text.strip().isdigit():
                    return min(int(text.strip()), config.MAX_PAGES)

        except Exception as e:
            logger.debug(f"  获取总页数失败: {e}")

        # 默认返回已知的最大值
        return config.MAX_PAGES

    # ============================================================
    # URL 构建
    # ============================================================
    def _build_search_url(self, page: int = 1, l2_cid: int = None) -> str:
        """构建搜索 URL"""
        params = {
            "region": config.REGION,
            "page": page,
            "l1_cid": config.L1_CID,
            "pagesize": config.PAGE_SIZE,
        }
        if l2_cid:
            params["l2_cid"] = l2_cid

        return f"{config.SEARCH_URL}?{urlencode(params)}"

    # ============================================================
    # 数据保存
    # ============================================================
    def _deduplicate(self, products: list) -> list:
        """去重"""
        seen = set()
        unique = []

        id_fields = [
            "product_id", "goods_id", "id", "productId",
            "goodsId", "itemId", "asin",
        ]

        id_field = None
        if products:
            sample = products[0]
            for f in id_fields:
                if f in sample:
                    id_field = f
                    break

        if id_field:
            for p in products:
                pid = str(p.get(id_field, ""))
                if pid and pid not in seen:
                    seen.add(pid)
                    unique.append(p)
        else:
            # 用 JSON 哈希去重
            for p in products:
                key = json.dumps(p, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    unique.append(p)

        return unique

    def _save_subcategory_data(self, subcategory_name: str):
        """保存单个子分类的数据"""
        if not self.all_products:
            return

        unique = self._deduplicate(self.all_products)
        logger.info(f"[{subcategory_name}] 去重: {len(self.all_products)} → {len(unique)} 条")

        # JSON
        json_path = os.path.join(
            config.OUTPUT_DIR,
            f"fastmoss_{subcategory_name}_{config.TIMESTAMP}.json",
        )
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "meta": {
                        "scrape_time": datetime.now().isoformat(),
                        "region": config.REGION,
                        "category": config.L1_NAME,
                        "subcategory": subcategory_name,
                        "total_records": self.total_records,
                        "scraped_count": len(unique),
                    },
                    "products": unique,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info(f"[OK] JSON: {json_path}")

        # Excel
        try:
            df = pd.json_normalize(unique, sep="_")
            excel_path = os.path.join(
                config.OUTPUT_DIR,
                f"fastmoss_{subcategory_name}_{config.TIMESTAMP}.xlsx",
            )
            df.to_excel(excel_path, index=False, engine="openpyxl")
            logger.info(f"[OK] Excel: {excel_path} ({len(df)} 行, {len(df.columns)} 列)")
        except Exception as e:
            logger.error(f"Excel 保存失败: {e}")

    def _save_checkpoint(self, suffix: str):
        """保存检查点数据"""
        if not self.all_products:
            return

        json_path = os.path.join(
            config.OUTPUT_DIR,
            f"checkpoint_{suffix}_{config.TIMESTAMP}.json",
        )
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.all_products, f, ensure_ascii=False, indent=2)
        logger.info(f"[OK] 检查点已保存: {json_path} ({len(self.all_products)} 条)")

    def _save_final_data(self):
        """保存最终合并数据"""
        if not self.total_products:
            logger.warning("没有数据可保存")
            return

        unique = self._deduplicate(self.total_products)
        logger.info(f"最终去重: {len(self.total_products)} → {len(unique)} 条")

        # JSON
        json_path = os.path.join(
            config.OUTPUT_DIR,
            f"fastmoss_{config.L1_NAME}_全部_{config.TIMESTAMP}.json",
        )
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "meta": {
                        "scrape_time": datetime.now().isoformat(),
                        "region": config.REGION,
                        "category": config.L1_NAME,
                        "total_records": self.total_records,
                        "scraped_count": len(unique),
                        "subcategories_crawled": [
                            sc["name"] for sc in (self.discovered_subcategories or config.SUBCATEGORIES)
                        ],
                    },
                    "products": unique,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info(f"[OK] 最终 JSON: {json_path}")

        # Excel
        try:
            df = pd.json_normalize(unique, sep="_")
            excel_path = os.path.join(
                config.OUTPUT_DIR,
                f"fastmoss_{config.L1_NAME}_全部_{config.TIMESTAMP}.xlsx",
            )
            df.to_excel(excel_path, index=False, engine="openpyxl")
            logger.info(
                f"[OK] 最终 Excel: {excel_path} ({len(df)} 行, {len(df.columns)} 列)"
            )
            logger.info(f"  列名: {list(df.columns[:15])}{'...' if len(df.columns) > 15 else ''}")
        except Exception as e:
            logger.error(f"最终 Excel 保存失败: {e}")
            # 尝试简化版
            try:
                df = pd.DataFrame(unique)
                excel_path = os.path.join(
                    config.OUTPUT_DIR,
                    f"fastmoss_{config.L1_NAME}_全部_{config.TIMESTAMP}_simple.xlsx",
                )
                df.to_excel(excel_path, index=False, engine="openpyxl")
                logger.info(f"[OK] 简化 Excel: {excel_path}")
            except Exception as e2:
                logger.error(f"简化保存也失败: {e2}")


# ============================================================
# 主程序入口
# ============================================================
async def main():
    spider = FastMossSpider()
    await spider.run()


if __name__ == "__main__":
    asyncio.run(main())
