# -*- coding: utf-8 -*-
"""
快速测试脚本 - 先登录并探测 SellerSprite API 结构

用途:
    1. 验证登录是否正常
    2. 发现真实的 API 端点和数据结构
    3. 保存一份样本数据

运行: python test_login.py
"""

import asyncio
import json
import os
from datetime import datetime

from playwright.async_api import async_playwright

import config


async def test():
    """测试登录并探测 API"""
    print("=" * 60)
    print("SellerSprite 登录测试 & API 探测")
    print("=" * 60)

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.BROWSER_STATE_DIR, exist_ok=True)

    captured_apis = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        # 记录所有网络请求
        async def on_response(response):
            url = response.url
            if response.status != 200:
                return
            # 记录所有 API 调用
            if "/v3/" in url and ("api" in url or "product" in url or "research" in url):
                content_type = response.headers.get("content-type", "")
                if "json" in content_type:
                    try:
                        body = await response.json()
                        entry = {
                            "url": url,
                            "method": response.request.method,
                            "status": response.status,
                            "keys": list(body.keys()) if isinstance(body, dict) else str(type(body)),
                        }

                        # 如果有 data 字段，进一步探测
                        if isinstance(body, dict) and "data" in body:
                            data = body["data"]
                            if isinstance(data, dict):
                                entry["data_keys"] = list(data.keys())
                                # 检查是否有产品列表
                                for key in ["items", "records", "list", "rows", "content", "products"]:
                                    if key in data and isinstance(data[key], list):
                                        items = data[key]
                                        entry["items_key"] = key
                                        entry["items_count"] = len(items)
                                        entry["total"] = data.get("total") or data.get("totalCount")
                                        if items:
                                            entry["sample_item_keys"] = list(items[0].keys())
                                        break

                        captured_apis.append(entry)
                        print(f"\n  📡 API: {url[:100]}")
                        print(f"     Method: {entry['method']}, Keys: {entry['keys']}")
                        if "items_count" in entry:
                            print(f"     ✓ 发现产品数据! items={entry['items_count']}, total={entry.get('total')}")
                            print(f"     字段: {entry.get('sample_item_keys', [])[:15]}")

                    except Exception:
                        pass

        page.on("response", on_response)

        # Step 1: 导航到登录页
        print("\n[1] 导航到 SellerSprite...")
        await page.goto(config.LOGIN_URL, timeout=config.PAGE_TIMEOUT)
        await page.wait_for_load_state("networkidle")

        print("\n" + "=" * 60)
        print("请在浏览器中完成登录操作")
        print("登录成功后，系统将自动跳转到产品调研页面")
        print("=" * 60)
        input("\n>>> 登录完成后按 Enter 继续...")

        # Step 2: 导航到产品调研页面
        print("\n[2] 导航到产品调研页面 (Automotive 分类)...")
        target_url = (
            f"{config.PRODUCT_RESEARCH_URL}?"
            f"market={config.MARKET}&page=1&size={config.PAGE_SIZE}"
            f"&symbolFlag=false&monthName=bsr_sales_nearly&selectType=2"
            f"&filterSub=false&weightUnit=g"
            f"&order%5Bfield%5D={config.ORDER_FIELD}"
            f"&order%5Bdesc%5D={'true' if config.ORDER_DESC else 'false'}"
            f"&productTags=%5B%5D"
            f"&nodeIdPaths=%5B%22{config.CATEGORY_NODE_ID}%22%5D"
            f"&sellerTypes=%5B%5D&eligibility=%5B%5D"
            f"&pkgDimensionTypeList=%5B%5D&sellerNationList=%5B%5D"
            f"&lowPrice=N&minSales={config.MIN_SALES}"
        )
        await page.goto(target_url, timeout=config.PAGE_TIMEOUT)
        await page.wait_for_load_state("networkidle")

        print("等待数据加载...")
        await asyncio.sleep(10)  # 等待 API 请求完成

        # Step 3: 保存结果
        print(f"\n[3] 共捕获 {len(captured_apis)} 个 API 调用")

        # 保存到文件
        result_path = os.path.join(config.OUTPUT_DIR, "api_discovery.json")
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(captured_apis, f, ensure_ascii=False, indent=2, default=str)
        print(f"✓ API 发现结果已保存: {result_path}")

        # 保存浏览器状态（下次可复用登录）
        state_file = os.path.join(config.BROWSER_STATE_DIR, "state.json")
        await context.storage_state(path=state_file)
        print(f"✓ 浏览器状态已保存: {state_file}")

        # 截图
        screenshot_path = os.path.join(config.OUTPUT_DIR, "page_screenshot.png")
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"✓ 页面截图已保存: {screenshot_path}")

        # 汇总
        print("\n" + "=" * 60)
        print("API 探测结果汇总:")
        print("=" * 60)
        for api in captured_apis:
            print(f"\n  URL: {api['url'][:120]}")
            print(f"  Method: {api.get('method')}")
            if "items_count" in api:
                print(f"  ⭐ 产品数据: {api['items_count']} 条, 总计: {api.get('total')}")
                print(f"  字段: {api.get('sample_item_keys', [])}")
            if "data_keys" in api:
                print(f"  Data keys: {api['data_keys']}")

        print("\n" + "=" * 60)
        input("按 Enter 关闭浏览器...")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(test())
