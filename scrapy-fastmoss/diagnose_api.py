# -*- coding: utf-8 -*-
"""
诊断脚本：
1. 用浏览器登录 FastMoss
2. 拦截完整的 API 请求（URL + Headers）
3. 提取 cookies
4. 用 requests 直接调 API 对比
"""

import asyncio
import json
import os
import sys
import io
import requests

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

import config


async def main():
    print("=" * 60)
    print("FastMoss API 诊断工具")
    print("=" * 60)

    user_data_dir = os.path.join(config.BROWSER_STATE_DIR, "chrome_profile")
    os.makedirs(user_data_dir, exist_ok=True)

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir,
            headless=False,
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--no-first-run",
                "--no-default-browser-check",
            ],
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )

        stealth = Stealth(chrome_runtime=True, navigator_languages_override=("zh-CN", "zh"))
        await stealth.apply_stealth_async(ctx)

        pages = ctx.pages
        page = pages[0] if pages else await ctx.new_page()

        # ---- 拦截请求和响应 ----
        captured_requests = []

        async def on_request(request):
            url = request.url
            if "search" in url and "goods" in url.lower() or ("order=" in url and "l1_cid" in url):
                headers = request.headers
                print(f"\n{'='*60}")
                print(f"[REQUEST] {request.method} {url}")
                print(f"  Headers:")
                for k, v in headers.items():
                    print(f"    {k}: {v}")
                captured_requests.append({
                    "url": url,
                    "method": request.method,
                    "headers": dict(headers),
                })

        async def on_response(response):
            url = response.url
            if "order=" in url and "l1_cid" in url:
                try:
                    body = await response.json()
                    print(f"\n[RESPONSE] {url[-100:]}")
                    print(f"  Status: {response.status}")
                    print(f"  Body: {json.dumps(body, ensure_ascii=False)[:500]}")
                except:
                    pass

        page.on("request", on_request)
        page.on("response", on_response)

        # 导航到搜索页
        target_url = f"https://www.fastmoss.com/zh/e-commerce/search?region=US&page=1&l1_cid=23&pagesize=10&l2_cid=930184"
        print(f"\n导航到: {target_url}")

        try:
            await page.goto(target_url, timeout=30000, wait_until="domcontentloaded")
        except:
            pass

        await asyncio.sleep(5)

        # 等待所有请求完成
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except:
            pass

        await asyncio.sleep(3)

        # ---- 提取 cookies ----
        cookies = await ctx.cookies()
        print(f"\n{'='*60}")
        print(f"Cookies ({len(cookies)} 个):")
        cookie_dict = {}
        for c in cookies:
            if "fastmoss" in c.get("domain", ""):
                cookie_dict[c["name"]] = c["value"]
                print(f"  {c['name']}: {c['value'][:50]}...")

        # ---- 用 requests 直接调 API ----
        print(f"\n{'='*60}")
        print("尝试用 requests 直接调 API...")

        if captured_requests:
            req = captured_requests[0]
            print(f"  使用捕获的 URL: {req['url'][:100]}...")

            session = requests.Session()
            session.cookies.update(cookie_dict)

            # 复制浏览器请求的 headers
            req_headers = {
                "User-Agent": req["headers"].get("user-agent", ""),
                "Accept": req["headers"].get("accept", "application/json, text/plain, */*"),
                "Accept-Language": req["headers"].get("accept-language", "zh-CN,zh;q=0.9"),
                "Referer": req["headers"].get("referer", target_url),
                "Origin": "https://www.fastmoss.com",
            }
            # 复制所有 x- 开头的自定义 headers
            for k, v in req["headers"].items():
                if k.startswith("x-") or k.startswith("authorization"):
                    req_headers[k] = v

            print(f"  Headers: {json.dumps(req_headers, indent=2)}")

            resp = session.get(req["url"], headers=req_headers)
            print(f"\n  [requests 结果]")
            print(f"  Status: {resp.status_code}")
            print(f"  Body: {resp.text[:500]}")
        else:
            print("  未捕获到搜索 API 请求!")

            # 手动构造 API URL 测试
            import time
            _time = int(time.time())
            import random
            cnonce = random.randint(10000000, 99999999)
            api_url = (
                f"https://www.fastmoss.com/api/goods/search"
                f"?page=1&pagesize=10&order=2,2&region=US"
                f"&l1_cid=23&l2_cid=930184"
                f"&_time={_time}&cnonce={cnonce}"
            )
            print(f"  手动构造 URL: {api_url}")

            session = requests.Session()
            session.cookies.update(cookie_dict)
            req_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": target_url,
                "Origin": "https://www.fastmoss.com",
            }
            resp = session.get(api_url, headers=req_headers)
            print(f"  Status: {resp.status_code}")
            print(f"  Body: {resp.text[:500]}")

        # 截图看页面当前状态
        await page.screenshot(path="data/automotive/debug_diagnose.png")
        print(f"\n截图已保存: data/automotive/debug_diagnose.png")

        await ctx.close()

if __name__ == "__main__":
    asyncio.run(main())
