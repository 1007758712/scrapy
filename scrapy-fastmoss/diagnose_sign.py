# -*- coding: utf-8 -*-
"""找到 fm-sign 的生成方式"""
import asyncio, sys, io, os
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
import config

async def main():
    user_data_dir = os.path.join(config.BROWSER_STATE_DIR, "chrome_profile")
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir, headless=False, channel="chrome",
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        )
        stealth = Stealth(chrome_runtime=True)
        await stealth.apply_stealth_async(ctx)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # 导航
        await page.goto("https://www.fastmoss.com/zh/e-commerce/search?region=US&page=1&l1_cid=23&pagesize=10&l2_cid=930184", timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(8)

        # 方法1: 查找axios实例并检查其拦截器
        result = await page.evaluate("""
            () => {
                const info = {};
                
                // 检查常见的全局变量
                info.hasAxios = typeof axios !== 'undefined';
                info.hasVueAxios = !!(window.__VUE_DEVTOOLS_GLOBAL_HOOK__);
                
                // 检查 window 上的 http/request 相关对象
                const keys = Object.keys(window).filter(k => 
                    k.toLowerCase().includes('axios') || 
                    k.toLowerCase().includes('http') ||
                    k.toLowerCase().includes('request') ||
                    k.toLowerCase().includes('api') ||
                    k.toLowerCase().includes('sign') ||
                    k.toLowerCase().includes('fm')
                );
                info.windowKeys = keys;

                // 检查 XMLHttpRequest 是否被重写
                info.xhrModified = XMLHttpRequest.prototype.open.toString().length > 100;
                info.xhrOpenStr = XMLHttpRequest.prototype.open.toString().substring(0, 200);
                
                // 检查 fetch 是否被重写
                info.fetchStr = window.fetch.toString().substring(0, 200);
                
                // 查找 Next.js 或 React 的数据层
                info.hasNext = typeof __NEXT_DATA__ !== 'undefined';
                if (info.hasNext) {
                    try {
                        info.nextBuildId = __NEXT_DATA__.buildId;
                    } catch(e) {}
                }
                
                return info;
            }
        """)
        print("=== 页面环境分析 ===")
        for k, v in result.items():
            print(f"  {k}: {v}")

        # 方法2: 用 XMLHttpRequest 代替 fetch（可能会经过拦截器）
        print("\n=== 用 XMLHttpRequest 试调 API ===")
        xhr_result = await page.evaluate("""
            () => {
                return new Promise((resolve) => {
                    const ts = Math.floor(Date.now() / 1000);
                    const cnonce = Math.floor(Math.random() * 90000000) + 10000000;
                    const url = `/api/goods/V2/search?page=1&pagesize=10&order=2,2&region=US&l1_cid=23&l2_cid=930184&_time=${ts}&cnonce=${cnonce}`;
                    
                    const xhr = new XMLHttpRequest();
                    xhr.open('GET', url, true);
                    xhr.setRequestHeader('Accept', 'application/json, text/plain, */*');
                    
                    xhr.onload = function() {
                        resolve({
                            status: xhr.status,
                            body: xhr.responseText.substring(0, 300),
                            requestHeaders: 'N/A (can only see response)',
                        });
                    };
                    xhr.onerror = function() {
                        resolve({ error: 'XHR failed' });
                    };
                    xhr.send();
                });
            }
        """)
        print(f"  XHR result: {xhr_result}")

        # 方法3: 查找 fd_tk cookie（可能是签名的 key）
        cookies = await ctx.cookies()
        print("\n=== 关键 Cookies ===")
        for c in cookies:
            if any(k in c['name'].lower() for k in ['fd_tk', 'token', 'sign', 'auth', 'session', 'fm']):
                print(f"  {c['name']}: {c['value'][:60]}...")

        # 方法4: 拦截真实的 axios 请求，查看 fm-sign 怎么生成
        print("\n=== 拦截 axios 请求头 ===")
        captured = []
        async def on_req(request):
            if "V2/search" in request.url:
                captured.append(dict(request.headers))
        page.on("request", on_req)
        
        # 刷新页面让它重新发请求
        await page.reload(wait_until="domcontentloaded")
        await asyncio.sleep(8)
        
        if captured:
            print(f"  捕获到 {len(captured)} 个请求")
            for h in captured:
                fm_sign = h.get("fm-sign", "MISSING")
                fd_tk = h.get("fd-tk", h.get("fd_tk", "MISSING"))
                print(f"  fm-sign: {fm_sign}")
                print(f"  All custom headers: { {k:v for k,v in h.items() if k.startswith(('fm','fd','x-','auth','source','lang','region'))} }")
        else:
            print("  未捕获到 V2/search 请求")

        await ctx.close()

asyncio.run(main())
