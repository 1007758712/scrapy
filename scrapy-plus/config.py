# -*- coding: utf-8 -*-
"""
SellerSprite 爬虫配置文件
"""

# ============================================================
# 登录信息 - 请填写你的卖家精灵账号
# ============================================================
SELLERSPRITE_USERNAME = ""   # 填写你的用户名/邮箱
SELLERSPRITE_PASSWORD = ""   # 填写你的密码

# ============================================================
# 爬取参数
# ============================================================
# 目标市场
MARKET = "US"

# 目标分类节点 ID
# 女士内裤: 7141123011:7147440011:1040660:9522931011:14333511:1044958
# CATEGORY_NODE_ID = "7141123011:7147440011:1040660:9522931011:14333511:1044958"
# CATEGORY_NAME = "Women's Panties"

CATEGORY_NODE_ID = "15684181"
CATEGORY_NAME = "automotive"

# 每页条数（SellerSprite 最大支持 100）
PAGE_SIZE = 100

# ============================================================
# 自动分片配置 — 突破 20 页限制
# ============================================================
# 启用自动销量区间分片（True=自动拆分, False=使用固定 MIN/MAX_SALES）
AUTO_SLICE = True

# SellerSprite 单次查询最大页数（硬限制）
MAX_PAGES_PER_QUERY = 20

# 单次查询最大数据条数 = MAX_PAGES_PER_QUERY * PAGE_SIZE
MAX_ITEMS_PER_QUERY = MAX_PAGES_PER_QUERY * PAGE_SIZE

# 最小月销量筛选（自动分片的起始值）
MIN_SALES = 350

# 最大月销量筛选（自动分片的结束值）
MAX_SALES = 450

# 排序设置
ORDER_FIELD = "amz_unit"   # 按亚马逊销量排序
ORDER_DESC = True           # 降序

# 目标数据量（约 10000 条）
MAX_PRODUCTS = 10000

# 每个文件保存的数据条数（每满 SAVE_BATCH_SIZE 条就存一个新文件）
SAVE_BATCH_SIZE = 2000



# ============================================================
# 网络与重试
# ============================================================
# 页面加载超时（毫秒）
PAGE_TIMEOUT = 60000

# API 响应等待超时（毫秒）
API_TIMEOUT = 30000

# 翻页间隔（秒） - 避免请求过快被封
PAGE_DELAY_MIN = 3
PAGE_DELAY_MAX = 6

# 最大重试次数
MAX_RETRIES = 3

# ============================================================
# 输出配置
# ============================================================
import os
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", CATEGORY_NAME)
OUTPUT_FILENAME = f"sellersprite_automotive_panties_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ============================================================
# 浏览器设置
# ============================================================
HEADLESS = False  # 设为 True 可隐藏浏览器窗口，调试时建议设为 False
BROWSER_STATE_DIR = os.path.join(os.path.dirname(__file__), "browser_state")

# ============================================================
# SellerSprite URLs
# ============================================================
BASE_URL = "https://www.sellersprite.com"
LOGIN_URL = f"{BASE_URL}/v3/login"
PRODUCT_RESEARCH_URL = f"{BASE_URL}/v3/product-research"
