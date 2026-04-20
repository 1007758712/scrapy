# -*- coding: utf-8 -*-
"""
SellerSprite 爬虫配置文件
"""

# ============================================================
# 登录信息 - 请填写你的卖家精灵账号
# ============================================================
SELLERSPRITE_USERNAME = "PK6777"   # 填写你的用户名/邮箱
SELLERSPRITE_PASSWORD = "PK6777"   # 填写你的密码

# ============================================================
# 爬取参数
# ============================================================
# 目标市场
MARKET = "US"

# 目标分类节点 ID
# 女士内裤: 7141123011:7147440011:1040660:9522931011:14333511:1044958
CATEGORY_NODE_ID = "7141123011:7147440011:1040660:9522931011:14333511:1044958"
CATEGORY_NAME = "Women's Panties"

# 每页条数（SellerSprite 最大支持 60）
PAGE_SIZE = 100

# 最小月销量筛选
MIN_SALES = 0

# 最大月销量筛选 (设为 None 或空表示无上限)
MAX_SALES = 1

# 排序设置
ORDER_FIELD = "amz_unit"   # 按亚马逊销量排序
ORDER_DESC = True           # 降序

# 目标数据量（约 10000 条）
MAX_PRODUCTS = 10000



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

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data", "womens_panties")
OUTPUT_FILENAME = f"sellersprite_womens_panties_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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
