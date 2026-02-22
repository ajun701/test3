# app/core/constants.py
import re

# ====== 1. 列名变体候选 ======
COL_AMOUNT_CANDIDATES = [
    "*寄回快递实付金额", "寄回快递实付金额", "*寄回运费金额", "寄回运费金额", "退回运费金额", "*退回运费金额"
]
COL_ALIPAY_ACCOUNT_CANDIDATES = [
    "*退运费的支付宝账号", "退运费的支付宝账号", "支付宝账号", "收款支付宝账号", "支付宝收款账号"
]
COL_ALIPAY_NAME_CANDIDATES = [
    "*退运费的支付宝实名", "退运费的支付宝实名", "支付宝实名", "收款人姓名", "收款人"
]
COL_LOGISTICS_NO_CANDIDATES = [
    "*寄回换货快递单号", "寄回换货快递单号", "*退回物流单号", "退回物流单号", "寄回物流单号", "快递单号"
]
COL_SCREENSHOT_CANDIDATES = [
    "*商品瑕疵+金额截图", "商品瑕疵+金额截图", "寄回运费截图", "运费截图", "截图", "图片URL", "图片链接"
]
COL_ID_CANDIDATES = [
    "ID", "id", "*ID", "旺旺ID", "*旺旺ID", "用户ID", "买家ID", "会员ID"
]
COL_ORDER_NO_CANDIDATES = [
    "订单号", "*订单号", "订单编号", "主订单号", "子订单号", "多笔订单号",
    "订单号（多笔订单分开提交）", "*订单号（多笔订单分开提交）"
]

IDENTIFIER_COLUMN_KEYWORDS = (
    "订单", "单号", "物流", "快递", "账号", "支付宝", "流水", "编号", "ID", "id"
)

# ====== 2. 核心正则与基础配置 ======
MAX_REFUND_AMOUNT = 12.0

REGEX_PHONE = re.compile(r"^1[3-9]\d{9}$")
REGEX_EMAIL = re.compile(r"^[A-Za-z0-9_.+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9-.]+$")
REGEX_CN_NAME = re.compile(r"^[\u4e00-\u9fa5]{2,5}$")
REGEX_LOGISTICS = re.compile(r"^(?=.*\d)[A-Za-z0-9]{10,16}$")
REGEX_MONEY_CLEAN = re.compile(r"[^0-9.\-]")
REGEX_NON_ALNUM = re.compile(r"[^A-Za-z0-9]")
REGEX_URL_IN_PARENS = re.compile(r"\((https?://[^\s)]+)\)")
REGEX_URL_GENERIC = re.compile(r"(https?://[^\s\]\"')]+)")
REGEX_PREVIEW_SPLIT = re.compile(r"__|;|\s+")
REGEX_SCI_NUMBER = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)[eE][+-]?\d+$")
REGEX_EXCEL_HYPERLINK_FORMULA = re.compile(r'HYPERLINK\(\s*"([^"]+)"\s*[,;]\s*', re.IGNORECASE)
REGEX_EXCEL_URL_FALLBACK = re.compile(r"(https?://[^\s\"')]+)")

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".bmp")

# ====== 3. 特殊列与标识 ======
COL_ABNORMAL_REASON = "异常原因"
COL_INBOUND_FLAG = "是否已入库"
COL_INBOUND_NOTE = "入库匹配说明"
COL_AI_EXTRACTED_AMOUNT = "AI提取运费金额"
COL_AI_MATCH = "AI是否一致"
COL_AI_NOTE = "AI异常说明"
HYPERLINK_SUFFIX = "__hyperlink"