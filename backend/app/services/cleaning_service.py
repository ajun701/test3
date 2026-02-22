# app/services/cleaning_service.py
import math
from collections import Counter
from functools import lru_cache
from typing import List, Tuple, Optional, Dict, Any

import pandas as pd

from app.core.constants import (
    COL_AMOUNT_CANDIDATES, COL_ALIPAY_ACCOUNT_CANDIDATES,
    COL_ALIPAY_NAME_CANDIDATES, COL_LOGISTICS_NO_CANDIDATES,
    COL_SCREENSHOT_CANDIDATES, COL_ID_CANDIDATES, COL_ORDER_NO_CANDIDATES,
    MAX_REFUND_AMOUNT, REGEX_PHONE, REGEX_EMAIL, REGEX_CN_NAME,
    REGEX_LOGISTICS, REGEX_MONEY_CLEAN, REGEX_NON_ALNUM,
    COL_ABNORMAL_REASON
)
from app.utils.excel_utils import (
    read_table, attach_hyperlink_helper_column, _normalize_identifier_cell
)

# =======================
# 核心字段校验与工具
# =======================

def find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def find_column_with_fallback(df: pd.DataFrame, candidates: List[str], fuzzy_keywords: Optional[List[str]] = None) -> Optional[str]:
    col = find_first_existing_column(df, candidates)
    if col:
        return col
    if not fuzzy_keywords:
        return None
    for c in df.columns:
        name = str(c).strip().lower()
        if any(k.lower() in name for k in fuzzy_keywords):
            return c
    return None

def ensure_required_columns(df: pd.DataFrame, required_map: Dict[str, List[str]]) -> Dict[str, str]:
    matched = {}
    missing = []
    cols = set(df.columns)
    for desc, candidates in required_map.items():
        col = None
        for candidate in candidates:
            if candidate in cols:
                col = candidate
                break
        if not col:
            missing.append(f"{desc}（候选：{candidates}）")
        else:
            matched[desc] = col
    if missing:
        raise ValueError("缺少必要列：\n- " + "\n- ".join(missing))
    return matched

@lru_cache(maxsize=8192)
def _parse_money_text(value_text: str) -> Optional[float]:
    s2 = REGEX_MONEY_CLEAN.sub("", value_text)
    if s2 in ("", ".", "-", "-."):
        return None
    try:
        return float(s2)
    except Exception:
        return None

def parse_money(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip().replace("￥", "").replace("¥", "").replace("元", "").replace(",", "")
    return _parse_money_text(s)

@lru_cache(maxsize=16384)
def _normalize_logistics_text(value: str) -> str:
    return REGEX_NON_ALNUM.sub("", value).strip()

def normalize_logistics_no(raw: Any) -> str:
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return ""
    return _normalize_logistics_text(str(raw).strip())

def validate_row(amount: Any, alipay_account: Any, alipay_name: Any, logistics_no: Any) -> Tuple[bool, str]:
    reasons = []
    money = parse_money(amount)
    if money is None:
        reasons.append("金额异常（非数字）")
    elif money > MAX_REFUND_AMOUNT:
        reasons.append("金额异常（金额超标）")

    acct = "" if alipay_account is None else str(alipay_account).strip()
    if acct == "" or (not REGEX_PHONE.match(acct) and not REGEX_EMAIL.match(acct)):
        reasons.append("账号异常（支付宝账号格式不符）")

    name = "" if alipay_name is None else str(alipay_name).strip()
    if name == "" or not REGEX_CN_NAME.match(name):
        reasons.append("实名异常（需2~5个汉字）")

    lno = normalize_logistics_no(logistics_no)
    if lno == "" or not REGEX_LOGISTICS.match(lno):
        reasons.append("单号异常（物流单号需10~16位字母数字且包含数字）")

    if reasons:
        return False, "；".join(reasons)
    return True, ""

def build_row_identity_keys(df: pd.DataFrame, id_col: str, order_col: str, logistics_col: str) -> Tuple[List[Tuple[str, str, str]], List[str]]:
    keys = []
    logistics_keys = []
    for id_v, order_v, logistics_v in zip(df[id_col], df[order_col], df[logistics_col]):
        id_key = _normalize_identifier_cell(id_v)
        order_key = _normalize_identifier_cell(order_v)
        logistics_key = normalize_logistics_no(logistics_v)
        keys.append((id_key, order_key, logistics_key))
        if logistics_key:
            logistics_keys.append(logistics_key)
    return keys, logistics_keys

def compare_source_and_processed(source_df: pd.DataFrame, processed_df: pd.DataFrame, stage_name: str) -> Dict[str, Any]:
    """生成行数一致性校验报告"""
    report = {"stage": stage_name, "can_compare": False, "ok": False, "source_rows": len(source_df), "processed_rows": len(processed_df)}
    if source_df.empty or processed_df.empty:
        report["message"] = "源数据或处理后数据为空，无法校验。"
        return report

    src_id = find_column_with_fallback(source_df, COL_ID_CANDIDATES, ["id", "旺旺"])
    src_order = find_column_with_fallback(source_df, COL_ORDER_NO_CANDIDATES, ["订单"])
    src_lno = find_column_with_fallback(source_df, COL_LOGISTICS_NO_CANDIDATES, ["物流", "快递"])

    dst_id = find_column_with_fallback(processed_df, COL_ID_CANDIDATES, ["id", "旺旺"])
    dst_order = find_column_with_fallback(processed_df, COL_ORDER_NO_CANDIDATES, ["订单"])
    dst_lno = find_column_with_fallback(processed_df, COL_LOGISTICS_NO_CANDIDATES, ["物流", "快递"])

    if not (src_id and dst_id and src_order and dst_order and src_lno and dst_lno):
        report["message"] = "缺少对比字段"
        return report

    src_keys, src_logistics = build_row_identity_keys(source_df, src_id, src_order, src_lno)
    dst_keys, dst_logistics = build_row_identity_keys(processed_df, dst_id, dst_order, dst_lno)

    missing_counter = Counter(src_keys) - Counter(dst_keys)
    extra_counter = Counter(dst_keys) - Counter(src_keys)

    report.update({
        "can_compare": True,
        "ok": (sum(missing_counter.values()) == 0 and sum(extra_counter.values()) == 0),
        "missing_rows": sum(missing_counter.values()),
        "extra_rows": sum(extra_counter.values()),
        "source_duplicate_logistics": sum(1 for c in Counter(src_logistics).values() if c > 1),
        "processed_duplicate_logistics": sum(1 for c in Counter(dst_logistics).values() if c > 1),
    })
    return report

# =======================
# 主业务流程：清洗步骤一
# =======================

def process_cleaning(file_bytes: bytes, filename: str) -> Dict[str, Any]:
    """
    处理 Tab1 清洗逻辑
    """
    df_raw = read_table(file_bytes, filename)
    if df_raw.empty:
        raise ValueError("读取到的表格为空")

    required = {
        "退回运费金额": COL_AMOUNT_CANDIDATES,
        "支付宝账号": COL_ALIPAY_ACCOUNT_CANDIDATES,
        "支付宝实名": COL_ALIPAY_NAME_CANDIDATES,
        "退回物流单号": COL_LOGISTICS_NO_CANDIDATES,
    }
    matched_cols = ensure_required_columns(df_raw, required)

    col_amount = matched_cols["退回运费金额"]
    col_account = matched_cols["支付宝账号"]
    col_name = matched_cols["支付宝实名"]
    col_lno = matched_cols["退回物流单号"]
    shot_col = find_first_existing_column(df_raw, COL_SCREENSHOT_CANDIDATES)

    if shot_col and filename.lower().endswith((".xlsx", ".xls")):
        df_raw = attach_hyperlink_helper_column(df_raw, file_bytes, shot_col)

    df = df_raw.copy()
    validation_results = [
        validate_row(amount, account, name, lno)
        for amount, account, name, lno in zip(
            df[col_amount], df[col_account], df[col_name], df[col_lno]
        )
    ]
    
    flags = [ok for ok, _ in validation_results]
    reasons = [reason for _, reason in validation_results]
    
    df[COL_ABNORMAL_REASON] = reasons
    valid_mask = pd.Series(flags, index=df.index)
    
    df_normal = df[valid_mask].drop(columns=[COL_ABNORMAL_REASON], errors="ignore").copy()
    df_abnormal = df[~valid_mask].copy()
    report = compare_source_and_processed(df_raw, df, stage_name="步骤一清洗")

    return {
        "df_raw": df_raw,
        "df_normal": df_normal,
        "df_abnormal": df_abnormal,
        "report": report,
        "shot_col": shot_col
    }