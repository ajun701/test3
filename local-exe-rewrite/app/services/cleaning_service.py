from __future__ import annotations

import math
from collections import Counter
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.core.constants import (
    COL_ABNORMAL_REASON,
    COL_ALIPAY_ACCOUNT_CANDIDATES,
    COL_ALIPAY_NAME_CANDIDATES,
    COL_AMOUNT_CANDIDATES,
    COL_ID_CANDIDATES,
    COL_LOGISTICS_NO_CANDIDATES,
    COL_ORDER_NO_CANDIDATES,
    COL_SCREENSHOT_CANDIDATES,
    MAX_REFUND_AMOUNT,
    REGEX_CN_NAME,
    REGEX_EMAIL,
    REGEX_LOGISTICS,
    REGEX_MONEY_CLEAN,
    REGEX_PHONE,
)
from app.utils.excel_utils import (
    _normalize_identifier_cell,
    attach_hyperlink_helper_column,
    read_table,
)



def find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None



def find_column_with_fallback(
    df: pd.DataFrame,
    candidates: List[str],
    fuzzy_keywords: Optional[List[str]] = None,
) -> Optional[str]:
    col = find_first_existing_column(df, candidates)
    if col:
        return col
    if not fuzzy_keywords:
        return None

    for c in df.columns:
        name = str(c).strip().lower()
        if any(str(key).lower() in name for key in fuzzy_keywords):
            return c
    return None



def ensure_required_columns(df: pd.DataFrame, required_map: Dict[str, List[str]]) -> Dict[str, str]:
    matched: Dict[str, str] = {}
    missing: List[str] = []
    cols = set(df.columns)

    for desc, candidates in required_map.items():
        col = None
        for candidate in candidates:
            if candidate in cols:
                col = candidate
                break
        if col is None:
            missing.append(f"{desc}（候选：{candidates}）")
        else:
            matched[desc] = col

    if missing:
        raise ValueError("缺少必要列：\n- " + "\n- ".join(missing))
    return matched


@lru_cache(maxsize=8192)
def _parse_money_text(value_text: str) -> Optional[float]:
    clean = REGEX_MONEY_CLEAN.sub("", value_text)
    if clean in ("", ".", "-", "-."):
        return None
    try:
        return float(clean)
    except Exception:
        return None



def parse_money(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None

    text = str(value).strip()
    for token in ("￥", "¥", "元", ",", "，", " "):
        text = text.replace(token, "")
    return _parse_money_text(text)


@lru_cache(maxsize=16384)
def _normalize_logistics_text(value: str) -> str:
    return value



def normalize_logistics_no(raw: Any) -> str:
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return ""
    if isinstance(raw, float):
        if raw.is_integer():
            return format(raw, ".0f")
        out = format(raw, "f").rstrip("0").rstrip(".")
        return out if out else "0"
    if isinstance(raw, int):
        return str(raw)
    return _normalize_logistics_text(str(raw).strip())



def validate_row_detail(
    amount: Any,
    alipay_account: Any,
    alipay_name: Any,
    logistics_no: Any,
) -> Tuple[bool, str, Dict[str, bool]]:
    reasons: List[str] = []

    money = parse_money(amount)
    amount_parse_ok = money is not None
    amount_over_limit = bool(amount_parse_ok and money > MAX_REFUND_AMOUNT)

    if not amount_parse_ok:
        reasons.append("金额异常（非数字）")
    elif amount_over_limit:
        reasons.append("金额异常（金额超标）")

    account = "" if alipay_account is None else str(alipay_account).strip()
    account_ok = bool(account and (REGEX_PHONE.match(account) or REGEX_EMAIL.match(account)))
    if not account_ok:
        reasons.append("账号异常（支付宝账号格式不符）")

    name = "" if alipay_name is None else str(alipay_name).strip()
    name_ok = bool(name and REGEX_CN_NAME.match(name))
    if not name_ok:
        reasons.append("实名异常（需2~5个汉字）")

    lno = normalize_logistics_no(logistics_no)
    logistics_ok = bool(lno and REGEX_LOGISTICS.match(lno))
    if not logistics_ok:
        reasons.append("单号异常（物流单号需10~16位字母数字）")

    detail = {
        "amount_parse_ok": amount_parse_ok,
        "amount_over_limit": amount_over_limit,
        "account_ok": account_ok,
        "name_ok": name_ok,
        "logistics_ok": logistics_ok,
    }

    if reasons:
        return False, "；".join(reasons), detail
    return True, "", detail



def build_row_identity_keys(
    df: pd.DataFrame,
    id_col: str,
    order_col: str,
    logistics_col: str,
) -> Tuple[List[Tuple[str, str, str]], List[str]]:
    keys: List[Tuple[str, str, str]] = []
    logistics_keys: List[str] = []

    for id_value, order_value, logistics_value in zip(df[id_col], df[order_col], df[logistics_col]):
        id_key = _normalize_identifier_cell(id_value)
        order_key = _normalize_identifier_cell(order_value)
        logistics_key = normalize_logistics_no(logistics_value)
        keys.append((id_key, order_key, logistics_key))
        if logistics_key:
            logistics_keys.append(logistics_key)

    return keys, logistics_keys



def _counter_to_samples(counter: Counter, limit: int = 20) -> List[Dict[str, Any]]:
    samples: List[Dict[str, Any]] = []
    for key_tuple, count in counter.items():
        try:
            id_key, order_key, logistics_key = key_tuple
        except Exception:
            continue
        samples.append(
            {
                "id_key": str(id_key or ""),
                "order_key": str(order_key or ""),
                "logistics_key": str(logistics_key or ""),
                "count": int(count or 0),
            }
        )
        if len(samples) >= limit:
            break
    return samples



def compare_source_and_processed(
    source_df: pd.DataFrame,
    processed_df: pd.DataFrame,
    stage_name: str,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "stage": stage_name,
        "can_compare": False,
        "ok": False,
        "source_rows": len(source_df),
        "processed_rows": len(processed_df),
    }

    if source_df.empty or processed_df.empty:
        report["message"] = "源数据或处理后数据为空，无法校验。"
        return report

    src_id = find_column_with_fallback(source_df, COL_ID_CANDIDATES, ["id", "旺旺"])
    src_order = find_column_with_fallback(source_df, COL_ORDER_NO_CANDIDATES, ["订单", "单号"])
    src_lno = find_column_with_fallback(source_df, COL_LOGISTICS_NO_CANDIDATES, ["物流", "快递", "运单"])

    dst_id = find_column_with_fallback(processed_df, COL_ID_CANDIDATES, ["id", "旺旺"])
    dst_order = find_column_with_fallback(processed_df, COL_ORDER_NO_CANDIDATES, ["订单", "单号"])
    dst_lno = find_column_with_fallback(processed_df, COL_LOGISTICS_NO_CANDIDATES, ["物流", "快递", "运单"])

    if not (src_id and src_order and src_lno and dst_id and dst_order and dst_lno):
        report["message"] = "缺少对比字段"
        return report

    src_keys, src_logistics = build_row_identity_keys(source_df, src_id, src_order, src_lno)
    dst_keys, dst_logistics = build_row_identity_keys(processed_df, dst_id, dst_order, dst_lno)

    missing_counter = Counter(src_keys) - Counter(dst_keys)
    extra_counter = Counter(dst_keys) - Counter(src_keys)

    report.update(
        {
            "can_compare": True,
            "ok": (sum(missing_counter.values()) == 0 and sum(extra_counter.values()) == 0),
            "missing_rows": sum(missing_counter.values()),
            "extra_rows": sum(extra_counter.values()),
            "source_duplicate_logistics": sum(1 for c in Counter(src_logistics).values() if c > 1),
            "processed_duplicate_logistics": sum(1 for c in Counter(dst_logistics).values() if c > 1),
            "source_key_columns": {"id": src_id, "order_no": src_order, "logistics_no": src_lno},
            "processed_key_columns": {"id": dst_id, "order_no": dst_order, "logistics_no": dst_lno},
            "missing_samples": _counter_to_samples(missing_counter),
            "extra_samples": _counter_to_samples(extra_counter),
        }
    )
    return report



def process_cleaning(file_bytes: bytes, filename: str) -> Dict[str, Any]:
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
        validate_row_detail(amount, account, name, lno)
        for amount, account, name, lno in zip(df[col_amount], df[col_account], df[col_name], df[col_lno])
    ]

    flags = [ok for ok, _, _ in validation_results]
    reasons = [reason for _, reason, _ in validation_results]
    over_limit_only_flags = [
        (
            detail.get("amount_over_limit", False)
            and detail.get("account_ok", False)
            and detail.get("name_ok", False)
            and detail.get("logistics_ok", False)
        )
        for _, _, detail in validation_results
    ]

    df[COL_ABNORMAL_REASON] = reasons
    valid_mask = pd.Series(flags, index=df.index)
    over_limit_only_mask = pd.Series(over_limit_only_flags, index=df.index)
    abnormal_mask = (~valid_mask) & (~over_limit_only_mask)

    df_normal = df[valid_mask].drop(columns=[COL_ABNORMAL_REASON], errors="ignore").copy()
    df_abnormal = df[abnormal_mask].copy()
    df_over_limit = df[over_limit_only_mask].copy()

    report = compare_source_and_processed(df_raw, df, stage_name="步骤一清洗")

    return {
        "df_raw": df_raw,
        "df_normal": df_normal,
        "df_abnormal": df_abnormal,
        "df_over_limit": df_over_limit,
        "report": report,
        "shot_col": shot_col,
    }
