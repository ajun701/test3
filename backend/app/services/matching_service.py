# app/services/matching_service.py
import pandas as pd
from typing import Dict, Any, Set

from app.core.constants import (
    COL_LOGISTICS_NO_CANDIDATES, COL_INBOUND_FLAG, COL_INBOUND_NOTE,
    COL_SCREENSHOT_CANDIDATES
)
from app.utils.excel_utils import read_table, attach_hyperlink_helper_column
from app.services.cleaning_service import (
    ensure_required_columns, normalize_logistics_no, 
    compare_source_and_processed, find_first_existing_column
)

# =======================
# 匹配逻辑辅助方法
# =======================

def build_inbound_set(df_inbound: pd.DataFrame, logistics_col: str) -> Set[str]:
    """把入库表指定列转为标准化单号集合"""
    normalized_values = (normalize_logistics_no(v) for v in df_inbound[logistics_col].tolist())
    return {v for v in normalized_values if v}

def attach_inbound_flag(df: pd.DataFrame, logistics_col: str, inbound_set: Set[str]) -> pd.DataFrame:
    """打上是否已入库标识"""
    df = df.copy()
    if COL_INBOUND_FLAG not in df.columns:
        df[COL_INBOUND_FLAG] = ""
    if COL_INBOUND_NOTE not in df.columns:
        df[COL_INBOUND_NOTE] = ""

    if not inbound_set:
        return df

    def _flag(x):
        return "已入库" if normalize_logistics_no(x) in inbound_set else ""

    df[COL_INBOUND_FLAG] = df[logistics_col].apply(_flag)
    df[COL_INBOUND_NOTE] = df[COL_INBOUND_FLAG].apply(lambda v: "匹配到已入库表" if v == "已入库" else "")
    return df

# =======================
# 主业务流程：匹配步骤二
# =======================

def process_matching(
    source_bytes: bytes, source_filename: str,
    inbound_bytes: bytes, inbound_filename: str
) -> Dict[str, Any]:
    """
    处理 Tab2 入库单号匹配逻辑
    """
    # 1. 读取源数据 (步骤一的正常表)
    df_source = read_table(source_bytes, source_filename)
    if df_source.empty:
        raise ValueError("待匹配源数据表为空")

    shot_col = find_first_existing_column(df_source, COL_SCREENSHOT_CANDIDATES)
    if shot_col and source_filename.lower().endswith((".xlsx", ".xls")):
        df_source = attach_hyperlink_helper_column(df_source, source_bytes, shot_col)

    required_source = {"退回物流单号": COL_LOGISTICS_NO_CANDIDATES}
    matched_source = ensure_required_columns(df_source, required_source)
    col_lno_source = matched_source["退回物流单号"]

    # 2. 读取入库表数据
    df_inbound = read_table(inbound_bytes, inbound_filename)
    if df_inbound.empty:
        raise ValueError("已入库单号表为空")

    required_inbound = {"已入库物流单号": COL_LOGISTICS_NO_CANDIDATES}
    matched_inbound = ensure_required_columns(df_inbound, required_inbound)
    col_lno_inbound = matched_inbound["已入库物流单号"]

    # 3. 提取入库单号集合并进行匹配
    inbound_set = build_inbound_set(df_inbound, col_lno_inbound)
    if not inbound_set:
        raise ValueError("入库表未提取到有效单号")

    df_matched = attach_inbound_flag(df_source, col_lno_source, inbound_set)
    
    # 4. 分流
    df_inbound_res = df_matched[df_matched[COL_INBOUND_FLAG] == "已入库"].copy()
    df_pending_res = df_matched[df_matched[COL_INBOUND_FLAG] != "已入库"].copy()
    
    # 5. 生成一致性报告
    report = compare_source_and_processed(df_source, df_matched, stage_name="步骤二入库匹配")

    return {
        "df_source": df_source,
        "df_inbound": df_inbound_res,
        "df_pending": df_pending_res,
        "report": report,
        "shot_col": shot_col
    }