import json
import re
import time
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, List

import dashscope
import pandas as pd

from app.core.config import settings
from app.core.constants import (
    COL_AI_EXTRACTED_AMOUNT,
    COL_AI_MATCH,
    COL_AI_NOTE,
    HYPERLINK_SUFFIX,
    IMAGE_EXTENSIONS,
)
from app.db.session import SessionLocal
from app.models import AITask, OperationHistory
from app.services.artifact_service import save_artifact
from app.services.cleaning_service import compare_source_and_processed, parse_money
from app.tasks.celery_app import celery_app
from app.utils.excel_utils import (
    df_to_excel_bytes,
    extract_image_urls_from_cell_value,
    normalize_preview_url,
)

dashscope.api_key = settings.DASHSCOPE_API_KEY


def make_vl_prompt(expected_amount: float) -> str:
    return f"""
你是电商售后财务审核助手。用户可能上传了多张截图（同一条售后记录的图片从上到下依次排列）。
你的任务是在所有图片中寻找“寄回运费/快递费/配送费/邮费/寄件费用/实付运费/总运费”等字段对应金额（元），并与用户填写金额核对。

用户填写的退回运费 expected_amount = {expected_amount:.2f} 元。

规则：
1) 只把与“运费/快递费/配送费/邮费/寄件费用/实付运费/总运费”明确相关的金额当作运费。
2) 若出现多个金额，优先选取紧邻“运费类字段”的金额，不要把商品金额当运费。
3) 运费可能是 0/0.00/免费，也要识别为 0。
4) 若所有图片都无法确定运费金额，返回 paid_amount=null，并在 reason 说明原因。
5) 金额比对允许误差 0.01；相等 is_match=true，不相等 is_match=false，无法识别 is_match=null。
6) 禁止猜测：不确定就返回 null。

只输出 JSON（不要额外文字），字段至少包含：
- paid_amount: number | null
- is_match: true | false | null
- reason: string
可选：
- image_index
- evidence_text
- confidence
""".strip()


def _parse_vl_json(raw_text: str, expected_amount: float) -> Dict[str, Any]:
    data = None
    try:
        data = json.loads(raw_text)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", raw_text)
        if m:
            try:
                data = json.loads(m.group(0))
            except Exception:
                data = None

    if not isinstance(data, dict):
        return {"paid_amount": None, "is_match": None, "reason": "输出非JSON", "raw_text": raw_text}

    paid_f = parse_money(data.get("paid_amount")) if data.get("paid_amount") is not None else None
    is_match = data.get("is_match", None)
    if is_match is None and paid_f is not None:
        is_match = abs(paid_f - expected_amount) <= 0.01

    reason = str(data.get("reason", "")).strip()
    if not reason:
        reason = "一致" if is_match else ("图片模糊或无法识别金额" if paid_f is None else "不一致")

    return {"paid_amount": paid_f, "is_match": is_match, "reason": reason, "raw_text": raw_text}


def call_qwen_vl_multi_with_retry(
    image_urls: List[str],
    expected_amount: float,
    model: str,
    max_retries: int,
    backoff_base_sec: float,
) -> Dict[str, Any]:
    prompt = make_vl_prompt(expected_amount)
    content = [{"image": u} for u in image_urls]
    content.append({"text": prompt})
    messages = [{"role": "user", "content": content}]

    attempt = 0
    while True:
        try:
            resp = dashscope.MultiModalConversation.call(model=model, messages=messages)
            if hasattr(resp, "status_code") and resp.status_code != HTTPStatus.OK:
                reason = f"API失败: {getattr(resp, 'code', '')} {getattr(resp, 'message', '')}".strip()
                status_code = getattr(resp, "status_code", None)
                if status_code in (429, 502, 503, 504) and attempt < max_retries:
                    time.sleep(backoff_base_sec * (2**attempt))
                    attempt += 1
                    continue
                return {"paid_amount": None, "is_match": None, "reason": reason}

            raw_text = resp.output.choices[0]["message"]["content"][0]["text"]
            return _parse_vl_json(raw_text, expected_amount)
        except Exception as e:
            if attempt < max_retries:
                time.sleep(backoff_base_sec * (2**attempt))
                attempt += 1
                continue
            return {"paid_amount": None, "is_match": None, "reason": f"异常: {e}"}


@celery_app.task(bind=True, name="app.tasks.ai_tasks.run_ai_task")
def run_ai_task(self, task_id: str, api_key: str = ""):
    db = SessionLocal()
    task = None

    try:
        task = db.query(AITask).filter(AITask.task_id == task_id).first()
        if not task or task.status != "running":
            return

        operator = str(task.operator or "").strip() or "system"
        effective_api_key = str(api_key or "").strip() or str(settings.DASHSCOPE_API_KEY or "").strip()
        if not effective_api_key:
            task.status = "error"
            task.error_message = "缺少 DashScope API Key，无法执行 AI 任务"
            task.finished_at = datetime.utcnow()
            db.add(
                OperationHistory(
                    stage="步骤三AI复核",
                    action="AI任务异常",
                    operator=operator,
                    input_rows=task.total,
                    output_rows=task.next_idx,
                    detail={"task_id": task.task_id, "error": task.error_message},
                )
            )
            db.commit()
            return

        dashscope.api_key = effective_api_key
        df_work = pd.read_pickle(task.df_work_path)
        if task.total > len(df_work):
            task.total = len(df_work)
            db.commit()

        last_call_ts = 0.0
        while task.next_idx < task.total:
            db.refresh(task)
            if task.status != "running":
                df_work.to_pickle(task.df_work_path)
                return

            idx = task.next_idx
            row = df_work.iloc[idx]

            now_m = time.monotonic()
            wait = max(0.0, task.min_interval_sec - (now_m - last_call_ts))
            if wait > 0:
                time.sleep(wait)
            last_call_ts = time.monotonic()

            expected = parse_money(row.get(task.col_amount))
            raw_cell = row.get(task.col_shot + HYPERLINK_SUFFIX) or row.get(task.col_shot)
            img_urls = extract_image_urls_from_cell_value(raw_cell, max_images=task.max_images)
            if not img_urls and isinstance(raw_cell, str) and raw_cell.strip().startswith("http"):
                expanded = normalize_preview_url(raw_cell.strip())
                img_urls = [u for u in expanded if u.lower().endswith(IMAGE_EXTENSIONS)][: task.max_images]

            if expected is None:
                res = {"paid_amount": None, "is_match": False, "reason": "金额字段无法解析为数字"}
            elif not img_urls:
                res = {"paid_amount": None, "is_match": None, "reason": "未找到可用图片URL"}
            else:
                res = call_qwen_vl_multi_with_retry(
                    img_urls,
                    float(expected),
                    task.model_name,
                    task.max_retries,
                    task.backoff_base_sec,
                )

            df_work.at[idx, COL_AI_EXTRACTED_AMOUNT] = res.get("paid_amount")
            df_work.at[idx, COL_AI_MATCH] = bool(res.get("is_match") is True)
            df_work.at[idx, COL_AI_NOTE] = "" if res.get("is_match") else (res.get("reason") or "AI判定异常")

            task.next_idx += 1
            task.updated_at = datetime.utcnow()
            if task.next_idx % 10 == 0 or task.next_idx >= task.total:
                df_work.to_pickle(task.df_work_path)
            db.commit()

        task.status = "completed"
        task.finished_at = datetime.utcnow()

        processed_mask = df_work[COL_AI_MATCH].notna()
        df_processed = df_work[processed_mask]
        df_ok = df_processed[df_processed[COL_AI_MATCH] == True]
        df_bad = df_processed[df_processed[COL_AI_MATCH] != True]
        df_pending = pd.DataFrame()
        report_step3: Dict[str, Any] = {}
        try:
            df_source = pd.read_pickle(task.source_df_path)
            if len(df_source) > task.total:
                df_pending = df_source.iloc[task.total:].copy()
            src_scope = df_source.iloc[: min(max(task.total, 0), len(df_source))].copy()
            report_step3 = compare_source_and_processed(src_scope, df_work, stage_name="步骤三AI复核")
        except Exception:
            df_pending = pd.DataFrame()
            report_step3 = {}

        artifacts: List[str] = []

        hyperlink_ok = [task.col_shot] if task.col_shot in df_ok.columns else None
        b_ok = df_to_excel_bytes(df_ok, sheet_name="AI可打款", hyperlink_cols=hyperlink_ok)
        artifacts.append(
            save_artifact(
                b_ok,
                "ai_ok_result",
                db=db,
                stage="步骤三AI复核",
                action="完成AI任务",
                operator=operator,
                source_file=task.source_file or "",
                task_id=task.task_id,
                input_rows=task.total,
                output_rows=len(df_ok),
                payload={"kind": "ok"},
            )
        )

        hyperlink_bad = [task.col_shot] if task.col_shot in df_bad.columns else None
        b_bad = df_to_excel_bytes(df_bad, sheet_name="AI需回访", hyperlink_cols=hyperlink_bad)
        artifacts.append(
            save_artifact(
                b_bad,
                "ai_bad_result",
                db=db,
                stage="步骤三AI复核",
                action="完成AI任务",
                operator=operator,
                source_file=task.source_file or "",
                task_id=task.task_id,
                input_rows=task.total,
                output_rows=len(df_bad),
                payload={"kind": "bad"},
            )
        )

        if not df_pending.empty:
            hyperlink_pending = [task.col_shot] if task.col_shot in df_pending.columns else None
            b_pending = df_to_excel_bytes(df_pending, sheet_name="AI未处理", hyperlink_cols=hyperlink_pending)
            artifacts.append(
                save_artifact(
                    b_pending,
                    "ai_pending_result",
                    db=db,
                    stage="步骤三AI复核",
                    action="完成AI任务",
                    operator=operator,
                    source_file=task.source_file or "",
                    task_id=task.task_id,
                    input_rows=task.total,
                    output_rows=len(df_pending),
                    payload={"kind": "pending"},
                )
            )

        task.artifacts = artifacts
        db.add(
            OperationHistory(
                stage="步骤三AI复核",
                action="完成AI任务",
                operator=operator,
                input_rows=task.total,
                output_rows=task.next_idx,
                detail={"task_id": task.task_id, "artifacts": artifacts, "alignment_report": report_step3},
            )
        )
        db.commit()
    except Exception as e:
        db.rollback()
        if task is not None:
            operator = str(task.operator or "").strip() or "system"
            task.status = "error"
            task.error_message = str(e)
            task.finished_at = datetime.utcnow()
            db.add(
                OperationHistory(
                    stage="步骤三AI复核",
                    action="AI任务异常",
                    operator=operator,
                    input_rows=task.total,
                    output_rows=task.next_idx,
                    detail={"task_id": task.task_id, "error": str(e)},
                )
            )
            db.commit()
    finally:
        db.close()
